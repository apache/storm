/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "plan_compiler.h"
#include "stage_compiler.h"

#include <llvm/IR/Function.h>
#include <llvm/IR/GlobalValue.h>
#include <llvm/IR/IRBuilder.h>

#include <json11/json11.hpp>
#include <functional>
#include <vector>
#include <iostream>

using json11::Json;
using json11::JsonValue;
using llvm::BasicBlock;
using llvm::Function;
using llvm::FunctionType;
using llvm::GlobalValue;
using llvm::LLVMContext;
using llvm::MemoryBuffer;
using llvm::Module;
using llvm::SMDiagnostic;
using llvm::SourceMgr;
using llvm::StringRef;
using llvm::StructType;
using llvm::Type;
using llvm::Value;
using std::back_inserter;
using std::bind;
using std::make_pair;
using std::map;
using std::pair;
using std::string;
using std::to_string;
using std::transform;
using std::unique_ptr;
using std::vector;
using std::placeholders::_1;

namespace stormsql {

class PlanCompilerImpl : public PlanCompiler {
public:
  explicit PlanCompilerImpl(StringRef module_id, LLVMContext *ctx,
                            SMDiagnostic *err);
  virtual unique_ptr<Module> Compile(const MemoryBuffer &plan) override;

private:
  PlanCompilerImpl &LoadSchema();
  PlanCompilerImpl &CompileStage();
  PlanCompilerImpl &GenerateMain();
  static bool IsTableScan(const Json &stage);
  static string GetTableMetadata(const Json &stage);
  unique_ptr<Module> module_;
  TypeSystem types_;
  map<string, Table> schema_;
  SMDiagnostic *err_;
  bool has_error_;
  Json plan_;
  unique_ptr<StageCompiler> stages_;
};

unique_ptr<PlanCompiler>
PlanCompiler::Create(StringRef module_id, LLVMContext *ctx, SMDiagnostic *err) {
  return unique_ptr<PlanCompiler>(new PlanCompilerImpl(module_id, ctx, err));
}

PlanCompiler::~PlanCompiler() {}

PlanCompilerImpl::PlanCompilerImpl(StringRef module_id, LLVMContext *ctx,
                                   SMDiagnostic *err)
    : module_(new Module(module_id, *ctx)), types_(module_.get()), err_(err),
      has_error_(false) {}

unique_ptr<Module> PlanCompilerImpl::Compile(const MemoryBuffer &plan_json) {
  string message;
  plan_ = Json::parse(plan_json.getBufferStart(), message);
  if (message.size()) {
    *err_ =
        SMDiagnostic("", SourceMgr::DK_Error, "Cannot parse plan: " + message);
    return nullptr;
  }

  LoadSchema().CompileStage().GenerateMain();
  if (has_error_) {
    return nullptr;
  }
  return std::move(module_);
}

PlanCompilerImpl &PlanCompilerImpl::LoadSchema() {
  if (has_error_) {
    return *this;
  }

  for (const auto &table : plan_["tables"].array_items()) {
    string msg;
    if (!table.has_shape({{"name", Json::STRING}}, msg)) {
      *err_ = SMDiagnostic("", SourceMgr::DK_Error, "Invalid table schema");
      has_error_ = true;
      return *this;
    }
    std::vector<Table::Field> fields;
    for (const auto &field : table["fields"].array_items()) {
      if (!field.has_shape({{"name", Json::STRING}}, msg)) {
        *err_ = SMDiagnostic("", SourceMgr::DK_Error,
                             "Invalid schema for table " +
                                 table["name"].string_value());
        has_error_ = true;
        return *this;
      }
      fields.push_back(make_pair(field["name"].string_value(),
                                 types_.GetLLVMType(field["type"])));
    }
    schema_.insert(schema_.begin(), make_pair(table["name"].string_value(),
                                              Table(std::move(fields))));
  }
  return *this;
}

PlanCompilerImpl &PlanCompilerImpl::CompileStage() {
  if (has_error_) {
    return *this;
  }

  stages_.reset(new StageCompiler(&types_, &schema_, err_));
  string msg;
  for (const auto &stage : plan_["stages"].array_items()) {
    Function *f = stages_->Visit(stage);
    if (!f) {
      has_error_ = true;
      break;
    }
  }
  return *this;
}

PlanCompilerImpl &PlanCompilerImpl::GenerateMain() {
  if (has_error_) {
    return *this;
  }

  const auto &stages = stages_->stages();
  LLVMContext &ctx = module_->getContext();
  Function *result_func = stages.at(plan_["result"].int_value());
  FunctionType *FTy = FunctionType::get(result_func->getReturnType(), false);
  Function *f =
      Function::Create(FTy, GlobalValue::PrivateLinkage, "main", module_.get());
  BasicBlock *bb = BasicBlock::Create(ctx, "entry", f);
  llvm::IRBuilder<> builder(bb);

  map<int, Value *> values;
  // The serializer guarantees that the inputs for each stage much appear before
  // the stage.
  for (const auto &stage : plan_["stages"].array_items()) {
    int stage_id = stage["id"].int_value();
    Function *f = stages.at(stage_id);
    Value *ret = nullptr;
    vector<Value *> params;

    if (IsTableScan(stage)) {
      Value *v = builder.CreateGlobalStringPtr(GetTableMetadata(stage));
      params.push_back(v);
    } else {
      for (const auto &input : stage["inputs"].array_items()) {
        int input_id = input.int_value();
        Value *v = values[input_id];
        if (!v) {
          *err_ = SMDiagnostic(f->getName(), SourceMgr::DK_Error,
                               "Cannot find input " + to_string(input_id));
          has_error_ = true;
          return *this;
        }
        params.push_back(v);
      }
    }
    ret = builder.CreateCall(f, params);
    values.insert(make_pair(stage_id, ret));
  }
  builder.CreateRet(values.at(plan_["result"].int_value()));
  return *this;
}

bool PlanCompilerImpl::IsTableScan(const Json &stage) {
  return stage["type"] == "EnumerableTableScan";
}

string PlanCompilerImpl::GetTableMetadata(const Json &stage) {
  map<string, Json> metadata({{"table", stage["table"]}});
  return Json(metadata).dump();
}
}
