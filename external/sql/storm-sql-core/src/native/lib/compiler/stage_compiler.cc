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

#include "stage_compiler.h"
#include "expr_compiler.h"

#include <llvm/IR/Function.h>
#include <llvm/IR/GlobalValue.h>

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
using std::pair;
using std::string;
using std::to_string;
using std::transform;
using std::unique_ptr;
using std::vector;
using std::placeholders::_1;

namespace stormsql {

StageCompiler::StageCompiler(TypeSystem *types, const Schema *schema,
                             SMDiagnostic *err)
    : types_(types), schema_(schema), err_(err), module_(types->module()) {}

string StageCompiler::GetStageName(const Json &stage) {
  int id = stage["id"].int_value();
  return stage["type"].string_value() + "_" + to_string(id);
}

Function *StageCompiler::RecordStage(int stage, Function *f) {
  auto inserted = stages_.insert(stages_.begin(), make_pair(stage, f));
  if (!inserted->second) {
    string func_name = f->getName();
    *err_ = SMDiagnostic(func_name, SourceMgr::DK_Error,
                         "Duplicate stage " + func_name);
    return nullptr;
  }
  return f;
}

bool StageCompiler::CollectInputTypes(const Json &stage,
                                      vector<Type *> *input_types) const {
  const string func_name = GetStageName(stage);
  const auto &inputs = stage["inputs"].array_items();

  for (const auto &input : inputs) {
    int id = input.int_value();
    auto it = stages_.find(id);
    if (it == stages_.end()) {
      *err_ = SMDiagnostic(func_name, SourceMgr::DK_Error,
                           "Cannot find input " + to_string(id));
      return true;
    }
    input_types->push_back(it->second->getReturnType());
  }
  return false;
}

Function *StageCompiler::VisitTableScan(const Json &stage) {
  string tbl_name = stage["table"].string_value();
  int stage_id = stage["id"].int_value();
  string func_name = GetStageName(stage);

  auto it = schema_->find(tbl_name);
  if (it == schema_->end()) {
    *err_ = SMDiagnostic(func_name, SourceMgr::DK_Error,
                         "Cannot find table " + tbl_name);
    return nullptr;
  }
  const auto &tbl = it->second;
  vector<Type *> field_types;
  transform(tbl.fields().begin(), tbl.fields().end(),
            back_inserter(field_types),
            bind(&pair<string, Type *>::second, _1));

  LLVMContext &ctx = module_->getContext();
  /**
   * The table scan operators take an i8* as an argument which records all the
   * information about the table.
   **/
  FunctionType *ty = FunctionType::get(StructType::get(ctx, field_types),
                                       Type::getInt8PtrTy(ctx), false);
  Function *f =
      Function::Create(ty, GlobalValue::ExternalLinkage, func_name, module_);
  return RecordStage(stage_id, f);
}

Function *StageCompiler::VisitFilter(const Json &stage) {
  const string func_name = GetStageName(stage);
  int stage_id = stage["id"].int_value();

  string msg;
  if (!stage.has_shape({{"expr", Json::OBJECT}}, msg)) {
    *err_ = SMDiagnostic(func_name, SourceMgr::DK_Error,
                         "No conditional expression " + msg);
    return nullptr;
  }

  vector<Type *> input_types;
  if (CollectInputTypes(stage, &input_types)) {
    return nullptr;
  }

  if (input_types.size() != 1) {
    *err_ = SMDiagnostic(func_name, SourceMgr::DK_Error,
                         "Filter should only have a single input");
    return nullptr;
  }

  Type *InputTy = input_types.front();
  LLVMContext &ctx = module_->getContext();
  FunctionType *fty =
      FunctionType::get(InputTy, input_types, false);
  Function *f =
      Function::Create(fty, GlobalValue::PrivateLinkage, func_name, module_);
  BasicBlock *bb = BasicBlock::Create(ctx, "entry", f);
  BasicBlock *True = BasicBlock::Create(ctx, "true", f);
  BasicBlock *False = BasicBlock::Create(ctx, "false", f);
  llvm::IRBuilder<> builder(bb);
  ExprCompiler expr_compiler(func_name, types_, &builder, err_);
  Value *v = expr_compiler.Visit(stage["expr"]);
  if (!v) {
    return nullptr;
  }
  builder.CreateCondBr(v, True, False);
  builder.SetInsertPoint(True);
  builder.CreateRet(f->arg_begin());
  builder.SetInsertPoint(False);
  Value *NullRecord = types_->GetOrInsertNullRecord(InputTy);
  Value *LI = builder.CreateLoad(NullRecord);
  builder.CreateRet(LI);
  return RecordStage(stage_id, f);
}

Function *StageCompiler::VisitProject(const Json &stage) {
  const string func_name = GetStageName(stage);
  const auto &exprs = stage["exprs"].array_items();
  int stage_id = stage["id"].int_value();

  vector<Type *> input_types;
  if (CollectInputTypes(stage, &input_types)) {
    return nullptr;
  }
  vector<Type *> expr_types;
  transform(
      exprs.begin(), exprs.end(), back_inserter(expr_types),
      [this](const Json &t) { return types_->GetLLVMType(t["expr"]["type"]); });

  LLVMContext &ctx = module_->getContext();
  FunctionType *fty =
      FunctionType::get(StructType::create(expr_types), input_types, false);
  Function *f =
      Function::Create(fty, GlobalValue::PrivateLinkage, func_name, module_);
  BasicBlock *bb = BasicBlock::Create(ctx, "entry", f);
  llvm::IRBuilder<> builder(bb);

  ExprCompiler expr_compiler(func_name, types_, &builder, err_);
  vector<Value *> ret;
  for (const auto &e : exprs) {
    Value *v = expr_compiler.Visit(e["expr"]);
    if (!v) {
      return nullptr;
    }
    ret.push_back(v);
  }
  builder.CreateAggregateRet(&ret[0], ret.size());
  return RecordStage(stage_id, f);
}
}
