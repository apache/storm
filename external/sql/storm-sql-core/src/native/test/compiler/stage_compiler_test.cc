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

#include "compiler_input.h"
#include "compiler/stage_compiler.h"

#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Verifier.h>

#include <gtest/gtest.h>
#include <memory>

using llvm::isa;
using llvm::dyn_cast;
using llvm::ConstantStruct;
using llvm::Function;
using llvm::LLVMContext;
using llvm::Module;
using llvm::ReturnInst;
using llvm::SMDiagnostic;
using llvm::StructType;
using llvm::Type;
using llvm::Value;
using json11::Json;
using std::map;
using std::make_pair;
using std::move;
using std::string;
using std::to_string;
using std::unique_ptr;
using std::vector;

using namespace stormsql;

static inline StageCompiler::Schema GetSimpleSchema(LLVMContext &ctx) {
  vector<Table::Field> fields({make_pair("id", Type::getInt32Ty(ctx))});
  Table tbl(move(fields));
  StageCompiler::Schema schema({{"FOO", tbl}});
  return schema;
}

TEST(TestStageCompiler, TestTableScan) {
  LLVMContext ctx;
  unique_ptr<Module> M(new Module("test-table-scan", ctx));
  TypeSystem ts(M.get());
  SMDiagnostic err;

  StageCompiler::Schema schema(GetSimpleSchema(ctx));
  StageCompiler compiler(&ts, &schema, &err);
  map<string, Json> stage(
      {{"type", "EnumerableTableScan"}, {"id", 1}, {"table", "FOO"}});
  Function *f = compiler.Visit(stage);
  ASSERT_TRUE(f && f->arg_size() == 1);

  auto int_ty = ts.GetLLVMType(Json("INTEGER"));
  ASSERT_EQ(StructType::get(int_ty, nullptr), f->getReturnType());
}

TEST(TestStageCompiler, TestFilter) {
  LLVMContext ctx;
  unique_ptr<Module> M(new Module("test-filter", ctx));
  TypeSystem ts(M.get());
  SMDiagnostic err;

  StageCompiler::Schema schema(GetSimpleSchema(ctx));
  StageCompiler compiler(&ts, &schema, &err);
  map<string, Json> scan_stage(
      {{"type", "EnumerableTableScan"}, {"id", 1}, {"table", "FOO"}});
  map<string, Json> stage({{"type", "LogicalFilter"},
                           {"inputs", vector<Json>({Json(1)})},
                           {"id", 2},
                           {"expr", LiteralBool(true)}});
  Function *tbl_scan = compiler.Visit(scan_stage);
  ASSERT_TRUE(tbl_scan);
  Function *f = compiler.Visit(stage);

  ASSERT_TRUE(f && f->arg_size() == 1);
  const auto &fields = schema.at("FOO").fields();
  auto AI = f->arg_begin();
  auto ArgTy = dyn_cast<StructType>(AI->getType());
  ASSERT_TRUE(ArgTy);
  for (size_t i = 0; i < fields.size(); ++i) {
    ASSERT_EQ(fields.at(i).second, ArgTy->getElementType(i));
  }
}

TEST(TestStageCompiler, TestProjection) {
  LLVMContext ctx;
  unique_ptr<Module> M(new Module("test-projection", ctx));
  TypeSystem ts(M.get());
  SMDiagnostic err;

  StageCompiler::Schema schema;
  StageCompiler compiler(&ts, &schema, &err);
  Json expr =
      CallExpr("INTEGER", "+", vector<Json>({LiteralInt(1), LiteralInt(2)}));
  map<string, Json> json_expr({{"name", "$0"}, {"expr", expr}});
  map<string, Json> stage({{"type", "LogicalProject"},
                           {"id", 1},
                           {"exprs", vector<Json>({Json(json_expr)})}});
  Function *f = compiler.Visit(stage);

  ASSERT_TRUE(f && f->arg_empty());
  ReturnInst *RI = dyn_cast<ReturnInst>(f->getEntryBlock().getTerminator());
  ASSERT_TRUE(RI && isa<ConstantStruct>(RI->getReturnValue()));
}
