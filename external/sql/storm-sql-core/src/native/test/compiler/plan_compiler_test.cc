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
#include "compiler/plan_compiler.h"

#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Support/raw_ostream.h>

#include <gtest/gtest.h>
#include <memory>

using llvm::isa;
using llvm::dyn_cast;
using llvm::errs;
using llvm::verifyModule;
using llvm::ConstantStruct;
using llvm::Function;
using llvm::LLVMContext;
using llvm::MemoryBuffer;
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

TEST(TestPlanCompiler, TestFilterProject) {
  LLVMContext ctx;
  SMDiagnostic err;

  map<string, Json> scan_stage(
      {{"type", "EnumerableTableScan"}, {"id", 1}, {"table", "FOO"}});

  map<string, Json> filter_stage({{"type", "LogicalFilter"},
                                  {"id", 2},
                                  {"inputs", vector<Json>({Json(1)})},
                                  {"expr", LiteralBool(true)}});

  Json expr = InputRef("INTEGER", 0);
  map<string, Json> json_project_expr({{"name", "$0"}, {"expr", expr}});
  map<string, Json> project_stage(
      {{"type", "LogicalProject"},
       {"id", 3},
       {"inputs", vector<Json>({Json(2)})},
       {"exprs", vector<Json>({Json(json_project_expr)})}});

  map<string, Json> json_table_fields({{"name", "ID"}, {"type", "INTEGER"}});
  map<string, Json> json_table(
      {{"name", "FOO"}, {"fields", vector<Json>({json_table_fields})}});
  map<string, Json> json_plan(
      {{"result", Json(3)},
       {"tables", vector<Json>({json_table})},
       {"stages", vector<Json>({scan_stage, filter_stage, project_stage})}});

  unique_ptr<PlanCompiler> compiler =
      PlanCompiler::Create("test-filter-project", &ctx, &err);
  unique_ptr<MemoryBuffer> plan =
      MemoryBuffer::getMemBuffer(Json(json_plan).dump());
  unique_ptr<Module> M = compiler->Compile(*plan);
  ASSERT_TRUE(M.get() && !verifyModule(*M, &errs()));
}
