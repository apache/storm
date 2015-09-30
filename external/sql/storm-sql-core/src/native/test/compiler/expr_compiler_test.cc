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

#include "compiler/typesystem.h"
#include "compiler/expr_compiler.h"

#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/Module.h>

#include <gtest/gtest.h>
#include <memory>

using llvm::dyn_cast;
using llvm::APFloat;
using llvm::BasicBlock;
using llvm::ConstantDataSequential;
using llvm::ConstantInt;
using llvm::ConstantFP;
using llvm::Function;
using llvm::FunctionType;
using llvm::LLVMContext;
using llvm::Module;
using llvm::SMDiagnostic;
using llvm::Type;
using llvm::Value;
using json11::Json;
using std::map;
using std::string;
using std::to_string;
using std::unique_ptr;

using namespace stormsql;

static inline Json GetLiteral(const string &type, const string &value) {
  map<string, Json> v({{"inst", "literal"}, {"value", value}});
  map<string, Json> r({{"type", type}, {"value", Json(v)}});
  return Json(r);
}
static inline Json LiteralBool(bool v) {
  return GetLiteral("BOOLEAN", v ? "true" : "false");
}
static inline Json LiteralInt(int v) {
  return GetLiteral("INTEGER", to_string(v));
}
static inline Json LiteralDouble(double v) {
  return GetLiteral("DECIMAL", to_string(v));
}
static inline Json LiteralString(const string &v) {
  return GetLiteral("CHAR", v);
}

TEST(TestExprCompiler, TestLiteral) {
  LLVMContext ctx;
  unique_ptr<BasicBlock> BB(BasicBlock::Create(ctx));
  TypeSystem ts(&ctx);
  ExprCompiler compiler("TestLiteral", &ts);
  Value *v = compiler.Visit(LiteralBool(1));
  ConstantInt *CI = dyn_cast<ConstantInt>(v);
  ASSERT_TRUE(CI && CI == ConstantInt::getTrue(CI->getType()));

  v = compiler.Visit(LiteralInt(1));
  CI = dyn_cast<ConstantInt>(v);
  ASSERT_TRUE(CI && CI->getValue() == 1);

  v = compiler.Visit(LiteralDouble(1.0));
  ConstantFP *CF = dyn_cast<ConstantFP>(v);
  ASSERT_TRUE(CF &&
              CF->getValueAPF().compare(APFloat(1.0)) == APFloat::cmpEqual);

  v = compiler.Visit(LiteralString("foo"));
  ConstantDataSequential *CDS = dyn_cast<ConstantDataSequential>(v);
  ASSERT_TRUE(CDS && CDS->getAsCString() == "foo");
}
