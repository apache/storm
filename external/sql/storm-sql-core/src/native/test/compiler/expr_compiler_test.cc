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
#include "compiler/typesystem.h"
#include "compiler/expr_compiler.h"

#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Verifier.h>

#include <gtest/gtest.h>
#include <memory>

using llvm::dyn_cast;
using llvm::verifyModule;
using llvm::APFloat;
using llvm::BasicBlock;
using llvm::CallInst;
using llvm::ConstantDataSequential;
using llvm::ConstantInt;
using llvm::ConstantFP;
using llvm::FCmpInst;
using llvm::Function;
using llvm::FunctionType;
using llvm::GlobalValue;
using llvm::GlobalVariable;
using llvm::ICmpInst;
using llvm::LLVMContext;
using llvm::Module;
using llvm::SMDiagnostic;
using llvm::StructType;
using llvm::Type;
using llvm::Value;
using json11::Json;
using std::map;
using std::string;
using std::to_string;
using std::unique_ptr;
using std::vector;

using namespace stormsql;

TEST(TestExprCompiler, TestLiteral) {
  LLVMContext ctx;
  unique_ptr<Module> M(new Module("test-literal", ctx));
  unique_ptr<BasicBlock> BB(BasicBlock::Create(ctx));
  llvm::IRBuilder<> builder(BB.get());
  TypeSystem ts(M.get());
  SMDiagnostic err;
  ExprCompiler compiler("TestLiteral", &ts, &builder, &err);
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
  GlobalVariable *GV = dyn_cast<GlobalVariable>(v);
  ASSERT_TRUE(GV && GV->isConstant());
  ConstantDataSequential *CDS =
      dyn_cast<ConstantDataSequential>(GV->getInitializer());
  ASSERT_TRUE(CDS && CDS->getAsCString() == "foo");
}

TEST(TestExprCompiler, TestCompare) {
  LLVMContext ctx;
  unique_ptr<Module> M(new Module("test-compare", ctx));
  TypeSystem ts(M.get());
  FunctionType *FTy = FunctionType::get(
      Type::getVoidTy(ctx),
      StructType::get(Type::getInt32Ty(ctx), ts.llvm_string_type(),
                      Type::getDoubleTy(ctx), nullptr),
      false);
  Function *F =
      Function::Create(FTy, GlobalValue::PrivateLinkage, "dummy", M.get());
  BasicBlock *BB = BasicBlock::Create(ctx, "entry", F);
  llvm::IRBuilder<> builder(BB);
  SMDiagnostic err;
  ExprCompiler compiler("TestCompare", &ts, &builder, &err);
  Value *v = compiler.Visit(
      CallExpr("BOOLEAN", "<", {InputRef("INTEGER", 0), LiteralInt(2)}));
  ICmpInst *IC = dyn_cast<ICmpInst>(v);
  ASSERT_TRUE(IC && IC->getPredicate() == ICmpInst::ICMP_SLT);
  v = compiler.Visit(
      CallExpr("BOOLEAN", "=", {InputRef("CHAR", 1), LiteralString("2")}));
  CallInst *CI = dyn_cast<CallInst>(v);
  ASSERT_TRUE(CI && CI->getCalledFunction() == ts.equals());
  v = compiler.Visit(
      CallExpr("BOOLEAN", "<>", {InputRef("DECIMAL", 2), LiteralDouble(2)}));
  FCmpInst *FC = dyn_cast<FCmpInst>(v);
  ASSERT_TRUE(FC && FC->getPredicate() == FCmpInst::FCMP_ONE);
  ASSERT_TRUE(verifyModule(*M));
}
