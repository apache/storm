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

#include "expr_compiler.h"

#include <llvm/IR/Constants.h>
#include <functional>

using json11::Json;
using llvm::cast;
using llvm::APFloat;
using llvm::BinaryOperator;
using llvm::CmpInst;
using llvm::ConstantDataArray;
using llvm::ConstantFP;
using llvm::ConstantInt;
using llvm::Function;
using llvm::IntegerType;
using llvm::SMDiagnostic;
using llvm::SourceMgr;
using llvm::Type;
using llvm::Value;
using std::back_inserter;
using std::map;
using std::vector;
using std::string;
using std::to_string;
using std::transform;

namespace stormsql {

const map<CmpInst::Predicate, CmpInst::Predicate>
    ExprCompiler::floating_op_predicates_ = {
        {CmpInst::Predicate::ICMP_EQ, CmpInst::Predicate::FCMP_OEQ},
        {CmpInst::Predicate::ICMP_NE, CmpInst::Predicate::FCMP_ONE},
        {CmpInst::Predicate::ICMP_SGT, CmpInst::Predicate::FCMP_OGT},
        {CmpInst::Predicate::ICMP_SGE, CmpInst::Predicate::FCMP_OGE},
        {CmpInst::Predicate::ICMP_SLT, CmpInst::Predicate::FCMP_OLT},
        {CmpInst::Predicate::ICMP_SLE, CmpInst::Predicate::FCMP_OLE}};

const map<BinaryOperator::BinaryOps, BinaryOperator::BinaryOps>
    ExprCompiler::floating_binary_op_ = {
        {BinaryOperator::BinaryOps::Add, BinaryOperator::BinaryOps::FAdd},
        {BinaryOperator::BinaryOps::Sub, BinaryOperator::BinaryOps::FSub},
        {BinaryOperator::BinaryOps::Mul, BinaryOperator::BinaryOps::FMul},
        {BinaryOperator::BinaryOps::SDiv, BinaryOperator::BinaryOps::FDiv}};

ExprCompiler::ExprCompiler(const string &err_ctx, TypeSystem *typesystem,
                           llvm::IRBuilder<> *builder, SMDiagnostic *err)
    : err_ctx_(err_ctx), typesystem_(typesystem), builder_(builder), err_(err) {
}

Value *ExprCompiler::CompileValue(const Json &value) {
  Value *v = Visit(value);
  if (!v) {
    *err_ = SMDiagnostic(err_ctx_, SourceMgr::DK_Error,
                         "Failed to compile expression " + value.dump());
  }
  return v;
}

Value *ExprCompiler::VisitBinaryOperator(BinaryOperator::BinaryOps opcode,
                                         const Json &LHS, const Json &RHS) {
  Value *L, *R;
  if (!(L = CompileValue(LHS)) || !(R = CompileValue(RHS))) {
    return nullptr;
  }

  Type *t = L->getType();
  BinaryOperator::BinaryOps op = opcode;
  if (t == builder_->getFloatTy() || t == builder_->getDoubleTy()) {
    op = floating_binary_op_.at(opcode);
  }
  return builder_->CreateBinOp(op, L, R);
}

Value *ExprCompiler::VisitCmp(CmpInst::Predicate predicate, const Json &LHS,
                              const Json &RHS) {
  Value *L, *R;
  if (!(L = CompileValue(LHS)) || !(R = CompileValue(RHS))) {
    return nullptr;
  }

  // Assuming that the type promotion has been done
  TypeSystem::SqlType type_id = typesystem_->GetSqlTypeId(LHS["type"]);
  Value *v = nullptr;
  switch (type_id) {
  case TypeSystem::kBooleanTy:
  case TypeSystem::kIntegerTy:
    v = builder_->CreateICmp(predicate, L, R);
    break;
  case TypeSystem::kDecimalTy:
    v = builder_->CreateFCmp(floating_op_predicates_.at(predicate), L, R);
    break;
  case TypeSystem::kStringTy: {
    if (predicate != CmpInst::Predicate::ICMP_EQ &&
        predicate != CmpInst::Predicate::ICMP_NE) {
      *err_ =
          SMDiagnostic(err_ctx_, SourceMgr::DK_Error,
                       "Invalid predicate for string " + to_string(predicate));
      v = nullptr;
    } else {
      Function *f = typesystem_->equals();
      Value *CL = builder_->CreateBitCast(L, typesystem_->llvm_string_type());
      Value *CR = builder_->CreateBitCast(R, typesystem_->llvm_string_type());
      v = builder_->CreateCall(f, {CL, CR});
      if (predicate == CmpInst::Predicate::ICMP_NE) {
        v = builder_->CreateNot(v);
      }
    }
  } break;
  }
  return v;
}

Value *ExprCompiler::VisitInputRef(const Json &type, int index) {
  Function *F = builder_->GetInsertBlock()->getParent();
  auto AI = F->arg_begin();
  for (int i = 0; i < index; ++i) {
    ++AI;
  }
  Type *ty = typesystem_->GetLLVMType(type);
  assert(ty == AI->getType());
  return AI;
}

Value *ExprCompiler::VisitLiteral(const Json &type, const Json &value) {
  TypeSystem::SqlType sql_ty = typesystem_->GetSqlTypeId(type);
  Type *ty = typesystem_->GetLLVMType(type);
  switch (sql_ty) {
  case TypeSystem::kIntegerTy: {
    long long v = strtoll(value.string_value().c_str(), nullptr, 0);
    return ConstantInt::get(cast<IntegerType>(ty), v);
  }
  case TypeSystem::kBooleanTy: {
    return value.string_value() == "true" ? ConstantInt::getTrue(ty)
                                          : ConstantInt::getFalse(ty);
  }
  case TypeSystem::kDecimalTy: {
    APFloat fp(APFloat::IEEEdouble, value.string_value());
    return ConstantFP::get(ty->getContext(), fp);
  }
  case TypeSystem::kStringTy: {
    return typesystem_->GetOrInsertString(value.string_value());
  }
  }
}
}
