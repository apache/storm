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
using llvm::IntegerType;
using llvm::SMDiagnostic;
using llvm::SourceMgr;
using llvm::Type;
using llvm::Value;
using std::back_inserter;
using std::map;
using std::vector;
using std::string;
using std::transform;

namespace stormsql {

ExprCompiler::ExprCompiler(const string &err_ctx, TypeSystem *typesystem)
    : err_ctx_(err_ctx), typesystem_(typesystem) {}

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
    return ConstantDataArray::getString(ty->getContext(), value.string_value());
  }
  }
}
}
