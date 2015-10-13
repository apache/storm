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

#include "typesystem.h"
#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/GlobalVariable.h>
#include <array>
#include <cassert>

using json11::Json;
using llvm::cast;
using llvm::Constant;
using llvm::ConstantDataArray;
using llvm::ConstantExpr;
using llvm::ConstantInt;
using llvm::Function;
using llvm::FunctionType;
using llvm::GlobalValue;
using llvm::GlobalVariable;
using llvm::LLVMContext;
using llvm::Module;
using llvm::Type;
using std::array;
using std::map;
using std::make_pair;
using std::string;
using std::to_string;
using std::vector;

namespace stormsql {

const map<string, TypeSystem::SqlType> TypeSystem::sql_type_map_ = {
    {"INTEGER", TypeSystem::SqlType::kIntegerTy},
    {"BOOLEAN", TypeSystem::SqlType::kBooleanTy},
    {"DECIMAL", TypeSystem::SqlType::kDecimalTy},
    {"CHAR", TypeSystem::SqlType::kStringTy}};

Table::Table(std::vector<Field> &&fields) : fields_(std::move(fields)) {}

TypeSystem::TypeSystem(Module *module) : module_(module) {
  LLVMContext &ctx = module_->getContext();
  Type *PtrTy = Type::getInt8PtrTy(ctx);
  FunctionType *EQTy =
      FunctionType::get(Type::getInt1Ty(ctx), vector<Type*>({PtrTy, PtrTy}), false);
  equals_ =
      cast<Function>(module_->getOrInsertFunction("stormsql.equals", EQTy));
  string_equals_ignore_case_ = cast<Function>(
      module_->getOrInsertFunction("stormsql.string.equals_ignore_case", EQTy));
}

TypeSystem::SqlType TypeSystem::GetSqlTypeId(const Json &type) {
  auto it = sql_type_map_.find(type.string_value());
  assert(it != sql_type_map_.end() && "Unsupported type");
  return it->second;
}

Type *TypeSystem::GetLLVMType(const Json &type) {
  LLVMContext &ctx = module_->getContext();
  switch (GetSqlTypeId(type)) {
  case kIntegerTy:
    return Type::getInt32Ty(ctx);
  case kBooleanTy:
    return Type::getInt1Ty(ctx);
  case kDecimalTy:
    // SQL supports arbitary precision decimal type. For now the type system
    // returns a double
    return Type::getDoubleTy(ctx);
  case kStringTy:
    return Type::getInt8PtrTy(ctx);
  }
}

Constant *TypeSystem::GetOrInsertNullRecord(Type *type) {
  auto it = null_record_map_.find(type);
  Constant *C = nullptr;
  if (it != null_record_map_.end()) {
    C = it->second;
  } else {
    C = new GlobalVariable(*module_, type, true,
                           GlobalVariable::ExternalLinkage, nullptr,
                           "null." + to_string(null_record_map_.size()));
    null_record_map_.insert(make_pair(type, C));
  }
  return C;
}

Type *TypeSystem::llvm_string_type() const {
  LLVMContext &ctx = module_->getContext();
  return Type::getInt8PtrTy(ctx);
}

Constant *TypeSystem::GetOrInsertString(const string &str) {
  auto it = string_map_.find(str);
  if (it != string_map_.end()) {
    return it->second;
  }
  auto c = ConstantDataArray::getString(module_->getContext(), str);
  auto v = new GlobalVariable(*module_, c->getType(), true,
                              GlobalValue::PrivateLinkage, c);
  string_map_.insert(make_pair(str, v));
  return v;
}
}
