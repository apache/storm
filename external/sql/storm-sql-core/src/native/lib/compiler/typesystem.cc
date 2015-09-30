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
#include <llvm/IR/DerivedTypes.h>
#include <cassert>

using json11::Json;
using llvm::Type;
using std::map;
using std::string;

namespace stormsql {

const map<string, TypeSystem::SqlType> TypeSystem::sql_type_map_ = {
    {"INTEGER", TypeSystem::SqlType::kIntegerTy},
    {"BOOLEAN", TypeSystem::SqlType::kBooleanTy},
    {"DECIMAL", TypeSystem::SqlType::kDecimalTy},
    {"CHAR", TypeSystem::SqlType::kStringTy}};

Table::Table(std::vector<Field> &&fields) : fields_(std::move(fields)) {}

TypeSystem::TypeSystem(llvm::LLVMContext *ctx) : ctx_(*ctx) {}

TypeSystem::SqlType TypeSystem::GetSqlTypeId(const Json &type) {
  auto it = sql_type_map_.find(type.string_value());
  assert(it != sql_type_map_.end() && "Unsupported type");
  return it->second;
}

Type *TypeSystem::GetLLVMType(const Json &type) {
  switch (GetSqlTypeId(type)) {
  case kIntegerTy:
    return Type::getInt32Ty(ctx_);
  case kBooleanTy:
    return Type::getInt1Ty(ctx_);
  case kDecimalTy:
    // SQL supports arbitary precision decimal type. For now the type system
    // returns a double
    return Type::getDoubleTy(ctx_);
  case kStringTy:
    return Type::getInt8PtrTy(ctx_);
  }
}
}
