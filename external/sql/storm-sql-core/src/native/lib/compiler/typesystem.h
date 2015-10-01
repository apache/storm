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

#ifndef STORM_SQL_COMPILER_TYPESYSTEM_H_
#define STORM_SQL_COMPILER_TYPESYSTEM_H_

#include <json11/json11.hpp>
#include <llvm/IR/Function.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Type.h>

#include <map>
#include <memory>
#include <vector>

namespace stormsql {

class Table {
public:
  typedef std::pair<std::string, llvm::Type *> Field;
  explicit Table(std::vector<Field> &&fields);
  const std::vector<Field> &fields() const { return fields_; }

private:
  std::vector<Field> fields_;
};

class TypeSystem {
public:
  enum SqlType { kIntegerTy, kDecimalTy, kBooleanTy, kStringTy };
  explicit TypeSystem(llvm::Module *module);
  llvm::Type *GetLLVMType(const json11::Json &type);
  static SqlType GetSqlTypeId(const json11::Json &type);
  llvm::Module *module() const { return module_; }
  llvm::Function *equals() const { return equals_; }
  llvm::Function *string_equals_ignore_case() const {
    return string_equals_ignore_case_;
  }
  llvm::Type *llvm_string_type() const;
  llvm::Constant *GetOrInsertString(const std::string &str);

private:
  llvm::Module *module_;
  llvm::Function *equals_;
  llvm::Function *string_equals_ignore_case_;
  static const std::map<std::string, SqlType> sql_type_map_;
  std::map<std::string, llvm::Constant *> string_map_;
};
}

#endif
