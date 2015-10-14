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

#ifndef STORM_SQL_COMPILER_COMPILER_H_
#define STORM_SQL_COMPILER_COMPILER_H_

#include "typesystem.h"
#include "stage_visitor.h"

#include <llvm/IR/Module.h>
#include <llvm/Support/SourceMgr.h>

#include <map>
#include <memory>

namespace stormsql {

/**
 * The compiler compiles the Calcite execution plan in JSON format into LLVM
 * Module.
 **/
class StageCompiler : public StageVisitor<StageCompiler, llvm::Function *> {
public:
  typedef std::map<std::string, Table> Schema;
  explicit StageCompiler(TypeSystem *types, const Schema *schema,
                         llvm::SMDiagnostic *err);
  const std::map<int, llvm::Function *> &stages() const { return stages_; }
  llvm::Function *VisitTableScan(const Json &stage);
  llvm::Function *VisitFilter(const Json &stage);
  llvm::Function *VisitProject(const Json &stage);

private:
  static std::string GetStageName(const Json &stage);
  llvm::Function *RecordStage(int stage, llvm::Function *f);
  bool CollectInputTypes(const Json &stage, std::vector<llvm::Type *> *input_types) const;

  TypeSystem *types_;
  const Schema *schema_;
  llvm::SMDiagnostic *err_;
  std::map<int, llvm::Function *> stages_;
  llvm::Module *const module_;
};
}

#endif
