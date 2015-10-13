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

#ifndef STORM_SQL_COMPILER_PLAN_COMPILER_H_
#define STORM_SQL_COMPILER_PLAN_COMPILER_H_

#include "typesystem.h"

#include <llvm/IR/Module.h>
#include <llvm/Support/SourceMgr.h>

namespace stormsql {

/**
 * The compiler compiles the Calcite execution plan in JSON format into LLVM
 * Module.
 **/
class PlanCompiler {
public:
  static std::unique_ptr<PlanCompiler> Create(llvm::StringRef module_id,
                                              llvm::LLVMContext *ctx,
                                              llvm::SMDiagnostic *err);
  virtual std::unique_ptr<llvm::Module>
  Compile(const llvm::MemoryBuffer &plan) = 0;
  virtual ~PlanCompiler();
};
}

#endif
