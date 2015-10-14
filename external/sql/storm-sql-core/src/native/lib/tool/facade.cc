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

#include "stormsql/stormsql.h"
#include "compiler/plan_compiler.h"

#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>

using llvm::LLVMContext;
using llvm::MemoryBuffer;
using llvm::Module;
using llvm::SMDiagnostic;
using std::string;
using std::unique_ptr;

namespace stormsql {

/**
 * Compile the Calcite execution plan in JSON format into a lists of Java source
 * files in the working directory.
 *
 * Return 0 if succeeds.
 **/
int CompilePlanToTrident(const string &plan_json,
                         const string &working_dir, string *err) {
  LLVMContext ctx;
  unique_ptr<MemoryBuffer> plan = MemoryBuffer::getMemBuffer(plan_json);
  SMDiagnostic sm;
  unique_ptr<PlanCompiler> compiler = PlanCompiler::Create("trident", &ctx, &sm);
  std::unique_ptr<Module> m = compiler->Compile(*plan);
  if (!m) {
    *err = sm.getMessage();
    return -1;
  }
  (void)working_dir;
  (void)err;
  return 0;
}
}
