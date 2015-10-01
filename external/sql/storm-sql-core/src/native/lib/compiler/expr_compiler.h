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
#ifndef STORM_SQL_COMPILER_EXPR_COMPILER_H_
#define STORM_SQL_COMPILER_EXPR_COMPILER_H_

#include "expr_visitor.h"
#include "typesystem.h"
#include <llvm/IR/IRBuilder.h>
#include <llvm/Support/SourceMgr.h>
#include <map>

namespace stormsql {

/**
 * Compile a SQL expression down to LLVM IR
 **/
class ExprCompiler : public ExprVisitor<ExprCompiler, llvm::Value *> {
public:
  explicit ExprCompiler(const std::string &err_ctx, TypeSystem *typesystem,
                        llvm::IRBuilder<> *builder, llvm::SMDiagnostic *err);
  llvm::Value *VisitBinaryOperator(llvm::BinaryOperator::BinaryOps opcode,
                                   const Json &LHS, const Json &RHS);
  llvm::Value *VisitCmp(llvm::CmpInst::Predicate predicate, const Json &LHS,
                        const Json &RHS);
  llvm::Value *VisitInputRef(const Json &type, int index);
  llvm::Value *VisitLiteral(const Json &type, const Json &value);

private:
  llvm::Value *CompileValue(const Json &v);
  const std::string err_ctx_;
  TypeSystem *typesystem_;
  llvm::IRBuilder<> *builder_;
  llvm::SMDiagnostic *err_;
  static const std::map<llvm::CmpInst::Predicate, llvm::CmpInst::Predicate>
      floating_op_predicates_;
  static const std::map<llvm::BinaryOperator::BinaryOps,
                        llvm::BinaryOperator::BinaryOps> floating_binary_op_;
};
}
#endif
