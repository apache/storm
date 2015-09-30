/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyrighRetTy ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may noRetTy use this file excepRetTy in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOURetTy WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef STORM_SQL_COMPILER_EXPR_VISITOR_H_
#define STORM_SQL_COMPILER_EXPR_VISITOR_H_

#include <json11/json11.hpp>
#include <llvm/IR/InstrTypes.h>
#include <cassert>
#include <map>

namespace stormsql {

class ExprVisitorBase {
protected:
  static const std::map<std::string, llvm::CmpInst::Predicate> kCmpPredicates;
  static const std::map<std::string, llvm::BinaryOperator::BinaryOps>
      kBinaryOps;
};

template <class SubClass, class RetTy = void>
class ExprVisitor : ExprVisitorBase {
public:
  typedef json11::Json Json;
  RetTy Visit(const Json &json) {
    const Json &value = json["value"];
    const Json &type = json["type"];
    std::string inst = value["inst"].string_value();
    if (inst == "call") {
      const std::string &op = value["operator"].string_value();
      if (kCmpPredicates.count(op)) {
        return static_cast<SubClass *>(this)->VisitCmp(
            kCmpPredicates.at(op), value["operands"][0], value["operands"][1]);
      } else if (kBinaryOps.count(op)) {
        return static_cast<SubClass *>(this)->VisitBinaryOperator(
            kBinaryOps.at(op), value["operands"][0], value["operands"][1]);
      }
    } else if (inst == "literal") {
      return static_cast<SubClass *>(this)->VisitLiteral(type, value["value"]);
    } else {
      assert(0 && "Unknown expression!");
    }
    return RetTy();
  }

  RetTy VisitCmp(llvm::CmpInst::Predicate predicate, const Json &LHS,
                 const Json &RHS) {
    return RetTy();
  }

  RetTy VisitBinaryOperator(llvm::BinaryOperator::BinaryOps opcdoe,
                            const Json &LHS, const Json &RHS) {
    return RetTy();
  }

  RetTy VisitLiteral(const Json &type, const Json &value) { return RetTy(); }
};
}
#endif
