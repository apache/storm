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

#include "expr_visitor.h"

using llvm::BinaryOperator;
using llvm::CmpInst;
using std::map;
using std::string;

namespace stormsql {
const map<string, CmpInst::Predicate> ExprVisitorBase::kCmpPredicates = {
    {"=", CmpInst::Predicate::ICMP_EQ},
    {"<>", CmpInst::Predicate::ICMP_NE},
    {">", CmpInst::Predicate::ICMP_SGT},
    {">=", CmpInst::Predicate::ICMP_SGE},
    {"<", CmpInst::Predicate::ICMP_SLT},
    {"<=", CmpInst::Predicate::ICMP_SLE}};

const map<string, BinaryOperator::BinaryOps> ExprVisitorBase::kBinaryOps = {
    {"+", BinaryOperator::BinaryOps::Add},
    {"-", BinaryOperator::BinaryOps::Sub},
    {"*", BinaryOperator::BinaryOps::Mul},
    {"/", BinaryOperator::BinaryOps::SDiv},
    {"/INT", BinaryOperator::BinaryOps::SDiv},
    {"AND", BinaryOperator::BinaryOps::And},
    {"OR", BinaryOperator::BinaryOps::Or}};
}
