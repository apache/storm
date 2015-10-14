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
#ifndef STORM_SQL_COMPILER_STAGE_VISITOR_H_
#define STORM_SQL_COMPILER_STAGE_VISITOR_H_

#include <json11/json11.hpp>
#include <cassert>

namespace stormsql {

template <class SubClass, class RetTy = void> class StageVisitor {
public:
  typedef json11::Json Json;
  RetTy Visit(const json11::Json &json) {
    std::string type = json["type"].string_value();
    if (type == "EnumerableTableScan") {
      return static_cast<SubClass *>(this)->VisitTableScan(json);
    } else if (type == "LogicalFilter") {
      return static_cast<SubClass *>(this)->VisitFilter(json);
    } else if (type == "LogicalProject") {
      return static_cast<SubClass *>(this)->VisitProject(json);
    } else if (type == "LogicalDelta") {
      return static_cast<SubClass *>(this)->VisitDelta(json);
    } else {
      assert(0 && "Unknown type!");
    }
    return RetTy();
  }

  RetTy VisitTableScan(const Json &json) { return RetTy(); }
  RetTy VisitFilter(const Json &json) { return RetTy(); }
  RetTy VisitProject(const Json &json) { return RetTy(); }
  RetTy VisitDelta(const Json &json) { return RetTy(); }
};
}
#endif
