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

#include "compiler_input.h"

using json11::Json;
using std::map;
using std::string;
using std::to_string;
using std::vector;

namespace stormsql {

Json GetLiteral(const string &type, const string &value) {
  map<string, Json> v({{"inst", "literal"}, {"value", value}});
  map<string, Json> r({{"type", type}, {"value", Json(v)}});
  return Json(r);
}
Json LiteralBool(bool v) {
  return GetLiteral("BOOLEAN", v ? "true" : "false");
}
Json LiteralInt(int v) {
  return GetLiteral("INTEGER", to_string(v));
}
Json LiteralDouble(double v) {
  return GetLiteral("DECIMAL", to_string(v));
}
Json LiteralString(const string &v) {
  return GetLiteral("CHAR", v);
}
Json CallExpr(const string &type, const string &op,
                            const vector<Json> &operands) {
  map<string, Json> v(
      {{"inst", "call"}, {"operator", op}, {"operands", operands}});
  map<string, Json> r({{"type", type}, {"value", Json(v)}});
  return Json(r);
}
Json InputRef(const string &type, int idx) {
  map<string, Json> v({{"inst", "inputref"}, {"idx", idx}});
  map<string, Json> r({{"type", type}, {"value", Json(v)}});
  return Json(r);
}
}
