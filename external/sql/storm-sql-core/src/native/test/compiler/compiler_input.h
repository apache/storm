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

#ifndef COMPILER_INPUT_H_
#define COMPILER_INPUT_H_

#include <json11/json11.hpp>
#include <map>
#include <string>
#include <vector>

namespace stormsql {

json11::Json GetLiteral(const std::string &type, const std::string &value);
json11::Json LiteralBool(bool v);
json11::Json LiteralInt(int v);
json11::Json LiteralDouble(double v);
json11::Json LiteralString(const std::string &v);
json11::Json CallExpr(const std::string &type, const std::string &op,
                      const std::vector<json11::Json> &operands);
json11::Json InputRef(const std::string &type, int idx);
}
#endif
