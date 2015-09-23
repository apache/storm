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

#ifndef STORMSQL_STORM_SQL_H_
#define STORMSQL_STORM_SQL_H_

#include <string>

namespace stormsql {

/**
 * Compile the Calcite execution plan in JSON format into a lists of Java source
 * files in the working directory.
 *
 * Return 0 if succeeds.
 **/
int CompilePlanToTrident(const std::string &plan,
                         const std::string &working_dir, std::string *err);
}

#endif
