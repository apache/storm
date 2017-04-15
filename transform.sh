#!/bin/sh
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


find . -iname \*.java | xargs sed -i .back \
  -e 's/stormConf/topoConf/g' \
  -e 's/storm_conf/topoConf/g' \
  -e 's/topology_conf/topoConf/g' \
  -e 's/Map topoConf/Map<String, Object> topoConf/g' \
  -e 's/Map topologyConf/Map<String, Object> topologyConf/g' \
  -e 's/Map conf/Map<String, Object> conf/g' \
  -e 's/Map<String, *String> topoConf/Map<String, Object> topoConf/g' \
  -e 's/Map<String, *String> conf/Map<String, Object> conf/g' \
  -e 's/Map<String, *?> topoConf/Map<String, Object> topoConf/g' \
  -e 's/Map<String, *?> conf/Map<String, Object> conf/g' \
  -e 's/Map<Object, *Object> conf/Map<String, Object> conf/g' \
  -e 's/conf = new HashMap<String, *String>/conf = new HashMap<>/g'
find . -iname \*.java.back | xargs rm -f
