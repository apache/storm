#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
USER_SCRIPT="user-script.sh"
[[ -f $USER_SCRIPT ]] && echo "Running ${USER_SCRIPT}" && bash ${USER_SCRIPT} || echo "${USER_SCRIPT} not found/executed, continuing."
#apt-get update
#apt-get --yes remove openjdk-6-jre-headless
#apt-get --yes install openjdk-7-jdk
