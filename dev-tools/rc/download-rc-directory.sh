#!/bin/bash
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

TARGET_URL=$1

if [ "${TARGET_URL}" == "" ];
then
  echo "USAGE: $0 [target url]"
  exit 1
fi

echo "> downloading all files in RC directory..."

wget -r -nH -nd -np -R "index.html*" $1

echo "Done..."
