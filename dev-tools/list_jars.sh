#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# This script print the names of jars in the target directories and subdirectories.
#
# Listing the jars in the binary distribution helps validate that the LICENSE-binary file is complete.

set -Eeuo pipefail

SRC=${1:-.}

USAGE="list_jars <SOURCE_DIRECTORY:-.>"

if [ "${SRC}" = "-h" ]; then
	echo "${USAGE}"
	exit 0
fi

JARS=""
for dir in $@
do
  JARS+=`find -L "${SRC}" -name "*.jar" -printf "%f\n"`
  JARS+=$'\n'
done
echo "$JARS" | sort | uniq
