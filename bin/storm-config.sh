#!/bin/bash
#
# Copyright 2011 The Apache Software Foundation
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

# Ensure STORM_BASE_DIR is set
if [[ -z "$STORM_BASE_DIR" ]]; then
  echo "Error: STORM_BASE_DIR is not set"
  exit 1
fi
  
#check to see if the conf dir is given as an optional argument
if [ $# -gt 1 ]
then
    if [ "--config" = "$1" ]
	  then
	      conf_file=$2
	      if [ ! -f "$conf_file" ]; then
                echo "Error: Cannot find configuration directory: $conf_file"
                exit 1
             fi
	      STORM_CONF_FILE=$conf_file
	      STORM_CONF_DIR=`dirname $conf_file`
    fi
fi

export STORM_CONF_DIR="${STORM_CONF_DIR:-$STORM_BASE_DIR/conf}"
export STORM_CONF_FILE="${STORM_CONF_FILE:-$STORM_BASE_DIR/conf/storm.yaml}"

if [ -f "${STORM_CONF_DIR}/storm-env.sh" ]; then
  . "${STORM_CONF_DIR}/storm-env.sh"
fi
