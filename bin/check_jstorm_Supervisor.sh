#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
JAVA_HOME=/opt/taobao/java
export PATH=$PATH:$JAVA_HOME/bin

LOG=/home/admin/logs/check.log
SP=`ps -ef |grep com.alibaba.jstorm.daemon.supervisor.Supervisor |grep -v grep |wc -l`
if [ $SP -lt 1 ];then
	mkdir -p /home/admin/logs
        echo -e "`date` [ERROR] no process and restart Jstorm Suppervisor" >>$LOG
        cd /home/admin/bin; nohup /home/admin/jstorm/bin/jstorm supervisor >/dev/null 2>&1  &
else
        echo -e "`date`  [INFO:] return $SP Jstorm Supervisor ok " >>$LOG
fi
