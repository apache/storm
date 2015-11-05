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
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
if [ -e  ~/.bashrc ]
then
    source ~/.bashrc
fi

if [ -e  ~/.bash_profile ]
then
    source ~/.bash_profile
fi

if [ "x$JAVA_HOME" != "x" ]
then
    echo "JAVA_HOME has been set " 
else
    export JAVA_HOME=/opt/taobao/java
fi
echo "JAVA_HOME =" $JAVA_HOME

if [ "x$JSTORM_HOME" != "x" ]
then
    echo "JSTORM_HOME has been set "
else
    export JSTORM_HOME=/home/admin/jstorm
fi
echo "JSTORM_HOME =" $JSTORM_HOME

if [ "x$JSTORM_CONF_DIR_PATH" != "x" ]
then
    echo "JSTORM_CONF_DIR_PATH has been set " 
else
    export JSTORM_CONF_DIR_PATH=$JSTORM_HOME/conf
fi
echo "JSTORM_CONF_DIR_PATH =" $JSTORM_CONF_DIR_PATH



export PATH=$JAVA_HOME/bin:$JSTORM_HOME/bin:$PATH


which java

if [ $? -eq 0 ]
then
    echo "Find java"
else
    echo "No java, please install java firstly !!!"
    exit 1
fi

function startJStorm()
{
	PROCESS=$1
  echo "start $PROCESS"
  cd $JSTORM_HOME/bin; nohup $JSTORM_HOME/bin/jstorm $PROCESS >/dev/null 2>&1 &
	sleep 4
	rm -rf nohup
	ps -ef|grep $2
}



HOSTNAME=`hostname -i`
NIMBUS_HOST=`grep "nimbus.host:" $JSTORM_CONF_DIR_PATH/storm.yaml  | grep -w $HOSTNAME`
SUPERVISOR_HOST_START=`grep "supervisor.host.start:" $JSTORM_CONF_DIR_PATH/storm.yaml  | grep -w "false"`

if [ "X${NIMBUS_HOST}" != "X" ]
then
	startJStorm "nimbus" "NimbusServer"
fi

if [ "X${SUPERVISOR_HOST_START}" == "X" ]
then
	startJStorm "supervisor" "Supervisor"
fi

echo "Successfully  start jstorm daemon...."
