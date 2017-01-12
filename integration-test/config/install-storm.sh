#!/usr/bin/env bash
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
# $1 is the storm binary zip file
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

groupadd storm
useradd --gid storm --home-dir /home/storm --create-home --shell /bin/bash storm

unzip -o "$1" -d /usr/share/
chown -R storm:storm /usr/share/apache-storm*
ln -s /usr/share/apache-storm* /usr/share/storm
ln -s /usr/share/storm/bin/storm /usr/bin/storm

mkdir -p /etc/storm/conf
chown -R storm:storm /etc/storm

rm /usr/share/storm/conf/storm.yaml
cp "${SCRIPT_DIR}/storm.yaml" /usr/share/storm/conf/
cp "${SCRIPT_DIR}/cluster.xml" /usr/share/storm/logback/
ln -s /usr/share/storm/conf/storm.yaml /etc/storm/conf/storm.yaml

mkdir /var/log/storm
chown storm:storm /var/log/storm

#sed -i 's/${storm.home}\/logs/\/var\/log\/storm/g' /usr/share/storm/logback/cluster.xml
