#!/usr/bin/env bash
# -*- compile-command: "cd config/ && vagrant destroy -f; vagrant up" -*-
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
set -x
set -e
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo SCRIPT_DIR="${SCRIPT_DIR}"
STORM_SRC_DIR=$(dirname "${SCRIPT_DIR}")
echo SCRIPT_SRC_DIR="${SCRIPT_SRC_DIR}"
function die() {
  echo $*
  exit 1
}
function list_storm_processes() {
    (ps -ef | grep -i -e zookeeper | grep -v grep) && (ps -ef | grep -i -e storm.home  | grep -v grep)
}

list_storm_processes || true
if [ "$(uname -m)" != aarch64 ]; then
  # increasing swap space so we can run lots of workers
  sudo dd if=/dev/zero of=/swapfile.img bs=4096 count=1M
  sudo mkswap /swapfile.img
  sudo swapon /swapfile.img
fi


if [[ "${USER}" == "vagrant" ]]; then # install oracle jdk8 or openjdk11
    sudo apt-get update
    sudo apt-get -y install python-software-properties
    if [[ "${JDK_VERSION}" -ne "11" ]]
    then
      sudo apt-add-repository -y ppa:webupd8team/java
      sudo apt-get update
      echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true" | sudo debconf-set-selections
      sudo apt-get install -y oracle-java8-installer
    else 
      sudo add-apt-repository ppa:openjdk-r/ppa
      sudo apt-get update
      sudo apt-get install -y openjdk-11-jdk
    fi
    sudo apt-get -y install maven
    sudo apt-get install unzip
    java -version
    mvn --version
    export MAVEN_OPTS="-Xmx3000m"
else
    if [[ "${USER}" == "github" ]]; then
        ( cd "${STORM_SRC_DIR}/storm-dist/binary" && mvn clean package -Dgpg.skip=true )
    fi
    (( $(find "${STORM_SRC_DIR}/storm-dist/binary" -iname 'apache-storm*.zip' | wc -l) == 1 )) || die "expected exactly one zip file, did you run: cd ${STORM_SRC_DIR}/storm-dist/binary && mvn clean package -Dgpg.skip=true"
fi

storm_binary_zip=$(find "${STORM_SRC_DIR}/storm-dist" -iname '*.zip')
storm_binary_name=$(basename "${storm_binary_zip}")
export STORM_VERSION=$(grep -oPe '\d.*(?=.zip)' <<<"${storm_binary_name}")
echo "Using storm version:" ${STORM_VERSION}

# setup storm cluster
list_storm_processes || true
sudo bash "${SCRIPT_DIR}/config/common.sh"
sudo bash "${SCRIPT_DIR}/config/install-storm.sh" "$storm_binary_zip"
if [[ "$TRAVIS_JDK_VERSION" == "openjdk11" ]] || [[ "${JDK_VERSION}" == "11" ]]
then
  cat "${SCRIPT_DIR}/config/storm-java9.yaml" | sudo tee -a /usr/share/storm/conf/storm.yaml
fi
export JAVA_HOME="${JAVA_HOME}"
env
function start_storm_process() {
    echo starting: storm $1
    sudo su storm -c "export JAVA_HOME=\"${JAVA_HOME}\" && cd /usr/share/storm && storm $1" &
}
start_storm_process dev-zookeeper
start_storm_process nimbus
start_storm_process ui
start_storm_process supervisor
start_storm_process logviewer
#start_storm_process drpc
pushd "${SCRIPT_DIR}"
mvn clean package  -DskipTests
for i in {1..20} ; do
    list_storm_processes && break
    sleep 6
done
list_storm_processes
mvn test -DfailIfNoTests=false -DskipTests=false -Dstorm.version=${STORM_VERSION} -Dui.url=http://localhost:8744

# Kill all storm processes after tests running, otherwise the Travis CI(ARM) will still wait and then fail by
# timeout error even though all the tests passed
sudo pkill -9 -u storm
