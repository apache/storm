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

echo "Python3 version:  " $(python3 -V 2>&1)
echo "Ruby version   :  " $(ruby -v)
echo "NodeJs version :  " $(node -v)
echo "Maven version  :  " $(mvn -v)

set -x

STORM_SRC_ROOT_DIR=$1

THIS_SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

cd "${STORM_SRC_ROOT_DIR}" || (echo "Cannot cd to ${STORM_SRC_ROOT_DIR}"; exit 1)

if [ "$2" == "Integration-Test" ]
    then
    exec ./integration-test/run-it.sh
elif [ "$2" == "Check-Updated-License-Files" ]
    then
    exec python3 dev-tools/validate-license-files.py --skip-build-storm
elif [ "$2" == "Client" ]
then
    TEST_MODULES=storm-client
elif [ "$2" == "Server" ]
then
    TEST_MODULES=storm-server,storm-webapp
elif [ "$2" == "Core" ]
then
    TEST_MODULES=storm-core
elif [ "$2" == "External" ]
then
    TEST_MODULES='!storm-client,!storm-server,!storm-core,!storm-webapp,!storm-shaded-deps'
fi
# We should be concerned that Travis CI could be very slow because it uses VM
export STORM_TEST_TIMEOUT_MS=150000
# Github Action Runner only has 7GB of memory, lets use 1.5GB for build, with enough stack to run tests
export MAVEN_OPTS="-Xmx2048m"

mvn --batch-mode test -fae -Pnative,all-tests,examples,externals -Prat -pl "$TEST_MODULES"
BUILD_RET_VAL=$?

for dir in $(find . -type d -and -wholename \*/target/\*-reports)
do
  echo "Looking for errors in ${dir}"
  python3 "${THIS_SCRIPT_DIR}"/print-errors-from-test-reports.py "${dir}"
done

exit ${BUILD_RET_VAL}
