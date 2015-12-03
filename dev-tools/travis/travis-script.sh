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

PYTHON_VERSION_TO_FILE=`python -V > /tmp/python_version 2>&1`
PYTHON_VERSION=`cat /tmp/python_version`
RUBY_VERSION=`ruby -v`
NODEJS_VERSION=`node -v`
MVN_VERSION=`mvn -v`

echo "Python version : $PYTHON_VERSION"
echo "Ruby version : $RUBY_VERSION"
echo "NodeJs version : $NODEJS_VERSION"
echo "mvn version : $MVN_VERSION"

STORM_SRC_ROOT_DIR=$1

TRAVIS_SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

cd ${STORM_SRC_ROOT_DIR}

# We should be concerned that Travis CI could be very slow because it uses VM
export STORM_TEST_TIMEOUT_MS=150000

# We now lean on Travis CI's implicit behavior, ```mvn clean install -DskipTests``` before running script
mvn --batch-mode test -fae -Pnative -pl $2
BUILD_RET_VAL=$?

for dir in `find . -type d -and -wholename \*/target/\*-reports`;
do
  echo "Looking for errors in ${dir}"
  python ${TRAVIS_SCRIPT_DIR}/print-errors-from-test-reports.py "${dir}"
done

echo "Looking for unapproved licenses"
for rat in `find . -name rat.txt`;
do
  python ${TRAVIS_SCRIPT_DIR}/ratprint.py "${rat}"
done

# try the python3 test... use PATH hack to make "python" point to python3
alias python2=`which python`
mkdir ~/bin
ln -s /usr/bin/python3 ~/bin/python
export PATH=~/bin:$PATH
python2 ${TRAVIS_SCRIPT_DIR}/save-logs.py "python3.txt" mvn test -DfailIfNoTests=false -Dtest=backtype.storm.multilang-test
BUILD_RET_VAL=$?

if [[ "$BUILD_RET_VAL" != "0" ]];
then
  cat "python3.txt"
  for dir in `find . -type d -and -wholename \*/target/\*-reports`;
  do
    echo "Looking for errors in ${dir}"
    python2 ${TRAVIS_SCRIPT_DIR}/print-errors-from-test-reports.py "${dir}"
  done
  exit ${BUILD_RET_VAL}
fi

exit ${BUILD_RET_VAL}
