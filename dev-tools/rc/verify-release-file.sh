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

TARGET_FILE=$1

if [ "${TARGET_FILE}" == "" ];
then
  echo "USAGE: $0 [target file path]"
  exit 1
fi

echo "> checking file ${TARGET_FILE}"

# verifying
ASC_TARGET_FILE="${TARGET_FILE}.asc"

echo ">> verifying signature... (${ASC_TARGET_FILE})"
gpg --verify ${ASC_TARGET_FILE} ${TARGET_FILE}

if [ $? -eq 0 ];
then
  echo 'Signature seems correct'
else
  echo 'Signature seems not correct'
fi

# checking SHA
GPG_SHA_FILE="/tmp/${TARGET_FILE}_GPG.sha512"
gpg --print-md SHA512 ${TARGET_FILE} > ${GPG_SHA_FILE}
SHA_TARGET_FILE="${TARGET_FILE}.sha512"

echo ">> checking SHA file... (${SHA_TARGET_FILE})"
diff /tmp/${TARGET_FILE}_GPG.sha512 ${SHA_TARGET_FILE}

if [ $? -eq 0 ];
then
  echo 'SHA file is correct'
else
  echo 'SHA file is not correct'
fi
