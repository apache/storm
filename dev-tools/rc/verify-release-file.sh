#!/bin/bash

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

# checking MD5
GPG_MD5_FILE="/tmp/${TARGET_FILE}_GPG.md5"
gpg --print-md MD5 ${TARGET_FILE} > ${GPG_MD5_FILE}
MD5_TARGET_FILE="${TARGET_FILE}.md5"

echo ">> checking MD5 file... (${MD5_TARGET_FILE})"
diff ${GPG_MD5_FILE} ${MD5_TARGET_FILE}

if [ $? -eq 0 ];
then
  echo 'MD5 file is correct'
else
  echo 'MD5 file is not correct'
fi

# checking SHA
GPG_SHA_FILE="/tmp/${TARGET_FILE}_GPG.sha"
gpg --print-md SHA512 ${TARGET_FILE} > ${GPG_SHA_FILE}
SHA_TARGET_FILE="${TARGET_FILE}.sha"

echo ">> checking SHA file... (${SHA_TARGET_FILE})"
diff /tmp/${TARGET_FILE}_GPG.sha ${SHA_TARGET_FILE}

if [ $? -eq 0 ];
then
  echo 'SHA file is correct'
else
  echo 'SHA file is not correct'
fi
