#!/bin/bash

TARGET_URL=$1

if [ "${TARGET_URL}" == "" ];
then
  echo "USAGE: $0 [target url]"
  exit 1
fi

echo "> downloading all files in RC directory..."

wget -r -nH -nd -np -R "index.html*" $1

echo "Done..."
