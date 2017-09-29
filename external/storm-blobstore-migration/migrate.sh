#!/usr/bin/env bash
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

. config
VERSION=`cat VERSION`
MIGRATION_JAR=blobstore-migrator-${VERSION}.jar

if [ -n "$JAAS_CONF" ]; then
    java -cp $HADOOP_CLASSPATH:$MIGRATION_JAR org.apache.storm.blobstore.MigratorMain migrate $LOCAL_BLOBSTORE_DIR $HDFS_BLOBSTORE_DIR $BLOBSTORE_PRINCIPAL $KEYTAB_FILE
else
    java -Djava.security.auth.login.config=$JAAS_CONF -cp $HADOOP_CLASSPATH:$MIGRATION_JAR org.apache.storm.blobstore.MigratorMain migrate $LOCAL_BLOBSTORE_DIR $HDFS_BLOBSTORE_DIR $BLOBSTORE_PRINCIPAL $KEYTAB_FILE
fi

echo "Double check everything is correct, then start nimbus."
