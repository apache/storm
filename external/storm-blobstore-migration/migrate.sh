#!/usr/bin/env bash

. config
VERSION=`cat VERSION`
MIGRATION_JAR=blobstore-migrator-${VERSION}.jar

if [ -n "$JAAS_CONF" ]; then
    java -cp $HADOOP_CLASSPATH:$MIGRATION_JAR org.apache.storm.blobstore.MigratorMain migrate $LOCAL_BLOBSTORE_DIR $HDFS_BLOBSTORE_DIR $BLOBSTORE_PRINCIPAL $KEYTAB_FILE
else
    java -Djava.security.auth.login.config=$JAAS_CONF -cp $HADOOP_CLASSPATH:$MIGRATION_JAR org.apache.storm.blobstore.MigratorMain migrate $LOCAL_BLOBSTORE_DIR $HDFS_BLOBSTORE_DIR $BLOBSTORE_PRINCIPAL $KEYTAB_FILE
fi

echo "Double check everything is correct, then start nimbus."
