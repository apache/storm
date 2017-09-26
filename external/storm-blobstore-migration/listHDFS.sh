#!/usr/bin/env bash

. config
VERSION=`cat VERSION`
MIGRATION_JAR=blobstore-migrator-${VERSION}.jar

if [ -n "$JAAS_CONF" ]; then
    java -Djava.security.auth.login.config=$JAAS_CONF -cp $HADOOP_CLASSPATH:$MIGRATION_JAR org.apache.storm.blobstore.MigratorMain listHDFS $HDFS_BLOBSTORE_DIR $BLOBSTORE_PRINCIPAL $KEYTAB_FILE;
else
    java -cp $HADOOP_CLASSPATH:$MIGRATION_JAR org.apache.storm.blobstore.MigratorMain listHDFS $HDFS_BLOBSTORE_DIR $BLOBSTORE_PRINCIPAL $KEYTAB_FILE;
fi
