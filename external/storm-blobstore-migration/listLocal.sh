#!/usr/bin/env bash

. config
VERSION=`cat VERSION`
MIGRATION_JAR=blobstore-migrator-${VERSION}.jar

if [ -n "$JAAS_CONF" ]; then
    java -Djava.security.auth.login.config=$JAAS_CONF -cp $HADOOP_CLASSPATH:$MIGRATION_JAR org.apache.storm.blobstore.MigratorMain listLocalFs $LOCAL_BLOBSTORE_DIR
else
    java -cp $HADOOP_CLASSPATH:$MIGRATION_JAR org.apache.storm.blobstore.MigratorMain listLocalFs $LOCAL_BLOBSTORE_DIR
fi
