#!/bin/sh

JAVA_HOME=/opt/taobao/java
export PATH=$PATH:$JAVA_HOME/bin

HSOTNAME=`hostname`

ALOHA_HOME=/home/admin/aloha

cd `dirname $0`

function checkAndStart()
{
     PROCESS=$1
     jps=`ps aux | grep $PROCESS | grep -v 'grep' | awk '{print $2}'`
     if [ "$jps" = "" ]
     then
        echo "start $PROCESS"
        if [ "$PROCESS" = "NimbusServer" ]
        then
            $ALOHA_HOME/target/aloha-run/bin/aloha nimbus &
        elif [ "$PROCESS" = "Supervisor" ]
        then    
            $ALOHA_HOME/target/aloha-run/bin/aloha supervisor &
        else
            echo "$PROCESS not support in jstorm commond"
        fi
     else
        echo "Already exist $PROCESS"
     fi
}

cp -f $ALOHA_HOME/conf/storm.yaml ../conf/

rm -rf ../logs
mkdir ../logs

NIMBUS_MASTER=`grep "nimbus.host:" ../conf/storm.yaml  | awk  '{print $2}'`
NIMBUS_SLAVE=`grep "nimbus.host:" ../conf/storm.yaml  | awk  '{print $3}'`

if [ "$HOSTNAME" = "${NIMBUS_MASTER}" ] || [ "$HOSTNAME" = "${NIMBUS_SLAVE}" ]
then
   checkAndStart "NimbusServer"
   checkAndStart "Supervisor"
else
   checkAndStart "Supervisor"
fi
   echo "end start...."
