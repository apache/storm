#!/bin/sh

JAVA_HOME=/opt/taobao/java
export PATH=$PATH:$JAVA_HOME/bin

HSOTNAME=`hostname`

function checkAndKill()
{
     PROCESS=$1
     jps=`ps aux | grep $PROCESS | grep -v 'grep' | awk '{print $2}'`
     if [ "$jps" = "" ]
     then
        echo "not exist $PROCESS"
     else
        echo "kill $PROCESS"
        test $jps && kill -9 $jps && sleep 5
     fi
}

NIMBUS_MASTER=`grep "nimbus.host:" ../conf/storm.yaml  | awk  '{print $2}'`
NIMBUS_SLAVE=`grep "nimbus.host:" ../conf/storm.yaml  | awk  '{print $3}'`

if [ "$HOSTNAME" = "${NIMBUS_MASTER}" ] || [ "$HOSTNAME" = "${NIMBUS_SLAVE}" ]
then
   checkAndKill "NimbusServer"
   checkAndKill "Supervisor"
else
   checkAndKill "Supervisor"
fi
   echo "end stop..."
