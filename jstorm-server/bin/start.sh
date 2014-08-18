#!/bin/sh

if [ "x$JAVA_HOME" != "x" ]
then
    echo "JAVA_HOME has been set"
else
    export JAVA_HOME=/opt/taobao/java
fi

export JSTORM_HOME=/home/admin/jstorm-current
export PATH=$JAVA_HOME/bin:$JSTORM_HOME/bin:$PATH



function startJStorm()
{
	PROCESS=$1
  echo "start $PROCESS"
  cd $JSTORM_HOME/bin; nohup $JSTORM_HOME/bin/jstorm $PROCESS >/dev/null 2>&1 &
	sleep 4
	rm -rf nohup
	ps -ef|grep $2
}



HOSTNAME=`hostname -i`
NIMBUS_HOST=`grep "nimbus.host:" $JSTORM_HOME/conf/storm.yaml  | grep $HOSTNAME`

if [ "X${NIMBUS_HOST}" != "X" ] 
then
	startJStorm "nimbus" "NimbusServer"
	startJStorm "supervisor" "Supervisor"
else
	startJStorm "supervisor" "Supervisor"
fi

echo "Successfully  start jstorm daemon...."
