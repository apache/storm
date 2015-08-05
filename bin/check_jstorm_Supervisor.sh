#!/bin/bash
JAVA_HOME=/opt/taobao/java
export PATH=$PATH:$JAVA_HOME/bin

LOG=/home/admin/logs/check.log
SP=`ps -ef |grep com.alibaba.jstorm.daemon.supervisor.Supervisor |grep -v grep |wc -l`
if [ $SP -lt 1 ];then
	mkdir -p /home/admin/logs
        echo -e "`date` [ERROR] no process and restart Jstorm Suppervisor" >>$LOG
        cd /home/admin/bin; nohup /home/admin/jstorm/bin/jstorm supervisor >/dev/null 2>&1  &
else
        echo -e "`date`  [INFO:] return $SP Jstorm Supervisor ok " >>$LOG
fi
