#!/bin/sh


function killJStorm()
{
  ps -ef|grep $1|grep -v grep |awk '{print $2}' |xargs kill
	sleep 1
	ps -ef|grep $1

	echo "kill "$1
}

killJStorm "Supervisor"
killJStorm "NimbusServer"
echo "Successfully stop jstorm"
