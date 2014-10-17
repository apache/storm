#!/usr/bin/env bash


# Runs a Storm command as a daemon.
#
# Environment Variables
#
#   STORM_CONF_DIR  Alternate conf dir. Default is ${STORM_HOME}/conf.
#   STORM_LOG_DIR   Where log files are stored.  PWD by default.
#   STORM_PID_DIR   The pid files are stored. /tmp by default.
##

usage="Usage: storm-daemon.sh [--config <conf-dir>] [--hosts hostlistfile] [--script script] (start|stop) <storm-command> <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

. "$bin"/storm-config.sh

# get arguments

#default value
stormScript="$STORM_HOME"/bin/storm
if [ "--script" = "$1" ]
  then
    shift
    stormScript=$1
    shift
fi
startStop=$1
shift
command=$1
shift

storm_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
	num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
	while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
	    num=$prev
	done
	mv "$log" "$log.$num";
    fi
}

if [ -f "${STORM_CONF_DIR}/storm-env.sh" ]; then
  . "${STORM_CONF_DIR}/storm-env.sh"
fi

# get log directory
if [ "$STORM_LOG_DIR" = "" ]; then
  export STORM_LOG_DIR="$STORM_HOME/logs"
fi
mkdir -p "$STORM_LOG_DIR"

if [ "$STORM_PID_DIR" = "" ]; then
  STORM_PID_DIR=/tmp
fi


# some variables
export STORM_LOGFILE=storm-$STORM_IDENT_STRING-$command-$HOSTNAME.log
export STORM_ROOT_LOGGER="INFO,DRFA"
log=$STORM_LOG_DIR/storm-$STORM_IDENT_STRING-$command-$HOSTNAME.out
pid=$STORM_PID_DIR/storm-$STORM_IDENT_STRING-$command.pid

# Set default scheduling priority
if [ "$STORM_NICENESS" = "" ]; then
    export STORM_NICENESS=0
fi

case $startStop in

  (start)

    mkdir -p "$STORM_PID_DIR"

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    storm_rotate_log $log
    echo starting $command, logging to $log
    cd "$STORM_HOME"
    nohup nice -n $STORM_NICENESS $stormScript --config $STORM_CONF_DIR $command "$@" > "$log" 2>&1 < /dev/null &
    echo $! > $pid
    sleep 1; head "$log"
    ;;
          
  (stop)

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo stopping $command
        kill `cat $pid`
      else
        echo no $command to stop
      fi
      sleep 3
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo stopping $command
        kill -9  `cat $pid`
      fi
    else
      echo no $command to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac


