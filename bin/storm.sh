#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/storm-config.sh

function print_usage(){
  echo "Usage: storm [--config confdir] COMMAND"
  echo "         COMMAND       : Syntax"
  echo "           nimbus      : storm nimbus"
  echo "           supervisor  : storm supervisor"
  echo "           drpc        : storm drpc"
  echo "           dev-zookeeper	: storm dev-zookeeper"
  echo "           jar         : storm jar topology-jar-path class ..."
  echo "           rebalance   : storm rebalance
  echo "           kill        : storm kill topology-name [-w wait-time-secs]"
  echo "           activate    : storm activate topology-name"
  echo "           deactivate  : storm deactivate topology-name"
  echo "           list        : storm list"
  echo " "
  echo "           classpath   : storm classpath"
  echo " "
  echo "           version     : storm version"
  echo "Help: "
  echo "    help"
}

if [ $# = 0 ]; then
  print_usage
  exit
fi

COMMAND=$1
LOG_FILE="-Dlogfile.name=operations.log"
case $COMMAND in
   help|classpath)
    if [ "$COMMAND" = "help" ] ; then 
      print_usage
      exit
    elif [ "$COMMAND" = "classpath" ] ; then
      echo $CLASSPATH 
    else 
      CLASS=$COMMAND
    fi
    ;;
   jar)
    if [ $# -lt  3 ] ; then 
      echo "Warn: jar command neeed at least 2 args !"
      echo "Usage : "
      echo "storm jar jar_dir main_class [ topology_name [ args ]]"
      exit
    fi
    TYPE="-client"
    JAR_FILE=$2
    if [ ! -f "$JAR_FILE" ]; then
      echo "Jar File " $JAR_FILE  " does not exists!"
      exit
    fi
    STORM_JAR="-Dstorm.jar=$JAR_FILE"
    KCLASS=$3
    if $cygwin; then
      CLASSPATH=`cygpath -p -w "$CLASSPATH"`
    fi
    export CLASSPATH=$CLASSPATH:$JAR_FILE
    exec "$JAVA" $TYPE $JAVA_HEAP_MAX $STORM_OPTS $LOG_FILE $STORM_JAR $KCLASS "${@:4}"
    ;;

   kill|activate|deactivate|rebalance|list|zktool|shell|cli)
    if [ "$COMMAND" = "kill" ] ; then
      CLASS=backtype.storm.command.kill_topology
      TYPE="-client"
      STORM_OPTS="$STORM_OPTS $STORM_CLIENT_OPTS"
    elif [ "$COMMAND" = "activate" ] ; then
      CLASS=backtype.storm.command.activate
      TYPE="-client"
      STORM_OPTS="$STORM_OPTS $STORM_CLIENT_OPTS"
    elif [ "$COMMAND" = "deactivate" ] ; then
      CLASS=backtype.storm.command.deactivate
      TYPE="-client"
      STORM_OPTS="$STORM_OPTS $STORM_CLIENT_OPTS"
    elif [ "$COMMAND" = "list" ] ; then
      CLASS=backtype.storm.command.list
      TYPE="-client"
      STORM_OPTS="$STORM_OPTS $STORM_CLIENT_OPTS"
    elif [ "$COMMAND" = "shell" ] ; then
      CLASS=shell_submission
      TYPE="-client"
      STORM_OPTS="$STORM_OPTS $STORM_CLIENT_OPTS"
    elif [ "$COMMAND" = "rebalance" ] ; then
      CLASS=backtype.storm.command.rebalance
      TYPE="-client"
      STORM_OPTS="$STORM_OPTS $STORM_CLIENT_OPTS"
    else
       CLASS=$COMMAND
    fi
    shift

    if $cygwin; then
      CLASSPATH=`cygpath -p -w "$CLASSPATH"`
    fi
    export CLASSPATH=$CLASSPATH
    exec "$JAVA" $TYPE $JAVA_HEAP_MAX $STORM_OPTS $LOG_FILE $CLASS "$@"
    ;;

   *)
    # the core commands
    if [ "$COMMAND" = "nimbus" ] ; then
      CLASS=backtype.storm.daemon.nimbus
      TYPE="-server"
      LOG_FILE="-Dlogfile.name=nimbus.log"
      JAVA_HEAP_MAX=$JAVA_NIBMUS_CHILDOPTS
      STORM_OPTS="$STORM_OPTS $STORM_CLIENT_OPTS"
    elif [ "$COMMAND" = "supervisor" ] ; then
      CLASS=backtype.storm.daemon.supervisor
      TYPE="-server"
      JAVA_HEAP_MAX=$JAVA_SUPERVISOR_CHILDOPTS
      LOG_FILE="-Dlogfile.name=supervisor.log"
      STORM_OPTS="$STORM_OPTS $STORM_CLIENT_OPTS"
    elif [ "$COMMAND" = "drpc" ] ; then
      CLASS=backtype.storm.daemon.drpc
      TYPE="-server"
      JAVA_HEAP_MAX=$JAVA_DRPC_CHILDOPTS
      LOG_FILE="-Dlogfile.name=drpc.log"
      STORM_OPTS="$STORM_OPTS $STORM_CLIENT_OPTS"
      echo "DRPC Server Starting Now ... ..."
    elif [ "$COMMAND" = "dev-zookeeper" ] ; then
      CLASS=backtype.storm.command.dev_zookeeper
      TYPE="-server"
      JAVA_HEAP_MAX=$JAVA_DRPC_CHILDOPTS
      LOG_FILE="-Dlogfile.name=dev-zookeeper.log"
      STORM_OPTS="$STORM_OPTS $STORM_CLIENT_OPTS"

    elif [ "$COMMAND" = "version" ] ; then
      CLASS=backtype.storm.utils.version.VersionInfo
      STORM_OPTS="$STORM_OPTS $STORM_CLIENT_OPTS"
    else
      echo "WARNINNG: Do not known such command: " $COMMAND
      print_usage
      exit
    fi
    shift
    
    #if $cygwin; then
    #  CLASSPATH=`cygpath -p -w "$CLASSPATH"`
    #fi
    export CLASSPATH=$CLASSPATH
    exec "$JAVA" $TYPE $JAVA_HEAP_MAX $STORM_OPTS  $LOG_FILE $CLASS "$@"
    ;;
esac
