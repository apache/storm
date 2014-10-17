this="${BASH_SOURCE-$0}"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

common_bin=`dirname "$this"`
script=`basename "$this"`
common_bin=`cd "$common_bin"; pwd`
this="$common_bin/$script"
STORM_OPTS="-Dstorm.options="
export STORM_HOME=`dirname "$this"`/..

#check to see if the conf dir is given as an optional argument
while [ $# -gt 0 ]; do    # Until you run out of parameters . . .
  case "$1" in
    --config)
        shift
        confdir=$1
        shift
        STORM_CONF_DIR=$confdir
        ;;
    -c)
        shift
        STORM_OPTS=$STORM_OPTS,$1
        shift
        ;;
    *)
        break;
        ;;
  esac
done

if [ -f "${common_bin}/storm-env.sh" ]; then
  . "${common_bin}/storm-env.sh"
fi

export STORM_CONF_DIR="${STORM_CONF_DIR:-$STORM_HOME/conf}"
LOG4J_CONF="${STORM_CONF_DIR}/storm.log4j.properties"
cygwin=false
case "`uname`" in
CYGWIN*) cygwin=true;;
esac

# some Java parameters
if [ "$JAVA_HOME" != "" ]; then
  #echo "run java in $JAVA_HOME"
  JAVA_HOME=$JAVA_HOME
fi

if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi

JAVA=$JAVA_HOME/bin/java
JAVA_HEAP_MAX=-Xmx1000m
if [ "$STORM_HEAPSIZE" != "" ]; then
  #echo "run with heapsize $STORM_HEAPSIZE"
  JAVA_HEAP_MAX="-Xmx""$STORM_HEAPSIZE""m"
  #echo $JAVA_HEAP_MAX
fi

JAVA_NIBMUS_CHILDOPTS=$JAVA_HEAP_MAX
JAVA_SUPERVISOR_CHILDOPTS=$JAVA_HEAP_MAX
if [ "$NIMBUS_CHILDOPTS" != "" ]; then
   JAVA_NIBMUS_CHILDOPTS=$NIMBUS_CHILDOPTS
fi
if [ "$SUPERVISOR_CHILDOPTS" != "" ]; then
   JAVA_SUPERVISOR_CHILDOPTS=$SUPERVISOR_CHILDOPTS
fi
if [ "$DRPC_CHILDOPTS" != "" ]; then
   JAVA_DRPC_CHILDOPTS=$DRPC_CHILDOPTS
fi

# CLASSPATH initially contains $STORM_CONF_DIR
CLASSPATH="${STORM_CONF_DIR}"
CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar
# for releases, add core storm jar & webapps to CLASSPATH
if [ -d "$STORM_HOME/webapps" ]; then
  CLASSPATH=${CLASSPATH}:$STORM_HOME
fi
for f in $STORM_HOME/storm-core-*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

# add libs to CLASSPATH
for f in $STORM_HOME/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

# add user-specified CLASSPATH last
if [ "$STORM_CLASSPATH" != "" ]; then
  CLASSPATH=${CLASSPATH}:${STORM_CLASSPATH}
fi

# default log directory & file
if [ "$STORM_LOG_DIR" = "" ]; then
  STORM_LOG_DIR="$STORM_HOME/logs"
fi
if [ "$STORM_LOGFILE" = "" ]; then
  STORM_LOGFILE="storm.log"
fi

# restore ordinary behaviour
unset IFS

# cygwin path translation
if $cygwin; then
  STORM_HOME=`cygpath -w "$STORM_HOME"`
  STORM_LOG_DIR=`cygpath -w "$STORM_LOG_DIR"`
fi
# setup 'java.library.path' for native-storm code if necessary
JAVA_LIBRARY_PATH=''
if [ -d "${STORM_HOME}/build/native" -o -d "${STORM_HOME}/lib/native" ]; then
  JAVA_PLATFORM=`CLASSPATH=${CLASSPATH} ${JAVA} -Xmx32m ${STORM_JAVA_PLATFORM_OPTS} com.tencent.jstorm.utils.PlatformName | sed -e "s/ /_/g"`

  if [ -d "$STORM_HOME/build/native" ]; then
    JAVA_LIBRARY_PATH=${STORM_HOME}/build/native/${JAVA_PLATFORM}/lib
  fi

  if [ -d "${STORM_HOME}/lib/native" ]; then
    if [ "x$JAVA_LIBRARY_PATH" != "x" ]; then
      JAVA_LIBRARY_PATH=${JAVA_LIBRARY_PATH}:${STORM_HOME}/lib/native/${JAVA_PLATFORM}
    else
      JAVA_LIBRARY_PATH=${STORM_HOME}/lib/native/${JAVA_PLATFORM}
    fi
  fi
fi

# cygwin path translation
if $cygwin; then
  JAVA_LIBRARY_PATH=`cygpath -p "$JAVA_LIBRARY_PATH"`
fi

STORM_OPTS="$STORM_OPTS -Dstorm.log.dir=$STORM_LOG_DIR"
STORM_OPTS="$STORM_OPTS -Dstorm.log.file=$STORM_LOGFILE"
STORM_OPTS="$STORM_OPTS -Dstorm.home=$STORM_HOME"
STORM_OPTS="$STORM_OPTS -Dstorm.conf.dir=$STORM_CONF_DIR"
STORM_OPTS="$STORM_OPTS -Dlog4j.configuration=File:${LOG4J_CONF}"
STORM_OPTS="$STORM_OPTS -Dstorm.root.logger=${STORM_ROOT_LOGGER:-INFO,DRFA}"
if [ "x$JAVA_LIBRARY_PATH" != "x" ]; then
  STORM_OPTS="$STORM_OPTS -Djava.library.path=$JAVA_LIBRARY_PATH"
fi
#STORM_OPTS="$STORM_OPTS -Dstorm.policy.file=$STORM_POLICYFILE"

