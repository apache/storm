export JAVA_HOME=$JAVA_HOME
export NIMBUS_CHILDOPTS=" -Xms1g -Xmx1g -XX:+UseConcMarkSweepGC -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=70 "
export SUPERVISOR_CHILDOPTS=" -Xms256m -Xmx256m -XX:+UseConcMarkSweepGC -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=70 "
export DRPC_CHILDOPTS="-Xmx768m"
