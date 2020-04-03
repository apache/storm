---
title: Command Line Client
layout: documentation
documentation: true
---
This page describes all the commands that are possible with the "storm" command line client. To learn how to set up your "storm" client to talk to a remote cluster, follow the instructions in [Setting up development environment](Setting-up-development-environment.html). See [Classpath handling](Classpath-handling.html) for details on using external libraries in these commands.

These commands are:

1. jar
1. local
1. sql
1. kill
1. activate
1. deactivate
1. rebalance
1. repl
1. classpath
1. server_classpath
1. localconfvalue
1. remoteconfvalue
1. nimbus
1. supervisor
1. ui
1. drpc
1. drpc-client
1. blobstore
1. dev-zookeeper
1. get-errors
1. heartbeats
1. kill_workers
1. list
1. logviewer
1. monitor
1. node-health-check
1. pacemaker
1. set_log_level
1. shell
1. upload-credentials
1. version
1. admin
1. help

### jar

Syntax: `storm jar topology-jar-path class ...`

Runs the main method of `class` with the specified arguments. The storm jars and configs in `~/.storm` are put on the classpath. The process is configured so that [StormSubmitter](javadocs/org/apache/storm/StormSubmitter.html) will upload the jar at `topology-jar-path` when the topology is submitted.

When you want to ship other jars which is not included to application jar, you can pass them to `--jars` option with comma-separated string.
For example, --jars "your-local-jar.jar,your-local-jar2.jar" will load your-local-jar.jar and your-local-jar2.jar.
And when you want to ship maven artifacts and its transitive dependencies, you can pass them to `--artifacts` with comma-separated string. You can also exclude some dependencies like what you're doing in maven pom. Please add exclusion artifacts with '^' separated string after the artifact. For example, `--artifacts "redis.clients:jedis:2.9.0,org.apache.kafka:kafka_2.10:0.8.2.2^org.slf4j:slf4j-log4j12"` will load jedis and kafka artifact and all of transitive dependencies but exclude slf4j-log4j12 from kafka.

When you need to pull the artifacts from other than Maven Central, you can pass remote repositories to --artifactRepositories option with comma-separated string. Repository format is "<name>^<url>". '^' is taken as separator because URL allows various characters. For example, --artifactRepositories "jboss-repository^http://repository.jboss.com/maven2,HDPRepo^http://repo.hortonworks.com/content/groups/public/" will add JBoss and HDP repositories for dependency resolver.

Complete example of both options is here: `./bin/storm jar example/storm-starter/storm-starter-topologies-*.jar org.apache.storm.starter.RollingTopWords blobstore-remote2 remote --jars "./external/storm-redis/storm-redis-1.1.0.jar,./external/storm-kafka-client/storm-kafka-client-1.1.0.jar" --artifacts "redis.clients:jedis:2.9.0,org.apache.kafka:kafka-clients:1.0.0^org.slf4j:slf4j-api" --artifactRepositories "jboss-repository^http://repository.jboss.com/maven2,HDPRepo^http://repo.hortonworks.com/content/groups/public/"`

When you pass jars and/or artifacts options, StormSubmitter will upload them when the topology is submitted, and they will be included to classpath of both the process which runs the class, and also workers for that topology.

### local

Syntax: `storm local topology-jar-path class ...`

The local command acts just like `storm jar` except instead of submitting a topology to a cluster it will run the cluster in local mode.  This means an embedded version of the storm daemons will be run within the same process as your topology for 30 seconds before it shuts down automatically.  As such the classpath of your topology will be extended to include everything needed to run those daemons.

### sql

Syntax: `storm sql sql-file topology-name`

Compiles the SQL statements into a Trident topology and submits it to Storm.

`--jars` and `--artifacts`, and `--artifactRepositories` options available for jar are also applied to sql command. Please refer "help jar" to see how to use --jars and --artifacts, and --artifactRepositories options. You normally want to pass these options since you need to set data source to your sql which is an external storage in many cases.

### kill

Syntax: `storm kill topology-name [-w wait-time-secs]`

Kills the topology with the name `topology-name`. Storm will first deactivate the topology's spouts for the duration of the topology's message timeout to allow all messages currently being processed to finish processing. Storm will then shutdown the workers and clean up their state. You can override the length of time Storm waits between deactivation and shutdown with the -w flag.

### activate

Syntax: `storm activate topology-name`

Activates the specified topology's spouts.

### deactivate

Syntax: `storm deactivate topology-name`

Deactivates the specified topology's spouts.

### rebalance

Syntax: `storm rebalance topology-name [-w wait-time-secs] [-n new-num-workers] [-e component=parallelism]*`

Sometimes you may wish to spread out where the workers for a topology are running. For example, let's say you have a 10 node cluster running 4 workers per node, and then let's say you add another 10 nodes to the cluster. You may wish to have Storm spread out the workers for the running topology so that each node runs 2 workers. One way to do this is to kill the topology and resubmit it, but Storm provides a "rebalance" command that provides an easier way to do this. 

Rebalance will first deactivate the topology for the duration of the message timeout (overridable with the -w flag) and then redistribute the workers evenly around the cluster. The topology will then return to its previous state of activation (so a deactivated topology will still be deactivated and an activated topology will go back to being activated).

The rebalance command can also be used to change the parallelism of a running topology. Use the -n and -e switches to change the number of workers or number of executors of a component respectively.

### repl

*DEPRECATED: This subcommand may be removed in a future release.*

Syntax: `storm repl`

Opens up a Clojure REPL with the storm jars and configuration on the classpath. Useful for debugging.

### classpath

Syntax: `storm classpath`

Prints the classpath used by the storm client when running commands.

### server_classpath

Syntax: `storm server_classpath`

Prints the classpath used by the storm daemons.

### localconfvalue

Syntax: `storm localconfvalue conf-name`

Prints out the value for `conf-name` in the local Storm configs. The local Storm configs are the ones in `~/.storm/storm.yaml` merged in with the configs in `defaults.yaml`.

### remoteconfvalue

Syntax: `storm remoteconfvalue conf-name`

Prints out the value for `conf-name` in the cluster's Storm configs. The cluster's Storm configs are the ones in `$STORM-PATH/conf/storm.yaml` merged in with the configs in `defaults.yaml`. This command must be run on a cluster machine.

### nimbus

Syntax: `storm nimbus`

Launches the nimbus daemon. This command should be run under supervision with a tool like [daemontools](http://cr.yp.to/daemontools.html) or [monit](http://mmonit.com/monit/). See [Setting up a Storm cluster](Setting-up-a-Storm-cluster.html) for more information.

### supervisor

Syntax: `storm supervisor`

Launches the supervisor daemon. This command should be run under supervision with a tool like [daemontools](http://cr.yp.to/daemontools.html) or [monit](http://mmonit.com/monit/). See [Setting up a Storm cluster](Setting-up-a-Storm-cluster.html) for more information.

### ui

Syntax: `storm ui`

Launches the UI daemon. The UI provides a web interface for a Storm cluster and shows detailed stats about running topologies. This command should be run under supervision with a tool like [daemontools](http://cr.yp.to/daemontools.html) or [monit](http://mmonit.com/monit/). See [Setting up a Storm cluster](Setting-up-a-Storm-cluster.html) for more information.

### drpc

Syntax: `storm drpc`

Launches a DRPC daemon. This command should be run under supervision with a tool like [daemontools](http://cr.yp.to/daemontools.html) or [monit](http://mmonit.com/monit/). See [Distributed RPC](Distributed-RPC.html) for more information.

### drpc-client

Syntax: `storm drpc-client [options] ([function argument]*)|(argument*)`

Provides a very simple way to send DRPC requests. If a `-f` argument is supplied, to set the function name, all of the arguments are treated
as arguments to the function.  If no function is given the arguments must be pairs of function argument.

*NOTE:* This is not really intended for production use.  This is mostly because parsing out the results can be a pain.

Creating an actual DRPC client only takes a few lines, so for production please go with that.

```java
Config conf = new Config();
try (DRPCClient drpc = DRPCClient.getConfiguredClient(conf)) {
  //User the drpc client
  String result = drpc.execute(function, argument);
}
```

#### Examples

`storm drpc-client exclaim a exclaim b test bar`

This will submit 3 separate DRPC request.
1. function = "exclaim" args = "a"
2. function = "exclaim" args = "b"
3. function = "test" args = "bar"

`storm drpc-client -f exclaim a b`

This will submit 2 separate DRPC request.
1. function = "exclaim" args = "a"
2. function = "exclaim" args = "b"

### blobstore

Syntax: `storm blobstore cmd`

list [KEY...] - lists blobs currently in the blob store

cat [-f FILE] KEY - read a blob and then either write it to a file, or STDOUT (requires read access).

create [-f FILE] [-a ACL ...] [--replication-factor NUMBER] KEY - create a new blob. Contents comes from a FILE or STDIN. ACL is in the form [uo]:[username]:[r-][w-][a-] can be comma separated list.

update [-f FILE] KEY - update the contents of a blob.  Contents comes from a FILE or STDIN (requires write access).

delete KEY - delete an entry from the blob store (requires write access).

set-acl [-s ACL] KEY - ACL is in the form [uo]:[username]:[r-][w-][a-] can be comma separated list (requires admin access).

replication --read KEY - Used to read the replication factor of the blob.

replication --update --replication-factor NUMBER KEY where NUMBER > 0. It is used to update the replication factor of a blob.

For example, the following would create a mytopo:data.tgz key using the data stored in data.tgz.  User alice would have full access, bob would have read/write access and everyone else would have read access.

storm blobstore create mytopo:data.tgz -f data.tgz -a u:alice:rwa,u:bob:rw,o::r

See [Blobstore(Distcahce)](distcache-blobstore.html) for more information.

### dev-zookeeper

Syntax: `storm dev-zookeeper`

Launches a fresh Zookeeper server using "dev.zookeeper.path" as its local dir and "storm.zookeeper.port" as its port. This is only intended for development/testing, the Zookeeper instance launched is not configured to be used in production.

### get-errors

Syntax: `storm get-errors topology-name`

Get the latest error from the running topology. The returned result contains the key value pairs for component-name and component-error for the components in error. The result is returned in json format.

### heartbeats

Syntax: `storm heartbeats [cmd]`

list PATH - lists heartbeats nodes under PATH currently in the ClusterState.
get  PATH - Get the heartbeat data at PATH

### kill_workers

Syntax: `storm kill_workers`

Kill the workers running on this supervisor. This command should be run on a supervisor node. If the cluster is running in secure mode, then user needs to have admin rights on the node to be able to successfully kill all workers.

### list

Syntax: `storm list`

List the running topologies and their statuses.

### logviewer

Syntax: `storm logviewer`

Launches the log viewer daemon. It provides a web interface for viewing storm log files. This command should be run under supervision with a tool like daemontools or monit.

See Setting up a Storm cluster for more information.(http://storm.apache.org/documentation/Setting-up-a-Storm-cluster)

### monitor

Syntax: `storm monitor topology-name [-i interval-secs] [-m component-id] [-s stream-id] [-w [emitted | transferred]]`

Monitor given topology's throughput interactively.
One can specify poll-interval, component-id, stream-id, watch-item[emitted | transferred]
  By default,
    poll-interval is 4 seconds;
    all component-ids will be list;
    stream-id is 'default';
    watch-item is 'emitted';

### node-health-check

Syntax: `storm node-health-check`

Run health checks on the local supervisor.

### pacemaker

Syntax: `storm pacemaker`

Launches the Pacemaker daemon. This command should be run under
supervision with a tool like daemontools or monit.

See Setting up a Storm cluster for more information.(http://storm.apache.org/documentation/Setting-up-a-Storm-cluster)

### set_log_level

Syntax: `storm set_log_level -l [logger name]=[log level][:optional timeout] -r [logger name] topology-name`

Dynamically change topology log levels
    
where log level is one of: ALL, TRACE, DEBUG, INFO, WARN, ERROR, FATAL, OFF
and timeout is integer seconds.

e.g.
  ./bin/storm set_log_level -l ROOT=DEBUG:30 topology-name

  Set the root logger's level to DEBUG for 30 seconds

  ./bin/storm set_log_level -l com.myapp=WARN topology-name

  Set the com.myapp logger's level to WARN for 30 seconds

  ./bin/storm set_log_level -l com.myapp=WARN -l com.myOtherLogger=ERROR:123 topology-name

  Set the com.myapp logger's level to WARN indefinitely, and com.myOtherLogger to ERROR for 123 seconds

  ./bin/storm set_log_level -r com.myOtherLogger topology-name

  Clears settings, resetting back to the original level

### shell

Syntax: `storm shell resourcesdir command args`

Makes constructing jar and uploading to nimbus for using non JVM languages

eg: `storm shell resources/ python topology.py arg1 arg2`

### upload-credentials

Syntax: `storm upload_credentials topology-name [credkey credvalue]*`

Uploads a new set of credentials to a running topology  
   * `-e --exception-when-empty`: optional flag. If set, command will fail and throw exception if no credentials were uploaded.
   

### version

Syntax: `storm version`

Prints the version number of this Storm release.

### admin

Syntax: `storm admin <cmd> [options]`

The storm admin command provides access to several operations that can help an administrator debug or fix a cluster.

`remove_corrupt_topologies` - This command should be run on a nimbus node as the same user nimbus runs as.  It will go directly to zookeeper + blobstore and find topologies that appear to be corrupted because of missing blobs. It will kill those topologies.

 `zk_cli [options]` - This command will launch a zookeeper cli pointing to the storm zookeeper instance logged in as the nimbus user.  It should be run on a nimbus server as the user nimbus runs as.
 
   * `-s --server <connection string>`: Set the connection string to use,
            defaults to storm connection string.
   * `-t --time-out <timeout>`:  Set the timeout to use, defaults to storm
            zookeeper timeout.
   * `-w --write`: Allow for writes, defaults to read only, we don't want to
            cause problems.
   * `-n --no-root`: Don't include the storm root on the default connection string.
   * `-j --jaas <jaas_file>`: Include a jaas file that should be used when
            authenticating with ZK defaults to the
            java.security.auth.login.config conf.

`creds <topology_id>` - Print the credential keys for a topology.

### help
Syntax: `storm help [command]`

Print one help message or list of available commands
