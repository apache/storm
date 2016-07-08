---
title: Command Line Client
layout: documentation
documentation: true
---
This page describes all the commands that are possible with the "storm" command line client. To learn how to set up your "storm" client to talk to a remote cluster, follow the instructions in [Setting up development environment](Setting-up-development-environment.html).

These commands are:

1. jar
1. kill
1. activate
1. deactivate
1. rebalance
1. repl
1. classpath
1. localconfvalue
1. remoteconfvalue
1. nimbus
1. supervisor
1. ui
1. drpc
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
1. sql
1. upload-credentials
1. version
1. help

### jar

Syntax: `storm jar topology-jar-path class ...`

Runs the main method of `class` with the specified arguments. The storm jars and configs in `~/.storm` are put on the classpath. The process is configured so that [StormSubmitter](javadocs/org/apache/storm/StormSubmitter.html) will upload the jar at `topology-jar-path` when the topology is submitted.

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

Syntax: `storm repl`

Opens up a Clojure REPL with the storm jars and configuration on the classpath. Useful for debugging.

### classpath

Syntax: `storm classpath`

Prints the classpath used by the storm client when running commands.

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

  Set the com.myapp logger's level to WARN indifinitely, and com.myOtherLogger to ERROR for 123 seconds

  ./bin/storm set_log_level -r com.myOtherLogger topology-name

  Clears settings, resetting back to the original level

### shell

Syntax: `storm shell resourcesdir command args`

Makes constructing jar and uploading to nimbus for using non JVM languages

eg: `storm shell resources/ python topology.py arg1 arg2`

### sql

Syntax: `storm sql sql-file topology-name`

Compiles the SQL statements into a Trident topology and submits it to Storm.

### upload-credentials

Syntax: `storm upload_credentials topology-name [credkey credvalue]*`

Uploads a new set of credentials to a running topology

### version

Syntax: `storm version`

Prints the version number of this Storm release.

### help
Syntax: `storm help [command]`

Print one help message or list of available commands
