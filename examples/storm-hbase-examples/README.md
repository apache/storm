# Storm HBase Integration Example 

This is a very simple set of topologies that show how to use the storm-hbase package for accessing HBase from storm.

## HBase Setup

First you need an instance of HBase that is setup and running.  If you have one already you can start setting up the table using hbase shell, if not download a copy from http://archive.apache.org/dist/hbase/1.4.4/ and untar the result into a directory you want to run it in. Then follow the instructions from https://hbase.apache.org/0.94/book/quickstart.html to setup a standalone HBase instance.  Be aware that when you run `start-hbase.sh` an instance of zookeeper will also be started.  If you are testing using a single node storm cluster you can skip running zookeeper yourself as the hbase zookeeper instance will work.

Before we can run the topology we need to setup a table in HBase to store the results. 

First launch the hbase shell

```
hbase shell
```

Next create a table called WordCount with a single column family called cf.

```
create 'WordCount', 'cf'
```

## PersistentWordCount

PersistentWordCount is a very simple topology that performs a word count and stores the results in HBase.

### Run The Topology

```
storm jar storm-hbase-examples-${STORM_VERSION}.jar org.apache.storm.hbase.topology.PersistentWordCount ${HBASE_HOME}
```

In this `${STORM_VERSION}` should be the version of storm you are running, and `${HBASE_HOME}` is where your installed version of HBase is.  `${HBASE_HOME}` is mostly to get the config started.  Please refer to the documentation for storm-hbase for more information on how to configure your topology.

### Verify The Results

If you want to see the results of the topology live you can run the command

```
storm jar storm-hbase-examples-${STORM_VERSION}.jar org.apache.storm.hbase.topology.WordCountClient ${HBASE_HOME}
```

## WordCountTrident

WordCountTrident does essentially the same as PersistentWordCount but using Trident instead.

### Run The Trident Topology

```
storm jar storm-hbase-examples-${STORM_VERSION}.jar org.apache.storm.hbase.trident.WordCountTrident ${HBASE_HOME}
```

### Verify The Trident Results

Verifying the results for the trident topology is a little more difficult.  The data is stored in the same format as PersistentWordCount, but the keys are different.  To verify that it is working you can look at the logs and you will occasionally see log messages like

```
o.a.s.h.t.PrintFunction Thread-16-b-1-executor[7, 7] [INFO] [storm, 10]
```

Or you can use the `hbase shell` to `scan 'WordCount'` and see that the values are being updated consistently.

