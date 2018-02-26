# Topologies for measuring Storm performance

This module includes topologies designed for measuring Storm performance.

## Overview
There are two basic modes for running these topologies

- **Cluster mode:** Submits the topology to a storm cluster. This mode is useful for benchmarking. It calculates throughput and latency numbers every minute and prints them on the console.
- **In-process mode:** Uses LocalCluster to run topology. This mode helps identify bottlenecks using profilers like JProfiler from within a IDE. This mode does not print metrics.

In both the modes, a shutdown hook is setup to terminate the topology when the program that is submitting the topology is terminated.

The bundled topologies can be classified into two types.

- Topologies that measure purely the internal functioning of Storm. Such topologies do not interact with external systems like Kafka or Hdfs.
- Topologies that measure speed of I/O with external systems like Kafka and Hdfs.

Topologies that measure internal performance can be run in either in-proc or cluster modes.
Topologies that measure I/O with external systems are designed to run in cluster mode only.

## Topologies List

1. **ConstSpoutOnlyTopo:** Helps measure how fast spout can emit. This topology has a spout and is not connected to any bolts. Supports cluster mode only.
2. **ConstSpoutNullBoltTopo:** Helps measure how fast spout can send data to a bolt. Spout emits a stream of constant values to a DevNull bolt which discards the incoming tuples. Supports cluster mode only.
3. **ConstSpoutIdBoltNullBoltTopo:** Helps measure speed of messaging between spouts and bolts. Spout emits a stream of constant values to an ID bolt which clones the tuple and emits it downstream to a DevNull bolt. Supports cluster mode only.
4. **FileReadWordCountTopo:** Measures speed of word counting. The spout loads a file into memory and emits these lines in an infinite loop. Supports cluster mode only.
5. **HdfsSpoutNullBoltTopo:** Measures speed at which HdfsSpout can read from HDFS. Supports cluster mode only.
6. **StrGenSpoutHdfsBoltTopo:** Measures speed at which HdfsBolt can write to HDFS. Supports cluster mode only.
7. **KafkaClientHdfsTopo:** Measures how fast Storm can read from Kafka and write to HDFS, using the storm-kafka-client spout. Supports cluster mode only
8. **KafkaClientSpoutNullBoltTopo:** Measures the speed at which the storm-kafka-client KafkaSpout can read from Kafka. Supports cluster mode only.


## How to run ?

### In-process mode:
This mode is intended for running the topology quickly and easily from within the IDE and does not expect any command line arguments.
Simply running the Topology's main() method without any arguments will get it running. The topology runs indefinitely till the program is terminated.


### Cluster mode:
When the topology is run with one or more than one cmd line arguments, the topology is submitted to the cluster.
The first argument indicates how long the topology should be run. Often the second argument refers to a yaml config
file which contains topology configuration settings. The conf/ directory in this module contains sample config files
with names matching the corresponding topology.

These topologies can be run using the standard storm jar command.

```
bin/storm jar  /path/storm-perf-1.1.0-jar-with-dependencies.jar org.apache.storm.perf.ConstSpoutNullBoltTopo  200  conf/ConstSpoutIdBoltNullBoltTopo.yaml
```