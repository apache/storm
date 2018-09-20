---
title: Structure of the Codebase
layout: documentation
documentation: true
---
There are three distinct layers to Storm's codebase.

First, Storm was designed from the very beginning to be compatible with multiple languages. Nimbus is a Thrift service and topologies are defined as Thrift structures. The usage of Thrift allows Storm to be used from any language.

Second, all of Storm's interfaces are specified as Java interfaces. This means that every feature of Storm is always available via Java.

The following sections explain each of these layers in more detail.

### storm.thrift

The first place to look to understand the structure of Storm's codebase is the [storm.thrift]({{page.git-blob-base}}/storm-client/src/storm.thrift) file.

Every spout or bolt in a topology is given a user-specified identifier called the "component id". The component id is used to specify subscriptions from a bolt to the output streams of other spouts or bolts. A [StormTopology]({{page.git-blob-base}}/storm-client/src/storm.thrift) structure contains a map from component id to component for each type of component (spouts and bolts).

Spouts and bolts have the same Thrift definition, so let's just take a look at the [Thrift definition for bolts]({{page.git-blob-base}}/storm-client/src/storm.thrift). It contains a `ComponentObject` struct and a `ComponentCommon` struct.

The `ComponentObject` defines the implementation for the bolt. It can be one of three types:

1. A serialized java object (that implements [IBolt]({{page.git-blob-base}}/storm-client/src/jvm/org/apache/storm/task/IBolt.java))
2. A `ShellComponent` object that indicates the implementation is in another language. Specifying a bolt this way will cause Storm to instantiate a [ShellBolt]({{page.git-blob-base}}/storm-client/src/jvm/org/apache/storm/task/ShellBolt.java) object to handle the communication between the JVM-based worker process and the non-JVM-based implementation of the component.
3. A `JavaObject` structure which tells Storm the classname and constructor arguments to use to instantiate that bolt. This is useful if you want to define a topology in a non-JVM language. This way, you can make use of JVM-based spouts and bolts without having to create and serialize a Java object yourself.

`ComponentCommon` defines everything else for this component. This includes:

1. What streams this component emits and the metadata for each stream (whether it's a direct stream, the fields declaration)
2. What streams this component consumes (specified as a map from component_id:stream_id to the stream grouping to use)
3. The parallelism for this component
4. The component-specific [configuration](Configuration.html) for this component

Note that the structure spouts also have a `ComponentCommon` field, and so spouts can also have declarations to consume other input streams. Yet the Storm Java API does not provide a way for spouts to consume other streams, and if you put any input declarations there for a spout you would get an error when you tried to submit the topology. The reason that spouts have an input declarations field is not for users to use, but for Storm itself to use. Storm adds implicit streams and bolts to the topology to set up the [acking framework](Acking-framework-implementation.html), and two of these implicit streams are from the acker bolt to each spout in the topology. The acker sends "ack" or "fail" messages along these streams whenever a tuple tree is detected to be completed or failed. The code that transforms the user's topology into the runtime topology is located [here]({{page.git-blob-base}}/storm-client/src/jvm/org/apache/storm/daemon/StormCommon.java).

### Java interfaces

The interfaces for Storm are generally specified as Java interfaces. The main interfaces are:

1. [IRichBolt](javadocs/org/apache/storm/topology/IRichBolt.html)
2. [IRichSpout](javadocs/org/apache/storm/topology/IRichSpout.html)
3. [TopologyBuilder](javadocs/org/apache/storm/topology/TopologyBuilder.html)

The strategy for the majority of the interfaces is to:

1. Specify the interface using a Java interface
2. Provide a base class that provides default implementations when appropriate

You can see this strategy at work with the [BaseRichSpout](javadocs/org/apache/storm/topology/base/BaseRichSpout.html) class.

Spouts and bolts are serialized into the Thrift definition of the topology as described above. 

One subtle aspect of the interfaces is the difference between `IBolt` and `ISpout` vs. `IRichBolt` and `IRichSpout`. The main difference between them is the addition of the `declareOutputFields` method in the "Rich" versions of the interfaces. The reason for the split is that the output fields declaration for each output stream needs to be part of the Thrift struct (so it can be specified from any language), but as a user you want to be able to declare the streams as part of your class. What `TopologyBuilder` does when constructing the Thrift representation is call `declareOutputFields` to get the declaration and convert it into the Thrift structure. The conversion happens in the `TopologyBuilder` code.


### Implementation

Specifying all the functionality via Java interfaces ensures that every feature of Storm is available via Java. Moreso, the focus on Java interfaces ensures that the user experience from Java-land is pleasant as well.

Storm was originally implemented in Clojure, but most of the code has since been ported to Java.

Here's a summary of the purpose of the main Java packages:

#### Java packages

[org.apache.storm.coordination]({{page.git-tree-base}}/storm-client/src/jvm/org/apache/storm/coordination): Implements the pieces required to coordinate batch-processing on top of Storm, which DRPC uses. `CoordinatedBolt` is the most important class here.

[org.apache.storm.drpc]({{page.git-tree-base}}/storm-client/src/jvm/org/apache/storm/drpc): Implementation of the DRPC higher level abstraction

[org.apache.storm.generated]({{page.git-tree-base}}/storm-client/src/jvm/org/apache/storm/generated): The generated Thrift code for Storm.

[org.apache.storm.grouping]({{page.git-tree-base}}/storm-client/src/jvm/org/apache/storm/grouping): Contains interface for making custom stream groupings

[org.apache.storm.hooks]({{page.git-tree-base}}/storm-client/src/jvm/org/apache/storm/hooks): Interfaces for hooking into various events in Storm, such as when tasks emit tuples, when tuples are acked, etc. User guide for hooks is [here](Hooks.html).

[org.apache.storm.serialization]({{page.git-tree-base}}/storm-client/src/jvm/org/apache/storm/serialization): Implementation of how Storm serializes/deserializes tuples. Built on top of [Kryo](https://github.com/EsotericSoftware/kryo).

[org.apache.storm.spout]({{page.git-tree-base}}/storm-client/src/jvm/org/apache/storm/spout): Definition of spout and associated interfaces (like the `SpoutOutputCollector`). Also contains `ShellSpout` which implements the protocol for defining spouts in non-JVM languages.

[org.apache.storm.task]({{page.git-tree-base}}/storm-client/src/jvm/org/apache/storm/task): Definition of bolt and associated interfaces (like `OutputCollector`). Also contains `ShellBolt` which implements the protocol for defining bolts in non-JVM languages. Finally, `TopologyContext` is defined here as well, which is provided to spouts and bolts so they can get data about the topology and its execution at runtime.

[org.apache.storm.testing]({{page.git-tree-base}}/storm-client/src/jvm/org/apache/storm/testing): Contains a variety of test bolts and utilities used in Storm's unit tests.

[org.apache.storm.topology]({{page.git-tree-base}}/storm-client/src/jvm/org/apache/storm/topology): Java layer over the underlying Thrift structure to provide a clean, pure-Java API to Storm (users don't have to know about Thrift). `TopologyBuilder` is here as well as the helpful base classes for the different spouts and bolts. The slightly-higher level `IBasicBolt` interface is here, which is a simpler way to write certain kinds of bolts.

[org.apache.storm.tuple]({{page.git-tree-base}}/storm-client/src/jvm/org/apache/storm/tuple): Implementation of Storm's tuple data model.

[org.apache.storm.utils]({{page.git-tree-base}}/storm-client/src/jvm/org/apache/storm/utils): Data structures and miscellaneous utilities used throughout the codebase. This includes utilities for time simulation.

[org.apache.storm.command.*]({{page.git-blob-base}}/storm-core/src/jvm/org/apache/storm/command): These implement various commands for the `storm` command line client. These implementations are very short.

[org.apache.storm.cluster]({{page.git-blob-base}}/storm-client/src/jvm/org/apache/storm/cluster): This code manages how cluster state (like what tasks are running where, what spout/bolt each task runs as) is stored, typically in Zookeeper.

[org.apache.storm.daemon.Acker]({{page.git-blob-base}}/storm-client/src/jvm/org/apache/storm/daemon/Acker.java): Implementation of the "acker" bolt, which is a key part of how Storm guarantees data processing.

[org.apache.storm.daemon.DrpcServer]({{page.git-blob-base}}/storm-webapp/src/jvm/org/apache/storm/daemon/DrpcServer.java): Implementation of the DRPC server for use with DRPC topologies.

[org.apache.storm.event]({{page.git-blob-base}}/storm-server/src/jvm/org/apache/storm/event): Implements a simple asynchronous function executor. Used in various places in Nimbus and Supervisor to make functions execute in serial to avoid any race conditions.

[org.apache.storm.LocalCluster]({{page.git-blob-base}}/storm-server/src/main/java/org/apache/storm/LocalCluster.java): Utility to boot up Storm inside an existing Java process. Often used in conjunction with `Testing.java` to implement integration tests.

[org.apache.storm.messaging.*]({{page.git-blob-base}}/storm-client/src/jvm/org/apache/storm/messaging): Defines a higher level interface to implementing point to point messaging. In local mode Storm uses in-memory Java queues to do this; on a cluster, it uses Netty, but it is pluggable.

[org.apache.storm.stats]({{page.git-blob-base}}/storm-client/src/jvm/org/apache/storm/stats): Implementation of stats rollup routines used when sending stats to ZK for use by the UI. Does things like windowed and rolling aggregations at multiple granularities.

[org.apache.storm.Thrift]({{page.git-blob-base}}/storm-client/src/jvm/org/apache/storm/Thrift.java): Wrappers around the generated Thrift API to make working with Thrift structures more pleasant.

[org.apache.storm.StormTimer]({{page.git-blob-base}}/storm-client/src/jvm/org/apache/storm/StormTimer.java): Implementation of a background timer to execute functions in the future or on a recurring interval. Storm couldn't use the [Timer](http://docs.oracle.com/javase/1.4.2/docs/api/java/util/Timer.html) class because it needed integration with time simulation in order to be able to unit test Nimbus and the Supervisor.

[org.apache.storm.daemon.nimbus]({{page.git-blob-base}}/storm-server/src/jvm/org/apache/storm/daemon/nimbus/Nimbus.java): Implementation of Nimbus.

[org.apache.storm.daemon.supervisor]({{page.git-blob-base}}/storm-server/src/jvm/org/apache/storm/daemon/supervisor/Supervisor.java): Implementation of Supervisor.

[org.apache.storm.daemon.task]({{page.git-blob-base}}/storm-client/src/jvm/org/apache/storm/daemon/Task.java): Implementation of an individual task for a spout or bolt. Handles message routing, serialization, stats collection for the UI, as well as the spout-specific and bolt-specific execution implementations.

[org.apache.storm.daemon.worker]({{page.git-blob-base}}/storm-client/src/jvm/org/apache/storm/daemon/worker/Worker.java): Implementation of a worker process (which will contain many tasks within). Implements message transferring and task launching.

[org.apache.storm.Testing]({{page.git-blob-base}}/storm-server/src/main/java/org/apache/storm/Testing.java): Various utilities for working with local clusters during tests, e.g. `completeTopology` for running a fixed set of tuples through a topology for capturing the output, tracker topologies for having fine grained control over detecting when a cluster is "idle", and other utilities.

#### Clojure namespaces

[org.apache.storm.clojure]({{page.git-blob-base}}/storm-clojure/src/clj/org/apache/storm/clojure.clj): Implementation of the Clojure DSL for Storm.

[org.apache.storm.config]({{page.git-blob-base}}/storm-clojure/src/clj/org/apache/storm/config.clj): Created clojure symbols for config names in [Config.java](javadocs/org/apache/storm/Config.html)
 
[org.apache.storm.log]({{page.git-blob-base}}/storm-clojure/src/clj/org/apache/storm/log.clj): Defines the functions used to log messages to log4j.

[org.apache.storm.ui.*]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/ui): Implementation of Storm UI. Completely independent from rest of code base and uses the Nimbus Thrift API to get data.
