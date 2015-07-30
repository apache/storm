---
layout: documentation
title: Structure of codebase
---
<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-12">
            	<h1 class="page-title">Structure of codebase</h1>
            </div>
        </div>
        <div class="row">
            <div class="col-md-12">
            	<p>There are three distinct layers to Storm's codebase.</p>

                <p>First, Storm was designed from the very beginning to be compatible with multiple languages. Nimbus is a Thrift service and topologies are defined as Thrift structures. The usage of Thrift allows Storm to be used from any language.</p>

                <p>Second, all of Storm's interfaces are specified as Java interfaces. So even though there's a lot of Clojure in Storm's implementation, all usage must go through the Java API. This means that every feature of Storm is always available via Java.</p>

                <p>Third, Storm's implementation is largely in Clojure. Line-wise, Storm is about half Java code, half Clojure code. But Clojure is much more expressive, so in reality the great majority of the implementation logic is in Clojure. </p>

                <p>The following sections explain each of these layers in more detail.</p>

                <h3 id="storm.thrift">storm.thrift</h3>

                <p>The first place to look to understand the structure of Storm's codebase is the <a href="https://github.com/apache/storm/blob/master/storm-core/src/storm.thrift">storm.thrift</a> file.</p>

                <p>Storm uses <a href="https://github.com/nathanmarz/thrift/tree/storm">this fork</a> of Thrift (branch 'storm') to produce the generated code. This "fork" is actually Thrift 7 with all the Java packages renamed to be <code>org.apache.thrift7</code>. Otherwise, it's identical to Thrift 7. This fork was done because of the lack of backwards compatibility in Thrift and the need for many people to use other versions of Thrift in their Storm topologies.</p>

                <p>Every spout or bolt in a topology is given a user-specified identifier called the "component id". The component id is used to specify subscriptions from a bolt to the output streams of other spouts or bolts. A <a href="https://github.com/apache/storm/blob/master/storm-core/src/storm.thrift#L91">StormTopology</a> structure contains a map from component id to component for each type of component (spouts and bolts).</p>

                <p>Spouts and bolts have the same Thrift definition, so let's just take a look at the <a href="https://github.com/apache/storm/blob/master/storm-core/src/storm.thrift#L79">Thrift definition for bolts</a>. It contains a <code>ComponentObject</code> struct and a <code>ComponentCommon</code> struct.</p>

                <p>The <code>ComponentObject</code> defines the implementation for the bolt. It can be one of three types:</p>

                <ol>
                <li>A serialized java object (that implements <a href="https://github.com/apache/storm/blob/master/storm-core/src/jvm/backtype/storm/task/IBolt.java">IBolt</a>)</li>
                <li>A <code>ShellComponent</code> object that indicates the implementation is in another language. Specifying a bolt this way will cause Storm to instantiate a <a href="https://github.com/apache/storm/blob/master/storm-core/src/jvm/backtype/storm/task/ShellBolt.java">ShellBolt</a> object to handle the communication between the JVM-based worker process and the non-JVM-based implementation of the component.</li>
                <li>A <code>JavaObject</code> structure which tells Storm the classname and constructor arguments to use to instantiate that bolt. This is useful if you want to define a topology in a non-JVM language. This way, you can make use of JVM-based spouts and bolts without having to create and serialize a Java object yourself.</li>
                </ol>

                <p><code>ComponentCommon</code> defines everything else for this component. This includes:</p>

                <ol>
                <li>What streams this component emits and the metadata for each stream (whether it's a direct stream, the fields declaration)</li>
                <li>What streams this component consumes (specified as a map from component_id:stream_id to the stream grouping to use)</li>
                <li>The parallelism for this component</li>
                <li>The component-specific <a href="https://github.com/apache/storm/wiki/Configuration">configuration</a> for this component</li>
                </ol>

                <p>Note that the structure spouts also have a <code>ComponentCommon</code> field, and so spouts can also have declarations to consume other input streams. Yet the Storm Java API does not provide a way for spouts to consume other streams, and if you put any input declarations there for a spout you would get an error when you tried to submit the topology. The reason that spouts have an input declarations field is not for users to use, but for Storm itself to use. Storm adds implicit streams and bolts to the topology to set up the <a href="https://github.com/apache/storm/wiki/Guaranteeing-message-processing">acking framework</a>, and two of these implicit streams are from the acker bolt to each spout in the topology. The acker sends "ack" or "fail" messages along these streams whenever a tuple tree is detected to be completed or failed. The code that transforms the user's topology into the runtime topology is located <a href="https://github.com/apache/storm/blob/master/storm-core/src/clj/backtype/storm/daemon/common.clj#L279">here</a>.</p>

                <h3 id="java-interfaces">Java interfaces</h3>

                <p>The interfaces for Storm are generally specified as Java interfaces. The main interfaces are:</p>

                <ol>
                <li><a href="/storm.apache.org/javadoc/apidocs/backtype/storm/topology/IRichBolt.html">IRichBolt</a></li>
                <li><a href="/storm.apache.org/javadoc/apidocs/backtype/storm/topology/IRichSpout.html">IRichSpout</a></li>
                <li><a href="/storm.apache.org/javadoc/apidocs/backtype/storm/topology/TopologyBuilder.html">TopologyBuilder</a></li>
                </ol>

                <p>The strategy for the majority of the interfaces is to:</p>

                <ol>
                <li>Specify the interface using a Java interface</li>
                <li>Provide a base class that provides default implementations when appropriate</li>
                </ol>

                <p>You can see this strategy at work with the <a href="/storm.apache.org/javadoc/apidocs/backtype/storm/topology/base/BaseRichSpout.html">BaseRichSpout</a> class.</p>

                <p>Spouts and bolts are serialized into the Thrift definition of the topology as described above. </p>

                <p>One subtle aspect of the interfaces is the difference between <code>IBolt</code> and <code>ISpout</code> vs. <code>IRichBolt</code> and <code>IRichSpout</code>. The main difference between them is the addition of the <code>declareOutputFields</code> method in the "Rich" versions of the interfaces. The reason for the split is that the output fields declaration for each output stream needs to be part of the Thrift struct (so it can be specified from any language), but as a user you want to be able to declare the streams as part of your class. What <code>TopologyBuilder</code> does when constructing the Thrift representation is call <code>declareOutputFields</code> to get the declaration and convert it into the Thrift structure. The conversion happens <a href="https://github.com/apache/storm/blob/master/storm-core/src/jvm/backtype/storm/topology/TopologyBuilder.java#L205">at this portion</a> of the <code>TopologyBuilder</code> code.</p>

                <h3 id="implementation">Implementation</h3>

                <p>Specifying all the functionality via Java interfaces ensures that every feature of Storm is available via Java. Moreso, the focus on Java interfaces ensures that the user experience from Java-land is pleasant as well.</p>

                <p>The implementation of Storm, on the other hand, is primarily in Clojure. While the codebase is about 50% Java and 50% Clojure in terms of LOC, most of the implementation logic is in Clojure. There are two notable exceptions to this, and that is the <a href="https://github.com/apache/storm/wiki/Distributed-RPC">DRPC</a> and <a href="https://github.com/apache/storm/wiki/Transactional-topologies">transactional topologies</a> implementations. These are implemented purely in Java. This was done to serve as an illustration for how to implement a higher level abstraction on Storm. The DRPC and transactional topologies implementations are in the <a href="https://github.com/apache/storm/tree/master/storm-core/src/jvm/backtype/storm/coordination">backtype.storm.coordination</a>, <a href="https://github.com/apache/storm/tree/master/storm-core/src/jvm/backtype/storm/drpc">backtype.storm.drpc</a>, and <a href="https://github.com/apache/storm/tree/master/storm-core/src/jvm/backtype/storm/transactional">backtype.storm.transactional</a> packages.</p>

                <p>Here's a summary of the purpose of the main Java packages and Clojure namespace:</p>

                <h4 id="java-packages">Java packages</h4>

                <p><a href="https://github.com/apache/storm/tree/master/storm-core/src/jvm/backtype/storm/coordination">backtype.storm.coordination</a>: Implements the pieces required to coordinate batch-processing on top of Storm, which both DRPC and transactional topologies use. <code>CoordinatedBolt</code> is the most important class here.</p>

                <p><a href="https://github.com/apache/storm/tree/master/storm-core/src/jvm/backtype/storm/drpc">backtype.storm.drpc</a>: Implementation of the DRPC higher level abstraction</p>

                <p><a href="https://github.com/apache/storm/tree/master/storm-core/src/jvm/backtype/storm/generated">backtype.storm.generated</a>: The generated Thrift code for Storm (generated using <a href="https://github.com/nathanmarz/thrift">this fork</a> of Thrift, which simply renames the packages to org.apache.thrift7 to avoid conflicts with other Thrift versions)</p>

                <p><a href="https://github.com/apache/storm/tree/master/storm-core/src/jvm/backtype/storm/grouping">backtype.storm.grouping</a>: Contains interface for making custom stream groupings</p>

                <p><a href="https://github.com/apache/storm/tree/master/storm-core/src/jvm/backtype/storm/hooks">backtype.storm.hooks</a>: Interfaces for hooking into various events in Storm, such as when tasks emit tuples, when tuples are acked, etc. User guide for hooks is <a href="https://github.com/apache/storm/wiki/Hooks">here</a>.</p>

                <p><a href="https://github.com/apache/storm/tree/master/storm-core/src/jvm/backtype/storm/serialization">backtype.storm.serialization</a>: Implementation of how Storm serializes/deserializes tuples. Built on top of <a href="http://code.google.com/p/kryo/">Kryo</a>.</p>

                <p><a href="https://github.com/apache/storm/tree/master/storm-core/src/jvm/backtype/storm/spout">backtype.storm.spout</a>: Definition of spout and associated interfaces (like the <code>SpoutOutputCollector</code>). Also contains <code>ShellSpout</code> which implements the protocol for defining spouts in non-JVM languages.</p>

                <p><a href="https://github.com/apache/storm/tree/master/storm-core/src/jvm/backtype/storm/task">backtype.storm.task</a>: Definition of bolt and associated interfaces (like <code>OutputCollector</code>). Also contains <code>ShellBolt</code> which implements the protocol for defining bolts in non-JVM languages. Finally, <code>TopologyContext</code> is defined here as well, which is provided to spouts and bolts so they can get data about the topology and its execution at runtime.</p>

                <p><a href="https://github.com/apache/storm/tree/master/storm-core/src/jvm/backtype/storm/testing">backtype.storm.testing</a>: Contains a variety of test bolts and utilities used in Storm's unit tests.</p>

                <p><a href="https://github.com/apache/storm/tree/master/storm-core/src/jvm/backtype/storm/topology">backtype.storm.topology</a>: Java layer over the underlying Thrift structure to provide a clean, pure-Java API to Storm (users don't have to know about Thrift). <code>TopologyBuilder</code> is here as well as the helpful base classes for the different spouts and bolts. The slightly-higher level <code>IBasicBolt</code> interface is here, which is a simpler way to write certain kinds of bolts.</p>

                <p><a href="https://github.com/apache/storm/tree/master/storm-core/src/jvm/backtype/storm/transactional">backtype.storm.transactional</a>: Implementation of transactional topologies.</p>

                <p><a href="https://github.com/apache/storm/tree/master/storm-core/src/jvm/backtype/storm/tuple">backtype.storm.tuple</a>: Implementation of Storm's tuple data model.</p>

                <p><a href="https://github.com/apache/storm/tree/master/storm-core/src/jvm/backtype/storm/tuple">backtype.storm.utils</a>: Data structures and miscellaneous utilities used throughout the codebase.</p>

                <h4 id="clojure-namespaces">Clojure namespaces</h4>

                <p><a href="https://github.com/apache/storm/blob/master/storm-core/src/clj/backtype/storm/bootstrap.clj">backtype.storm.bootstrap</a>: Contains a helpful macro to import all the classes and namespaces that are used throughout the codebase.</p>

                <p><a href="https://github.com/apache/storm/blob/master/storm-core/src/clj/backtype/storm/clojure.clj">backtype.storm.clojure</a>: Implementation of the Clojure DSL for Storm.</p>

                <p><a href="https://github.com/apache/storm/blob/master/storm-core/src/clj/backtype/storm/cluster.clj">backtype.storm.cluster</a>: All Zookeeper logic used in Storm daemons is encapsulated in this file. This code manages how cluster state (like what tasks are running where, what spout/bolt each task runs as) is mapped to the Zookeeper "filesystem" API.</p>

                <p><a href="https://github.com/apache/storm/blob/master/storm-core/src/clj/backtype/storm/command">backtype.storm.command.*</a>: These namespaces implement various commands for the <code>storm</code> command line client. These implementations are very short.</p>

                <p><a href="https://github.com/apache/storm/blob/master/storm-core/src/clj/backtype/storm/config.clj">backtype.storm.config</a>: Implementation of config reading/parsing code for Clojure. Also has utility functions for determining what local path nimbus/supervisor/daemons should be using for various things. e.g. the <code>master-inbox</code> function will return the local path that Nimbus should use when jars are uploaded to it.</p>

                <p><a href="https://github.com/apache/storm/blob/master/storm-core/src/clj/backtype/storm/daemon/acker.clj">backtype.storm.daemon.acker</a>: Implementation of the "acker" bolt, which is a key part of how Storm guarantees data processing.</p>

                <p><a href="https://github.com/apache/storm/blob/master/storm-core/src/clj/backtype/storm/daemon/common.clj">backtype.storm.daemon.common</a>: Implementation of common functions used in Storm daemons, like getting the id for a topology based on the name, mapping a user's topology into the one that actually executes (with implicit acking streams and acker bolt added - see <code>system-topology!</code> function), and definitions for the various heartbeat and other structures persisted by Storm.</p>

                <p><a href="https://github.com/apache/storm/blob/master/storm-core/src/clj/backtype/storm/daemon/drpc.clj">backtype.storm.daemon.drpc</a>: Implementation of the DRPC server for use with DRPC topologies.</p>

                <p><a href="https://github.com/apache/storm/blob/master/storm-core/src/clj/backtype/storm/daemon/nimbus.clj">backtype.storm.daemon.nimbus</a>: Implementation of Nimbus.</p>

                <p><a href="https://github.com/apache/storm/blob/master/storm-core/src/clj/backtype/storm/daemon/supervisor.clj">backtype.storm.daemon.supervisor</a>: Implementation of Supervisor.</p>

                <p><a href="https://github.com/apache/storm/blob/master/storm-core/src/clj/backtype/storm/daemon/task.clj">backtype.storm.daemon.task</a>: Implementation of an individual task for a spout or bolt. Handles message routing, serialization, stats collection for the UI, as well as the spout-specific and bolt-specific execution implementations.</p>

                <p><a href="https://github.com/apache/storm/blob/master/storm-core/src/clj/backtype/storm/daemon/worker.clj">backtype.storm.daemon.worker</a>: Implementation of a worker process (which will contain many tasks within). Implements message transferring and task launching.</p>

                <p><a href="https://github.com/apache/storm/blob/master/storm-core/src/clj/backtype/storm/event.clj">backtype.storm.event</a>: Implements a simple asynchronous function executor. Used in various places in Nimbus and Supervisor to make functions execute in serial to avoid any race conditions.</p>

                <p><a href="https://github.com/apache/storm/blob/master/storm-core/src/clj/backtype/storm/log.clj">backtype.storm.log</a>: Defines the functions used to log messages to log4j.</p>

                <p><a href="https://github.com/apache/storm/blob/master/storm-core/src/clj/backtype/storm/messaging">backtype.storm.messaging.*</a>: Defines a higher level interface to implementing point to point messaging. In local mode Storm uses in-memory Java queues to do this; on a cluster, it uses ZeroMQ. The generic interface is defined in protocol.clj.</p>

                <p><a href="https://github.com/apache/storm/blob/master/storm-core/src/clj/backtype/storm/stats.clj">backtype.storm.stats</a>: Implementation of stats rollup routines used when sending stats to ZK for use by the UI. Does things like windowed and rolling aggregations at multiple granularities.</p>

                <p><a href="https://github.com/apache/storm/blob/master/storm-core/src/clj/backtype/storm/testing.clj">backtype.storm.testing</a>: Implementation of facilities used to test Storm topologies. Includes time simulation, <code>complete-topology</code> for running a fixed set of tuples through a topology and capturing the output, tracker topologies for having fine grained control over detecting when a cluster is "idle", and other utilities.</p>

                <p><a href="https://github.com/apache/storm/blob/master/storm-core/src/clj/backtype/storm/thrift.clj">backtype.storm.thrift</a>: Clojure wrappers around the generated Thrift API to make working with Thrift structures more pleasant.</p>

                <p><a href="https://github.com/apache/storm/blob/master/storm-core/src/clj/backtype/storm/timer.clj">backtype.storm.timer</a>: Implementation of a background timer to execute functions in the future or on a recurring interval. Storm couldn't use the <a href="http://docs.oracle.com/javase/1.4.2/docs/api/java/util/Timer.html">Timer</a> class because it needed integration with time simulation in order to be able to unit test Nimbus and the Supervisor.</p>

                <p><a href="https://github.com/apache/storm/blob/master/storm-core/src/clj/backtype/storm/ui">backtype.storm.ui.*</a>: Implementation of Storm UI. Completely independent from rest of code base and uses the Nimbus Thrift API to get data.</p>

                <p><a href="https://github.com/apache/storm/blob/master/storm-core/src/clj/backtype/storm/util.clj">backtype.storm.util</a>: Contains generic utility functions used throughout the code base.</p>

                <p><a href="https://github.com/apache/storm/blob/master/storm-core/src/clj/backtype/storm/zookeeper.clj">backtype.storm.zookeeper</a>: Clojure wrapper around the Zookeeper API and implements some "high-level" stuff like "mkdirs" and "delete-recursive".</p>  
            </div>
        </div>
    </div>
</div>