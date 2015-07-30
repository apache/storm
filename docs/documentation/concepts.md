---
layout: documentation
title: Concepts
---
<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-12">
            	<h1 class="page-title">Concepts</h1>
                <p>This page lists the main concepts of Storm and links to resources where you can find more information. The concepts discussed are:</p>
                <ol>
                    <li>Topologies</li>
                    <li>Streams</li>
                    <li>Spouts</li>
                    <li>Bolts</li>
                    <li>Stream groupings</li>
                    <li>Reliability</li>
                    <li>Tasks</li>
                    <li>Workers</li>
                </ol>
                <h3>Topologies</h3>
                <p>The logic for a realtime application is packaged into a Storm topology. A Storm topology is analogous to a MapReduce job. One key difference is that a MapReduce job eventually finishes, whereas a topology runs forever (or until you kill it, of course). A topology is a graph of spouts and bolts that are connected with stream groupings. These concepts are described below.</p>
                <p><strong>Resources:</strong></p>
                <ul>
                    <li><a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/topology/TopologyBuilder.html">TopologyBuilder</a>: use this class to construct topologies in Java</li>
                    <li><a href="Running-topologies-on-a-production-cluster.html">Running topologies on a production cluster</a></li>
                    <li><a href="Local-mode.html">Local mode</a>: Read this to learn how to develop and test topologies in local mode.</li>
                </ul>
                <h3>Streams</h3>
                <p>The stream is the core abstraction in Storm. A stream is an unbounded sequence of tuples that is processed and created in parallel in a distributed fashion. Streams are defined with a schema that names the fields in the stream's tuples. By default, tuples can contain integers, longs, shorts, bytes, strings, doubles, floats, booleans, and byte arrays. You can also define your own serializers so that custom types can be used natively within tuples.</p>
                <p>Every stream is given an id when declared. Since single-stream spouts and bolts are so common, <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/topology/OutputFieldsDeclarer.html">OutputFieldsDeclarer</a> has convenience methods for declaring a single stream without specifying an id. In this case, the stream is given the default id of "default".</p>
                <p><strong>Resources:</strong></p>
                <ul>
                    <li><a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/tuple/Tuple.html">Tuple</a>: streams are composed of tuples</li>
                    <li><a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/topology/OutputFieldsDeclarer.html">OutputFieldsDeclarer</a>: used to declare streams and their schemas</li>
                    <li><a href="Serialization.html">Serialization</a>: Information about Storm's dynamic typing of tuples and declaring custom serializations</li>
                    <li><a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/serialization/ISerialization.html">ISerialization</a>: custom serializers must implement this interface</li>
                    <li><a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/Config.html#TOPOLOGY_SERIALIZATIONS">CONFIG.TOPOLOGY_SERIALIZATIONS</a>: custom serializers can be registered using this configuration</li>
                </ul>
                <h3>Spouts</h3>
                <p>A spout is a source of streams in a topology. Generally spouts will read tuples from an external source and emit them into the topology (e.g. a Kestrel queue or the Twitter API). Spouts can either be <strong>reliable</strong> or <strong>unreliable</strong>. A reliable spout is capable of replaying a tuple if it failed to be processed by Storm, whereas an unreliable spout forgets about the tuple as soon as it is emitted.</p>
                <p>Spouts can emit more than one stream. To do so, declare multiple streams using the <code>declareStream</code> method of <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/topology/OutputFieldsDeclarer.html">OutputFieldsDeclarer</a> and specify the stream to emit to when using the <code>emit</code> method on <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/spout/SpoutOutputCollector.html">SpoutOutputCollector</a>.</p>
                <p>The main method on spouts is <code>nextTuple</code>. <code>nextTuple</code> either emits a new tuple into the topology or simply returns if there are no new tuples to emit. It is imperative that <code>nextTuple</code> does not block for any spout implementation, because Storm calls all the spout methods on the same thread.</p>
                <p>The other main methods on spouts are <code>ack</code> and <code>fail</code>. These are called when Storm detects that a tuple emitted from the spout either successfully completed through the topology or failed to be completed. <code>ack</code> and <code>fail</code> are only called for reliable spouts. See <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/spout/ISpout.html">the Javadoc</a> for more information.</p>
                <p><strong>Resources:</strong></p>
                <ul>
                    <li><a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/topology/IRichSpout.html">IRichSpout</a>: this is the interface that spouts must implement.</li>
                    <li><a href="Guaranteeing-message-processing.html">Guaranteeing message processing</a></li>
                </ul>
                <h3>Bolts</h3>
                <p>All processing in topologies is done in bolts. Bolts can do anything from filtering, functions, aggregations, joins, talking to databases, and more. </p>
                <p>Bolts can do simple stream transformations. Doing complex stream transformations often requires multiple steps and thus multiple bolts. For example, transforming a stream of tweets into a stream of trending images requires at least two steps: a bolt to do a rolling count of retweets for each image, and one or more bolts to stream out the top X images (you can do this particular stream transformation in a more scalable way with three bolts than with two). </p>
                <p>Bolts can emit more than one stream. To do so, declare multiple streams using the <code>declareStream</code> method of <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/topology/OutputFieldsDeclarer.html">OutputFieldsDeclarer</a> and specify the stream to emit to when using the <code>emit</code> method on <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/task/OutputCollector.html">OutputCollector</a>.</p>
                <p>When you declare a bolt's input streams, you always subscribe to specific streams of another component. If you want to subscribe to all the streams of another component, you have to subscribe to each one individually. <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/topology/InputDeclarer.html">InputDeclarer</a> has syntactic sugar for subscribing to streams declared on the default stream id. Saying <code>declarer.shuffleGrouping("1")</code> subscribes to the default stream on component "1" and is equivalent to <code>declarer.shuffleGrouping("1", DEFAULT_STREAM_ID)</code>.</p>
                <p>The main method in bolts is the <code>execute</code> method which takes in as input a new tuple. Bolts emit new tuples using the <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/task/OutputCollector.html">OutputCollector</a> object. Bolts must call the <code>ack</code> method on the <code>OutputCollector</code> for every tuple they process so that Storm knows when tuples are completed (and can eventually determine that its safe to ack the original spout tuples). For the common case of processing an input tuple, emitting 0 or more tuples based on that tuple, and then acking the input tuple, Storm provides an <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/topology/IBasicBolt.html">IBasicBolt</a> interface which does the acking automatically.</p>
                <p>Its perfectly fine to launch new threads in bolts that do processing asynchronously. <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/task/OutputCollector.html">OutputCollector</a> is thread-safe and can be called at any time.</p>
                <p><strong>Resources:</strong></p>
                <ul>
                    <li><a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/topology/IRichBolt.html">IRichBolt</a>: this is general interface for bolts.</li>
                    <li><a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/topology/IBasicBolt.html">IBasicBolt</a>: this is a convenience interface for defining bolts that do filtering or simple functions.</li>
                    <li><a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/task/OutputCollector.html">OutputCollector</a>: bolts emit tuples to their output streams using an instance of this class</li>
                    <li><a href="Guaranteeing-message-processing.html">Guaranteeing message processing</a></li>
                </ul>
                <h3>Stream groupings</h3>
                <p>Part of defining a topology is specifying for each bolt which streams it should receive as input. A stream grouping defines how that stream should be partitioned among the bolt's tasks.</p>
                <p>There are seven built-in stream groupings in Storm, and you can implement a custom stream grouping by implementing the <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/grouping/CustomStreamGrouping.html">CustomStreamGrouping</a> interface:</p>
                <ol>
                    <li><strong>Shuffle grouping</strong>: Tuples are randomly distributed across the bolt's tasks in a way such that each bolt is guaranteed to get an equal number of tuples.</li>
                    <li><strong>Fields grouping</strong>: The stream is partitioned by the fields specified in the grouping. For example, if the stream is grouped by the "user-id" field, tuples with the same "user-id" will always go to the same task, but tuples with different "user-id"'s may go to different tasks.</li>
                    <li><strong>Partial Key grouping</strong>: The stream is partitioned by the fields specified in the grouping, like the Fields grouping, but are load balanced between two downstream bolts, which provides better utilization of resources when the incoming data is skewed. <a href="https://melmeric.files.wordpress.com/2014/11/the-power-of-both-choices-practical-load-balancing-for-distributed-stream-processing-engines.pdf">This paper</a> provides a good explanation of how it works and the advantages it provides.</li>
                    <li><strong>All grouping</strong>: The stream is replicated across all the bolt's tasks. Use this grouping with care.</li>
                    <li><strong>Global grouping</strong>: The entire stream goes to a single one of the bolt's tasks. Specifically, it goes to the task with the lowest id.</li>
                    <li><strong>None grouping</strong>: This grouping specifies that you don't care how the stream is grouped. Currently, none groupings are equivalent to shuffle groupings. Eventually though, Storm will push down bolts with none groupings to execute in the same thread as the bolt or spout they subscribe from (when possible).</li>
                    <li><strong>Direct grouping</strong>: This is a special kind of grouping. A stream grouped this way means that the <strong>producer</strong> of the tuple decides which task of the consumer will receive this tuple. Direct groupings can only be declared on streams that have been declared as direct streams. Tuples emitted to a direct stream must be emitted using one of the [emitDirect](/javadoc/apidocs/backtype/storm/task/OutputCollector.html#emitDirect(int, int, java.util.List) methods. A bolt can get the task ids of its consumers by either using the provided <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/task/TopologyContext.html">TopologyContext</a> or by keeping track of the output of the <code>emit</code> method in <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/task/OutputCollector.html">OutputCollector</a> (which returns the task ids that the tuple was sent to).</li>
                    <li><strong>Local or shuffle grouping</strong>: If the target bolt has one or more tasks in the same worker process, tuples will be shuffled to just those in-process tasks. Otherwise, this acts like a normal shuffle grouping.</li>
                </ol>
                <p><strong>Resources:</strong></p>
                <ul>
                    <li><a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/topology/TopologyBuilder.html">TopologyBuilder</a>: use this class to define topologies</li>
                    <li><a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/topology/InputDeclarer.html">InputDeclarer</a>: this object is returned whenever <code>setBolt</code> is called on <code>TopologyBuilder</code> and is used for declaring a bolt's input streams and how those streams should be grouped</li>
                    <li><a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/task/CoordinatedBolt.html">CoordinatedBolt</a>: this bolt is useful for distributed RPC topologies and makes heavy use of direct streams and direct groupings</li>
                </ul>
                <h3>Reliability</h3>
                <p>Storm guarantees that every spout tuple will be fully processed by the topology. It does this by tracking the tree of tuples triggered by every spout tuple and determining when that tree of tuples has been successfully completed. Every topology has a "message timeout" associated with it. If Storm fails to detect that a spout tuple has been completed within that timeout, then it fails the tuple and replays it later. </p>
                <p>To take advantage of Storm's reliability capabilities, you must tell Storm when new edges in a tuple tree are being created and tell Storm whenever you've finished processing an individual tuple. These are done using the <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/task/OutputCollector.html">OutputCollector</a> object that bolts use to emit tuples. Anchoring is done in the <code>emit</code> method, and you declare that you're finished with a tuple using the <code>ack</code> method.</p>
                <p>This is all explained in much more detail in <a href="Guaranteeing-message-processing.html">Guaranteeing message processing</a>. </p>
                <h3>Tasks</h3>
                <p>Each spout or bolt executes as many tasks across the cluster. Each task corresponds to one thread of execution, and stream groupings define how to send tuples from one set of tasks to another set of tasks. You set the parallelism for each spout or bolt in the <code>setSpout</code> and <code>setBolt</code> methods of <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/topology/TopologyBuilder.html">TopologyBuilder</a>.</p>
                <h3>Workers</h3>
                <p>Topologies execute across one or more worker processes. Each worker process is a physical JVM and executes a subset of all the tasks for the topology. For example, if the combined parallelism of the topology is 300 and 50 workers are allocated, then each worker will execute 6 tasks (as threads within the worker). Storm tries to spread the tasks evenly across all the workers.</p>
                <p><strong>Resources:</strong></p>
                <ul>
                    <li><a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/Config.html#TOPOLOGY_WORKERS">Config.TOPOLOGY_WORKERS</a>: this config sets the number of workers to allocate for executing the topology</li>
                </ul>
            </div>
        </div>
    </div>
</div>
<!--Content End-->
