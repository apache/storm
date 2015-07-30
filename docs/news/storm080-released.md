---
layout: news
title: Storm 0.8.0 and Trident released
---
<!--Post Header-->
<h3 class="news-title">Storm 0.8.0 and Trident released</h3>
<div class="news-meta">
    <i class="fa fa-calendar"></i> Aug 2, 2012 <i class="fa fa-user"></i> Nathan Marz
</div>
<!--Post Body-->
<p>I'm happy to announce the release of Storm 0.8.0. This is a <em>huge</em> release. Thanks to everyone who tested out the dev release and helped find and fix issues. And thanks to everyone who contributed pull requests. There's one big new thing available in the release: Trident. You may have heard me hint about Trident before, and now it's finally public. </p>
<p>Trident is a higher level abstraction for Storm. It allows you to seamlessly mix high throughput (millions of messages per second), stateful stream processing with low latency distributed querying. If you're familiar with high level batch processing tools like Pig or Cascading, the concepts of Trident will be very familiar - Trident has joins, aggregations, grouping, functions, and filters. In addition to these, Trident adds primitives for doing stateful, incremental processing on top of any database or persistence store. Trident has consistent, exactly-once semantics, so it is easy to reason about Trident topologies. Trident is bundled with Storm, and you can read documentation about it on <a href="https://github.com/apache/incubator-storm/wiki/Documentation" target="_blank">this page</a> (start with "Trident tutorial").</p>
<p>Trident supersedes both LinearDRPCTopologyBuilder and transactional topologies, both of which will be deprecated soon and eventually removed from the codebase. </p>
<p>Here are the other highlights of this release: </p>
<h4>Executors</h4>
<p>Prior to Storm 0.8.0, a running topology consisted of some number of workers and some number of tasks that ran on those workers. In the old model, worker = process and task = thread. Storm 0.8.0 changes this model by introducing executors. In this model, a worker = process, an executor = thread, and one executor runs many tasks from the same spout/bolt. </p>
<p>The reason for the change is that the old model complected the semantics of the topology with its physical execution. For example, if you had a bolt with 4 tasks doing a fields grouping on some stream, in order to maintain the semantics of the fields grouping (that the same value always goes to the same task id for that bolt), the number of tasks for that bolt needs to be fixed for the lifetime of the topology, and since task = thread, the number of threads for that bolt is fixed for the lifetime of the topology. In the new model, the number of threads for a bolt is disassociated from the number of tasks, meaning you can change the number of threads for a spout/bolt dynamically without affecting semantics. Similarly, if you're keeping large amounts of state in your bolts, and you want to increase the parallelism of the bolt without having to repartition the state, you can do this with the new executors. </p>
<p>At the API level, the "parallelism_hint" now specifies the initial number of executors for that bolt. You can specify the number of tasks using the TOPOLOGY_TASKS component config. For example:</p>
<p><code>
builder.setBolt(new MyBolt(), 3).setNumTasks(128).shuffleGrouping("spout"); 
</code></p>
<p>This sets the initial number of executors to 3 and the number of tasks to 128. If you don't specify the number of tasks for a component, it will be fixed to the initial number of executors for the lifetime of the topology. </p>
<p>Finally, you can change the number of workers and/or number of executors for components using the "storm rebalance" command. The following command changes the number of workers for the "demo" topology to 3, the number of executors for the "myspout" component to 5, and the number of executors for the "mybolt" component to 1: </p>
<p><code>
storm rebalance demo -n 3 -e myspout=5 -e mybolt=1 
</code></p>
<h4>Pluggable scheduler</h4>
<p>You can now implement your own scheduler to replace the default scheduler to assign executors to workers. You configure the class to use using the "storm.scheduler" config in your storm.yaml, and your scheduler must implement <a href="https://github.com/apache/incubator-storm/blob/master/src/jvm/backtype/storm/scheduler/IScheduler.java" target="_blank">this interface</a>.</p>
<h4>Throughput improvements</h4>
<p>The internals of Storm have been rearchitected for extremely significant performance gains. I'm seeing throughput increases of anywhere from 5-10x of what it was before. I've benchmarked Storm at 1M tuples / sec / node on an internal Twitter cluster. </p>
<p>The key changes made were: </p>
<p>a) Replacing all the internal in-memory queuing with the LMAX Disruptor 
b) Doing intelligent auto-batching of processing so that the consumers can keep up with the producers </p>
<p>Here are the configs which affect how buffering/batching is done: </p>
<p>topology.executor.receive.buffer.size 
topology.executor.send.buffer.size 
topology.receiver.buffer.size 
topology.transfer.buffer.size </p>
<p>These may require some tweaking to optimize your topologies, but most likely the default values will work fine for you out of the box. </p>
<h4>Decreased Zookeeper load / increased Storm UI performance</h4>
<p>Storm sends significantly less traffic to Zookeeper now (on the order of 10x less). Since it also uses so many fewer znodes to store state, the UI is significantly faster as well. </p>
<h4>Abstractions for shared resources</h4>
<p>The TopologyContext has methods "getTaskData", "getExecutorData", and "getResource" for sharing resources at the task level, executor level, or worker level respectively. Currently you can't set any worker resources, but this is in development. Storm currently provides a shared ExecutorService worker resource (via "getSharedExecutor" method) that can be used for launching background tasks on a shared thread pool. </p>
<h4>Tick tuples</h4>
<p>It's common to require a bolt to "do something" at a fixed interval, like flush writes to a database. Many people have been using variants of a ClockSpout to send these ticks. The problem with a ClockSpout is that you can't internalize the need for ticks within your bolt, so if you forget to set up your bolt correctly within your topology it won't work correctly. 0.8.0 introduces a new "tick tuple" config that lets you specify the frequency at which you want to receive tick tuples via the "topology.tick.tuple.freq.secs" component- specific config, and then your bolt will receive a tuple from the __system component and __tick stream at that frequency. </p>
<h4>Improved Storm UI</h4>
<p>The Storm UI now has a button for showing/hiding the "system stats" (tuples sent on streams other than ones you've defined, like acker streams), making it easier to digest what your topology is doing.</p>
<p>Here's the full changelog for Storm 0.8.0: </p>
<ul>
	<li>Added Trident, the new high-level abstraction for intermixing high throughput, stateful stream processing with low-latency distributed querying </li>
	<li>Added executor abstraction between workers and tasks. Workers = processes, executors = threads that run many tasks from the same spout or bolt. </li>
	<li>Pluggable scheduler (thanks xumingming) </li>
	<li>Eliminate explicit storage of task-&gt;component in Zookeeper </li>
	<li>Number of workers can be dynamically changed at runtime through rebalance command and -n switch </li>
	<li>Number of executors for a component can be dynamically changed at runtime through rebalance command and -e switch (multiple -e switches allowed) </li>
	<li>Use worker heartbeats instead of task heartbeats (thanks xumingming) </li>
	<li>UI performance for topologies with many executors/tasks much faster due to optimized usage of Zookeeper (10x improvement) </li>
	<li>Added button to show/hide system stats (e.g., acker component and stream stats) from the Storm UI (thanks xumingming) </li>
	<li>Stats are tracked on a per-executor basis instead of per-task basis </li>
	<li>Major optimization for unreliable spouts and unanchored tuples (will use far less CPU) </li>
	<li>Revamped internals of Storm to use LMAX disruptor for internal queuing. Dramatic reductions in contention and CPU usage. </li>
	<li>Numerous micro-optimizations all throughout the codebase to reduce CPU usage. </li>
	<li>Optimized internals of Storm to use much fewer threads - two fewer threads per spout and one fewer thread per acker. </li>
	<li>Removed error method from task hooks (to be re-added at a later time) </li>
	<li>Validate that subscriptions come from valid components and streams, and if it's a field grouping that the schema is correct (thanks xumingming) </li>
	<li>MemoryTransactionalSpout now works in cluster mode </li>
	<li>Only track errors on a component by component basis to reduce the amount stored in zookeeper (to speed up UI). A side effect of this change is the removal of the task page in the UI. </li>
	<li>Add TOPOLOGY-TICK-TUPLE-FREQ-SECS config to have Storm automatically send "tick" tuples to a bolt's execute method coming from the __system component and __tick stream at the configured frequency. Meant to be used as a component-specific configuration. </li>
	<li>Upgrade Kryo to v2.17 </li>
	<li>Tuple is now an interface and is much cleaner. The Clojure DSL helpers have been moved to TupleImpl </li>
	<li>Added shared worker resources. Storm provides a shared ExecutorService thread pool by default. The number of threads in the pool can be configured with topology.worker.shared.thread.pool.size </li>
	<li>Improve CustomStreamGrouping interface to make it more flexible by providing more information </li>
	<li>Enhanced INimbus interface to allow for forced schedulers and better integration with global scheduler </li>
	<li>Added assigned method to ISupervisor so it knows exactly what's running and not running </li>
	<li>Custom serializers can now have one of four constructors: (), (Kryo), (Class), or (Kryo, Class) </li>
	<li>Disallow ":", ".", and "\" from topology names </li>
	<li>Errors in multilang subprocesses that go to stderr will be captured and logged to the worker logs (thanks vinodc) </li>
	<li>Workers detect and warn for missing outbound connections from assignment, drop messages for which there's no outbound connection </li>
	<li>Zookeeper connection timeout is now configurable (via storm.zookeeper.connection.timeout config) </li>
	<li>Storm is now less aggressive about halting process when there are Zookeeper errors, preferring to wait until client calls return exceptions. </li>
	<li>Can configure Zookeeper authentication for Storm's Zookeeper clients via "storm.zookeeper.auth.scheme" and "storm.zookeeper.auth.payload" configs </li>
	<li>Supervisors only download code for topologies assigned to them </li>
	<li>Include task id information in task hooks (thanks velvia) </li>
	<li>Use execvp to spawn daemons (replaces the python launcher process) (thanks ept) </li>
	<li>Expanded INimbus/ISupervisor interfaces to provide more information (used in Storm/Mesos integration) </li>
	<li>Bug fix: Realize task ids when worker heartbeats to supervisor. Some users were hitting deserialization problems here in very rare cases (thanks herberteuler) </li>
	<li>Bug fix: Fix bug where a topology's status would get corrupted to true if nimbus is restarted while status is rebalancing </li>
</ul>
