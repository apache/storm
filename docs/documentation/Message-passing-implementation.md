---
layout: documentation
title: Message Passing Implementation
---
<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-12">
            	<h1 class="page-title">Message Passing Implementation</h1>
            </div>
        </div>
        <div class="row">
            <div class="col-md-12">
                <p>(Note: this walkthrough is out of date as of 0.8.0. 0.8.0 revamped the message passing infrastructure to be based on the Disruptor)</p>

				<p>This page walks through how emitting and transferring tuples works in Storm.</p>

				<ul>
				<li>Worker is responsible for message transfer

				<ul>
				<li><code>refresh-connections</code> is called every "task.refresh.poll.secs" or whenever assignment in ZK changes. It manages connections to other workers and maintains a mapping from task -&gt; worker <a href="https://github.com/apache/incubator-storm/blob/0.7.1/src/clj/backtype/storm/daemon/worker.clj#L123">code</a></li>
				<li>Provides a "transfer function" that is used by tasks to send tuples to other tasks. The transfer function takes in a task id and a tuple, and it serializes the tuple and puts it onto a "transfer queue". There is a single transfer queue for each worker. <a href="https://github.com/apache/incubator-storm/blob/0.7.1/src/clj/backtype/storm/daemon/worker.clj#L56">code</a></li>
				<li>The serializer is thread-safe <a href="https://github.com/apache/incubator-storm/blob/0.7.1/src/jvm/backtype/storm/serialization/KryoTupleSerializer.java#L26">code</a></li>
				<li>The worker has a single thread which drains the transfer queue and sends the messages to other workers <a href="https://github.com/apache/incubator-storm/blob/0.7.1/src/clj/backtype/storm/daemon/worker.clj#L185">code</a></li>
				<li>Message sending happens through this protocol: <a href="https://github.com/apache/incubator-storm/blob/0.7.1/src/clj/backtype/storm/messaging/protocol.clj">code</a></li>
				<li>The implementation for distributed mode uses ZeroMQ <a href="https://github.com/apache/incubator-storm/blob/0.7.1/src/clj/backtype/storm/messaging/zmq.clj">code</a></li>
				<li>The implementation for local mode uses in memory Java queues (so that it's easy to use Storm locally without needing to get ZeroMQ installed) <a href="https://github.com/apache/incubator-storm/blob/0.7.1/src/clj/backtype/storm/messaging/local.clj">code</a></li>
				</ul></li>
				<li>Receiving messages in tasks works differently in local mode and distributed mode

				<ul>
				<li>In local mode, the tuple is sent directly to an in-memory queue for the receiving task <a href="https://github.com/apache/incubator-storm/blob/master/src/clj/backtype/storm/messaging/local.clj#L21">code</a></li>
				<li>In distributed mode, each worker listens on a single TCP port for incoming messages and then routes those messages in-memory to tasks. The TCP port is called a "virtual port", because it receives [task id, message] and then routes it to the actual task. <a href="https://github.com/apache/incubator-storm/blob/master/src/clj/backtype/storm/daemon/worker.clj#L204">code</a>

				<ul>
				<li>The virtual port implementation is here: <a href="https://github.com/apache/incubator-storm/blob/master/src/clj/zilch/virtual_port.clj">code</a></li>
				<li>Tasks listen on an in-memory ZeroMQ port for messages from the virtual port <a href="https://github.com/apache/incubator-storm/blob/master/src/clj/backtype/storm/daemon/task.clj#L201">code</a></li>
				<li>Bolts listen here: <a href="https://github.com/apache/incubator-storm/blob/master/src/clj/backtype/storm/daemon/task.clj#L489">code</a></li>
				<li>Spouts listen here: <a href="https://github.com/apache/incubator-storm/blob/master/src/clj/backtype/storm/daemon/task.clj#L382">code</a></li>
				</ul></li>
				</ul></li>
				<li>Tasks are responsible for message routing. A tuple is emitted either to a direct stream (where the task id is specified) or a regular stream. In direct streams, the message is only sent if that bolt subscribes to that direct stream. In regular streams, the stream grouping functions are used to determine the task ids to send the tuple to.

				<ul>
				<li>Tasks have a routing map from {stream id} -&gt; {component id} -&gt; {stream grouping function} <a href="https://github.com/apache/incubator-storm/blob/master/src/clj/backtype/storm/daemon/task.clj#L198">code</a></li>
				<li>The "tasks-fn" returns the task ids to send the tuples to for either regular stream emit or direct stream emit <a href="https://github.com/apache/incubator-storm/blob/master/src/clj/backtype/storm/daemon/task.clj#L207">code</a></li>
				<li>After getting the output task ids, bolts and spouts use the transfer-fn provided by the worker to actually transfer the tuples

				<ul>
				<li>Bolt transfer code here: <a href="https://github.com/apache/incubator-storm/blob/master/src/clj/backtype/storm/daemon/task.clj#L429">code</a></li>
				<li>Spout transfer code here: <a href="https://github.com/apache/incubator-storm/blob/master/src/clj/backtype/storm/daemon/task.clj#L329">code</a></li>
				</ul></li>
				</ul></li>
				</ul>
            </div>
        </div>
    </div>
</div>