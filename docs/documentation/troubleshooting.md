---
layout: documentation
title: Troubleshooting
---

<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-12">
            	<h1 class="page-title">Troubleshooting</h1>
    <p>This page lists issues people have run into when using Storm along with their solutions.</p>

<h3>Worker processes are crashing on startup with no stack trace</h3>

<p>Possible symptoms:</p>

<ul>
<li>Topologies work with one node, but workers crash with multiple nodes</li>
</ul>

<p>Solutions:</p>

<ul>
<li>You may have a misconfigured subnet, where nodes can't locate other nodes based on their hostname. ZeroMQ sometimes crashes the process when it can't resolve a host. There are two solutions:

<ul>
<li>Make a mapping from hostname to IP address in /etc/hosts</li>
<li>Set up an internal DNS so that nodes can locate each other based on hostname.</li>
</ul></li>
</ul>

<h3>Nodes are unable to communicate with each other</h3>

<p>Possible symptoms:</p>

<ul>
<li>Every spout tuple is failing</li>
<li>Processing is not working</li>
</ul>

<p>Solutions:</p>

<ul>
<li>Storm doesn't work with ipv6. You can force ipv4 by adding <code>-Djava.net.preferIPv4Stack=true</code> to the supervisor child options and restarting the supervisor. </li>
<li>You may have a misconfigured subnet. See the solutions for <code>Worker processes are crashing on startup with no stack trace</code></li>
</ul>

<h3>Topology stops processing tuples after awhile</h3>

<p>Symptoms:</p>

<ul>
<li>Processing works fine for awhile, and then suddenly stops and spout tuples start failing en masse. </li>
</ul>

<p>Solutions:</p>

<ul>
<li>This is a known issue with ZeroMQ 2.1.10. Downgrade to ZeroMQ 2.1.7.</li>
</ul>

<h3>Not all supervisors appear in Storm UI</h3>

<p>Symptoms:</p>

<ul>
<li>Some supervisor processes are missing from the Storm UI</li>
<li>List of supervisors in Storm UI changes on refreshes</li>
</ul>

<p>Solutions:</p>

<ul>
<li>Make sure the supervisor local dirs are independent (e.g., not sharing a local dir over NFS)</li>
<li>Try deleting the local dirs for the supervisors and restarting the daemons. Supervisors create a unique id for themselves and store it locally. When that id is copied to other nodes, Storm gets confused. </li>
</ul>

<h3>"Multiple defaults.yaml found" error</h3>

<p>Symptoms:</p>

<ul>
<li>When deploying a topology with "storm jar", you get this error</li>
</ul>

<p>Solution:</p>

<ul>
<li>You're most likely including the Storm jars inside your topology jar. When packaging your topology jar, don't include the Storm jars as Storm will put those on the classpath for you.</li>
</ul>

<h3>"NoSuchMethodError" when running storm jar</h3>

<p>Symptoms:</p>

<ul>
<li>When running storm jar, you get a cryptic "NoSuchMethodError"</li>
</ul>

<p>Solution:</p>

<ul>
<li>You're deploying your topology with a different version of Storm than you built your topology against. Make sure the storm client you use comes from the same version as the version you compiled your topology against.</li>
</ul>

<h3>Kryo ConcurrentModificationException</h3>

<p>Symptoms:</p>

<ul>
<li>At runtime, you get a stack trace like the following:</li>
</ul>
<div class="highlight"><pre><code class="language-text" data-lang="text">java.lang.RuntimeException: java.util.ConcurrentModificationException
    at backtype.storm.utils.DisruptorQueue.consumeBatchToCursor(DisruptorQueue.java:84)
    at backtype.storm.utils.DisruptorQueue.consumeBatchWhenAvailable(DisruptorQueue.java:55)
    at backtype.storm.disruptor$consume_batch_when_available.invoke(disruptor.clj:56)
    at backtype.storm.disruptor$consume_loop_STAR_$fn__1597.invoke(disruptor.clj:67)
    at backtype.storm.util$async_loop$fn__465.invoke(util.clj:377)
    at clojure.lang.AFn.run(AFn.java:24)
    at java.lang.Thread.run(Thread.java:679)
Caused by: java.util.ConcurrentModificationException
    at java.util.LinkedHashMap$LinkedHashIterator.nextEntry(LinkedHashMap.java:390)
    at java.util.LinkedHashMap$EntryIterator.next(LinkedHashMap.java:409)
    at java.util.LinkedHashMap$EntryIterator.next(LinkedHashMap.java:408)
    at java.util.HashMap.writeObject(HashMap.java:1016)
    at sun.reflect.GeneratedMethodAccessor17.invoke(Unknown Source)
    at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
    at java.lang.reflect.Method.invoke(Method.java:616)
    at java.io.ObjectStreamClass.invokeWriteObject(ObjectStreamClass.java:959)
    at java.io.ObjectOutputStream.writeSerialData(ObjectOutputStream.java:1480)
    at java.io.ObjectOutputStream.writeOrdinaryObject(ObjectOutputStream.java:1416)
    at java.io.ObjectOutputStream.writeObject0(ObjectOutputStream.java:1174)
    at java.io.ObjectOutputStream.writeObject(ObjectOutputStream.java:346)
    at backtype.storm.serialization.SerializableSerializer.write(SerializableSerializer.java:21)
    at com.esotericsoftware.kryo.Kryo.writeClassAndObject(Kryo.java:554)
    at com.esotericsoftware.kryo.serializers.CollectionSerializer.write(CollectionSerializer.java:77)
    at com.esotericsoftware.kryo.serializers.CollectionSerializer.write(CollectionSerializer.java:18)
    at com.esotericsoftware.kryo.Kryo.writeObject(Kryo.java:472)
    at backtype.storm.serialization.KryoValuesSerializer.serializeInto(KryoValuesSerializer.java:27)
</code></pre></div>
<p>Solution: </p>

<ul>
<li>This means that you're emitting a mutable object as an output tuple. Everything you emit into the output collector must be immutable. What's happening is that your bolt is modifying the object while it is being serialized to be sent over the network.</li>
</ul>

<h3>NullPointerException from deep inside Storm</h3>

<p>Symptoms:</p>

<ul>
<li>You get a NullPointerException that looks something like:</li>
</ul>
<div class="highlight"><pre><code class="language-text" data-lang="text">java.lang.RuntimeException: java.lang.NullPointerException
    at backtype.storm.utils.DisruptorQueue.consumeBatchToCursor(DisruptorQueue.java:84)
    at backtype.storm.utils.DisruptorQueue.consumeBatchWhenAvailable(DisruptorQueue.java:55)
    at backtype.storm.disruptor$consume_batch_when_available.invoke(disruptor.clj:56)
    at backtype.storm.disruptor$consume_loop_STAR_$fn__1596.invoke(disruptor.clj:67)
    at backtype.storm.util$async_loop$fn__465.invoke(util.clj:377)
    at clojure.lang.AFn.run(AFn.java:24)
    at java.lang.Thread.run(Thread.java:662)
Caused by: java.lang.NullPointerException
    at backtype.storm.serialization.KryoTupleSerializer.serialize(KryoTupleSerializer.java:24)
    at backtype.storm.daemon.worker$mk_transfer_fn$fn__4126$fn__4130.invoke(worker.clj:99)
    at backtype.storm.util$fast_list_map.invoke(util.clj:771)
    at backtype.storm.daemon.worker$mk_transfer_fn$fn__4126.invoke(worker.clj:99)
    at backtype.storm.daemon.executor$start_batch_transfer__GT_worker_handler_BANG_$fn__3904.invoke(executor.clj:205)
    at backtype.storm.disruptor$clojure_handler$reify__1584.onEvent(disruptor.clj:43)
    at backtype.storm.utils.DisruptorQueue.consumeBatchToCursor(DisruptorQueue.java:81)
    ... 6 more
</code></pre></div>
<p>Solution:</p>

<ul>
<li>This is caused by having multiple threads issue methods on the <code>OutputCollector</code>. All emits, acks, and fails must happen on the same thread. One subtle way this can happen is if you make a <code>IBasicBolt</code> that emits on a separate thread. <code>IBasicBolt</code>'s automatically ack after execute is called, so this would cause multiple threads to use the <code>OutputCollector</code> leading to this exception. When using a basic bolt, all emits must happen in the same thread that runs <code>execute</code>.</li>
</ul>
            </div>
        </div>
    </div>
</div>
<!--Content End-->