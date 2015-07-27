---
layout: documentation
title: Transactional Topologies
---
<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-12">
            	<h1 class="page-title">Transactional Topologies <span>NOTE: Transactional topologies have been deprecated -- use the Trident framework instead.</span></h1>
                <article class="post-content">
<p>Storm <a href="guaranteeing-message-processing.html">guarantees data processing</a> by providing an at least once processing guarantee. The most common question asked about Storm is "Given that tuples can be replayed, how do you do things like counting on top of Storm? Won't you overcount?"</p>

<p>Storm 0.7.0 introduces transactional topologies, which enable you to get exactly once messaging semantics for pretty much any computation. So you can do things like counting in a fully-accurate, scalable, and fault-tolerant way.</p>

<p>Like <a href="distributed-rpc.html">Distributed RPC</a>, transactional topologies aren't so much a feature of Storm as they are a higher level abstraction built on top of Storm's primitives of streams, spouts, bolts, and topologies.</p>

<p>This page explains the transactional topology abstraction, how to use the API, and provides details as to its implementation.</p>

<h3>Concepts</h3>

<p>Let's build up to Storm's abstraction for transactional topologies one step at a time. Let's start by looking at the simplest possible approach, and then we'll iterate on the design until we reach Storm's design.</p>

<h3>Design 1</h3>

<p>The core idea behind transactional topologies is to provide a <em>strong ordering</em> on the processing of data. The simplest manifestation of this, and the first design we'll look at, is processing the tuples one at a time and not moving on to the next tuple until the current tuple has been successfully processed by the topology.</p>

<p>Each tuple is associated with a transaction id. If the tuple fails and needs to be replayed, then it is emitted with the exact same transaction id. A transaction id is an integer that increments for every tuple, so the first tuple will have transaction id <code>1</code>, the second id <code>2</code>, and so on.</p>

<p>The strong ordering of tuples gives you the capability to achieve exactly-once semantics even in the case of tuple replay. Let's look at an example of how you would do this.</p>

<p>Suppose you want to do a global count of the tuples in the stream. Instead of storing just the count in the database, you instead store the count and the latest transaction id together as one value in the database. When your code updates the count in the db, it should update the count <em>only if the transaction id in the database differs from the transaction id for the tuple currently being processed</em>. Consider the two cases:</p>

<ol>
<li><em>The transaction id in the database is different than the current transaction id:</em> Because of the strong ordering of transactions, we know for sure that the current tuple isn't represented in that count. So we can safely increment the count and update the transaction id.</li>
<li><em>The transaction id is the same as the current transaction id:</em> Then we know that this tuple is already incorporated into the count and can skip the update. The tuple must have failed after updating the database but before reporting success back to Storm.</li>
</ol>

<p>This logic and the strong ordering of transactions ensures that the count in the database will be accurate even if tuples are replayed.  Credit for this trick of storing a transaction id in the database along with the value goes to the Kafka devs, particularly <a href="http://incubator.apache.org/kafka/07/design.html" target="_blank">this design document</a>.</p>

<p>Furthermore, notice that the topology can safely update many sources of state in the same transaction and achieve exactly-once semantics. If there's a failure, any updates that already succeeded will skip on the retry, and any updates that failed will properly retry. For example, if you were processing a stream of tweeted urls, you could update a database that stores a tweet count for each url as well as a database that stores a tweet count for each domain.</p>

<p>There is a significant problem though with this design of processing one tuple at time. Having to wait for each tuple to be <em>completely processed</em> before moving on to the next one is horribly inefficient. It entails a huge amount of database calls (at least one per tuple), and this design makes very little use of the parallelization capabilities of Storm. So it isn't very scalable.</p>

<h3>Design 2</h3>

<p>Instead of processing one tuple at a time, a better approach is to process a batch of tuples for each transaction. So if you're doing a global count, you would increment the count by the number of tuples in the entire batch. If a batch fails, you replay the exact batch that failed. Instead of assigning a transaction id to each tuple, you assign a transaction id to each batch, and the processing of the batches is strongly ordered. Here's a diagram of this design:</p>

<p><img src="transactional-batches.png" alt="Storm cluster"></p>

<p>So if you're processing 1000 tuples per batch, your application will do 1000x less database operations than design 1. Additionally, it takes advantage of Storm's parallelization capabilities as the computation for each batch can be parallelized.</p>

<p>While this design is significantly better than design 1, it's still not as resource-efficient as possible. The workers in the topology spend a lot of time being idle waiting for the other portions of the computation to finish. For example, in a topology like this:</p>

<p><img src="transactional-design-2.png" alt="Storm cluster"></p>

<p>After bolt 1 finishes its portion of the processing, it will be idle until the rest of the bolts finish and the next batch can be emitted from the spout.</p>

<h3>Design 3 (Storm's design)</h3>

<p>A key realization is that not all the work for processing batches of tuples needs to be strongly ordered. For example, when computing a global count, there's two parts to the computation:</p>

<ol>
<li>Computing the partial count for the batch</li>
<li>Updating the global count in the database with the partial count</li>
</ol>

<p>The computation of #2 needs to be strongly ordered across the batches, but there's no reason you shouldn't be able to <em>pipeline</em> the computation of the batches by computing #1 for many batches in parallel. So while batch 1 is working on updating the database, batches 2 through 10 can compute their partial counts.</p>

<p>Storm accomplishes this distinction by breaking the computation of a batch into two phases:</p>

<ol>
<li>The processing phase: this is the phase that can be done in parallel for many batches</li>
<li>The commit phase: The commit phases for batches are strongly ordered. So the commit for batch 2 is not done until the commit for batch 1 has been successful.</li>
</ol>

<p>The two phases together are called a "transaction". Many batches can be in the processing phase at a given moment, but only one batch can be in the commit phase. If there's any failure in the processing or commit phase for a batch, the entire transaction is replayed (both phases).</p>

<h3>Design details</h3>

<p>When using transactional topologies, Storm does the following for you:</p>

<ol>
<li><em>Manages state:</em> Storm stores in Zookeeper all the state necessary to do transactional topologies. This includes the current transaction id as well as the metadata defining the parameters for each batch.</li>
<li><em>Coordinates the transactions:</em> Storm will manage everything necessary to determine which transactions should be processing or committing at any point.</li>
<li><em>Fault detection:</em> Storm leverages the acking framework to efficiently determine when a batch has successfully processed, successfully committed, or failed. Storm will then replay batches appropriately. You don't have to do any acking or anchoring -- Storm manages all of this for you.</li>
<li><em>First class batch processing API</em>: Storm layers an API on top of regular bolts to allow for batch processing of tuples. Storm manages all the coordination for determining when a task has received all the tuples for that particular transaction. Storm will also take care of cleaning up any accumulated state for each transaction (like the partial counts).</li>
</ol>

<p>Finally, another thing to note is that transactional topologies require a source queue that can replay an exact batch of messages. Technologies like <a href="https://github.com/robey/kestrel">Kestrel</a> can't do this. <a href="http://incubator.apache.org/kafka/index.html">Apache Kafka</a> is a perfect fit for this kind of spout, and <a href="https://github.com/apache/storm/tree/master/external/storm-kafka">storm-kafka</a> contains a transactional spout implementation for Kafka.</p>

<h3>The basics through example</h3>

<p>You build transactional topologies by using <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/transactional/TransactionalTopologyBuilder.html">TransactionalTopologyBuilder</a>. Here's the transactional topology definition for a topology that computes the global count of tuples from the input stream. This code comes from <a href="https://github.com/apache/storm/blob/master/examples/storm-starter/src/jvm/storm/starter/TransactionalGlobalCount.java">TransactionalGlobalCount</a> in storm-starter.</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="n">MemoryTransactionalSpout</span> <span class="n">spout</span> <span class="o">=</span> <span class="k">new</span> <span class="nf">MemoryTransactionalSpout</span><span class="o">(</span><span class="n">DATA</span><span class="o">,</span> <span class="k">new</span> <span class="nf">Fields</span><span class="o">(</span><span class="s">"word"</span><span class="o">),</span> <span class="n">PARTITION_TAKE_PER_BATCH</span><span class="o">);</span>
<span class="n">TransactionalTopologyBuilder</span> <span class="n">builder</span> <span class="o">=</span> <span class="k">new</span> <span class="nf">TransactionalTopologyBuilder</span><span class="o">(</span><span class="s">"global-count"</span><span class="o">,</span> <span class="s">"spout"</span><span class="o">,</span> <span class="n">spout</span><span class="o">,</span> <span class="mi">3</span><span class="o">);</span>
<span class="n">builder</span><span class="o">.</span><span class="na">setBolt</span><span class="o">(</span><span class="s">"partial-count"</span><span class="o">,</span> <span class="k">new</span> <span class="nf">BatchCount</span><span class="o">(),</span> <span class="mi">5</span><span class="o">)</span>
        <span class="o">.</span><span class="na">shuffleGrouping</span><span class="o">(</span><span class="s">"spout"</span><span class="o">);</span>
<span class="n">builder</span><span class="o">.</span><span class="na">setBolt</span><span class="o">(</span><span class="s">"sum"</span><span class="o">,</span> <span class="k">new</span> <span class="nf">UpdateGlobalCount</span><span class="o">())</span>
        <span class="o">.</span><span class="na">globalGrouping</span><span class="o">(</span><span class="s">"partial-count"</span><span class="o">);</span>
</code></pre></div>
<p><code>TransactionalTopologyBuilder</code> takes as input in the constructor an id for the transactional topology, an id for the spout within the topology, a transactional spout, and optionally the parallelism for the transactional spout. The id for the transactional topology is used to store state about the progress of topology in Zookeeper, so that if you restart the topology it will continue where it left off.</p>

<p>A transactional topology has a single <code>TransactionalSpout</code> that is defined in the constructor of <code>TransactionalTopologyBuilder</code>. In this example, <code>MemoryTransactionalSpout</code> is used which reads in data from an in-memory partitioned source of data (the <code>DATA</code> variable). The second argument defines the fields for the data, and the third argument specifies the maximum number of tuples to emit from each partition per batch of tuples. The interface for defining your own transactional spouts is discussed later on in this tutorial.</p>

<p>Now on to the bolts. This topology parallelizes the computation of the global count. The first bolt, <code>BatchCount</code>, randomly partitions the input stream using a shuffle grouping and emits the count for each partition. The second bolt, <code>UpdateGlobalCount</code>, does a global grouping and sums together the partial counts to get the count for the batch. It then updates the global count in the database if necessary.</p>

<p>Here's the definition of <code>BatchCount</code>:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="kd">public</span> <span class="kd">static</span> <span class="kd">class</span> <span class="nc">BatchCount</span> <span class="kd">extends</span> <span class="n">BaseBatchBolt</span> <span class="o">{</span>
    <span class="n">Object</span> <span class="n">_id</span><span class="o">;</span>
    <span class="n">BatchOutputCollector</span> <span class="n">_collector</span><span class="o">;</span>

    <span class="kt">int</span> <span class="n">_count</span> <span class="o">=</span> <span class="mi">0</span><span class="o">;</span>

    <span class="nd">@Override</span>
    <span class="kd">public</span> <span class="kt">void</span> <span class="nf">prepare</span><span class="o">(</span><span class="n">Map</span> <span class="n">conf</span><span class="o">,</span> <span class="n">TopologyContext</span> <span class="n">context</span><span class="o">,</span> <span class="n">BatchOutputCollector</span> <span class="n">collector</span><span class="o">,</span> <span class="n">Object</span> <span class="n">id</span><span class="o">)</span> <span class="o">{</span>
        <span class="n">_collector</span> <span class="o">=</span> <span class="n">collector</span><span class="o">;</span>
        <span class="n">_id</span> <span class="o">=</span> <span class="n">id</span><span class="o">;</span>
    <span class="o">}</span>

    <span class="nd">@Override</span>
    <span class="kd">public</span> <span class="kt">void</span> <span class="nf">execute</span><span class="o">(</span><span class="n">Tuple</span> <span class="n">tuple</span><span class="o">)</span> <span class="o">{</span>
        <span class="n">_count</span><span class="o">++;</span>
    <span class="o">}</span>

    <span class="nd">@Override</span>
    <span class="kd">public</span> <span class="kt">void</span> <span class="nf">finishBatch</span><span class="o">()</span> <span class="o">{</span>
        <span class="n">_collector</span><span class="o">.</span><span class="na">emit</span><span class="o">(</span><span class="k">new</span> <span class="nf">Values</span><span class="o">(</span><span class="n">_id</span><span class="o">,</span> <span class="n">_count</span><span class="o">));</span>
    <span class="o">}</span>

    <span class="nd">@Override</span>
    <span class="kd">public</span> <span class="kt">void</span> <span class="nf">declareOutputFields</span><span class="o">(</span><span class="n">OutputFieldsDeclarer</span> <span class="n">declarer</span><span class="o">)</span> <span class="o">{</span>
        <span class="n">declarer</span><span class="o">.</span><span class="na">declare</span><span class="o">(</span><span class="k">new</span> <span class="nf">Fields</span><span class="o">(</span><span class="s">"id"</span><span class="o">,</span> <span class="s">"count"</span><span class="o">));</span>
    <span class="o">}</span>
<span class="o">}</span>
</code></pre></div>
<p>A new instance of this object is created for every batch that's being processed. The actual bolt this runs within is called <a href="https://github.com/apache/storm/blob/0.7.0/src/jvm/backtype/storm/coordination/BatchBoltExecutor.java">BatchBoltExecutor</a> and manages the creation and cleanup for these objects.</p>

<p>The <code>prepare</code> method parameterizes this batch bolt with the Storm config, the topology context, an output collector, and the id for this batch of tuples. In the case of transactional topologies, the id will be a <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/transactional/TransactionAttempt.html">TransactionAttempt</a> object. The batch bolt abstraction can be used in Distributed RPC as well which uses a different type of id for the batches. <code>BatchBolt</code> can actually be parameterized with the type of the id, so if you only intend to use the batch bolt for transactional topologies, you can extend <code>BaseTransactionalBolt</code> which has this definition:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="kd">public</span> <span class="kd">abstract</span> <span class="kd">class</span> <span class="nc">BaseTransactionalBolt</span> <span class="kd">extends</span> <span class="n">BaseBatchBolt</span><span class="o">&lt;</span><span class="n">TransactionAttempt</span><span class="o">&gt;</span> <span class="o">{</span>
<span class="o">}</span>
</code></pre></div>
<p>All tuples emitted within a transactional topology must have the <code>TransactionAttempt</code> as the first field of the tuple. This lets Storm identify which tuples belong to which batches. So when you emit tuples you need to make sure to meet this requirement.</p>

<p>The <code>TransactionAttempt</code> contains two values: the "transaction id" and the "attempt id". The "transaction id" is the unique id chosen for this batch and is the same no matter how many times the batch is replayed. The "attempt id" is a unique id for this particular batch of tuples and lets Storm distinguish tuples from different emissions of the same batch. Without the attempt id, Storm could confuse a replay of a batch with tuples from a prior time that batch was emitted. This would be disastrous.</p>

<p>The transaction id increases by 1 for every batch emitted. So the first batch has id "1", the second has id "2", and so on.</p>

<p>The <code>execute</code> method is called for every tuple in the batch. You should accumulate state for the batch in a local instance variable every time this method is called. The <code>BatchCount</code> bolt increments a local counter variable for every tuple.</p>

<p>Finally, <code>finishBatch</code> is called when the task has received all tuples intended for it for this particular batch. <code>BatchCount</code> emits the partial count to the output stream when this method is called.</p>

<p>Here's the definition of <code>UpdateGlobalCount</code>:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="kd">public</span> <span class="kd">static</span> <span class="kd">class</span> <span class="nc">UpdateGlobalCount</span> <span class="kd">extends</span> <span class="n">BaseTransactionalBolt</span> <span class="kd">implements</span> <span class="n">ICommitter</span> <span class="o">{</span>
    <span class="n">TransactionAttempt</span> <span class="n">_attempt</span><span class="o">;</span>
    <span class="n">BatchOutputCollector</span> <span class="n">_collector</span><span class="o">;</span>

    <span class="kt">int</span> <span class="n">_sum</span> <span class="o">=</span> <span class="mi">0</span><span class="o">;</span>

    <span class="nd">@Override</span>
    <span class="kd">public</span> <span class="kt">void</span> <span class="nf">prepare</span><span class="o">(</span><span class="n">Map</span> <span class="n">conf</span><span class="o">,</span> <span class="n">TopologyContext</span> <span class="n">context</span><span class="o">,</span> <span class="n">BatchOutputCollector</span> <span class="n">collector</span><span class="o">,</span> <span class="n">TransactionAttempt</span> <span class="n">attempt</span><span class="o">)</span> <span class="o">{</span>
        <span class="n">_collector</span> <span class="o">=</span> <span class="n">collector</span><span class="o">;</span>
        <span class="n">_attempt</span> <span class="o">=</span> <span class="n">attempt</span><span class="o">;</span>
    <span class="o">}</span>

    <span class="nd">@Override</span>
    <span class="kd">public</span> <span class="kt">void</span> <span class="nf">execute</span><span class="o">(</span><span class="n">Tuple</span> <span class="n">tuple</span><span class="o">)</span> <span class="o">{</span>
        <span class="n">_sum</span><span class="o">+=</span><span class="n">tuple</span><span class="o">.</span><span class="na">getInteger</span><span class="o">(</span><span class="mi">1</span><span class="o">);</span>
    <span class="o">}</span>

    <span class="nd">@Override</span>
    <span class="kd">public</span> <span class="kt">void</span> <span class="nf">finishBatch</span><span class="o">()</span> <span class="o">{</span>
        <span class="n">Value</span> <span class="n">val</span> <span class="o">=</span> <span class="n">DATABASE</span><span class="o">.</span><span class="na">get</span><span class="o">(</span><span class="n">GLOBAL_COUNT_KEY</span><span class="o">);</span>
        <span class="n">Value</span> <span class="n">newval</span><span class="o">;</span>
        <span class="k">if</span><span class="o">(</span><span class="n">val</span> <span class="o">==</span> <span class="kc">null</span> <span class="o">||</span> <span class="o">!</span><span class="n">val</span><span class="o">.</span><span class="na">txid</span><span class="o">.</span><span class="na">equals</span><span class="o">(</span><span class="n">_attempt</span><span class="o">.</span><span class="na">getTransactionId</span><span class="o">()))</span> <span class="o">{</span>
            <span class="n">newval</span> <span class="o">=</span> <span class="k">new</span> <span class="nf">Value</span><span class="o">();</span>
            <span class="n">newval</span><span class="o">.</span><span class="na">txid</span> <span class="o">=</span> <span class="n">_attempt</span><span class="o">.</span><span class="na">getTransactionId</span><span class="o">();</span>
            <span class="k">if</span><span class="o">(</span><span class="n">val</span><span class="o">==</span><span class="kc">null</span><span class="o">)</span> <span class="o">{</span>
                <span class="n">newval</span><span class="o">.</span><span class="na">count</span> <span class="o">=</span> <span class="n">_sum</span><span class="o">;</span>
            <span class="o">}</span> <span class="k">else</span> <span class="o">{</span>
                <span class="n">newval</span><span class="o">.</span><span class="na">count</span> <span class="o">=</span> <span class="n">_sum</span> <span class="o">+</span> <span class="n">val</span><span class="o">.</span><span class="na">count</span><span class="o">;</span>
            <span class="o">}</span>
            <span class="n">DATABASE</span><span class="o">.</span><span class="na">put</span><span class="o">(</span><span class="n">GLOBAL_COUNT_KEY</span><span class="o">,</span> <span class="n">newval</span><span class="o">);</span>
        <span class="o">}</span> <span class="k">else</span> <span class="o">{</span>
            <span class="n">newval</span> <span class="o">=</span> <span class="n">val</span><span class="o">;</span>
        <span class="o">}</span>
        <span class="n">_collector</span><span class="o">.</span><span class="na">emit</span><span class="o">(</span><span class="k">new</span> <span class="nf">Values</span><span class="o">(</span><span class="n">_attempt</span><span class="o">,</span> <span class="n">newval</span><span class="o">.</span><span class="na">count</span><span class="o">));</span>
    <span class="o">}</span>

    <span class="nd">@Override</span>
    <span class="kd">public</span> <span class="kt">void</span> <span class="nf">declareOutputFields</span><span class="o">(</span><span class="n">OutputFieldsDeclarer</span> <span class="n">declarer</span><span class="o">)</span> <span class="o">{</span>
        <span class="n">declarer</span><span class="o">.</span><span class="na">declare</span><span class="o">(</span><span class="k">new</span> <span class="nf">Fields</span><span class="o">(</span><span class="s">"id"</span><span class="o">,</span> <span class="s">"sum"</span><span class="o">));</span>
    <span class="o">}</span>
<span class="o">}</span>
</code></pre></div>
<p><code>UpdateGlobalCount</code> is specific to transactional topologies so it extends <code>BaseTransactionalBolt</code>. In the <code>execute</code> method, <code>UpdateGlobalCount</code> accumulates the count for this batch by summing together the partial batches. The interesting stuff happens in <code>finishBatch</code>.</p>

<p>First, notice that this bolt implements the <code>ICommitter</code> interface. This tells Storm that the <code>finishBatch</code> method of this bolt should be part of the commit phase of the transaction. So calls to <code>finishBatch</code> for this bolt will be strongly ordered by transaction id (calls to <code>execute</code> on the other hand can happen during either the processing or commit phases). An alternative way to mark a bolt as a committer is to use the <code>setCommitterBolt</code> method in <code>TransactionalTopologyBuilder</code> instead of <code>setBolt</code>.</p>

<p>The code for <code>finishBatch</code> in <code>UpdateGlobalCount</code> gets the current value from the database and compares its transaction id to the transaction id for this batch. If they are the same, it does nothing. Otherwise, it increments the value in the database by the partial count for this batch.</p>

<p>A more involved transactional topology example that updates multiple databases idempotently can be found in storm-starter in the <a href="https://github.com/apache/storm/blob/master/examples/storm-starter/src/jvm/storm/starter/TransactionalWords.java" target="_blank">TransactionalWords</a> class.</p>

<h3>Transactional Topology API</h3>

<p>This section outlines the different pieces of the transactional topology API.</p>

<h3 id="bolts">Bolts</h3>

<p>There are three kinds of bolts possible in a transactional topology:</p>

<ol>
<li><a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/topology/base/BaseBasicBolt.html" target="_blank">BasicBolt</a>: This bolt doesn't deal with batches of tuples and just emits tuples based on a single tuple of input.</li>
<li><a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/topology/base/BaseBatchBolt.html" target="_blank">BatchBolt</a>: This bolt processes batches of tuples. <code>execute</code> is called for each tuple, and <code>finishBatch</code> is called when the batch is complete.</li>
<li>BatchBolt's that are marked as committers: The only difference between this bolt and a regular batch bolt is when <code>finishBatch</code> is called. A committer bolt has <code>finishedBatch</code> called during the commit phase. The commit phase is guaranteed to occur only after all prior batches have successfully committed, and it will be retried until all bolts in the topology succeed the commit for the batch. There are two ways to make a <code>BatchBolt</code> a committer, by having the <code>BatchBolt</code> implement the <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/transactional/ICommitter.html" target="_blank">ICommitter</a> marker interface, or by using the <code>setCommiterBolt</code> method in <code>TransactionalTopologyBuilder</code>.</li>
</ol>

<h3>Processing phase vs. commit phase in bolts</h3>

<p>To nail down the difference between the processing phase and commit phase of a transaction, let's look at an example topology:</p>

<p><img src="transactional-commit-flow.png" alt="Storm cluster"></p>

<p>In this topology, only the bolts with a red outline are committers.</p>

<p>During the processing phase, bolt A will process the complete batch from the spout, call <code>finishBatch</code> and send its tuples to bolts B and C. Bolt B is a committer so it will process all the tuples but finishBatch won't be called. Bolt C also will not have <code>finishBatch</code> called because it doesn't know if it has received all the tuples from Bolt B yet (because Bolt B is waiting for the transaction to commit). Finally, Bolt D will receive any tuples Bolt C emitted during invocations of its <code>execute</code> method.</p>

<p>When the batch commits, <code>finishBatch</code> is called on Bolt B. Once it finishes, Bolt C can now detect that it has received all the tuples and will call <code>finishBatch</code>. Finally, Bolt D will receive its complete batch and call <code>finishBatch</code>.</p>

<p>Notice that even though Bolt D is a committer, it doesn't have to wait for a second commit message when it receives the whole batch. Since it receives the whole batch during the commit phase, it goes ahead and completes the transaction.</p>

<p>Committer bolts act just like batch bolts during the commit phase. The only difference between committer bolts and batch bolts is that committer bolts will not call <code>finishBatch</code> during the processing phase of a transaction.</p>

<h3>Acking</h3>

<p>Notice that you don't have to do any acking or anchoring when working with transactional topologies. Storm manages all of that underneath the hood. The acking strategy is heavily optimized.</p>

<h3>Failing a transaction</h3>

<p>When using regular bolts, you can call the <code>fail</code> method on <code>OutputCollector</code> to fail the tuple trees of which that tuple is a member. Since transactional topologies hide the acking framework from you, they provide a different mechanism to fail a batch (and cause the batch to be replayed). Just throw a <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/topology/FailedException.html" target="_blank">FailedException</a>. Unlike regular exceptions, this will only cause that particular batch to replay and will not crash the process.</p>

<h3 id="transactional-spout">Transactional spout</h3>

<p>The <code>TransactionalSpout</code> interface is completely different from a regular <code>Spout</code> interface. A <code>TransactionalSpout</code> implementation emits batches of tuples and must ensure that the same batch of tuples is always emitted for the same transaction id.</p>

<p>A transactional spout looks like this while a topology is executing:</p>

<p><img src="transactional-spout-structure.png" alt="Storm cluster"></p>

<p>The coordinator on the left is a regular Storm spout that emits a tuple whenever a batch should be emitted for a transaction. The emitters execute as a regular Storm bolt and are responsible for emitting the actual tuples for the batch. The emitters subscribe to the "batch emit" stream of the coordinator using an all grouping.</p>

<p>The need to be idempotent with respect to the tuples it emits requires a <code>TransactionalSpout</code> to store a small amount of state. The state is stored in Zookeeper.</p>

<p>The details of implementing a <code>TransactionalSpout</code> are in <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/transactional/ITransactionalSpout.html" target="_blank">the Javadoc</a>.</p>

<h3>Partitioned Transactional Spout</h3>

<p>A common kind of transactional spout is one that reads the batches from a set of partitions across many queue brokers. For example, this is how <a href="https://github.com/apache/storm/tree/master/external/storm-kafka/src/jvm/storm/kafka/TransactionalKafkaSpout.java" target="_blank">TransactionalKafkaSpout</a> works. An <code>IPartitionedTransactionalSpout</code> automates the bookkeeping work of managing the state for each partition to ensure idempotent replayability. See <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/transactional/partitioned/IPartitionedTransactionalSpout.html" target="_blank">the Javadoc</a> for more details.</p>

<h3>Configuration</h3>

<p>There's two important bits of configuration for transactional topologies:</p>

<ol>
<li><em>Zookeeper:</em> By default, transactional topologies will store state in the same Zookeeper instance as used to manage the Storm cluster. You can override this with the "transactional.zookeeper.servers" and "transactional.zookeeper.port" configs.</li>
<li><em>Number of active batches permissible at once:</em> You must set a limit to the number of batches that can be processed at once. You configure this using the "topology.max.spout.pending" config. If you don't set this config, it will default to 1.</li>
</ol>

<h3>What if you can't emit the same batch of tuples for a given transaction id?</h3>

<p>So far the discussion around transactional topologies has assumed that you can always emit the exact same batch of tuples for the same transaction id. So what do you do if this is not possible?</p>

<p>Consider an example of when this is not possible. Suppose you are reading tuples from a partitioned message broker (stream is partitioned across many machines), and a single transaction will include tuples from all the individual machines. Now suppose one of the nodes goes down at the same time that a transaction fails. Without that node, it is impossible to replay the same batch of tuples you just played for that transaction id. The processing in your topology will halt as its unable to replay the identical batch. The only possible solution is to emit a different batch for that transaction id than you emitted before. Is it possible to still achieve exactly-once messaging semantics even if the batches change?</p>

<p>It turns out that you can still achieve exactly-once messaging semantics in your processing with a non-idempotent transactional spout, although this requires a bit more work on your part in developing the topology.</p>

<p>If a batch can change for a given transaction id, then the logic we've been using so far of "skip the update if the transaction id in the database is the same as the id for the current transaction" is no longer valid. This is because the current batch is different than the batch for the last time the transaction was committed, so the result will not necessarily be the same. You can fix this problem by storing a little bit more state in the database. Let's again use the example of storing a global count in the database and suppose the partial count for the batch is stored in the <code>partialCount</code> variable.</p>

<p>Instead of storing a value in the database that looks like this:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="kd">class</span> <span class="nc">Value</span> <span class="o">{</span>
  <span class="n">Object</span> <span class="n">count</span><span class="o">;</span>
  <span class="n">BigInteger</span> <span class="n">txid</span><span class="o">;</span>
<span class="o">}</span>
</code></pre></div>
<p>For non-idempotent transactional spouts you should instead store a value that looks like this:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="kd">class</span> <span class="nc">Value</span> <span class="o">{</span>
  <span class="n">Object</span> <span class="n">count</span><span class="o">;</span>
  <span class="n">BigInteger</span> <span class="n">txid</span><span class="o">;</span>
  <span class="n">Object</span> <span class="n">prevCount</span><span class="o">;</span>
<span class="o">}</span>
</code></pre></div>
<p>The logic for the update is as follows:</p>

<ol>
<li>If the transaction id for the current batch is the same as the transaction id in the database, set <code>val.count = val.prevCount + partialCount</code>.</li>
<li>Otherwise, set <code>val.prevCount = val.count</code>, <code>val.count = val.count + partialCount</code> and <code>val.txid = batchTxid</code>.</li>
</ol>

<p>This logic works because once you commit a particular transaction id for the first time, all prior transaction ids will never be committed again.</p>

<p>There's a few more subtle aspects of transactional topologies that make opaque transactional spouts possible.</p>

<p>When a transaction fails, all subsequent transactions in the processing phase are considered failed as well. Each of those transactions will be re-emitted and reprocessed. Without this behavior, the following situation could happen:</p>

<ol>
<li>Transaction A emits tuples 1-50</li>
<li>Transaction B emits tuples 51-100</li>
<li>Transaction A fails</li>
<li>Transaction A emits tuples 1-40</li>
<li>Transaction A commits</li>
<li>Transaction B commits</li>
<li>Transaction C emits tuples 101-150</li>
</ol>

<p>In this scenario, tuples 41-50 are skipped. By failing all subsequent transactions, this would happen instead:</p>

<ol>
<li>Transaction A emits tuples 1-50</li>
<li>Transaction B emits tuples 51-100</li>
<li>Transaction A fails (and causes Transaction B to fail)</li>
<li>Transaction A emits tuples 1-40</li>
<li>Transaction B emits tuples 41-90</li>
<li>Transaction A commits</li>
<li>Transaction B commits</li>
<li>Transaction C emits tuples 91-140</li>
</ol>

<p>By failing all subsequent transactions on failure, no tuples are skipped. This also shows that a requirement of transactional spouts is that they always emit where the last transaction left off.</p>

<p>A non-idempotent transactional spout is more concisely referred to as an "OpaqueTransactionalSpout" (opaque is the opposite of idempotent). <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/transactional/partitioned/IOpaquePartitionedTransactionalSpout.html" target="_blank">IOpaquePartitionedTransactionalSpout</a> is an interface for implementing opaque partitioned transactional spouts, of which <a href="https://github.com/apache/storm/tree/master/external/storm-kafka/src/jvm/storm/kafka/OpaqueTransactionalKafkaSpout.java" target="_blank">OpaqueTransactionalKafkaSpout</a> is an example. <code>OpaqueTransactionalKafkaSpout</code> can withstand losing individual Kafka nodes without sacrificing accuracy as long as you use the update strategy as explained in this section.</p>

<h3>Implementation</h3>

<p>The implementation for transactional topologies is very elegant. Managing the commit protocol, detecting failures, and pipelining batches seem complex, but everything turns out to be a straightforward mapping to Storm's primitives.</p>

<p>How the data flow works:</p>

<p>Here's how transactional spout works:</p>

<ol>
<li>Transactional spout is a subtopology consisting of a coordinator spout and an emitter bolt</li>
<li>The coordinator is a regular spout with a parallelism of 1</li>
<li>The emitter is a bolt with a parallelism of P, connected to the coordinator's "batch" stream using an all grouping</li>
<li>When the coordinator determines it's time to enter the processing phase for a transaction, it emits a tuple containing the TransactionAttempt and the metadata for that transaction to the "batch" stream</li>
<li>Because of the all grouping, every single emitter task receives the notification that it's time to emit its portion of the tuples for that transaction attempt</li>
<li>Storm automatically manages the anchoring/acking necessary throughout the whole topology to determine when a transaction has completed the processing phase. The key here is that *the root tuple was created by the coordinator, so the coordinator will receive an "ack" if the processing phase succeeds, and a "fail" if it doesn't succeed for any reason (failure or timeout).</li>
<li>If the processing phase succeeds, and all prior transactions have successfully committed, the coordinator emits a tuple containing the TransactionAttempt to the "commit" stream.</li>
<li>All committing bolts subscribe to the commit stream using an all grouping, so that they will all receive a notification when the commit happens.</li>
<li>Like the processing phase, the coordinator uses the acking framework to determine whether the commit phase succeeded or not. If it receives an "ack", it marks that transaction as complete in zookeeper.</li>
</ol>

<p>More notes:</p>

<ul>
<li>Transactional spouts are a sub-topology consisting of a spout and a bolt

<ul>
<li>the spout is the coordinator and contains a single task</li>
<li>the bolt is the emitter</li>
<li>the bolt subscribes to the coordinator with an all grouping</li>
<li>serialization of metadata is handled by kryo. kryo is initialized ONLY with the registrations defined in the component configuration for the transactionalspout</li>
</ul></li>
<li>the coordinator uses the acking framework to determine when a batch has been successfully processed, and then to determine when a batch has been successfully committed.</li>
<li>state is stored in zookeeper using RotatingTransactionalState</li>
<li>commiting bolts subscribe to the coordinators commit stream using an all grouping</li>
<li>CoordinatedBolt is used to detect when a bolt has received all the tuples for a particular batch.

<ul>
<li>this is the same abstraction that is used in DRPC</li>
<li>for commiting bolts, it waits to receive a tuple from the coordinator's commit stream before calling finishbatch</li>
<li>so it can't call finishbatch until it's received all tuples from all subscribed components AND its received the commit stream tuple (for committers). this ensures that it can't prematurely call finishBatch</li>
</ul></li>
</ul>
            </div>
        </div>
    </div>
</div>
<!--Content End-->
