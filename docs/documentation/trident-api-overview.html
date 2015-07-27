---
layout: documentation
title: Trident API Overview
---
<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-12">
            	<h1 class="page-title">Trident API Overview</h1>
                <p>The core data model in Trident is the "Stream", processed as a series of batches. A stream is partitioned among the nodes in the cluster, and operations applied to a stream are applied in parallel across each partition.</p>
                <p>There are five kinds of operations in Trident:</p>
                <ol>
                    <li>Operations that apply locally to each partition and cause no network transfer</li>
                    <li>Repartitioning operations that repartition a stream but otherwise don't change the contents (involves network transfer)</li>
                    <li>Aggregation operations that do network transfer as part of the operation</li>
                    <li>Operations on grouped streams</li>
                    <li>Merges and joins</li>
                </ol>
                <h3>Partition-local operations</h3>
                <p>Partition-local operations involve no network transfer and are applied to each batch partition independently.</p>
                <h3>Functions</h3>
                <p>A function takes in a set of input fields and emits zero or more tuples as output. The fields of the output tuple are appended to the original input tuple in the stream. If a function emits no tuples, the original input tuple is filtered out. Otherwise, the input tuple is duplicated for each output tuple. Suppose you have this function:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="kd">public</span> <span class="kd">class</span> <span class="nc">MyFunction</span> <span class="kd">extends</span> <span class="n">BaseFunction</span> <span class="o">{</span>
    <span class="kd">public</span> <span class="kt">void</span> <span class="nf">execute</span><span class="o">(</span><span class="n">TridentTuple</span> <span class="n">tuple</span><span class="o">,</span> <span class="n">TridentCollector</span> <span class="n">collector</span><span class="o">)</span> <span class="o">{</span>
        <span class="k">for</span><span class="o">(</span><span class="kt">int</span> <span class="n">i</span><span class="o">=</span><span class="mi">0</span><span class="o">;</span> <span class="n">i</span> <span class="o">&lt;</span> <span class="n">tuple</span><span class="o">.</span><span class="na">getInteger</span><span class="o">(</span><span class="mi">0</span><span class="o">);</span> <span class="n">i</span><span class="o">++)</span> <span class="o">{</span>
            <span class="n">collector</span><span class="o">.</span><span class="na">emit</span><span class="o">(</span><span class="k">new</span> <span class="nf">Values</span><span class="o">(</span><span class="n">i</span><span class="o">));</span>
        <span class="o">}</span>
    <span class="o">}</span>
<span class="o">}</span>
</code></pre></div>

                <p>Now suppose you have a stream in the variable "mystream" with the fields ["a", "b", "c"] with the following tuples:</p>

<div class="highlight"><pre><code class="language-text" data-lang="text">[1, 2, 3]
[4, 1, 6]
[3, 0, 8]
</code></pre></div>

                <p>If you run this code:</p>

<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="n">mystream</span><span class="o">.</span><span class="na">each</span><span class="o">(</span><span class="k">new</span> <span class="nf">Fields</span><span class="o">(</span><span class="s">"b"</span><span class="o">),</span> <span class="k">new</span> <span class="nf">MyFunction</span><span class="o">(),</span> <span class="k">new</span> <span class="nf">Fields</span><span class="o">(</span><span class="s">"d"</span><span class="o">)))</span>
</code></pre></div>
<p>The resulting tuples would have fields ["a", "b", "c", "d"] and look like this:</p>
<div class="highlight"><pre><code class="language-text" data-lang="text">[1, 2, 3, 0]
[1, 2, 3, 1]
[4, 1, 6, 0]
</code></pre></div>
<h3 id="filters">Filters</h3>

                <p>Filters take in a tuple as input and decide whether or not to keep that tuple or not. Suppose you had this filter:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="kd">public</span> <span class="kd">class</span> <span class="nc">MyFilter</span> <span class="kd">extends</span> <span class="n">BaseFilter</span> <span class="o">{</span>
    <span class="kd">public</span> <span class="kt">boolean</span> <span class="nf">isKeep</span><span class="o">(</span><span class="n">TridentTuple</span> <span class="n">tuple</span><span class="o">)</span> <span class="o">{</span>
        <span class="k">return</span> <span class="n">tuple</span><span class="o">.</span><span class="na">getInteger</span><span class="o">(</span><span class="mi">0</span><span class="o">)</span> <span class="o">==</span> <span class="mi">1</span> <span class="o">&amp;&amp;</span> <span class="n">tuple</span><span class="o">.</span><span class="na">getInteger</span><span class="o">(</span><span class="mi">1</span><span class="o">)</span> <span class="o">==</span> <span class="mi">2</span><span class="o">;</span>
    <span class="o">}</span>
<span class="o">}</span>
</code></pre></div>
                <p>Now suppose you had these tuples with fields ["a", "b", "c"]:</p>
<div class="highlight"><pre><code class="language-text" data-lang="text">[1, 2, 3]
[2, 1, 1]
[2, 3, 4]
</code></pre></div>
                <p>If you ran this code:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="n">mystream</span><span class="o">.</span><span class="na">each</span><span class="o">(</span><span class="k">new</span> <span class="nf">Fields</span><span class="o">(</span><span class="s">"b"</span><span class="o">,</span> <span class="s">"a"</span><span class="o">),</span> <span class="k">new</span> <span class="nf">MyFilter</span><span class="o">())</span>
</code></pre></div>
                <p>The resulting tuples would be:</p>
<div class="highlight"><pre><code class="language-text" data-lang="text">[2, 1, 1]
</code></pre></div>
                <h3>partitionAggregate</h3>
                <p>partitionAggregate runs a function on each partition of a batch of tuples. Unlike functions, the tuples emitted by partitionAggregate replace the input tuples given to it. Consider this example:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="n">mystream</span><span class="o">.</span><span class="na">partitionAggregate</span><span class="o">(</span><span class="k">new</span> <span class="nf">Fields</span><span class="o">(</span><span class="s">"b"</span><span class="o">),</span> <span class="k">new</span> <span class="nf">Sum</span><span class="o">(),</span> <span class="k">new</span> <span class="nf">Fields</span><span class="o">(</span><span class="s">"sum"</span><span class="o">))</span>
</code></pre></div>
                <p>Suppose the input stream contained fields ["a", "b"] and the following partitions of tuples:</p>
<div class="highlight"><pre><code class="language-text" data-lang="text">Partition 0:
["a", 1]
["b", 2]

Partition 1:
["a", 3]
["c", 8]

Partition 2:
["e", 1]
["d", 9]
["d", 10]
</code></pre></div>
                <p>Then the output stream of that code would contain these tuples with one field called "sum":</p>
<div class="highlight"><pre><code class="language-text" data-lang="text">Partition 0:
[3]

Partition 1:
[11]

Partition 2:
[20]
</code></pre></div>
                <p>There are three different interfaces for defining aggregators: CombinerAggregator, ReducerAggregator, and Aggregator.</p>
                <p>Here's the interface for CombinerAggregator:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="kd">public</span> <span class="kd">interface</span> <span class="nc">CombinerAggregator</span><span class="o">&lt;</span><span class="n">T</span><span class="o">&gt;</span> <span class="kd">extends</span> <span class="n">Serializable</span> <span class="o">{</span>
    <span class="n">T</span> <span class="nf">init</span><span class="o">(</span><span class="n">TridentTuple</span> <span class="n">tuple</span><span class="o">);</span>
    <span class="n">T</span> <span class="nf">combine</span><span class="o">(</span><span class="n">T</span> <span class="n">val1</span><span class="o">,</span> <span class="n">T</span> <span class="n">val2</span><span class="o">);</span>
    <span class="n">T</span> <span class="nf">zero</span><span class="o">();</span>
<span class="o">}</span>
</code></pre></div>
                <p>A CombinerAggregator returns a single tuple with a single field as output. CombinerAggregators run the init function on each input tuple and use the combine function to combine values until there's only one value left. If there's no tuples in the partition, the CombinerAggregator emits the output of the zero function. For example, here's the implementation of Count:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="kd">public</span> <span class="kd">class</span> <span class="nc">Count</span> <span class="kd">implements</span> <span class="n">CombinerAggregator</span><span class="o">&lt;</span><span class="n">Long</span><span class="o">&gt;</span> <span class="o">{</span>
    <span class="kd">public</span> <span class="n">Long</span> <span class="nf">init</span><span class="o">(</span><span class="n">TridentTuple</span> <span class="n">tuple</span><span class="o">)</span> <span class="o">{</span>
        <span class="k">return</span> <span class="mi">1L</span><span class="o">;</span>
    <span class="o">}</span>

    <span class="kd">public</span> <span class="n">Long</span> <span class="nf">combine</span><span class="o">(</span><span class="n">Long</span> <span class="n">val1</span><span class="o">,</span> <span class="n">Long</span> <span class="n">val2</span><span class="o">)</span> <span class="o">{</span>
        <span class="k">return</span> <span class="n">val1</span> <span class="o">+</span> <span class="n">val2</span><span class="o">;</span>
    <span class="o">}</span>

    <span class="kd">public</span> <span class="n">Long</span> <span class="nf">zero</span><span class="o">()</span> <span class="o">{</span>
        <span class="k">return</span> <span class="mi">0L</span><span class="o">;</span>
    <span class="o">}</span>
<span class="o">}</span>
</code></pre></div>
                <p>The benefits of CombinerAggregators are seen when you use them with the aggregate method instead of partitionAggregate. In that case, Trident automatically optimizes the computation by doing partial aggregations before transferring tuples over the network.</p>
                <p>A ReducerAggregator has the following interface:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="kd">public</span> <span class="kd">interface</span> <span class="nc">ReducerAggregator</span><span class="o">&lt;</span><span class="n">T</span><span class="o">&gt;</span> <span class="kd">extends</span> <span class="n">Serializable</span> <span class="o">{</span>
    <span class="n">T</span> <span class="nf">init</span><span class="o">();</span>
    <span class="n">T</span> <span class="nf">reduce</span><span class="o">(</span><span class="n">T</span> <span class="n">curr</span><span class="o">,</span> <span class="n">TridentTuple</span> <span class="n">tuple</span><span class="o">);</span>
<span class="o">}</span>
</code></pre></div>
                <p>A ReducerAggregator produces an initial value with init, and then it iterates on that value for each input tuple to produce a single tuple with a single value as output. For example, here's how you would define Count as a ReducerAggregator:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="kd">public</span> <span class="kd">class</span> <span class="nc">Count</span> <span class="kd">implements</span> <span class="n">ReducerAggregator</span><span class="o">&lt;</span><span class="n">Long</span><span class="o">&gt;</span> <span class="o">{</span>
    <span class="kd">public</span> <span class="n">Long</span> <span class="nf">init</span><span class="o">()</span> <span class="o">{</span>
        <span class="k">return</span> <span class="mi">0L</span><span class="o">;</span>
    <span class="o">}</span>

    <span class="kd">public</span> <span class="n">Long</span> <span class="nf">reduce</span><span class="o">(</span><span class="n">Long</span> <span class="n">curr</span><span class="o">,</span> <span class="n">TridentTuple</span> <span class="n">tuple</span><span class="o">)</span> <span class="o">{</span>
        <span class="k">return</span> <span class="n">curr</span> <span class="o">+</span> <span class="mi">1</span><span class="o">;</span>
    <span class="o">}</span>
<span class="o">}</span>
</code></pre></div>
                <p>ReducerAggregator can also be used with persistentAggregate, as you'll see later.</p>
                <p>The most general interface for performing aggregations is Aggregator, which looks like this:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="kd">public</span> <span class="kd">interface</span> <span class="nc">Aggregator</span><span class="o">&lt;</span><span class="n">T</span><span class="o">&gt;</span> <span class="kd">extends</span> <span class="n">Operation</span> <span class="o">{</span>
    <span class="n">T</span> <span class="nf">init</span><span class="o">(</span><span class="n">Object</span> <span class="n">batchId</span><span class="o">,</span> <span class="n">TridentCollector</span> <span class="n">collector</span><span class="o">);</span>
    <span class="kt">void</span> <span class="nf">aggregate</span><span class="o">(</span><span class="n">T</span> <span class="n">state</span><span class="o">,</span> <span class="n">TridentTuple</span> <span class="n">tuple</span><span class="o">,</span> <span class="n">TridentCollector</span> <span class="n">collector</span><span class="o">);</span>
    <span class="kt">void</span> <span class="nf">complete</span><span class="o">(</span><span class="n">T</span> <span class="n">state</span><span class="o">,</span> <span class="n">TridentCollector</span> <span class="n">collector</span><span class="o">);</span>
<span class="o">}</span>
</code></pre></div>
                <p>Aggregators can emit any number of tuples with any number of fields. They can emit tuples at any point during execution. Aggregators execute in the following way:</p>
                <ol>
                    <li>The init method is called before processing the batch. The return value of init is an Object that will represent the state of the aggregation and will be passed into the aggregate and complete methods.</li>
                    <li>The aggregate method is called for each input tuple in the batch partition. This method can update the state and optionally emit tuples.</li>
                    <li>The complete method is called when all tuples for the batch partition have been processed by aggregate. </li>
                </ol>
                <p>Here's how you would implement Count as an Aggregator:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="kd">public</span> <span class="kd">class</span> <span class="nc">CountAgg</span> <span class="kd">extends</span> <span class="n">BaseAggregator</span><span class="o">&lt;</span><span class="n">CountState</span><span class="o">&gt;</span> <span class="o">{</span>
    <span class="kd">static</span> <span class="kd">class</span> <span class="nc">CountState</span> <span class="o">{</span>
        <span class="kt">long</span> <span class="n">count</span> <span class="o">=</span> <span class="mi">0</span><span class="o">;</span>
    <span class="o">}</span>

    <span class="kd">public</span> <span class="n">CountState</span> <span class="nf">init</span><span class="o">(</span><span class="n">Object</span> <span class="n">batchId</span><span class="o">,</span> <span class="n">TridentCollector</span> <span class="n">collector</span><span class="o">)</span> <span class="o">{</span>
        <span class="k">return</span> <span class="k">new</span> <span class="nf">CountState</span><span class="o">();</span>
    <span class="o">}</span>

    <span class="kd">public</span> <span class="kt">void</span> <span class="nf">aggregate</span><span class="o">(</span><span class="n">CountState</span> <span class="n">state</span><span class="o">,</span> <span class="n">TridentTuple</span> <span class="n">tuple</span><span class="o">,</span> <span class="n">TridentCollector</span> <span class="n">collector</span><span class="o">)</span> <span class="o">{</span>
        <span class="n">state</span><span class="o">.</span><span class="na">count</span><span class="o">+=</span><span class="mi">1</span><span class="o">;</span>
    <span class="o">}</span>

    <span class="kd">public</span> <span class="kt">void</span> <span class="nf">complete</span><span class="o">(</span><span class="n">CountState</span> <span class="n">state</span><span class="o">,</span> <span class="n">TridentCollector</span> <span class="n">collector</span><span class="o">)</span> <span class="o">{</span>
        <span class="n">collector</span><span class="o">.</span><span class="na">emit</span><span class="o">(</span><span class="k">new</span> <span class="nf">Values</span><span class="o">(</span><span class="n">state</span><span class="o">.</span><span class="na">count</span><span class="o">));</span>
    <span class="o">}</span>
<span class="o">}</span>
</code></pre></div>
<p>Sometimes you want to execute multiple aggregators at the same time. This is called chaining and can be accomplished like this:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="n">mystream</span><span class="o">.</span><span class="na">chainedAgg</span><span class="o">()</span>
        <span class="o">.</span><span class="na">partitionAggregate</span><span class="o">(</span><span class="k">new</span> <span class="nf">Count</span><span class="o">(),</span> <span class="k">new</span> <span class="nf">Fields</span><span class="o">(</span><span class="s">"count"</span><span class="o">))</span>
        <span class="o">.</span><span class="na">partitionAggregate</span><span class="o">(</span><span class="k">new</span> <span class="nf">Fields</span><span class="o">(</span><span class="s">"b"</span><span class="o">),</span> <span class="k">new</span> <span class="nf">Sum</span><span class="o">(),</span> <span class="k">new</span> <span class="nf">Fields</span><span class="o">(</span><span class="s">"sum"</span><span class="o">))</span>
        <span class="o">.</span><span class="na">chainEnd</span><span class="o">()</span>
</code></pre></div>
<p>This code will run the Count and Sum aggregators on each partition. The output will contain a single tuple with the fields ["count", "sum"].</p>

<h3>stateQuery and partitionPersist</h3>

<p>stateQuery and partitionPersist query and update sources of state, respectively. You can read about how to use them on <a href="Trident-state.html">Trident state doc</a>.</p>

<h3>projection</h3>

<p>The projection method on Stream keeps only the fields specified in the operation. If you had a Stream with fields ["a", "b", "c", "d"] and you ran this code:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="n">mystream</span><span class="o">.</span><span class="na">project</span><span class="o">(</span><span class="k">new</span> <span class="nf">Fields</span><span class="o">(</span><span class="s">"b"</span><span class="o">,</span> <span class="s">"d"</span><span class="o">))</span>
</code></pre></div>
<p>The output stream would contain only the fields ["b", "d"].</p>

<h3>Repartitioning operations</h3>

<p>Repartitioning operations run a function to change how the tuples are partitioned across tasks. The number of partitions can also change as a result of repartitioning (for example, if the parallelism hint is greater after repartioning). Repartitioning requires network transfer. Here are the repartitioning functions:</p>

<ol>
<li>shuffle: Use random round robin algorithm to evenly redistribute tuples across all target partitions</li>
<li>broadcast: Every tuple is replicated to all target partitions. This can useful during DRPC – for example, if you need to do a stateQuery on every partition of data.</li>
<li>partitionBy: partitionBy takes in a set of fields and does semantic partitioning based on that set of fields. The fields are hashed and modded by the number of target partitions to select the target partition. partitionBy guarantees that the same set of fields always goes to the same target partition.</li>
<li>global: All tuples are sent to the same partition. The same partition is chosen for all batches in the stream.</li>
<li>batchGlobal: All tuples in the batch are sent to the same partition. Different batches in the stream may go to different partitions. </li>
<li>partition: This method takes in a custom partitioning function that implements backtype.storm.grouping.CustomStreamGrouping</li>
</ol>

<h3>Aggregation operations</h3>

<p>Trident has aggregate and persistentAggregate methods for doing aggregations on Streams. aggregate is run on each batch of the stream in isolation, while persistentAggregate will aggregation on all tuples across all batches in the stream and store the result in a source of state.</p>

<p>Running aggregate on a Stream does a global aggregation. When you use a ReducerAggregator or an Aggregator, the stream is first repartitioned into a single partition, and then the aggregation function is run on that partition. When you use a CombinerAggregator, on the other hand, first Trident will compute partial aggregations of each partition, then repartition to a single partition, and then finish the aggregation after the network transfer. CombinerAggregator's are far more efficient and should be used when possible.</p>

<p>Here's an example of using aggregate to get a global count for a batch:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="n">mystream</span><span class="o">.</span><span class="na">aggregate</span><span class="o">(</span><span class="k">new</span> <span class="nf">Count</span><span class="o">(),</span> <span class="k">new</span> <span class="nf">Fields</span><span class="o">(</span><span class="s">"count"</span><span class="o">))</span>
</code></pre></div>
<p>Like partitionAggregate, aggregators for aggregate can be chained. However, if you chain a CombinerAggregator with a non-CombinerAggregator, Trident is unable to do the partial aggregation optimization.</p>

<p>You can read more about how to use persistentAggregate in the <a href="https://github.com/apache/storm/wiki/Trident-state">Trident state doc</a>.</p>

<h3>Operations on grouped streams</h3>

<p>The groupBy operation repartitions the stream by doing a partitionBy on the specified fields, and then within each partition groups tuples together whose group fields are equal. For example, here's an illustration of a groupBy operation:</p>

<p><img src="grouping.png" alt="Grouping"></p>

<p>If you run aggregators on a grouped stream, the aggregation will be run within each group instead of against the whole batch. persistentAggregate can also be run on a GroupedStream, in which case the results will be stored in a <a href="https://github.com/apache/storm/blob/master/storm-core/src/jvm/storm/trident/state/map/MapState.java">MapState</a> with the key being the grouping fields. You can read more about persistentAggregate in the <a href="Trident-state.html">Trident state doc</a>.</p>

<p>Like regular streams, aggregators on grouped streams can be chained.</p>

<h3>Merges and joins</h3>

<p>The last part of the API is combining different streams together. The simplest way to combine streams is to merge them into one stream. You can do that with the TridentTopology#merge method, like so:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="n">topology</span><span class="o">.</span><span class="na">merge</span><span class="o">(</span><span class="n">stream1</span><span class="o">,</span> <span class="n">stream2</span><span class="o">,</span> <span class="n">stream3</span><span class="o">);</span>
</code></pre></div>
<p>Trident will name the output fields of the new, merged stream as the output fields of the first stream.</p>

<p>Another way to combine streams is with a join. Now, a standard join, like the kind from SQL, require finite input. So they don't make sense with infinite streams. Joins in Trident only apply within each small batch that comes off of the spout. </p>

<p>Here's an example join between a stream containing fields ["key", "val1", "val2"] and another stream containing ["x", "val1"]:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="n">topology</span><span class="o">.</span><span class="na">join</span><span class="o">(</span><span class="n">stream1</span><span class="o">,</span> <span class="k">new</span> <span class="nf">Fields</span><span class="o">(</span><span class="s">"key"</span><span class="o">),</span> <span class="n">stream2</span><span class="o">,</span> <span class="k">new</span> <span class="nf">Fields</span><span class="o">(</span><span class="s">"x"</span><span class="o">),</span> <span class="k">new</span> <span class="nf">Fields</span><span class="o">(</span><span class="s">"key"</span><span class="o">,</span> <span class="s">"a"</span><span class="o">,</span> <span class="s">"b"</span><span class="o">,</span> <span class="s">"c"</span><span class="o">));</span>
</code></pre></div>
<p>This joins stream1 and stream2 together using "key" and "x" as the join fields for each respective stream. Then, Trident requires that all the output fields of the new stream be named, since the input streams could have overlapping field names. The tuples emitted from the join will contain:</p>
<ol>
<li>First, the list of join fields. In this case, "key" corresponds to "key" from stream1 and "x" from stream2.</li>
<li>Next, a list of all non-join fields from all streams, in order of how the streams were passed to the join method. In this case, "a" and "b" correspond to "val1" and "val2" from stream1, and "c" corresponds to "val1" from stream2.</li>
</ol>
<p>When a join happens between streams originating from different spouts, those spouts will be synchronized with how they emit batches. That is, a batch of processing will include tuples from each spout.</p>
<p>You might be wondering – how do you do something like a "windowed join", where tuples from one side of the join are joined against the last hour of tuples from the other side of the join.</p>
<p>To do this, you would make use of partitionPersist and stateQuery. The last hour of tuples from one side of the join would be stored and rotated in a source of state, keyed by the join field. Then the stateQuery would do lookups by the join field to perform the "join".</p>
            </div>
        </div>
    </div>
</div>
<!--Content End-->
