---
layout: documentation
title: Trident Spouts
---
<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-12">
            	<h1 class="page-title">Trident Spouts</h1>
<p>Like in the vanilla Storm API, spouts are the source of streams in a Trident topology. On top of the vanilla Storm spouts, Trident exposes additional APIs for more sophisticated spouts.</p>

<p>There is an inextricable link between how you source your data streams and how you update state (e.g. databases) based on those data streams. See <a href="Trident-state.html">Trident state doc</a> for an explanation of this – understanding this link is imperative for understanding the spout options available.</p>

<p>Regular Storm spouts will be non-transactional spouts in a Trident topology. To use a regular Storm IRichSpout, create the stream like this in a TridentTopology:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="n">TridentTopology</span> <span class="n">topology</span> <span class="o">=</span> <span class="k">new</span> <span class="nf">TridentTopology</span><span class="o">();</span>
<span class="n">topology</span><span class="o">.</span><span class="na">newStream</span><span class="o">(</span><span class="s">"myspoutid"</span><span class="o">,</span> <span class="k">new</span> <span class="nf">MyRichSpout</span><span class="o">());</span>
</code></pre></div>
<p>All spouts in a Trident topology are required to be given a unique identifier for the stream – this identifier must be unique across all topologies run on the cluster. Trident will use this identifier to store metadata about what the spout has consumed in Zookeeper, including the txid and any metadata associated with the spout.</p>

<p>You can configure the Zookeeper storage of spout metadata via the following configuration options:</p>

<ol>
<li><code>transactional.zookeeper.servers</code>: A list of Zookeeper hostnames </li>
<li><code>transactional.zookeeper.port</code>: The port of the Zookeeper cluster</li>
<li><code>transactional.zookeeper.root</code>: The root dir in Zookeeper where metadata is stored. Metadata will be stored at the path <root path="">/<spout id=""></spout></root></li>
</ol>

<h3>Pipelining</h3>

<p>By default, Trident processes a single batch at a time, waiting for the batch to succeed or fail before trying another batch. You can get significantly higher throughput – and lower latency of processing of each batch – by pipelining the batches. You configure the maximum amount of batches to be processed simultaneously with the "topology.max.spout.pending" property. </p>

<p>Even while processing multiple batches simultaneously, Trident will order any state updates taking place in the topology among batches. For example, suppose you're doing a global count aggregation into a database. The idea is that while you're updating the count in the database for batch 1, you can still be computing the partial counts for batches 2 through 10. Trident won't move on to the state updates for batch 2 until the state updates for batch 1 have succeeded. This is essential for achieving exactly-once processing semantics, as outline in <a href="Trident-state.html">Trident state doc</a>.</p>

<h3>Trident spout types</h3>

<p>Here are the following spout APIs available:</p>

<ol>
<li><a href="https://github.com/apache/storm/blob/master/storm-core/src/jvm/storm/trident/spout/ITridentSpout.java" target="_blank">ITridentSpout</a>: The most general API that can support transactional or opaque transactional semantics. Generally you'll use one of the partitioned flavors of this API rather than this one directly.</li>
<li><a href="https://github.com/apache/storm/blob/master/storm-core/src/jvm/storm/trident/spout/IBatchSpout.java" target="_blank">IBatchSpout</a>: A non-transactional spout that emits batches of tuples at a time</li>
<li><a href="https://github.com/apache/storm/blob/master/storm-core/src/jvm/storm/trident/spout/IPartitionedTridentSpout.java" target="_blank">IPartitionedTridentSpout</a>: A transactional spout that reads from a partitioned data source (like a cluster of Kafka servers)</li>
<li><a href="https://github.com/apache/storm/blob/master/storm-core/src/jvm/storm/trident/spout/IOpaquePartitionedTridentSpout.java" target="_blank">IOpaquePartitionedTridentSpout</a>: An opaque transactional spout that reads from a partitioned data source</li>
</ol>

<p>And, like mentioned in the beginning of this tutorial, you can use regular IRichSpout's as well.</p>
            </div>
        </div>
    </div>
</div>
<!--Content End-->
