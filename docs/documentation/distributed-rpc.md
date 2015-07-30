---
layout: documentation
title: Distributed RPC
---
<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-12">
            	<h1 class="page-title">Distributed RPC</h1>
<p>The idea behind distributed RPC (DRPC) is to parallelize the computation of really intense functions on the fly using Storm. The Storm topology takes in as input a stream of function arguments, and it emits an output stream of the results for each of those function calls. </p>

<p>DRPC is not so much a feature of Storm as it is a pattern expressed from Storm's primitives of streams, spouts, bolts, and topologies. DRPC could have been packaged as a separate library from Storm, but it's so useful that it's bundled with Storm.</p>

<h3>High level overview</h3>

<p>Distributed RPC is coordinated by a "DRPC server" (Storm comes packaged with an implementation of this). The DRPC server coordinates receiving an RPC request, sending the request to the Storm topology, receiving the results from the Storm topology, and sending the results back to the waiting client. From a client's perspective, a distributed RPC call looks just like a regular RPC call. For example, here's how a client would compute the results for the "reach" function with the argument "<a href="http://twitter.com%22:" target="_blank">http://twitter.com":</a></p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="n">DRPCClient</span> <span class="n">client</span> <span class="o">=</span> <span class="k">new</span> <span class="nf">DRPCClient</span><span class="o">(</span><span class="s">"drpc-host"</span><span class="o">,</span> <span class="mi">3772</span><span class="o">);</span>
<span class="n">String</span> <span class="n">result</span> <span class="o">=</span> <span class="n">client</span><span class="o">.</span><span class="na">execute</span><span class="o">(</span><span class="s">"reach"</span><span class="o">,</span> <span class="s">"http://twitter.com"</span><span class="o">);</span>
</code></pre></div>
<p>The distributed RPC workflow looks like this:</p>

<p><img src="drpc-workflow.png" alt="Tasks in a topology"></p>

<p>A client sends the DRPC server the name of the function to execute and the arguments to that function. The topology implementing that function uses a <code>DRPCSpout</code> to receive a function invocation stream from the DRPC server. Each function invocation is tagged with a unique id by the DRPC server. The topology then computes the result and at the end of the topology a bolt called <code>ReturnResults</code> connects to the DRPC server and gives it the result for the function invocation id. The DRPC server then uses the id to match up that result with which client is waiting, unblocks the waiting client, and sends it the result.</p>

<h3>LinearDRPCTopologyBuilder</h3>

<p>Storm comes with a topology builder called <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/drpc/LinearDRPCTopologyBuilder.html" target="_blank">LinearDRPCTopologyBuilder</a> that automates almost all the steps involved for doing DRPC. These include:</p>

<ol>
<li>Setting up the spout</li>
<li>Returning the results to the DRPC server</li>
<li>Providing functionality to bolts for doing finite aggregations over groups of tuples</li>
</ol>

<p>Let's look at a simple example. Here's the implementation of a DRPC topology that returns its input argument with a "!" appended:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="kd">public</span> <span class="kd">static</span> <span class="kd">class</span> <span class="nc">ExclaimBolt</span> <span class="kd">extends</span> <span class="n">BaseBasicBolt</span> <span class="o">{</span>
    <span class="kd">public</span> <span class="kt">void</span> <span class="nf">execute</span><span class="o">(</span><span class="n">Tuple</span> <span class="n">tuple</span><span class="o">,</span> <span class="n">BasicOutputCollector</span> <span class="n">collector</span><span class="o">)</span> <span class="o">{</span>
        <span class="n">String</span> <span class="n">input</span> <span class="o">=</span> <span class="n">tuple</span><span class="o">.</span><span class="na">getString</span><span class="o">(</span><span class="mi">1</span><span class="o">);</span>
        <span class="n">collector</span><span class="o">.</span><span class="na">emit</span><span class="o">(</span><span class="k">new</span> <span class="nf">Values</span><span class="o">(</span><span class="n">tuple</span><span class="o">.</span><span class="na">getValue</span><span class="o">(</span><span class="mi">0</span><span class="o">),</span> <span class="n">input</span> <span class="o">+</span> <span class="s">"!"</span><span class="o">));</span>
    <span class="o">}</span>

    <span class="kd">public</span> <span class="kt">void</span> <span class="nf">declareOutputFields</span><span class="o">(</span><span class="n">OutputFieldsDeclarer</span> <span class="n">declarer</span><span class="o">)</span> <span class="o">{</span>
        <span class="n">declarer</span><span class="o">.</span><span class="na">declare</span><span class="o">(</span><span class="k">new</span> <span class="nf">Fields</span><span class="o">(</span><span class="s">"id"</span><span class="o">,</span> <span class="s">"result"</span><span class="o">));</span>
    <span class="o">}</span>
<span class="o">}</span>

<span class="kd">public</span> <span class="kd">static</span> <span class="kt">void</span> <span class="nf">main</span><span class="o">(</span><span class="n">String</span><span class="o">[]</span> <span class="n">args</span><span class="o">)</span> <span class="kd">throws</span> <span class="n">Exception</span> <span class="o">{</span>
    <span class="n">LinearDRPCTopologyBuilder</span> <span class="n">builder</span> <span class="o">=</span> <span class="k">new</span> <span class="nf">LinearDRPCTopologyBuilder</span><span class="o">(</span><span class="s">"exclamation"</span><span class="o">);</span>
    <span class="n">builder</span><span class="o">.</span><span class="na">addBolt</span><span class="o">(</span><span class="k">new</span> <span class="nf">ExclaimBolt</span><span class="o">(),</span> <span class="mi">3</span><span class="o">);</span>
    <span class="c1">// ...</span>
<span class="o">}</span>
</code></pre></div>
<p>As you can see, there's very little to it. When creating the <code>LinearDRPCTopologyBuilder</code>, you tell it the name of the DRPC function for the topology. A single DRPC server can coordinate many functions, and the function name distinguishes the functions from one another. The first bolt you declare will take in as input 2-tuples, where the first field is the request id and the second field is the arguments for that request. <code>LinearDRPCTopologyBuilder</code> expects the last bolt to emit an output stream containing 2-tuples of the form [id, result]. Finally, all intermediate tuples must contain the request id as the first field.</p>

<p>In this example, <code>ExclaimBolt</code> simply appends a "!" to the second field of the tuple. <code>LinearDRPCTopologyBuilder</code> handles the rest of the coordination of connecting to the DRPC server and sending results back.</p>

<h3>Local mode DRPC</h3>

<p>DRPC can be run in local mode. Here's how to run the above example in local mode:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="n">LocalDRPC</span> <span class="n">drpc</span> <span class="o">=</span> <span class="k">new</span> <span class="nf">LocalDRPC</span><span class="o">();</span>
<span class="n">LocalCluster</span> <span class="n">cluster</span> <span class="o">=</span> <span class="k">new</span> <span class="nf">LocalCluster</span><span class="o">();</span>

<span class="n">cluster</span><span class="o">.</span><span class="na">submitTopology</span><span class="o">(</span><span class="s">"drpc-demo"</span><span class="o">,</span> <span class="n">conf</span><span class="o">,</span> <span class="n">builder</span><span class="o">.</span><span class="na">createLocalTopology</span><span class="o">(</span><span class="n">drpc</span><span class="o">));</span>

<span class="n">System</span><span class="o">.</span><span class="na">out</span><span class="o">.</span><span class="na">println</span><span class="o">(</span><span class="s">"Results for 'hello':"</span> <span class="o">+</span> <span class="n">drpc</span><span class="o">.</span><span class="na">execute</span><span class="o">(</span><span class="s">"exclamation"</span><span class="o">,</span> <span class="s">"hello"</span><span class="o">));</span>

<span class="n">cluster</span><span class="o">.</span><span class="na">shutdown</span><span class="o">();</span>
<span class="n">drpc</span><span class="o">.</span><span class="na">shutdown</span><span class="o">();</span>
</code></pre></div>
<p>First you create a <code>LocalDRPC</code> object. This object simulates a DRPC server in process, just like how <code>LocalCluster</code> simulates a Storm cluster in process. Then you create the <code>LocalCluster</code> to run the topology in local mode. <code>LinearDRPCTopologyBuilder</code> has separate methods for creating local topologies and remote topologies. In local mode the <code>LocalDRPC</code> object does not bind to any ports so the topology needs to know about the object to communicate with it. This is why <code>createLocalTopology</code> takes in the <code>LocalDRPC</code> object as input.</p>

<p>After launching the topology, you can do DRPC invocations using the <code>execute</code> method on <code>LocalDRPC</code>.</p>

<h3>Remote mode DRPC</h3>

<p>Using DRPC on an actual cluster is also straightforward. There's three steps:</p>

<ol>
<li>Launch DRPC server(s)</li>
<li>Configure the locations of the DRPC servers</li>
<li>Submit DRPC topologies to Storm cluster</li>
</ol>

<p>Launching a DRPC server can be done with the <code>storm</code> script and is just like launching Nimbus or the UI:</p>
<div class="highlight"><pre><code class="language-text" data-lang="text">bin/storm drpc
</code></pre></div>
<p>Next, you need to configure your Storm cluster to know the locations of the DRPC server(s). This is how <code>DRPCSpout</code> knows from where to read function invocations. This can be done through the <code>storm.yaml</code> file or the topology configurations. Configuring this through the <code>storm.yaml</code> looks something like this:</p>
<div class="highlight"><pre><code class="language-yaml" data-lang="yaml"><span class="l-Scalar-Plain">drpc.servers</span><span class="p-Indicator">:</span>
  <span class="p-Indicator">-</span> <span class="s">"drpc1.foo.com"</span>
  <span class="p-Indicator">-</span> <span class="s">"drpc2.foo.com"</span>
</code></pre></div>
<p>Finally, you launch DRPC topologies using <code>StormSubmitter</code> just like you launch any other topology. To run the above example in remote mode, you do something like this:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="n">StormSubmitter</span><span class="o">.</span><span class="na">submitTopology</span><span class="o">(</span><span class="s">"exclamation-drpc"</span><span class="o">,</span> <span class="n">conf</span><span class="o">,</span> <span class="n">builder</span><span class="o">.</span><span class="na">createRemoteTopology</span><span class="o">());</span>
</code></pre></div>
<p><code>createRemoteTopology</code> is used to create topologies suitable for Storm clusters.</p>

<h3>A more complex example</h3>

<p>The exclamation DRPC example was a toy example for illustrating the concepts of DRPC. Let's look at a more complex example which really needs the parallelism a Storm cluster provides for computing the DRPC function. The example we'll look at is computing the reach of a URL on Twitter.</p>

<p>The reach of a URL is the number of unique people exposed to a URL on Twitter. To compute reach, you need to:</p>

<ol>
<li>Get all the people who tweeted the URL</li>
<li>Get all the followers of all those people</li>
<li>Unique the set of followers</li>
<li>Count the unique set of followers</li>
</ol>

<p>A single reach computation can involve thousands of database calls and tens of millions of follower records during the computation. It's a really, really intense computation. As you're about to see, implementing this function on top of Storm is dead simple. On a single machine, reach can take minutes to compute; on a Storm cluster, you can compute reach for even the hardest URLs in a couple seconds.</p>

<p>A sample reach topology is defined in storm-starter <a href="https://github.com/apache/storm/blob/master/examples/storm-starter/src/jvm/storm/starter/ReachTopology.java">here</a>. Here's how you define the reach topology:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="n">LinearDRPCTopologyBuilder</span> <span class="n">builder</span> <span class="o">=</span> <span class="k">new</span> <span class="nf">LinearDRPCTopologyBuilder</span><span class="o">(</span><span class="s">"reach"</span><span class="o">);</span>
<span class="n">builder</span><span class="o">.</span><span class="na">addBolt</span><span class="o">(</span><span class="k">new</span> <span class="nf">GetTweeters</span><span class="o">(),</span> <span class="mi">3</span><span class="o">);</span>
<span class="n">builder</span><span class="o">.</span><span class="na">addBolt</span><span class="o">(</span><span class="k">new</span> <span class="nf">GetFollowers</span><span class="o">(),</span> <span class="mi">12</span><span class="o">)</span>
        <span class="o">.</span><span class="na">shuffleGrouping</span><span class="o">();</span>
<span class="n">builder</span><span class="o">.</span><span class="na">addBolt</span><span class="o">(</span><span class="k">new</span> <span class="nf">PartialUniquer</span><span class="o">(),</span> <span class="mi">6</span><span class="o">)</span>
        <span class="o">.</span><span class="na">fieldsGrouping</span><span class="o">(</span><span class="k">new</span> <span class="nf">Fields</span><span class="o">(</span><span class="s">"id"</span><span class="o">,</span> <span class="s">"follower"</span><span class="o">));</span>
<span class="n">builder</span><span class="o">.</span><span class="na">addBolt</span><span class="o">(</span><span class="k">new</span> <span class="nf">CountAggregator</span><span class="o">(),</span> <span class="mi">2</span><span class="o">)</span>
        <span class="o">.</span><span class="na">fieldsGrouping</span><span class="o">(</span><span class="k">new</span> <span class="nf">Fields</span><span class="o">(</span><span class="s">"id"</span><span class="o">));</span>
</code></pre></div>
<p>The topology executes as four steps:</p>

<ol>
<li><code>GetTweeters</code> gets the users who tweeted the URL. It transforms an input stream of <code>[id, url]</code> into an output stream of <code>[id, tweeter]</code>. Each <code>url</code> tuple will map to many <code>tweeter</code> tuples.</li>
<li><code>GetFollowers</code> gets the followers for the tweeters. It transforms an input stream of <code>[id, tweeter]</code> into an output stream of <code>[id, follower]</code>. Across all the tasks, there may of course be duplication of follower tuples when someone follows multiple people who tweeted the same URL.</li>
<li><code>PartialUniquer</code> groups the followers stream by the follower id. This has the effect of the same follower going to the same task. So each task of <code>PartialUniquer</code> will receive mutually independent sets of followers. Once <code>PartialUniquer</code> receives all the follower tuples directed at it for the request id, it emits the unique count of its subset of followers.</li>
<li>Finally, <code>CountAggregator</code> receives the partial counts from each of the <code>PartialUniquer</code> tasks and sums them up to complete the reach computation.</li>
</ol>

<p>Let's take a look at the <code>PartialUniquer</code> bolt:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="kd">public</span> <span class="kd">class</span> <span class="nc">PartialUniquer</span> <span class="kd">extends</span> <span class="n">BaseBatchBolt</span> <span class="o">{</span>
    <span class="n">BatchOutputCollector</span> <span class="n">_collector</span><span class="o">;</span>
    <span class="n">Object</span> <span class="n">_id</span><span class="o">;</span>
    <span class="n">Set</span><span class="o">&lt;</span><span class="n">String</span><span class="o">&gt;</span> <span class="n">_followers</span> <span class="o">=</span> <span class="k">new</span> <span class="n">HashSet</span><span class="o">&lt;</span><span class="n">String</span><span class="o">&gt;();</span>

    <span class="nd">@Override</span>
    <span class="kd">public</span> <span class="kt">void</span> <span class="nf">prepare</span><span class="o">(</span><span class="n">Map</span> <span class="n">conf</span><span class="o">,</span> <span class="n">TopologyContext</span> <span class="n">context</span><span class="o">,</span> <span class="n">BatchOutputCollector</span> <span class="n">collector</span><span class="o">,</span> <span class="n">Object</span> <span class="n">id</span><span class="o">)</span> <span class="o">{</span>
        <span class="n">_collector</span> <span class="o">=</span> <span class="n">collector</span><span class="o">;</span>
        <span class="n">_id</span> <span class="o">=</span> <span class="n">id</span><span class="o">;</span>
    <span class="o">}</span>

    <span class="nd">@Override</span>
    <span class="kd">public</span> <span class="kt">void</span> <span class="nf">execute</span><span class="o">(</span><span class="n">Tuple</span> <span class="n">tuple</span><span class="o">)</span> <span class="o">{</span>
        <span class="n">_followers</span><span class="o">.</span><span class="na">add</span><span class="o">(</span><span class="n">tuple</span><span class="o">.</span><span class="na">getString</span><span class="o">(</span><span class="mi">1</span><span class="o">));</span>
    <span class="o">}</span>

    <span class="nd">@Override</span>
    <span class="kd">public</span> <span class="kt">void</span> <span class="nf">finishBatch</span><span class="o">()</span> <span class="o">{</span>
        <span class="n">_collector</span><span class="o">.</span><span class="na">emit</span><span class="o">(</span><span class="k">new</span> <span class="nf">Values</span><span class="o">(</span><span class="n">_id</span><span class="o">,</span> <span class="n">_followers</span><span class="o">.</span><span class="na">size</span><span class="o">()));</span>
    <span class="o">}</span>

    <span class="nd">@Override</span>
    <span class="kd">public</span> <span class="kt">void</span> <span class="nf">declareOutputFields</span><span class="o">(</span><span class="n">OutputFieldsDeclarer</span> <span class="n">declarer</span><span class="o">)</span> <span class="o">{</span>
        <span class="n">declarer</span><span class="o">.</span><span class="na">declare</span><span class="o">(</span><span class="k">new</span> <span class="nf">Fields</span><span class="o">(</span><span class="s">"id"</span><span class="o">,</span> <span class="s">"partial-count"</span><span class="o">));</span>
    <span class="o">}</span>
<span class="o">}</span>
</code></pre></div>
<p><code>PartialUniquer</code> implements <code>IBatchBolt</code> by extending <code>BaseBatchBolt</code>. A batch bolt provides a first class API to processing a batch of tuples as a concrete unit. A new instance of the batch bolt is created for each request id, and Storm takes care of cleaning up the instances when appropriate. </p>

<p>When <code>PartialUniquer</code> receives a follower tuple in the <code>execute</code> method, it adds it to the set for the request id in an internal <code>HashSet</code>. </p>

<p>Batch bolts provide the <code>finishBatch</code> method which is called after all the tuples for this batch targeted at this task have been processed. In the callback, <code>PartialUniquer</code> emits a single tuple containing the unique count for its subset of follower ids.</p>

<p>Under the hood, <code>CoordinatedBolt</code> is used to detect when a given bolt has received all of the tuples for any given request id. <code>CoordinatedBolt</code> makes use of direct streams to manage this coordination.</p>

<p>The rest of the topology should be self-explanatory. As you can see, every single step of the reach computation is done in parallel, and defining the DRPC topology was extremely simple.</p>

<h3>Non-linear DRPC topologies</h3>

<p><code>LinearDRPCTopologyBuilder</code> only handles "linear" DRPC topologies, where the computation is expressed as a sequence of steps (like reach). It's not hard to imagine functions that would require a more complicated topology with branching and merging of the bolts. For now, to do this you'll need to drop down into using <code>CoordinatedBolt</code> directly. Be sure to talk about your use case for non-linear DRPC topologies on the mailing list to inform the construction of more general abstractions for DRPC topologies.</p>

<h3>How LinearDRPCTopologyBuilder works</h3>

<ul>
<li>DRPCSpout emits [args, return-info]. return-info is the host and port of the DRPC server as well as the id generated by the DRPC server</li>
<li>constructs a topology comprising of:

<ul>
<li>DRPCSpout</li>
<li>PrepareRequest (generates a request id and creates a stream for the return info and a stream for the args)</li>
<li>CoordinatedBolt wrappers and direct groupings</li>
<li>JoinResult (joins the result with the return info)</li>
<li>ReturnResult (connects to the DRPC server and returns the result)</li>
</ul></li>
<li>LinearDRPCTopologyBuilder is a good example of a higher level abstraction built on top of Storm's primitives</li>
</ul>

<h3>Advanced</h3>

<ul>
<li>KeyedFairBolt for weaving the processing of multiple requests at the same time</li>
<li>How to use <code>CoordinatedBolt</code> directly</li>
</ul>
            </div>
        </div>
    </div>
</div>
<!--Content End-->
