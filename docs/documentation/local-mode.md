---
layout: documentation
title: Local mode
---
<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-12">
            	<h1 class="page-title">Local Mode</h1>
    <p>Local mode simulates a Storm cluster in process and is useful for developing and testing topologies. Running topologies in local mode is similar to running topologies <a href="Running-topologies-on-a-production-cluster.html">on a cluster</a>. </p>

<p>To create an in-process cluster, simply use the <code>LocalCluster</code> class. For example:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="kn">import</span> <span class="nn">backtype.storm.LocalCluster</span><span class="o">;</span>

<span class="n">LocalCluster</span> <span class="n">cluster</span> <span class="o">=</span> <span class="k">new</span> <span class="nf">LocalCluster</span><span class="o">();</span>
</code></pre></div>
<p>You can then submit topologies using the <code>submitTopology</code> method on the <code>LocalCluster</code> object. Just like the corresponding method on <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/StormSubmitter.html">StormSubmitter</a>, <code>submitTopology</code> takes a name, a topology configuration, and the topology object. You can then kill a topology using the <code>killTopology</code> method which takes the topology name as an argument.</p>

<p>To shutdown a local cluster, simple call:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="n">cluster</span><span class="o">.</span><span class="na">shutdown</span><span class="o">();</span>
</code></pre></div>
<h3 id="common-configurations-for-local-mode">Common configurations for local mode</h3>

<p>You can see a full list of configurations <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/Config.html">here</a>.</p>

<ol>
<li><strong>Config.TOPOLOGY_MAX_TASK_PARALLELISM</strong>: This config puts a ceiling on the number of threads spawned for a single component. Oftentimes production topologies have a lot of parallelism (hundreds of threads) which places unreasonable load when trying to test the topology in local mode. This config lets you easy control that parallelism.</li>
<li><strong>Config.TOPOLOGY_DEBUG</strong>: When this is set to true, Storm will log a message every time a tuple is emitted from any spout or bolt. This is extremely useful for debugging.</li>
</ol>
            </div>
        </div>
    </div>
</div>
<!--Content End-->
