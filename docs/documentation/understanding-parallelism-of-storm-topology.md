---
layout: documentation
title:  Storm Parallelism
---

<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-12">
            	<h1 class="page-title">Understanding the Parallelism of a Storm Topology</h1>
                <h3>What makes a running topology: worker processes, executors and tasks</h3>
                <p>Storm distinguishes between the following three main entities that are used to actually run a topology in a Storm cluster:</p>
                <ol>
                    <li>Worker processes</li>
                    <li>Executors (threads)</li>
                    <li>Tasks</li>
                </ol>
                <p>Here is a simple illustration of their relationships:</p>
                <p><img src="relationships-worker-processes-executors-tasks.png" alt="The relationships of worker processes, executors (threads) and tasks in Storm"></p>
                <p>A <em>worker process</em> executes a subset of a topology. A worker process belongs to a specific topology and may run one or more executors for one or more components (spouts or bolts) of this topology. A running topology consists of many such processes running on many machines within a Storm cluster.</p>
                <p>An <em>executor</em> is a thread that is spawned by a worker process. It may run one or more tasks for the same component (spout or bolt).</p>
                <p>A <em>task</em> performs the actual data processing — each spout or bolt that you implement in your code executes as many tasks across the cluster. The number of tasks for a component is always the same throughout the lifetime of a topology, but the number of executors (threads) for a component can change over time. This means that the following condition holds true: <code>#threads ≤ #tasks</code>. By default, the number of tasks is set to be the same as the number of executors, i.e. Storm will run one task per thread.</p>
                <h3>Configuring the parallelism of a topology</h3>
                <p>Note that in Storm’s terminology "parallelism" is specifically used to describe the so-called <em>parallelism hint</em>, which means the initial number of executor (threads) of a component. In this document though we use the term "parallelism" in a more general sense to describe how you can configure not only the number of executors but also the number of worker processes and the number of tasks of a Storm topology. We will specifically call out when "parallelism" is used in the normal, narrow definition of Storm.</p>
                <p>The following sections give an overview of the various configuration options and how to set them in your code. There is more than one way of setting these options though, and the table lists only some of them. Storm currently has the following <a href="Configuration.html">order of precedence for configuration settings</a>: <code>defaults.yaml</code> &lt; <code>storm.yaml</code> &lt; topology-specific configuration &lt; internal component-specific configuration &lt; external component-specific configuration.</p>
                <h3>Number of worker processes</h3>
                <ul>
                    <li>Description: How many worker processes to create <em>for the topology</em> across machines in the cluster.</li>
                    <li>Configuration option: <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/Config.html#TOPOLOGY_WORKERS">TOPOLOGY_WORKERS</a></li>
                    <li>How to set in your code (examples):
                        <ul>
                            <li><a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/Config.html">Config#setNumWorkers</a></li>
                        </ul>
                    </li>
                </ul>
                <h3>Number of executors (threads)</h3>
                <ul>
                    <li>Description: How many executors to spawn <em>per component</em>.</li>
                    <li>Configuration option: ?</li>
                    <li>How to set in your code (examples):
                        <ul>
                            <li><a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/topology/TopologyBuilder.html">TopologyBuilder#setSpout()</a></li>
                            <li><a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/topology/TopologyBuilder.html">TopologyBuilder#setBolt()</a></li>
                            <li>Note that as of Storm 0.8 the <code>parallelism_hint</code> parameter now specifies the initial number of executors (not tasks!) for that bolt.</li>
                        </ul>
                    </li>
                </ul>
                <h3>Number of tasks</h3>
                <ul>
                    <li>Description: How many tasks to create <em>per component</em>.</li>
                    <li>Configuration option: <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/Config.html#TOPOLOGY_TASKS">TOPOLOGY_TASKS</a></li>
                    <li>How to set in your code (examples):
                        <ul>
                            <li><a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/topology/ComponentConfigurationDeclarer.html">ComponentConfigurationDeclarer#setNumTasks()</a></li>
                        </ul>
                    </li>
                </ul>
                <p>Here is an example code snippet to show these settings in practice:</p>

<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="n">topologyBuilder</span><span class="o">.</span><span class="na">setBolt</span><span class="o">(</span><span class="s">"green-bolt"</span><span class="o">,</span> <span class="k">new</span> <span class="nf">GreenBolt</span><span class="o">(),</span> <span class="mi">2</span><span class="o">)</span>
               <span class="o">.</span><span class="na">setNumTasks</span><span class="o">(</span><span class="mi">4</span><span class="o">)</span>
               <span class="o">.</span><span class="na">shuffleGrouping</span><span class="o">(</span><span class="err">"</span><span class="n">blue</span><span class="o">-</span><span class="n">spout</span><span class="o">);</span>
</code></pre></div>

                <p>In the above code we configured Storm to run the bolt <code>GreenBolt</code> with an initial number of two executors and four associated tasks. Storm will run two tasks per executor (thread). If you do not explicitly configure the number of tasks, Storm will run by default one task per executor.</p>
                <h3>Example of a running topology</h3>
                <p>The following illustration shows how a simple topology would look like in operation. The topology consists of three components: one spout called <code>BlueSpout</code> and two bolts called <code>GreenBolt</code> and <code>YellowBolt</code>. The components are linked such that <code>BlueSpout</code> sends its output to <code>GreenBolt</code>, which in turns sends its own output to <code>YellowBolt</code>.</p>
                <p><img src="example-of-a-running-topology.png" alt="Example of a running topology in Storm"></p>
                <p>The <code>GreenBolt</code> was configured as per the code snippet above whereas <code>BlueSpout</code> and <code>YellowBolt</code> only set the parallelism hint (number of executors). Here is the relevant code:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="n">Config</span> <span class="n">conf</span> <span class="o">=</span> <span class="k">new</span> <span class="nf">Config</span><span class="o">();</span>
<span class="n">conf</span><span class="o">.</span><span class="na">setNumWorkers</span><span class="o">(</span><span class="mi">2</span><span class="o">);</span> <span class="c1">// use two worker processes</span>

<span class="n">topologyBuilder</span><span class="o">.</span><span class="na">setSpout</span><span class="o">(</span><span class="s">"blue-spout"</span><span class="o">,</span> <span class="k">new</span> <span class="nf">BlueSpout</span><span class="o">(),</span> <span class="mi">2</span><span class="o">);</span> <span class="c1">// set parallelism hint to 2</span>

<span class="n">topologyBuilder</span><span class="o">.</span><span class="na">setBolt</span><span class="o">(</span><span class="s">"green-bolt"</span><span class="o">,</span> <span class="k">new</span> <span class="nf">GreenBolt</span><span class="o">(),</span> <span class="mi">2</span><span class="o">)</span>
               <span class="o">.</span><span class="na">setNumTasks</span><span class="o">(</span><span class="mi">4</span><span class="o">)</span>
               <span class="o">.</span><span class="na">shuffleGrouping</span><span class="o">(</span><span class="s">"blue-spout"</span><span class="o">);</span>

<span class="n">topologyBuilder</span><span class="o">.</span><span class="na">setBolt</span><span class="o">(</span><span class="s">"yellow-bolt"</span><span class="o">,</span> <span class="k">new</span> <span class="nf">YellowBolt</span><span class="o">(),</span> <span class="mi">6</span><span class="o">)</span>
               <span class="o">.</span><span class="na">shuffleGrouping</span><span class="o">(</span><span class="s">"green-bolt"</span><span class="o">);</span>

<span class="n">StormSubmitter</span><span class="o">.</span><span class="na">submitTopology</span><span class="o">(</span>
        <span class="s">"mytopology"</span><span class="o">,</span>
        <span class="n">conf</span><span class="o">,</span>
        <span class="n">topologyBuilder</span><span class="o">.</span><span class="na">createTopology</span><span class="o">()</span>
    <span class="o">);</span>
</code></pre></div>

                <p>And of course Storm comes with additional configuration settings to control the parallelism of a topology, including:</p>
                <ul>
                    <li><a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/Config.html#TOPOLOGY_MAX_TASK_PARALLELISM">TOPOLOGY_MAX_TASK_PARALLELISM</a>: This setting puts a ceiling on the number of executors that can be spawned for a single component. It is typically used during testing to limit the number of threads spawned when running a topology in local mode. You can set this option via e.g. <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/Config.html#setMaxTaskParallelism(int)">Config#setMaxTaskParallelism()</a>.</li>
                </ul>
                <h3>How to change the parallelism of a running topology</h3>
                <p>A nifty feature of Storm is that you can increase or decrease the number of worker processes and/or executors without being required to restart the cluster or the topology. The act of doing so is called rebalancing.</p>
                <p>You have two options to rebalance a topology:</p>
                <ol>
                    <li>Use the Storm web UI to rebalance the topology.</li>
                    <li>Use the CLI tool storm rebalance as described below.</li>
                </ol>
                <p>Here is an example of using the CLI tool:</p>
<div class="highlight"><pre><code class="language-text" data-lang="text">## Reconfigure the topology "mytopology" to use 5 worker processes,
## the spout "blue-spout" to use 3 executors and
## the bolt "yellow-bolt" to use 10 executors.

$ storm rebalance mytopology -n 5 -e blue-spout=3 -e yellow-bolt=10
</code></pre></div>

                <h2>References</h2>
                <ul>
                    <li><a href="concepts.html">Concepts</a></li>
                    <li><a href="configuration.html">Configuration</a></li>
                    <li><a href="running-topologies-on-production-cluster.html">Running topologies on a production cluster</a>]</li>
                    <li><a href="local-mode.html">Local mode</a></li>
                    <li><a href="../tutorial.html">Tutorial</a></li>
                    <li><a href="https://storm.apache.org/javadoc/apidocs/">Storm API documentation</a>, most notably the class <code>Config</code></li>
                </ul>
            </div>
        </div>
    </div>
</div>
<!--Content End-->
