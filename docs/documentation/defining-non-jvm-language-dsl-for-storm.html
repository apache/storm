---
layout: documentation
title: Non JVM languages
---
<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-12">
            	<h1 class="page-title">Defining a Non-JVM DSL for Storm</h1>
    <p>The right place to start to learn how to make a non-JVM DSL for Storm is <a href="https://github.com/apache/storm/blob/master/storm-core/src/storm.thrift" target="_blank">storm-core/src/storm.thrift</a>. Since Storm topologies are just Thrift structures, and Nimbus is a Thrift daemon, you can create and submit topologies in any language.</p>

<p>When you create the Thrift structs for spouts and bolts, the code for the spout or bolt is specified in the ComponentObject struct:</p>
<div class="highlight"><pre><code class="language-text" data-lang="text">union ComponentObject {
  1: binary serialized_java;
  2: ShellComponent shell;
  3: JavaObject java_object;
}
</code></pre></div>
<p>For a Python DSL, you would want to make use of "2" and "3". ShellComponent lets you specify a script to run that component (e.g., your python code). And JavaObject lets you specify native java spouts and bolts for the component (and Storm will use reflection to create that spout or bolt).</p>

<p>There's a "storm shell" command that will help with submitting a topology. Its usage is like this:</p>
<div class="highlight"><pre><code class="language-text" data-lang="text">storm shell resources/ python topology.py arg1 arg2
</code></pre></div>
<p>storm shell will then package resources/ into a jar, upload the jar to Nimbus, and call your topology.py script like this:</p>
<div class="highlight"><pre><code class="language-text" data-lang="text">python topology.py arg1 arg2 {nimbus-host} {nimbus-port} {uploaded-jar-location}
</code></pre></div>
<p>Then you can connect to Nimbus using the Thrift API and submit the topology, passing {uploaded-jar-location} into the submitTopology method. For reference, here's the submitTopology definition:</p>
<div class="highlight"><pre><code class="language-java" data-lang="java"><span class="kt">void</span> <span class="nf">submitTopology</span><span class="o">(</span><span class="mi">1</span><span class="o">:</span> <span class="n">string</span> <span class="n">name</span><span class="o">,</span> <span class="mi">2</span><span class="o">:</span> <span class="n">string</span> <span class="n">uploadedJarLocation</span><span class="o">,</span> <span class="mi">3</span><span class="o">:</span> <span class="n">string</span> <span class="n">jsonConf</span><span class="o">,</span> <span class="mi">4</span><span class="o">:</span> <span class="n">StormTopology</span> <span class="n">topology</span><span class="o">)</span> <span class="kd">throws</span> <span class="o">(</span><span class="mi">1</span><span class="o">:</span> <span class="n">AlreadyAliveException</span> <span class="n">e</span><span class="o">,</span> <span class="mi">2</span><span class="o">:</span> <span class="n">InvalidTopologyException</span> <span class="n">ite</span><span class="o">);</span>
</code></pre></div>
<p>Finally, one of the key things to do in a non-JVM DSL is make it easy to define the entire topology in one file (the bolts, spouts, and the definition of the topology).</p>
            </div>
        </div>
    </div>
</div>
<!--Content End-->
