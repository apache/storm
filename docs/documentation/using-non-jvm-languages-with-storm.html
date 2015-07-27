---
layout: documentation
title: Non JVM Language and Storm
---
<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-12">
            	<h1 class="page-title">Using non-JVM languages with Storm</h1>
    <ul>
<li>two pieces: creating topologies and implementing spouts and bolts in other languages</li>
<li>creating topologies in another language is easy since topologies are just thrift structures (link to storm.thrift)</li>
<li>implementing spouts and bolts in another language is called a "multilang components" or "shelling"

<ul>
<li>Here's a specification of the protocol: <a href="multilang-protocol.html">Multilang protocol</a></li>
<li>the thrift structure lets you define multilang components explicitly as a program and a script (e.g., python and the file implementing your bolt)</li>
<li>In Java, you override ShellBolt or ShellSpout to create multilang components

<ul>
<li>note that output fields declarations happens in the thrift structure, so in Java you create multilang components like the following:

<ul>
<li>declare fields in java, processing code in the other language by specifying it in constructor of shellbolt</li>
</ul></li>
</ul></li>
<li>multilang uses json messages over stdin/stdout to communicate with the subprocess</li>
<li>storm comes with ruby, python, and fancy adapters that implement the protocol. show an example of python

<ul>
<li>python supports emitting, anchoring, acking, and logging</li>
</ul></li>
</ul></li>
<li>"storm shell" command makes constructing jar and uploading to nimbus easy

<ul>
<li>makes jar and uploads it</li>
<li>calls your program with host/port of nimbus and the jarfile id</li>
</ul></li>
</ul>

<h3>Notes on implementing a DSL in a non-JVM language</h3>

<p>The right place to start is src/storm.thrift. Since Storm topologies are just Thrift structures, and Nimbus is a Thrift daemon, you can create and submit topologies in any language.</p>

<p>When you create the Thrift structs for spouts and bolts, the code for the spout or bolt is specified in the ComponentObject struct:</p>
<div class="highlight"><pre><code class="language-text" data-lang="text">union ComponentObject {
  1: binary serialized_java;
  2: ShellComponent shell;
  3: JavaObject java_object;
}
</code></pre></div>
<p>For a non-JVM DSL, you would want to make use of "2" and "3". ShellComponent lets you specify a script to run that component (e.g., your python code). And JavaObject lets you specify native java spouts and bolts for the component (and Storm will use reflection to create that spout or bolt).</p>

<p>There's a "storm shell" command that will help with submitting a topology. Its usage is like this:</p>
<div class="highlight"><pre><code class="language-text" data-lang="text">storm shell resources/ python topology.py arg1 arg2
</code></pre></div>
<p>storm shell will then package resources/ into a jar, upload the jar to Nimbus, and call your topology.py script like this:</p>
<div class="highlight"><pre><code class="language-text" data-lang="text">python topology.py arg1 arg2 {nimbus-host} {nimbus-port} {uploaded-jar-location}
</code></pre></div>
<p>Then you can connect to Nimbus using the Thrift API and submit the topology, passing {uploaded-jar-location} into the submitTopology method. For reference, here's the submitTopology definition:</p>
<div class="highlight"><pre><code class="language-text" data-lang="text">void submitTopology(1: string name, 2: string uploadedJarLocation, 3: string jsonConf, 4: StormTopology topology)
    throws (1: AlreadyAliveException e, 2: InvalidTopologyException ite);
</code></pre></div>
            </div>
        </div>
    </div>
</div>
<!--Content End-->