---
layout: documentation
title: Clojure-dsl
---
<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-12">
            	<h1 class="page-title">Clojure DSL</h1>
    <p>Storm comes with a Clojure DSL for defining spouts, bolts, and topologies. The Clojure DSL has access to everything the Java API exposes, so if you're a Clojure user you can code Storm topologies without touching Java at all. The Clojure DSL is defined in the source in the <a href="https://github.com/apache/storm/blob/0.5.3/src/clj/backtype/storm/clojure.clj" target="_blank">backtype.storm.clojure</a> namespace.</p>

<p>This page outlines all the pieces of the Clojure DSL, including:</p>

<ol>
<li>Defining topologies</li>
<li><code>defbolt</code></li>
<li><code>defspout</code></li>
<li>Running topologies in local mode or on a cluster</li>
<li>Testing topologies</li>
</ol>

<h3>Defining topologies</h3>

<p>To define a topology, use the <code>topology</code> function. <code>topology</code> takes in two arguments: a map of "spout specs" and a map of "bolt specs". Each spout and bolt spec wires the code for the component into the topology by specifying things like inputs and parallelism.</p>

<p>Let's take a look at an example topology definition <a href="https://github.com/apache/storm/blob/master/examples/storm-starter/src/clj/storm/starter/clj/word_count.clj" target="_blank">from the storm-starter project</a>:</p>
<div class="highlight"><pre><code class="language-clojure" data-lang="clojure"><span class="p">(</span><span class="nf">topology</span>
 <span class="p">{</span><span class="s">"1"</span> <span class="p">(</span><span class="nf">spout-spec</span> <span class="nv">sentence-spout</span><span class="p">)</span>
  <span class="s">"2"</span> <span class="p">(</span><span class="nf">spout-spec</span> <span class="p">(</span><span class="nf">sentence-spout-parameterized</span>
                   <span class="p">[</span><span class="s">"the cat jumped over the door"</span>
                    <span class="s">"greetings from a faraway land"</span><span class="p">])</span>
                   <span class="ss">:p</span> <span class="mi">2</span><span class="p">)}</span>
 <span class="p">{</span><span class="s">"3"</span> <span class="p">(</span><span class="nf">bolt-spec</span> <span class="p">{</span><span class="s">"1"</span> <span class="ss">:shuffle</span> <span class="s">"2"</span> <span class="ss">:shuffle</span><span class="p">}</span>
                 <span class="nv">split-sentence</span>
                 <span class="ss">:p</span> <span class="mi">5</span><span class="p">)</span>
  <span class="s">"4"</span> <span class="p">(</span><span class="nf">bolt-spec</span> <span class="p">{</span><span class="s">"3"</span> <span class="p">[</span><span class="s">"word"</span><span class="p">]}</span>
                 <span class="nv">word-count</span>
                 <span class="ss">:p</span> <span class="mi">6</span><span class="p">)})</span>
</code></pre></div>
<p>The maps of spout and bolt specs are maps from the component id to the corresponding spec. The component ids must be unique across the maps. Just like defining topologies in Java, component ids are used when declaring inputs for bolts in the topology.</p>

<h4>spout-spec</h4>

<p><code>spout-spec</code> takes as arguments the spout implementation (an object that implements <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/topology/IRichSpout.html">IRichSpout</a>) and optional keyword arguments. The only option that exists currently is the <code>:p</code> option, which specifies the parallelism for the spout. If you omit <code>:p</code>, the spout will execute as a single task.</p>

<h4>bolt-spec</h4>

<p><code>bolt-spec</code> takes as arguments the input declaration for the bolt, the bolt implementation (an object that implements <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/topology/IRichBolt.html" target="_blank">IRichBolt</a>), and optional keyword arguments.</p>

<p>The input declaration is a map from stream ids to stream groupings. A stream id can have one of two forms:</p>

<ol>
<li><code>[==component id== ==stream id==]</code>: Subscribes to a specific stream on a component</li>
<li><code>==component id==</code>: Subscribes to the default stream on a component</li>
</ol>

<p>A stream grouping can be one of the following:</p>

<ol>
<li><code>:shuffle</code>: subscribes with a shuffle grouping</li>
<li>Vector of field names, like <code>["id" "name"]</code>: subscribes with a fields grouping on the specified fields</li>
<li><code>:global</code>: subscribes with a global grouping</li>
<li><code>:all</code>: subscribes with an all grouping</li>
<li><code>:direct</code>: subscribes with a direct grouping</li>
</ol>

<p>See <a href="concepts.html">Concepts</a> for more info on stream groupings. Here's an example input declaration showcasing the various ways to declare inputs:</p>
<div class="highlight"><pre><code class="language-clojure" data-lang="clojure"><span class="p">{[</span><span class="s">"2"</span> <span class="s">"1"</span><span class="p">]</span> <span class="ss">:shuffle</span>
 <span class="s">"3"</span> <span class="p">[</span><span class="s">"field1"</span> <span class="s">"field2"</span><span class="p">]</span>
 <span class="p">[</span><span class="s">"4"</span> <span class="s">"2"</span><span class="p">]</span> <span class="ss">:global</span><span class="p">}</span>
</code></pre></div>
<p>This input declaration subscribes to three streams total. It subscribes to stream "1" on component "2" with a shuffle grouping, subscribes to the default stream on component "3" with a fields grouping on the fields "field1" and "field2", and subscribes to stream "2" on component "4" with a global grouping.</p>

<p>Like <code>spout-spec</code>, the only current supported keyword argument for <code>bolt-spec</code> is <code>:p</code> which specifies the parallelism for the bolt.</p>

<h4>shell-bolt-spec</h4>

<p><code>shell-bolt-spec</code> is used for defining bolts that are implemented in a non-JVM language. It takes as arguments the input declaration, the command line program to run, the name of the file implementing the bolt, an output specification, and then the same keyword arguments that <code>bolt-spec</code> accepts.</p>

<p>Here's an example <code>shell-bolt-spec</code>:</p>
<div class="highlight"><pre><code class="language-clojure" data-lang="clojure"><span class="p">(</span><span class="nf">shell-bolt-spec</span> <span class="p">{</span><span class="s">"1"</span> <span class="ss">:shuffle</span> <span class="s">"2"</span> <span class="p">[</span><span class="s">"id"</span><span class="p">]}</span>
                 <span class="s">"python"</span>
                 <span class="s">"mybolt.py"</span>
                 <span class="p">[</span><span class="s">"outfield1"</span> <span class="s">"outfield2"</span><span class="p">]</span>
                 <span class="ss">:p</span> <span class="mi">25</span><span class="p">)</span>
</code></pre></div>
<p>The syntax of output declarations is described in more detail in the <code>defbolt</code> section below. See <a href="Using-non-JVM-languages-with-Storm.html">Using non JVM languages with Storm</a> for more details on how multilang works within Storm.</p>

<h3>defbolt</h3>

<p><code>defbolt</code> is used for defining bolts in Clojure. Bolts have the constraint that they must be serializable, and this is why you can't just reify <code>IRichBolt</code> to implement a bolt (closures aren't serializable). <code>defbolt</code> works around this restriction and provides a nicer syntax for defining bolts than just implementing a Java interface.</p>

<p>At its fullest expressiveness, <code>defbolt</code> supports parameterized bolts and maintaining state in a closure around the bolt implementation. It also provides shortcuts for defining bolts that don't need this extra functionality. The signature for <code>defbolt</code> looks like the following:</p>

<p>(defbolt <em>name</em> <em>output-declaration</em> *<em>option-map</em> &amp; <em>impl</em>)</p>

<p>Omitting the option map is equivalent to having an option map of <code>{:prepare false}</code>.</p>

<h4>Simple bolts</h4>

<p>Let's start with the simplest form of <code>defbolt</code>. Here's an example bolt that splits a tuple containing a sentence into a tuple for each word:</p>
<div class="highlight"><pre><code class="language-clojure" data-lang="clojure"><span class="p">(</span><span class="nf">defbolt</span> <span class="nv">split-sentence</span> <span class="p">[</span><span class="s">"word"</span><span class="p">]</span> <span class="p">[</span><span class="nv">tuple</span> <span class="nv">collector</span><span class="p">]</span>
  <span class="p">(</span><span class="k">let </span><span class="p">[</span><span class="nv">words</span> <span class="p">(</span><span class="nf">.split</span> <span class="p">(</span><span class="nf">.getString</span> <span class="nv">tuple</span> <span class="mi">0</span><span class="p">)</span> <span class="s">" "</span><span class="p">)]</span>
    <span class="p">(</span><span class="nb">doseq </span><span class="p">[</span><span class="nv">w</span> <span class="nv">words</span><span class="p">]</span>
      <span class="p">(</span><span class="nf">emit-bolt!</span> <span class="nv">collector</span> <span class="p">[</span><span class="nv">w</span><span class="p">]</span> <span class="ss">:anchor</span> <span class="nv">tuple</span><span class="p">))</span>
    <span class="p">(</span><span class="nf">ack!</span> <span class="nv">collector</span> <span class="nv">tuple</span><span class="p">)</span>
    <span class="p">))</span>
</code></pre></div>
<p>Since the option map is omitted, this is a non-prepared bolt. The DSL simply expects an implementation for the <code>execute</code> method of <code>IRichBolt</code>. The implementation takes two parameters, the tuple and the <code>OutputCollector</code>, and is followed by the body of the <code>execute</code> function. The DSL automatically type-hints the parameters for you so you don't need to worry about reflection if you use Java interop.</p>

<p>This implementation binds <code>split-sentence</code> to an actual <code>IRichBolt</code> object that you can use in topologies, like so:</p>
<div class="highlight"><pre><code class="language-clojure" data-lang="clojure"><span class="p">(</span><span class="nf">bolt-spec</span> <span class="p">{</span><span class="s">"1"</span> <span class="ss">:shuffle</span><span class="p">}</span>
           <span class="nv">split-sentence</span>
           <span class="ss">:p</span> <span class="mi">5</span><span class="p">)</span>
</code></pre></div>
<h4>Parameterized bolts</h4>

<p>Many times you want to parameterize your bolts with other arguments. For example, let's say you wanted to have a bolt that appends a suffix to every input string it receives, and you want that suffix to be set at runtime. You do this with <code>defbolt</code> by including a <code>:params</code> option in the option map, like so:</p>
<div class="highlight"><pre><code class="language-clojure" data-lang="clojure"><span class="p">(</span><span class="nf">defbolt</span> <span class="nv">suffix-appender</span> <span class="p">[</span><span class="s">"word"</span><span class="p">]</span> <span class="p">{</span><span class="ss">:params</span> <span class="p">[</span><span class="nv">suffix</span><span class="p">]}</span>
  <span class="p">[</span><span class="nv">tuple</span> <span class="nv">collector</span><span class="p">]</span>
  <span class="p">(</span><span class="nf">emit-bolt!</span> <span class="nv">collector</span> <span class="p">[(</span><span class="nb">str </span><span class="p">(</span><span class="nf">.getString</span> <span class="nv">tuple</span> <span class="mi">0</span><span class="p">)</span> <span class="nv">suffix</span><span class="p">)]</span> <span class="ss">:anchor</span> <span class="nv">tuple</span><span class="p">)</span>
  <span class="p">)</span>
</code></pre></div>
<p>Unlike the previous example, <code>suffix-appender</code> will be bound to a function that returns an <code>IRichBolt</code> rather than be an <code>IRichBolt</code> object directly. This is caused by specifying <code>:params</code> in its option map. So to use <code>suffix-appender</code> in a topology, you would do something like:</p>
<div class="highlight"><pre><code class="language-clojure" data-lang="clojure"><span class="p">(</span><span class="nf">bolt-spec</span> <span class="p">{</span><span class="s">"1"</span> <span class="ss">:shuffle</span><span class="p">}</span>
           <span class="p">(</span><span class="nf">suffix-appender</span> <span class="s">"-suffix"</span><span class="p">)</span>
           <span class="ss">:p</span> <span class="mi">10</span><span class="p">)</span>
</code></pre></div>
<h4>Prepared bolts</h4>

<p>To do more complex bolts, such as ones that do joins and streaming aggregations, the bolt needs to store state. You can do this by creating a prepared bolt which is specified by including <code>{:prepare true}</code> in the option map. Consider, for example, this bolt that implements word counting:</p>
<div class="highlight"><pre><code class="language-clojure" data-lang="clojure"><span class="p">(</span><span class="nf">defbolt</span> <span class="nv">word-count</span> <span class="p">[</span><span class="s">"word"</span> <span class="s">"count"</span><span class="p">]</span> <span class="p">{</span><span class="ss">:prepare</span> <span class="nv">true</span><span class="p">}</span>
  <span class="p">[</span><span class="nv">conf</span> <span class="nv">context</span> <span class="nv">collector</span><span class="p">]</span>
  <span class="p">(</span><span class="k">let </span><span class="p">[</span><span class="nv">counts</span> <span class="p">(</span><span class="nf">atom</span> <span class="p">{})]</span>
    <span class="p">(</span><span class="nf">bolt</span>
     <span class="p">(</span><span class="nf">execute</span> <span class="p">[</span><span class="nv">tuple</span><span class="p">]</span>
       <span class="p">(</span><span class="k">let </span><span class="p">[</span><span class="nv">word</span> <span class="p">(</span><span class="nf">.getString</span> <span class="nv">tuple</span> <span class="mi">0</span><span class="p">)]</span>
         <span class="p">(</span><span class="nf">swap!</span> <span class="nv">counts</span> <span class="p">(</span><span class="nb">partial merge-with </span><span class="nv">+</span><span class="p">)</span> <span class="p">{</span><span class="nv">word</span> <span class="mi">1</span><span class="p">})</span>
         <span class="p">(</span><span class="nf">emit-bolt!</span> <span class="nv">collector</span> <span class="p">[</span><span class="nv">word</span> <span class="p">(</span><span class="o">@</span><span class="nv">counts</span> <span class="nv">word</span><span class="p">)]</span> <span class="ss">:anchor</span> <span class="nv">tuple</span><span class="p">)</span>
         <span class="p">(</span><span class="nf">ack!</span> <span class="nv">collector</span> <span class="nv">tuple</span><span class="p">)</span>
         <span class="p">)))))</span>
</code></pre></div>
<p>The implementation for a prepared bolt is a function that takes as input the topology config, <code>TopologyContext</code>, and <code>OutputCollector</code>, and returns an implementation of the <code>IBolt</code> interface. This design allows you to have a closure around the implementation of <code>execute</code> and <code>cleanup</code>. </p>

<p>In this example, the word counts are stored in the closure in a map called <code>counts</code>. The <code>bolt</code> macro is used to create the <code>IBolt</code> implementation. The <code>bolt</code> macro is a more concise way to implement the interface than reifying, and it automatically type-hints all of the method parameters. This bolt implements the execute method which updates the count in the map and emits the new word count.</p>

<p>Note that the <code>execute</code> method in prepared bolts only takes as input the tuple since the <code>OutputCollector</code> is already in the closure of the function (for simple bolts the collector is a second parameter to the <code>execute</code> function).</p>

<p>Prepared bolts can be parameterized just like simple bolts.</p>

<h4>Output declarations</h4>

<p>The Clojure DSL has a concise syntax for declaring the outputs of a bolt. The most general way to declare the outputs is as a map from stream id a stream spec. For example:</p>
<div class="highlight"><pre><code class="language-clojure" data-lang="clojure"><span class="p">{</span><span class="s">"1"</span> <span class="p">[</span><span class="s">"field1"</span> <span class="s">"field2"</span><span class="p">]</span>
 <span class="s">"2"</span> <span class="p">(</span><span class="nf">direct-stream</span> <span class="p">[</span><span class="s">"f1"</span> <span class="s">"f2"</span> <span class="s">"f3"</span><span class="p">])</span>
 <span class="s">"3"</span> <span class="p">[</span><span class="s">"f1"</span><span class="p">]}</span>
</code></pre></div>
<p>The stream id is a string, while the stream spec is either a vector of fields or a vector of fields wrapped by <code>direct-stream</code>. <code>direct stream</code> marks the stream as a direct stream (See <a href="Concepts.html">Concepts</a> and <a href="">Direct groupings</a> for more details on direct streams).</p>

<p>If the bolt only has one output stream, you can define the default stream of the bolt by using a vector instead of a map for the output declaration. For example:</p>
<div class="highlight"><pre><code class="language-clojure" data-lang="clojure"><span class="p">[</span><span class="s">"word"</span> <span class="s">"count"</span><span class="p">]</span>
</code></pre></div>
<p>This declares the output of the bolt as the fields ["word" "count"] on the default stream id.</p>

<h4>Emitting, acking, and failing</h4>

<p>Rather than use the Java methods on <code>OutputCollector</code> directly, the DSL provides a nicer set of functions for using <code>OutputCollector</code>: <code>emit-bolt!</code>, <code>emit-direct-bolt!</code>, <code>ack!</code>, and <code>fail!</code>.</p>

<ol>
<li><code>emit-bolt!</code>: takes as parameters the <code>OutputCollector</code>, the values to emit (a Clojure sequence), and keyword arguments for <code>:anchor</code> and <code>:stream</code>. <code>:anchor</code> can be a single tuple or a list of tuples, and <code>:stream</code> is the id of the stream to emit to. Omitting the keyword arguments emits an unanchored tuple to the default stream.</li>
<li><code>emit-direct-bolt!</code>: takes as parameters the <code>OutputCollector</code>, the task id to send the tuple to, the values to emit, and keyword arguments for <code>:anchor</code> and <code>:stream</code>. This function can only emit to streams declared as direct streams.</li>
<li><code>ack!</code>: takes as parameters the <code>OutputCollector</code> and the tuple to ack.</li>
<li><code>fail!</code>: takes as parameters the <code>OutputCollector</code> and the tuple to fail.</li>
</ol>

<p>See <a href="Guaranteeing-message-processing.html">Guaranteeing message processing</a> for more info on acking and anchoring.</p>

<h3>defspout</h3>

<p><code>defspout</code> is used for defining spouts in Clojure. Like bolts, spouts must be serializable so you can't just reify <code>IRichSpout</code> to do spout implementations in Clojure. <code>defspout</code> works around this restriction and provides a nicer syntax for defining spouts than just implementing a Java interface.</p>

<p>The signature for <code>defspout</code> looks like the following:</p>

<p>(defspout <em>name</em> <em>output-declaration</em> *<em>option-map</em> &amp; <em>impl</em>)</p>

<p>If you leave out the option map, it defaults to {:prepare true}. The output declaration for <code>defspout</code> has the same syntax as <code>defbolt</code>.</p>

<p>Here's an example <code>defspout</code> implementation from <a href="https://github.com/apache/storm/blob/master/examples/storm-starter/src/clj/storm/starter/clj/word_count.clj">storm-starter</a>:</p>
<div class="highlight"><pre><code class="language-clojure" data-lang="clojure"><span class="p">(</span><span class="nf">defspout</span> <span class="nv">sentence-spout</span> <span class="p">[</span><span class="s">"sentence"</span><span class="p">]</span>
  <span class="p">[</span><span class="nv">conf</span> <span class="nv">context</span> <span class="nv">collector</span><span class="p">]</span>
  <span class="p">(</span><span class="k">let </span><span class="p">[</span><span class="nv">sentences</span> <span class="p">[</span><span class="s">"a little brown dog"</span>
                   <span class="s">"the man petted the dog"</span>
                   <span class="s">"four score and seven years ago"</span>
                   <span class="s">"an apple a day keeps the doctor away"</span><span class="p">]]</span>
    <span class="p">(</span><span class="nf">spout</span>
     <span class="p">(</span><span class="nf">nextTuple</span> <span class="p">[]</span>
       <span class="p">(</span><span class="nf">Thread/sleep</span> <span class="mi">100</span><span class="p">)</span>
       <span class="p">(</span><span class="nf">emit-spout!</span> <span class="nv">collector</span> <span class="p">[(</span><span class="nf">rand-nth</span> <span class="nv">sentences</span><span class="p">)])</span>         
       <span class="p">)</span>
     <span class="p">(</span><span class="nf">ack</span> <span class="p">[</span><span class="nv">id</span><span class="p">]</span>
        <span class="c1">;; You only need to define this method for reliable spouts</span>
        <span class="c1">;; (such as one that reads off of a queue like Kestrel)</span>
        <span class="c1">;; This is an unreliable spout, so it does nothing here</span>
        <span class="p">))))</span>
</code></pre></div>
<p>The implementation takes in as input the topology config, <code>TopologyContext</code>, and <code>SpoutOutputCollector</code>. The implementation returns an <code>ISpout</code> object. Here, the <code>nextTuple</code> function emits a random sentence from <code>sentences</code>. </p>

<p>This spout isn't reliable, so the <code>ack</code> and <code>fail</code> methods will never be called. A reliable spout will add a message id when emitting tuples, and then <code>ack</code> or <code>fail</code> will be called when the tuple is completed or failed respectively. See <a href="Guaranteeing-message-processing.html">Guaranteeing message processing</a> for more info on how reliability works within Storm.</p>

<p><code>emit-spout!</code> takes in as parameters the <code>SpoutOutputCollector</code> and the new tuple to be emitted, and accepts as keyword arguments <code>:stream</code> and <code>:id</code>. <code>:stream</code> specifies the stream to emit to, and <code>:id</code> specifies a message id for the tuple (used in the <code>ack</code> and <code>fail</code> callbacks). Omitting these arguments emits an unanchored tuple to the default output stream.</p>

<p>There is also a <code>emit-direct-spout!</code> function that emits a tuple to a direct stream and takes an additional argument as the second parameter of the task id to send the tuple to.</p>

<p>Spouts can be parameterized just like bolts, in which case the symbol is bound to a function returning <code>IRichSpout</code> instead of the <code>IRichSpout</code> itself. You can also declare an unprepared spout which only defines the <code>nextTuple</code> method. Here is an example of an unprepared spout that emits random sentences parameterized at runtime:</p>
<div class="highlight"><pre><code class="language-clojure" data-lang="clojure"><span class="p">(</span><span class="nf">defspout</span> <span class="nv">sentence-spout-parameterized</span> <span class="p">[</span><span class="s">"word"</span><span class="p">]</span> <span class="p">{</span><span class="ss">:params</span> <span class="p">[</span><span class="nv">sentences</span><span class="p">]</span> <span class="ss">:prepare</span> <span class="nv">false</span><span class="p">}</span>
  <span class="p">[</span><span class="nv">collector</span><span class="p">]</span>
  <span class="p">(</span><span class="nf">Thread/sleep</span> <span class="mi">500</span><span class="p">)</span>
  <span class="p">(</span><span class="nf">emit-spout!</span> <span class="nv">collector</span> <span class="p">[(</span><span class="nf">rand-nth</span> <span class="nv">sentences</span><span class="p">)]))</span>
</code></pre></div>
<p>The following example illustrates how to use this spout in a <code>spout-spec</code>:</p>
<div class="highlight"><pre><code class="language-clojure" data-lang="clojure"><span class="p">(</span><span class="nf">spout-spec</span> <span class="p">(</span><span class="nf">sentence-spout-parameterized</span>
                   <span class="p">[</span><span class="s">"the cat jumped over the door"</span>
                    <span class="s">"greetings from a faraway land"</span><span class="p">])</span>
            <span class="ss">:p</span> <span class="mi">2</span><span class="p">)</span>
</code></pre></div>
<h3>Running topologies in local mode or on a cluster</h3>

<p>That's all there is to the Clojure DSL. To submit topologies in remote mode or local mode, just use the <code>StormSubmitter</code> or <code>LocalCluster</code> classes just like you would from Java.</p>

<p>To create topology configs, it's easiest to use the <code>backtype.storm.config</code> namespace which defines constants for all of the possible configs. The constants are the same as the static constants in the <code>Config</code> class, except with dashes instead of underscores. For example, here's a topology config that sets the number of workers to 15 and configures the topology in debug mode:</p>
<div class="highlight"><pre><code class="language-clojure" data-lang="clojure"><span class="p">{</span><span class="nv">TOPOLOGY-DEBUG</span> <span class="nv">true</span>
 <span class="nv">TOPOLOGY-WORKERS</span> <span class="mi">15</span><span class="p">}</span>
</code></pre></div>
<h3>Testing topologies</h3>

<p><a href="http://www.pixelmachine.org/2011/12/17/Testing-Storm-Topologies.html" target="_blank">This blog post</a> and its <a href="http://www.pixelmachine.org/2011/12/21/Testing-Storm-Topologies-Part-2.html" target="_blank">follow-up</a> give a good overview of Storm's powerful built-in facilities for testing topologies in Clojure.</p>
            </div>
        </div>
    </div>
</div>
<!--Content End-->
