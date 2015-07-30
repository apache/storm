---
layout: documentation
title: Storm Multi-Lang Protocol (Versions 0.7.0 and below)
---
<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-12">
            	<h1 class="page-title">Storm Multi-Lang Protocol (Versions 0.7.0 and below)</h1>
            </div>
        </div>
        <div class="row">
            <div class="col-md-12">
            	<p>This page explains the multilang protocol for versions 0.7.0 and below. The protocol changed in version 0.7.1.</p>

				<h1 id="storm-multi-language-protocol">Storm Multi-Language Protocol</h1>

				<h2 id="the-shellbolt">The ShellBolt</h2>

				<p>Support for multiple languages is implemented via the ShellBolt class.  This
				class implements the IBolt interfaces and implements the protocol for
				executing a script or program via the shell using Java's ProcessBuilder class.</p>

				<h2 id="output-fields">Output fields</h2>

				<p>Output fields are part of the Thrift definition of the topology. This means that when you multilang in Java, you need to create a bolt that extends ShellBolt, implements IRichBolt, and declared the fields in <code>declareOutputFields</code>. 
				You can learn more about this on <a href="/documentation/concepts.html">Concepts</a></p>

				<h2 id="protocol-preamble">Protocol Preamble</h2>

				<p>A simple protocol is implemented via the STDIN and STDOUT of the executed
				script or program. A mix of simple strings and JSON encoded data are exchanged
				with the process making support possible for pretty much any language.</p>

				<h1 id="packaging-your-stuff">Packaging Your Stuff</h1>

				<p>To run a ShellBolt on a cluster, the scripts that are shelled out to must be
				in the <code>resources/</code> directory within the jar submitted to the master.</p>

				<p>However, During development or testing on a local machine, the resources
				directory just needs to be on the classpath.</p>

				<h2 id="the-protocol">The Protocol</h2>

				<p>Notes:
				* Both ends of this protocol use a line-reading mechanism, so be sure to
				trim off newlines from the input and to append them to your output.
				* All JSON inputs and outputs are terminated by a single line contained "end".
				* The bullet points below are written from the perspective of the script writer's
				STDIN and STDOUT.</p>

				<ul>
				<li>Your script will be executed by the Bolt.</li>
				<li>STDIN: A string representing a path. This is a PID directory.
				Your script should create an empty file named with it's pid in this directory. e.g.
				the PID is 1234, so an empty file named 1234 is created in the directory. This
				file lets the supervisor know the PID so it can shutdown the process later on.</li>
				<li>STDOUT: Your PID. This is not JSON encoded, just a string. ShellBolt will log the PID to its log.</li>
				<li>STDIN: (JSON) The Storm configuration.  Various settings and properties.</li>
				<li>STDIN: (JSON) The Topology context</li>
				<li>The rest happens in a while(true) loop</li>
				<li>STDIN: A tuple! This is a JSON encoded structure like this:</li>
				</ul>
				<div class="highlight"><pre><code class="language-text" data-lang="text">{
				    // The tuple's id
				    "id": -6955786537413359385,
				    // The id of the component that created this tuple
				    "comp": 1,
				    // The id of the stream this tuple was emitted to
				    "stream": 1,
				    // The id of the task that created this tuple
				    "task": 9,
				    // All the values in this tuple
				    "tuple": ["snow white and the seven dwarfs", "field2", 3]
				}
				</code></pre></div>
				<ul>
				<li>STDOUT: The results of your bolt, JSON encoded. This can be a sequence of acks, fails, emits, and/or logs. Emits look like:</li>
				</ul>
				<div class="highlight"><pre><code class="language-text" data-lang="text">{
				    "command": "emit",
				    // The ids of the tuples this output tuples should be anchored to
				    "anchors": [1231231, -234234234],
				    // The id of the stream this tuple was emitted to. Leave this empty to emit to default stream.
				    "stream": 1,
				    // If doing an emit direct, indicate the task to sent the tuple to
				    "task": 9,
				    // All the values in this tuple
				    "tuple": ["field1", 2, 3]
				}
				</code></pre></div>
				<p>An ack looks like:</p>
				<div class="highlight"><pre><code class="language-text" data-lang="text">{
				    "command": "ack",
				    // the id of the tuple to ack
				    "id": 123123
				}
				</code></pre></div>
				<p>A fail looks like:</p>
				<div class="highlight"><pre><code class="language-text" data-lang="text">{
				    "command": "fail",
				    // the id of the tuple to fail
				    "id": 123123
				}
				</code></pre></div>
				<p>A "log" will log a message in the worker log. It looks like:</p>
				<div class="highlight"><pre><code class="language-text" data-lang="text">{
				    "command": "log",
				    // the message to log
				    "msg": "hello world!"

				}
				</code></pre></div>
				<ul>
				<li>STDOUT: emit "sync" as a single line by itself when the bolt has finished emitting/acking/failing and is ready for the next input</li>
				</ul>

				<h3 id="sync">sync</h3>

				<p>Note: This command is not JSON encoded, it is sent as a simple string.</p>

				<p>This lets the parent bolt know that the script has finished processing and is ready for another tuple.</p>
            </div>
        </div>
    </div>
</div>