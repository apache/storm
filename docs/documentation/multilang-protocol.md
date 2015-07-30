---
layout: documentation
title: Multilang Protocols
---
<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-12">
            	<h1 class="page-title">Multi-Lang Protocol</h1>
    <p>This page explains the multilang protocol as of Storm 0.7.1. Versions prior to 0.7.1 used a somewhat different protocol, documented [here](Storm-multi-language-protocol-(versions-0.7.0-and-below).html).</p>

<h2>Storm Multi-Language Protocol</h2>

<h3>Shell Components</h3>

<p>Support for multiple languages is implemented via the ShellBolt,
ShellSpout, and ShellProcess classes.  These classes implement the
IBolt and ISpout interfaces and the protocol for executing a script or
program via the shell using Java's ProcessBuilder class.</p>

<h3>Output fields</h3>

<p>Output fields are part of the Thrift definition of the topology. This means that when you multilang in Java, you need to create a bolt that extends ShellBolt, implements IRichBolt, and declare the fields in <code>declareOutputFields</code> (similarly for ShellSpout).</p>

<p>You can learn more about this on <a href="concepts.html">Concepts</a></p>

<h3>Protocol Preamble</h3>

<p>A simple protocol is implemented via the STDIN and STDOUT of the
executed script or program. All data exchanged with the process is
encoded in JSON, making support possible for pretty much any language.</p>

<h2>Packaging Your Stuff</h2>

<p>To run a shell component on a cluster, the scripts that are shelled
out to must be in the <code>resources/</code> directory within the jar submitted
to the master.</p>

<p>However, during development or testing on a local machine, the resources
directory just needs to be on the classpath.</p>

<h3>The Protocol</h3>

<p>Notes:</p>

<ul>
<li>Both ends of this protocol use a line-reading mechanism, so be sure to
trim off newlines from the input and to append them to your output.</li>
<li>All JSON inputs and outputs are terminated by a single line containing "end". Note that this delimiter is not itself JSON encoded.</li>
<li>The bullet points below are written from the perspective of the script writer's
STDIN and STDOUT.</li>
</ul>

<h3>Initial Handshake</h3>

<p>The initial handshake is the same for both types of shell components:</p>

<ul>
<li>STDIN: Setup info. This is a JSON object with the Storm configuration, a PID directory, and a topology context, like this:</li>
</ul>
<div class="highlight"><pre><code class="language-text" data-lang="text">{
    "conf": {
        "topology.message.timeout.secs": 3,
        // etc
    },
    "pidDir": "...",
    "context": {
        "task-&gt;component": {
            "1": "example-spout",
            "2": "__acker",
            "3": "example-bolt1",
            "4": "example-bolt2"
        },
        "taskid": 3,
        // Everything below this line is only available in Storm 0.11.0+
        "componentid": "example-bolt"
        "stream-&gt;target-&gt;grouping": {
            "default": {
                "example-bolt2": {
                    "type": "SHUFFLE"}}},
        "streams": ["default"],
        "stream-&gt;outputfields": {"default": ["word"]},
        "source-&gt;stream-&gt;grouping": {
            "example-spout": {
                "default": {
                    "type": "FIELDS",
                    "fields": ["word"]
                }
            }
        }
    }
}
</code></pre></div>
<p>Your script should create an empty file named with its PID in this directory. e.g.
the PID is 1234, so an empty file named 1234 is created in the directory. This
file lets the supervisor know the PID so it can shutdown the process later on.</p>

<p>As of Storm 0.11.0, the context sent by Storm to shell components has been
enhanced substantially to include all aspects of the topology context available
to JVM components.  One key addition is the ability to determine a shell
component's source and targets (i.e., inputs and outputs) in the topology via
the <code>stream-&gt;target-&gt;grouping</code> and <code>source-&gt;stream-&gt;grouping</code> dictionaries.  At
the innermost level of these nested dictionaries, groupings are represented as
a dictionary that minimally has a <code>type</code> key, but can also have a <code>fields</code> key
to specify which fields are involved in a <code>FIELDS</code> grouping.</p>

<ul>
<li>STDOUT: Your PID, in a JSON object, like <code>{"pid": 1234}</code>. The shell component will log the PID to its log.</li>
</ul>

<p>What happens next depends on the type of component:</p>

<h3>Spouts</h3>

<p>Shell spouts are synchronous. The rest happens in a while(true) loop:</p>

<ul>
<li>STDIN: Either a next, ack, or fail command.</li>
</ul>

<p>"next" is the equivalent of ISpout's <code>nextTuple</code>. It looks like:</p>
<div class="highlight"><pre><code class="language-text" data-lang="text">{"command": "next"}
</code></pre></div>
<p>"ack" looks like:</p>
<div class="highlight"><pre><code class="language-text" data-lang="text">{"command": "ack", "id": "1231231"}
</code></pre></div>
<p>"fail" looks like:</p>
<div class="highlight"><pre><code class="language-text" data-lang="text">{"command": "fail", "id": "1231231"}
</code></pre></div>
<ul>
<li>STDOUT: The results of your spout for the previous command. This can
be a sequence of emits and logs.</li>
</ul>

<p>An emit looks like:</p>
<div class="highlight"><pre><code class="language-text" data-lang="text">{
    "command": "emit",
    // The id for the tuple. Leave this out for an unreliable emit. The id can
    // be a string or a number.
    "id": "1231231",
    // The id of the stream this tuple was emitted to. Leave this empty to emit to default stream.
    "stream": "1",
    // If doing an emit direct, indicate the task to send the tuple to
    "task": 9,
    // All the values in this tuple
    "tuple": ["field1", 2, 3]
}
</code></pre></div>
<p>If not doing an emit direct, you will immediately receive the task ids to which the tuple was emitted on STDIN as a JSON array.</p>

<p>A "log" will log a message in the worker log. It looks like:</p>
<div class="highlight"><pre><code class="language-text" data-lang="text">{
    "command": "log",
    // the message to log
    "msg": "hello world!"
}
</code></pre></div>
<ul>
<li>STDOUT: a "sync" command ends the sequence of emits and logs. It looks like:</li>
</ul>
<div class="highlight"><pre><code class="language-text" data-lang="text">{"command": "sync"}
</code></pre></div>
<p>After you sync, ShellSpout will not read your output until it sends another next, ack, or fail command.</p>

<p>Note that, similarly to ISpout, all of the spouts in the worker will be locked up after a next, ack, or fail, until you sync. Also like ISpout, if you have no tuples to emit for a next, you should sleep for a small amount of time before syncing. ShellSpout will not automatically sleep for you.</p>

<h3>Bolts</h3>

<p>The shell bolt protocol is asynchronous. You will receive tuples on STDIN as soon as they are available, and you may emit, ack, and fail, and log at any time by writing to STDOUT, as follows:</p>

<ul>
<li>STDIN: A tuple! This is a JSON encoded structure like this:</li>
</ul>
<div class="highlight"><pre><code class="language-text" data-lang="text">{
    // The tuple's id - this is a string to support languages lacking 64-bit precision
    "id": "-6955786537413359385",
    // The id of the component that created this tuple
    "comp": "1",
    // The id of the stream this tuple was emitted to
    "stream": "1",
    // The id of the task that created this tuple
    "task": 9,
    // All the values in this tuple
    "tuple": ["snow white and the seven dwarfs", "field2", 3]
}
</code></pre></div>
<ul>
<li>STDOUT: An ack, fail, emit, or log. Emits look like:</li>
</ul>
<div class="highlight"><pre><code class="language-text" data-lang="text">{
    "command": "emit",
    // The ids of the tuples this output tuples should be anchored to
    "anchors": ["1231231", "-234234234"],
    // The id of the stream this tuple was emitted to. Leave this empty to emit to default stream.
    "stream": "1",
    // If doing an emit direct, indicate the task to send the tuple to
    "task": 9,
    // All the values in this tuple
    "tuple": ["field1", 2, 3]
}
</code></pre></div>
<p>If not doing an emit direct, you will receive the task ids to which
the tuple was emitted on STDIN as a JSON array. Note that, due to the
asynchronous nature of the shell bolt protocol, when you read after
emitting, you may not receive the task ids. You may instead read the
task ids for a previous emit or a new tuple to process. You will
receive the task id lists in the same order as their corresponding
emits, however.</p>

<p>An ack looks like:</p>
<div class="highlight"><pre><code class="language-text" data-lang="text">{
    "command": "ack",
    // the id of the tuple to ack
    "id": "123123"
}
</code></pre></div>
<p>A fail looks like:</p>
<div class="highlight"><pre><code class="language-text" data-lang="text">{
    "command": "fail",
    // the id of the tuple to fail
    "id": "123123"
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
<li>Note that, as of version 0.7.1, there is no longer any need for a
shell bolt to 'sync'.</li>
</ul>

<h3>Handling Heartbeats (0.9.3 and later)</h3>

<p>As of Storm 0.9.3, heartbeats have been between ShellSpout/ShellBolt and their
multi-lang subprocesses to detect hanging/zombie subprocesses.  Any libraries
for interfacing with Storm via multi-lang must take the following actions
regarding hearbeats:</p>

<h4>Spout</h4>

<p>Shell spouts are synchronous, so subprocesses always send <code>sync</code> commands at the
end of <code>next()</code>,  so you should not have to do much to support heartbeats for
spouts.  That said, you must not let subprocesses sleep more than the worker
timeout during <code>next()</code>.</p>

<h4>Bolt</h4>

<p>Shell bolts are asynchronous, so a ShellBolt will send heartbeat tuples to its
subprocess periodically.  Heartbeat tuple looks like:</p>
<div class="highlight"><pre><code class="language-text" data-lang="text">{
    "id": "-6955786537413359385",
    "comp": "1",
    "stream": "__heartbeat",
    // this shell bolt's system task id
    "task": -1,
    "tuple": []
}
</code></pre></div>
<p>When subprocess receives heartbeat tuple, it must send a <code>sync</code> command back to
ShellBolt.</p>
            </div>
        </div>
    </div>
</div>
<!--Content End-->