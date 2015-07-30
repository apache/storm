---
layout: documentation
title: Acking Framework Implementation
---
<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-12">
            	<h1 class="page-title">Acking Framework Implementation</h1>
            </div>
        </div>
        <div class="row">
            <div class="col-md-12">
	            <p><a href="https://github.com/apache/storm/blob/46c3ba7/storm-core/src/clj/backtype/storm/daemon/acker.clj#L28">Storm's acker</a> tracks completion of each tupletree with a checksum hash: each time a tuple is sent, its value is XORed into the checksum, and each time a tuple is acked its value is XORed in again. If all tuples have been successfully acked, the checksum will be zero (the odds that the checksum will be zero otherwise are vanishingly small).</p>

				<p>You can read a bit more about the <a href="guaranteeing-message-processing.html#what-is-storms-reliability-api">reliability mechanism</a> elsewhere on the wiki -- this explains the internal details.</p>

				<h3 id="acker-execute()">acker <code>execute()</code></h3>

				<p>The acker is actually a regular bolt, with its  <a href="https://github.com/apache/storm/blob/46c3ba7/storm-core/src/clj/backtype/storm/daemon/acker.clj#L36">execute method</a> defined withing <code>mk-acker-bolt</code>.  When a new tupletree is born, the spout sends the XORed edge-ids of each tuple recipient, which the acker records in its <code>pending</code> ledger. Every time an executor acks a tuple, the acker receives a partial checksum that is the XOR of the tuple's own edge-id (clearing it from the ledger) and the edge-id of each downstream tuple the executor emitted (thus entering them into the ledger).</p>

				<p>This is accomplished as follows.</p>

				<p>On a tick tuple, just advance pending tupletree checksums towards death and return. Otherwise, update or create the record for this tupletree:</p>

				<ul>
				<li>on init: initialize with the given checksum value, and record the spout's id for later.</li>
				<li>on ack:  xor the partial checksum into the existing checksum value</li>
				<li>on fail: just mark it as failed</li>
				</ul>

				<p>Next, <a href="https://github.com/apache/storm/blob/46c3ba7/storm-core/src/clj/backtype/storm/daemon/acker.clj#L50">put the record</a>),  into the RotatingMap (thus resetting is countdown to expiry) and take action:</p>

				<ul>
				<li>if the total checksum is zero, the tupletree is complete: remove it from the pending collection and notify the spout of success</li>
				<li>if the tupletree has failed, it is also complete:   remove it from the pending collection and notify the spout of failure</li>
				</ul>

				<p>Finally, pass on an ack of our own.</p>

				<h3 id="pending-tuples-and-the-rotatingmap">Pending tuples and the <code>RotatingMap</code></h3>

				<p>The acker stores pending tuples in a <a href="https://github.com/apache/storm/blob/master/storm-core/src/jvm/backtype/storm/utils/RotatingMap.java#L19"><code>RotatingMap</code></a>, a simple device used in several places within Storm to efficiently time-expire a process.</p>

				<p>The RotatingMap behaves as a HashMap, and offers the same O(1) access guarantees.</p>

				<p>Internally, it holds several HashMaps ('buckets') of its own, each holding a cohort of records that will expire at the same time.  Let's call the longest-lived bucket death row, and the most recent the nursery. Whenever a value is <code>.put()</code> to the RotatingMap, it is relocated to the nursery -- and removed from any other bucket it might have been in (effectively resetting its death clock).</p>

				<p>Whenever its owner calls <code>.rotate()</code>, the RotatingMap advances each cohort one step further towards expiration. (Typically, Storm objects call rotate on every receipt of a system tick stream tuple.) If there are any key-value pairs in the former death row bucket, the RotatingMap invokes a callback (given in the constructor) for each key-value pair, letting its owner take appropriate action (eg, failing a tuple.</p>
            </div>
        </div>
    </div>
</div>