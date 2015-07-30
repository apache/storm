---
layout: documentation
title: Fault tolerance
---
<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-12">
            	<h1 class="page-title">Fault Tolerance</h1>
                <p>This page explains the design details of Storm that make it a fault-tolerant system.</p>
                <h3>What happens when a worker dies?</h3>
                <p>When a worker dies, the supervisor will restart it. If it continuously fails on startup and is unable to heartbeat to Nimbus, Nimbus will reassign the worker to another machine.</p>
                <h3>What happens when a node dies?</h3>
                <p>The tasks assigned to that machine will time-out and Nimbus will reassign those tasks to other machines.</p>
                <h3>What happens when Nimbus or Supervisor daemons die?</h3>
                <p>The Nimbus and Supervisor daemons are designed to be fail-fast (process self-destructs whenever any unexpected situation is encountered) and stateless (all state is kept in Zookeeper or on disk). As described in <a href="setting-up-Storm-cluster.html">Setting up a Storm cluster</a>, the Nimbus and Supervisor daemons must be run under supervision using a tool like daemontools or monit. So if the Nimbus or Supervisor daemons die, they restart like nothing happened.</p>
                <p>Most notably, no worker processes are affected by the death of Nimbus or the Supervisors. This is in contrast to Hadoop, where if the JobTracker dies, all the running jobs are lost. </p>
                <h3>Is Nimbus a single point of failure?</h3>
                <p>If you lose the Nimbus node, the workers will still continue to function. Additionally, supervisors will continue to restart workers if they die. However, without Nimbus, workers won't be reassigned to other machines when necessary (like if you lose a worker machine). </p>
                <p>So the answer is that Nimbus is "sort of" a SPOF. In practice, it's not a big deal since nothing catastrophic happens when the Nimbus daemon dies. There are plans to make Nimbus highly available in the future.</p>
                <h3>How does Storm guarantee data processing?</h3>
                <p>Storm provides mechanisms to guarantee data processing even if nodes die or messages are lost. See <a href="guaranteeing-message-processing.html">Guaranteeing message processing</a> for the details.</p>
            </div>
        </div>
    </div>
</div>
<!--Content End-->
