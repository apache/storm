---
layout: documentation
title: Command Line Client
---
<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-12">
            	<h1 class="page-title">Command Line Client</h1>
                <p>This page describes all the commands that are possible with the "storm" command line client. To learn how to set up your "storm" client to talk to a remote cluster, follow the instructions in <a href="setting-up-Storm-cluster.html">Setting up development environment</a>.</p>
                <p>These commands are:</p>
                <ol>
                    <li>jar</li>
                    <li>kill</li>
                    <li>activate</li>
                    <li>deactivate</li>
                    <li>rebalance</li>
                    <li>repl</li>
                    <li>classpath</li>
                    <li>localconfvalue</li>
                    <li>remoteconfvalue</li>
                    <li>nimbus</li>
                    <li>supervisor</li>
                    <li>ui</li>
                    <li>drpc</li>
                </ol>
                <h3>jar</h3>
                <p>Syntax: <code>storm jar topology-jar-path class ...</code></p>
                <p>Runs the main method of <code>class</code> with the specified arguments. The storm jars and configs in <code>~/.storm</code> are put on the classpath. The process is configured so that <a href="https://storm.apache.org/javadoc/apidocs/backtype/storm/StormSubmitter.html">StormSubmitter</a> will upload the jar at <code>topology-jar-path</code> when the topology is submitted.</p>
                <h3>kill</h3>
                <p>Syntax: <code>storm kill topology-name [-w wait-time-secs]</code></p>
                <p>Kills the topology with the name <code>topology-name</code>. Storm will first deactivate the topology's spouts for the duration of the topology's message timeout to allow all messages currently being processed to finish processing. Storm will then shutdown the workers and clean up their state. You can override the length of time Storm waits between deactivation and shutdown with the -w flag.</p>
                <h3>activate</h3>
                <p>Syntax: <code>storm activate topology-name</code></p>
                <p>Activates the specified topology's spouts.</p>
                <h3>deactivate</h3>
                <p>Syntax: <code>storm deactivate topology-name</code></p>
                <p>Deactivates the specified topology's spouts.</p>
                <h3>rebalance</h3>
                <p>Syntax: <code>storm rebalance topology-name [-w wait-time-secs]</code></p>
                <p>Sometimes you may wish to spread out where the workers for a topology are running. For example, let's say you have a 10 node cluster running 4 workers per node, and then let's say you add another 10 nodes to the cluster. You may wish to have Storm spread out the workers for the running topology so that each node runs 2 workers. One way to do this is to kill the topology and resubmit it, but Storm provides a "rebalance" command that provides an easier way to do this. </p>
                <p>Rebalance will first deactivate the topology for the duration of the message timeout (overridable with the -w flag) and then redistribute the workers evenly around the cluster. The topology will then return to its previous state of activation (so a deactivated topology will still be deactivated and an activated topology will go back to being activated).</p>
                <h3>repl</h3>
                <p>Syntax: <code>storm repl</code></p>
                <p>Opens up a Clojure REPL with the storm jars and configuration on the classpath. Useful for debugging.</p>
                <h3>classpath</h3>
                <p>Syntax: <code>storm classpath</code></p>
                <p>Prints the classpath used by the storm client when running commands.</p>
                <h3>localconfvalue</h3>
                <p>Syntax: <code>storm localconfvalue conf-name</code></p>
                <p>Prints out the value for <code>conf-name</code> in the local Storm configs. The local Storm configs are the ones in <code>~/.storm/storm.yaml</code> merged in with the configs in <code>defaults.yaml</code>.</p>
                <h3>remoteconfvalue</h3>
                <p>Syntax: <code>storm remoteconfvalue conf-name</code></p>
                <p>Prints out the value for <code>conf-name</code> in the cluster's Storm configs. The cluster's Storm configs are the ones in <code>$STORM-PATH/conf/storm.yaml</code> merged in with the configs in <code>defaults.yaml</code>. This command must be run on a cluster machine.</p>
                <h3>nimbus</h3>
                <p>Syntax: <code>storm nimbus</code></p>
                <p>Launches the nimbus daemon. This command should be run under supervision with a tool like <a href="http://cr.yp.to/daemontools.html" target="_blank">daemontools</a> or <a href="http://mmonit.com/monit/" target="_blank">monit</a>. See <a href="setting-up-Storm-cluster.html">Setting up a Storm cluster</a> for more information.</p>
                <h3>supervisor</h3>
                <p>Syntax: <code>storm supervisor</code></p>
                <p>Launches the supervisor daemon. This command should be run under supervision with a tool like <a href="http://cr.yp.to/daemontools.html" target="_blank">daemontools</a> or <a href="http://mmonit.com/monit/" target="_blank">monit</a>. See <a href="setting-up-Storm-cluster.html">Setting up a Storm cluster</a> for more information.</p>
                <h3>ui</h3>
                <p>Syntax: <code>storm ui</code></p>
                <p>Launches the UI daemon. The UI provides a web interface for a Storm cluster and shows detailed stats about running topologies. This command should be run under supervision with a tool like <a href="http://cr.yp.to/daemontools.html" target="_blank">daemontools</a> or <a href="http://mmonit.com/monit/" target="_blank">monit</a>. See <a href="setting-up-Storm-cluster.html">Setting up a Storm cluster</a> for more information.</p>
                <h3>drpc</h3>
                <p>Syntax: <code>storm drpc</code></p>
                <p>Launches a DRPC daemon. This command should be run under supervision with a tool like <a href="http://cr.yp.to/daemontools.html" target="_blank">daemontools</a> or <a href="http://mmonit.com/monit/" target="_blank">monit</a>. See <a href="setting-up-Storm-cluster.html">Distributed RPC</a> for more information.</p>
            </div>
        </div>
    </div>
</div>
<!--Content End-->
