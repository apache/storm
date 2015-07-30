---
layout: documentation
title: Setting Up a Storm CLuster
---
<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-12">
            	<h1 class="page-title">Setting up a Storm Cluster</h1>
    <p>This page outlines the steps for getting a Storm cluster up and running. If you're on AWS, you should check out the <a href="https://github.com/nathanmarz/storm-deploy/wiki" target="_blank">storm-deploy</a> project. <a href="https://github.com/nathanmarz/storm-deploy/wiki" target="_blank">storm-deploy</a> completely automates the provisioning, configuration, and installation of Storm clusters on EC2. It also sets up Ganglia for you so you can monitor CPU, disk, and network usage.</p>
<p>If you run into difficulties with your Storm cluster, first check for a solution is in the <a href="troubleshooting.html">Troubleshooting</a> page. Otherwise, email the mailing list.</p>

<p>Here's a summary of the steps for setting up a Storm cluster:</p>

<ol>
<li>Set up a Zookeeper cluster</li>
<li>Install dependencies on Nimbus and worker machines</li>
<li>Download and extract a Storm release to Nimbus and worker machines</li>
<li>Fill in mandatory configurations into storm.yaml</li>
<li>Launch daemons under supervision using "storm" script and a supervisor of your choice</li>
</ol>

<h3>Set up a Zookeeper cluster</h3>

<p>Storm uses Zookeeper for coordinating the cluster. Zookeeper <strong>is not</strong> used for message passing, so the load Storm places on Zookeeper is quite low. Single node Zookeeper clusters should be sufficient for most cases, but if you want failover or are deploying large Storm clusters you may want larger Zookeeper clusters. Instructions for deploying Zookeeper are <a href="http://zookeeper.apache.org/doc/r3.3.3/zookeeperAdmin.html" target="_blank">here</a>. </p>

<p>A few notes about Zookeeper deployment:</p>

<ol>
<li>It's critical that you run Zookeeper under supervision, since Zookeeper is fail-fast and will exit the process if it encounters any error case. See <a href="http://zookeeper.apache.org/doc/r3.3.3/zookeeperAdmin.html#sc_supervision" target="_blank">here</a> for more details. </li>
<li>It's critical that you set up a cron to compact Zookeeper's data and transaction logs. The Zookeeper daemon does not do this on its own, and if you don't set up a cron, Zookeeper will quickly run out of disk space. See <a href="http://zookeeper.apache.org/doc/r3.3.3/zookeeperAdmin.html#sc_maintenance" target="_blank">here</a> for more details.</li>
</ol>

<h3>Install dependencies on Nimbus and worker machines</h3>

<p>Next you need to install Storm's dependencies on Nimbus and the worker machines. These are:</p>

<ol>
<li>Java 6</li>
<li>Python 2.6.6</li>
</ol>

<p>These are the versions of the dependencies that have been tested with Storm. Storm may or may not work with different versions of Java and/or Python.</p>

<h3>Download and extract a Storm release to Nimbus and worker machines</h3>

<p>Next, download a Storm release and extract the zip file somewhere on Nimbus and each of the worker machines. The Storm releases can be downloaded <a href="http://github.com/apache/storm/releases" target="_blank">from here</a>.</p>

<h3>Fill in mandatory configurations into storm.yaml</h3>

<p>The Storm release contains a file at <code>conf/storm.yaml</code> that configures the Storm daemons. You can see the default configuration values <a href="https://github.com/apache/storm/blob/master/conf/defaults.yaml" target="_blank">here</a>. storm.yaml overrides anything in defaults.yaml. There's a few configurations that are mandatory to get a working cluster:</p>

<p>1) <strong>storm.zookeeper.servers</strong>: This is a list of the hosts in the Zookeeper cluster for your Storm cluster. It should look something like:</p>
<div class="highlight"><pre><code class="language-yaml" data-lang="yaml"><span class="l-Scalar-Plain">storm.zookeeper.servers</span><span class="p-Indicator">:</span>
  <span class="p-Indicator">-</span> <span class="s">"111.222.333.444"</span>
  <span class="p-Indicator">-</span> <span class="s">"555.666.777.888"</span>
</code></pre></div>
<p>If the port that your Zookeeper cluster uses is different than the default, you should set <strong>storm.zookeeper.port</strong> as well.</p>

<p>2) <strong>storm.local.dir</strong>: The Nimbus and Supervisor daemons require a directory on the local disk to store small amounts of state (like jars, confs, and things like that). You should create that directory on each machine, give it proper permissions, and then fill in the directory location using this config. For example:</p>
<div class="highlight"><pre><code class="language-yaml" data-lang="yaml"><span class="l-Scalar-Plain">storm.local.dir</span><span class="p-Indicator">:</span> <span class="s">"/mnt/storm"</span>
</code></pre></div>
<p>3) <strong>nimbus.host</strong>: The worker nodes need to know which machine is the master in order to download topology jars and confs. For example:</p>
<div class="highlight"><pre><code class="language-yaml" data-lang="yaml"><span class="l-Scalar-Plain">nimbus.host</span><span class="p-Indicator">:</span> <span class="s">"111.222.333.44"</span>
</code></pre></div>
<p>4) <strong>supervisor.slots.ports</strong>: For each worker machine, you configure how many workers run on that machine with this config. Each worker uses a single port for receiving messages, and this setting defines which ports are open for use. If you define five ports here, then Storm will allocate up to five workers to run on this machine. If you define three ports, Storm will only run up to three. By default, this setting is configured to run 4 workers on the ports 6700, 6701, 6702, and 6703. For example:</p>
<div class="highlight"><pre><code class="language-yaml" data-lang="yaml"><span class="l-Scalar-Plain">supervisor.slots.ports</span><span class="p-Indicator">:</span>
    <span class="p-Indicator">-</span> <span class="l-Scalar-Plain">6700</span>
    <span class="p-Indicator">-</span> <span class="l-Scalar-Plain">6701</span>
    <span class="p-Indicator">-</span> <span class="l-Scalar-Plain">6702</span>
    <span class="p-Indicator">-</span> <span class="l-Scalar-Plain">6703</span>
</code></pre></div>
<h3>Configure external libraries and environmental variables (optional)</h3>

<p>If you need support from external libraries or custom plugins, you can place such jars into the extlib/ and extlib-daemon/ directories. Note that the extlib-daemon/ directory stores jars used only by daemons (Nimbus, Supervisor, DRPC, UI, Logviewer), e.g., HDFS and customized scheduling libraries. Accordingly, two environmental variables STORM_EXT_CLASSPATH and STORM_EXT_CLASSPATH_DAEMON can be configured by users for including the external classpath and daemon-only external classpath.</p>

<h3>Launch daemons under supervision using "storm" script and a supervisor of your choice</h3>
<p>The last step is to launch all the Storm daemons. It is critical that you run each of these daemons under supervision. Storm is a <strong>fail-fast</strong> system which means the processes will halt whenever an unexpected error is encountered. Storm is designed so that it can safely halt at any point and recover correctly when the process is restarted. This is why Storm keeps no state in-process -- if Nimbus or the Supervisors restart, the running topologies are unaffected. Here's how to run the Storm daemons:</p>

<ol>
<li><strong>Nimbus</strong>: Run the command "bin/storm nimbus" under supervision on the master machine.</li>
<li><strong>Supervisor</strong>: Run the command "bin/storm supervisor" under supervision on each worker machine. The supervisor daemon is responsible for starting and stopping worker processes on that machine.</li>
<li><strong>UI</strong>: Run the Storm UI (a site you can access from the browser that gives diagnostics on the cluster and topologies) by running the command "bin/storm ui" under supervision. The UI can be accessed by navigating your web browser to http://{nimbus host}:8080. </li>
</ol>

<p>As you can see, running the daemons is very straightforward. The daemons will log to the logs/ directory in wherever you extracted the Storm release.</p>
            </div>
        </div>
    </div>
</div>
<!--Content End-->