---
layout: news
title: Storm 0.9.2 released
---
<!--Post Header-->
<h3 class="news-title">Storm 0.9.2 released</h3>
<div class="news-meta">
    <i class="fa fa-calendar"></i> Jun 25, 2014 <i class="fa fa-user"></i> P. Taylor Goetz
</div>
<!--Post Body-->
<p>We are pleased to announce that Storm 0.9.2-incubating has been released and is available from <a href="/downloads.html">the downloads page</a>. This release includes many important fixes and improvements.</p>
<h4>Netty Transport Improvements</h4>
<p>Storm's Netty-based transport has been overhauled to significantly improve performance through better utilization of thread, CPU, and network resources, particularly in cases where message sizes are small. Storm contributor Sean Zhong (<a href="https://github.com/clockfly">@clockfly</a>) deserves a great deal of credit not only for discovering, analyzing, documenting and fixing the root cause, but also for persevering through an extended review process and promptly addressing all concerns.</p>
<p>Those interested in the technical details and evolution of this patch can find out more in the <a href="https://issues.apache.org/jira/browse/STORM-297" target="_blank">JIRA ticket for STORM-297</a>.</p>
<p>Sean also discovered and fixed an <a href="https://issues.apache.org/jira/browse/STORM-342" target="_blank">elusive bug</a> in Storm's usage of the Disruptor queue that could lead to out-of-order or lost messages. </p>
<p>Many thanks to Sean for contributing these important fixes.</p>
<h4>Storm UI Improvements</h4>
<p>This release also includes a number of improvements to the Storm UI service. Contributor Sriharsha Chintalapani(<a href="https://github.com/harshach">@harshach</a>) added a REST API to the Storm UI service to expose metrics and operations in JSON format, and updated the UI to use that API.</p>
<p>The new REST API will make it considerably easier for other services to consume availabe cluster and topology metrics for monitoring and visualization applications. Kyle Nusbaum (<a href="https://github.com/knusbaum">@knusbaum</a>) has already leveraged the REST API to create a topology visualization tool now included in Storm UI and illustrated in the screenshot below.</p>
<p><img src="ui_topology_viz.png" alt="Storm UI Topology Visualization" class="img-responsive"></p>
<p>In the visualization, spout components are represented as blue, while bolts are colored between green and red depending on their associated capacity metric. The width of the lines between the components represent the flow of tuples relative to the other visible streams. </p>
<h4>Kafka Spout</h4>
<p>This is the first Storm release to include official support for consuming data from Kafka 0.8.x. In the past, development of Kafka spouts for Storm had become somewhat fragmented and finding an implementation that worked with certain versions of Storm and Kafka proved burdensome for some developers. This is no longer the case, as the <code>storm-kafka</code> module is now part of the Storm project and associated artifacts are released to official channels (Maven Central) along with Storm's other components.</p>
<p>Thanks are due to GitHub user <a href="">@wurstmeister</a> for picking up Nathan Marz' original Kafka 0.7.x implementation, updating it to work with Kafka 0.8.x, and contributing that work back to the Storm community.</p>
<p>The <code>storm-kafka</code> module can be found in the <code>/external/</code> directory of the source tree and binary distributions. The <code>external</code> area has been set up to contain projects that while not required by Storm, are often used in conjunction with Storm to integrate with some other technology. Such projects also come with a maintenance committment from at least one Storm committer to ensure compatibility with Storm's main codebase as it evolves.</p>
<p>The <code>storm-kafka</code> dependency is available now from Maven Central at the following coordinates:</p>
<pre>groupId: org.apache.storm
artifactId: storm-kafka
version: 0.9.2-incubating
</pre>
<p>Users, and Scala developers in particular, should note that the Kafka dependency is listed as <code>provided</code>. This allows users to choose a specific Scala version as described in the <a href="https://github.com/apache/incubator-storm/tree/v0.9.2-incubating/external/storm-kafka" target="_blank">project README</a>.</p>
<h4>Storm Starter and Examples</h4>
<p>Similar to the <code>external</code> section of the codebase, we have also added an <code>examples</code> directory and pulled in the <code>storm-starter</code> project to ensure it will be maintained in lock-step with Storm's main codebase.</p>
<p>Thank you to Storm committer Michael G. Noll for his continued work in maintaining and improving the <code>storm-starter</code> project.</p>
<h4>Plugable Serialization for Multilang</h4>
<p>In previous versions of Storm, serialization of data to and from multilang components was limited to JSON, imposing somewhat of performance penalty. Thanks to a contribution from John Gilmore (<a href="https://github.com/jsgilmore">@jsgilmore</a>) the serialization mechanism is now plugable and enables the use of more performant serialization frameworks like protocol buffers in addition to JSON.</p>
<h4>Thanks</h4>
<p>Special thanks are due to all those who have contributed to Storm -- whether through direct code contributions, documentation, bug reports, or helping other users on the mailing lists. Your efforts are much appreciated.</p>
<h4>Changelog</h4>
<ul>
	<li>STORM-352: [storm-kafka] PartitionManager does not save offsets to ZooKeeper</li>
	<li>STORM-66: send taskid on initial handshake</li>
	<li>STORM-342: Contention in Disruptor Queue which may cause out of order or lost messages</li>
	<li>STORM-338: Move towards idiomatic Clojure style </li>
	<li>STORM-335: add drpc test for removing timed out requests from queue</li>
	<li>STORM-69: Storm UI Visualizations for Topologies</li>
	<li>STORM-297: Performance scaling with CPU</li>
	<li>STORM-244: DRPC timeout can return null instead of throwing an exception</li>
	<li>STORM-63: remove timeout drpc request from its function's request queue</li>
	<li>STORM-313: Remove log-level-page from logviewer</li>
	<li>STORM-205: Add REST API To Storm UI</li>
	<li>STORM-326: tasks send duplicate metrics</li>
	<li>STORM-331: Update the Kafka dependency of storm-kafka to 0.8.1.1</li>
	<li>STORM-308: Add support for config_value to {supervisor,nimbus,ui,drpc,logviewer} childopts</li>
	<li>STORM-309: storm-starter Readme: windows documentation update</li>
	<li>STORM-318: update storm-kafka to use apache curator-2.4.0</li>
	<li>STORM-303: storm-kafka reliability improvements</li>
	<li>STORM-233: Removed inline heartbeat to nimbus to avoid workers being killed when under heavy ZK load</li>
	<li>STORM-267: fix package name of LoggingMetricsConsumer in storm.yaml.example</li>
	<li>STORM-265: upgrade to clojure 1.5.1</li>
	<li>STORM-232: ship JNI dependencies with the topology jar</li>
	<li>STORM-295: Add storm configuration to define JAVA_HOME</li>
	<li>STORM-138: Pluggable serialization for multilang</li>
	<li>STORM-264: Removes references to the deprecated topology.optimize</li>
	<li>STORM-245: implement Stream.localOrShuffle() for trident</li>
	<li>STORM-317: Add SECURITY.md to release binaries</li>
	<li>STORM-310: Change Twitter authentication</li>
	<li>STORM-305: Create developer documentation</li>
	<li>STORM-280: storm unit tests are failing on windows</li>
	<li>STORM-298: Logback file does not include full path for metrics appender fileNamePattern</li>
	<li>STORM-316: added validation to registermetrics to have timebucketSizeInSecs &gt;= 1</li>
	<li>STORM-315: Added progress bar when submitting topology</li>
	<li>STORM-214: Windows: storm.cmd does not properly handle multiple -c arguments</li>
	<li>STORM-306: Add security documentation</li>
	<li>STORM-302: Fix Indentation for pom.xml in storm-dist</li>
	<li>STORM-235: Registering a null metric should blow up early</li>
	<li>STORM-113: making thrift usage thread safe for local cluster</li>
	<li>STORM-223: use safe parsing for reading YAML</li>
	<li>STORM-238: LICENSE and NOTICE files are duplicated in storm-core jar</li>
	<li>STORM-276: Add support for logviewer in storm.cmd.</li>
	<li>STORM-286: Use URLEncoder#encode with the encoding specified.</li>
	<li>STORM-296: Storm kafka unit tests are failing on windows</li>
	<li>STORM-291: upgrade http-client to 4.3.3</li>
	<li>STORM-252: Upgrade curator to latest version</li>
	<li>STORM-294: Commas not escaped in command line</li>
	<li>STORM-287: Fix the positioning of documentation strings in clojure code</li>
	<li>STORM-290: Fix a log binding conflict caused by curator dependencies</li>
	<li>STORM-289: Fix Trident DRPC memory leak</li>
	<li>STORM-173: Treat command line "-c" option number config values as such</li>
	<li>STORM-194: Support list of strings in *.worker.childopts, handle spaces</li>
	<li>STORM-288: Fixes version spelling in pom.xml</li>
	<li>STORM-208: Add storm-kafka as an external module</li>
	<li>STORM-285: Fix storm-core shade plugin config</li>
	<li>STORM-12: reduce thread usage of netty transport</li>
	<li>STORM-281: fix and issue with config parsing that could lead to leaking file descriptors</li>
	<li>STORM-196: When JVM_OPTS are set, storm jar fails to detect storm.jar from environment</li>
	<li>STORM-260: Fix a potential race condition with simulated time in Storm's unit tests</li>
	<li>STORM-258: Update commons-io version to 2.4</li>
	<li>STORM-270: don't package .clj files in release jars.</li>
	<li>STORM-273: Error while running storm topologies on Windows using "storm jar"</li>
	<li>STROM-247: Replace links to github resources in storm script</li>
	<li>STORM-263: Update Kryo version to 2.21+</li>
	<li>STORM-187: Fix Netty error "java.lang.IllegalArgumentException: timeout value is negative"</li>
	<li>STORM-186: fix float secs to millis long convertion</li>
	<li>STORM-70: Upgrade to ZK-3.4.5 and curator-1.3.3</li>
	<li>STORM-146: Unit test regression when storm is compiled with 3.4.5 zookeeper</li>
</ul>









