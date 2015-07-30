---
layout: news
title: Storm 0.10.0-beta Released
---
<!--Post Header-->
<h3 class="news-title">Storm 0.10.0-beta Released</h3>
<div class="news-meta">
    <i class="fa fa-calendar"></i> Jun 15, 2015 <i class="fa fa-user"></i> P. Taylor Goetz
</div>
<!--Post Body-->
<p>Fast on the heals of the 0.9.5 maintenance release, the Apache Storm community is pleased to announce that Apache Storm 0.10.0-beta has been released and is now available on <a href="/downloads.html">the downloads page.</a></p>
<p>Aside from many stability and performance improvements, this release includes a number of important new features, some of which are highlighted below.</p>
<h4>Secure, Multi-Tenant Deployment</h4>
<p>Much like the early days of Hadoop, Apache Storm originally evolved in an environment where security was not a high-priority concern. Rather, it was assumed that Storm would be deployed to environments suitably cordoned off from security threats. While a large number of users were comfortable setting up their own security measures for Storm (usually at the Firewall/OS level), this proved a hindrance to broader adoption among larger enterprises where security policies prohibited deployment without specific safeguards.</p>
<p>Yahoo! hosts one of the largest Storm deployments in the world, and their engineering team recognized the need for security early on, so it implemented many of the features necessary to secure its own Apache Storm deployment. Yahoo!, Hortonworks, Symantec, and the broader Apache Storm community have worked together to bring those security innovations into the main Apache Storm code base.</p>
<p>We are pleased to announce that work is now complete. Some of the highlights of Storm's new security features include:</p>
<ul>
    <li>Kerberos Authentication with Automatic Credential Push and Renewal</li>
    <li>Pluggable Authorization and ACLs</li>
    <li>Multi-Tenant Scheduling with Per-User isolation and configurable resource limits.</li>
    <li>User Impersonation</li>
    <li>SSL Support for Storm UI, Log Viewer, and DRPC (Distributed Remote Procedure Call)</li>
    <li>Secure integration with other Hadoop Projects (such as ZooKeeper, HDFS, HBase, etc.)</li>
    <li>User isolation (Storm topologies run as the user who submitted them)</li>
</ul>
<p>For more details and instructions for securing Storm, please see <a href="https://github.com/apache/storm/blob/v0.10.0-beta/SECURITY.md" target="_blank">the security documentation</a></p>
<h4>A Foundation for Rolling Upgrades and Continuity of Operations</h4>
<p>In the past, upgrading a Storm cluster could be an arduous process that involved un-deploying existing topologies, removing state from local disk and ZooKeeper, installing the upgrade, and finally redeploying topologies. From an operations perspective, this process was disruptive to say the very least.</p>
<p>The underlying cause of this headache was rooted in the data format Storm processes used to store both local and distributed state. Between versions, these data structures would change in incompatible ways.</p>
<p>Beginning with version 0.10.0, this limitation has been eliminated. In the future, upgrading from Storm 0.10.0 to a newer version can be accomplished seamlessly, with zero down time. In fact, for users who use <a href="https://ambari.apache.org" target="_blank">Apache Ambari</a> for cluster provisioning and management, the process can be completely automated.</p>
<h4>Easier Deployment and Declarative Topology Wiring with Flux</h4>
<p>Apache Storm 0.10.0 now includes Flux, which is a framework and set of utilities that make defining and deploying Storm topologies less painful and developer-intensive. A common pain point mentioned by Storm users is the fact that the wiring for a Topology graph is often tied up in Java code, and that any changes require recompilation and repackaging of the topology jar file. Flux aims to alleviate that pain by allowing you to package all your Storm components in a single jar, and use an external text file to define the layout and configuration of your topologies.</p>
<p>Some of Flux' features include:</p>
<ul>
    <li>Easily configure and deploy Storm topologies (Both Storm core and Micro-batch API) without embedding configuration in your topology code</li>
    <li>Support for existing topology code</li>
    <li>Define Storm Core API (Spouts/Bolts) using a flexible YAML DSL</li>
    <li>YAML DSL support for most Storm components (storm-kafka, storm-hdfs, storm-hbase, etc.)</li>
    <li>Convenient support for multi-lang components</li>
    <li>External property substitution/filtering for easily switching between configurations/environments (similar to Maven-style <code>${variable.name}</code> substitution)</li>
</ul>
<p>You can read more about Flux on the <a href="https://github.com/apache/storm/blob/v0.10.0-beta/external/flux/README.md" target="_blank">Flux documentation page</a>.</p>
<h4>Partial Key Groupings</h4>
<p>In addition to the standard Stream Groupings Storm has always supported, version 0.10.0 introduces a new grouping named "Partial Key Grouping". With the Partial Stream Grouping, the tuple stream is partitioned by the fields specified in the grouping, like the Fields Grouping, but are load balanced between two downstream bolts, which provides better utilization of resources when the incoming data is skewed.</p>
<p>Documentation for the Partial Key Grouping and other stream groupings supported by Storm can be found here. <a href="https://melmeric.files.wordpress.com/2014/11/the-power-of-both-choices-practical-load-balancing-for-distributed-stream-processing-engines.pdf" target="_blank">This research paper</a> provides additional details regarding how it works and its advantages.</p>
<h4>Improved Logging Framework</h4>
<p>Debugging distributed applications can be difficult, and usually focuses on one main source of information: application log files. But in a very low latency system like Storm where every millisecond counts, logging can be a double-edged sword: If you log too little information you may miss the information you need to solve a problem; log too much and you risk degrading the overall performance of your application as resources are consumed by the logging framework.</p>
<p>In version 0.10.0 Storm's logging framework now uses <a href="http://logging.apache.org/log4j/2.x/" target="_blank">Apache Log4j 2</a> which, like Storm's internal messaging subsystem, uses the extremely performant <a href="https://lmax-exchange.github.io/disruptor/" target="_blank">LMAX Disruptor</a> messaging library. Log4j 2 boast an 18x higher throughput and orders of magnitude lower latency than Storm's previous logging framework. More efficient resource utilization at the logging level means more resources are available where they matter most: executing your business logic.</p>
<p>A few of the important features these changes bring include:</p>
<ul>
    <li>Rolling log files with size, duration, and date-based triggers that are composable</li>
    <li>Dynamic log configuration updates without dropping log messages</li>
    <li>Remote log monitoring and (re)configuration via JMX</li>
    <li>A Syslog/<a href="https://tools.ietf.org/html/rfc5424" target="_blank">RFC-5424</a>-compliant appender.</li>
    <li>Integration with log aggregators such as syslog-ng</li>
</ul>
<h4>Streaming ingest with Apache Hive</h4>
<p>Introduced in version 0.13, <a href="https://hive.apache.org" target="_blank">Apache Hive</a> includes a <a href="https://cwiki.apache.org/confluence/display/Hive/Streaming+Data+Ingest" target="_blank">Streaming Data Ingest API</a> that allows data to be written continuously into Hive. The incoming data can be continuously committed in small batches of records into existing Hive partition or table. Once the data is committed its immediately visible to all hive queries.</p>
<p>Apache Storm 0.10.0 introduces both a Storm Core API bolt implementation that allows users to stream data from Storm directly into hive. Storm's Hive integration also includes a <a href="https://storm.apache.org/documentation/Trident-state" target="_blank">State</a> implementation for Storm's Micro-batching/Transactional API (Trident) that allows you to write to Hive from a micro-batch/transactional topology and supports exactly-once semantics for data persistence.</p>
<p>For more information on Storm's Hive integration, see the <a href="https://github.com/apache/storm/blob/v0.10.0-beta/external/storm-hive/README.md" target="_blank">storm-hive documentation</a>.</p>
<h4>Microsoft Azure Event Hubs Integration</h4>
<p>With Microsoft Azure's <a href="https://azure.microsoft.com/en-us/documentation/articles/hdinsight-storm-overview/" target="_blank">support for running Storm on HDInsight</a>, Storm is now a first class citizen of the Azure cloud computing platform. To better support Storm integration with Azure services, Microsoft engineers have contributed several components that allow Storm to integrate directly with <a href="http://azure.microsoft.com/en-us/services/event-hubs/" target="_blank">Microsoft Azure Event Hubs</a>.</p>
<p>Storm's Event Hubs integration includes both spout and bolt implementations for reading from, and writing to Event Hubs. The Event Hub integration also includes a Micro-batching/Transactional (Trident) spout implementation that supports fully fault-tolerant and reliable processing, as well as support for exactly-once message processing semantics.</p>
<h4>Redis Support</h4>
<p>Apache Storm 0.10.0 also introduces support for the <a href="http://redis.io" target="_blank">Redis</a> data structure server. Storm's Redis support includes bolt implementations for both writing to and querying Redis from a Storm topology, and is easily extended for custom use cases. For Storm's micro-batching/transactional API, the Redis support includes both <a href="https://storm.apache.org/documentation/Trident-state.html">Trident State and MapState</a> implementations for fault-tolerant state management with Redis.</p>
<p>Further information can be found in the <a href="https://github.com/apache/storm/blob/v0.10.0-beta/external/storm-redis/README.md" target="_blank">storm-redis documentation</a>.</p>
<h4>JDBC/RDBMS Integration</h4>
<p>Many stream processing data flows require accessing data from or writing data to a relational data store. Storm 0.10.0 introduces highly flexible and customizable support for integrating with virtually any JDBC-compliant database.</p>
<p>The Storm-JDBC package includes core Storm bolt and Trident state implementations that allow a storm topology to either insert Storm tuple data into a database table or execute select queries against a database to enrich streaming data in a storm topology.</p>
<p>Further details and instructions can be found in the <a href="https://github.com/apache/storm/blob/v0.10.0-beta/external/storm-jdbc/README.md" target="_blank">Storm-JDBC documentation</a>.</p>
<h4>Reduced Dependency Conflicts</h4>
<p>In previous Storm releases, it was not uncommon for users' topology dependencies to conflict with the libraries used by Storm. In Storm 0.9.3 several dependency packages that were common sources of conflicts have been package-relocated (shaded) to avoid this situation. In 0.10.0 this list has been expanded.</p>
<p>Developers are free to use the Storm-packaged versions, or supply their own version. </p>
<p>The full list of Storm's package relocations can be found <a href="https://github.com/apache/storm/blob/v0.10.0-beta/storm-core/pom.xml#L439" target="_blank">here</a>.</p>
<h4>Future Work</h4>
<p>While the 0.10.0 release is an important milestone in the evolution of Apache Storm, the Storm community is actively working on new improvements, both near and long term, and continuously exploring the realm of the possible.</p>
<p>Twitter recently announced the Heron project, which claims to provide substantial performance improvements while maintaining 100% API compatibility with Storm. The corresponding research paper provides additional details regarding the architectural improvements. The fact that Twitter chose to maintain API compatibility with Storm is a testament to the power and flexibility of that API. Twitter has also expressed a desire to share their experiences and work with the Apache Storm community.</p>
<p>A number of concepts expressed in the Heron paper were already in the implementation stage by the Storm community even before it was published, and we look forward to working with Twitter to bring those and other improvements to Storm.</p>
<h4>Thanks</h4>
<p>Special thanks are due to all those who have contributed to Apache Storm -- whether through direct code contributions, documentation, bug reports, or helping other users on the mailing lists. Your efforts are very much valued and appreciated.</p>
<h4>Full Change Log</h4>
<ul>
    <li>STORM-856: use serialized value of delay secs for topo actions</li>
    <li>STORM-852: Replaced Apache Log4j Logger with SLF4J API</li>
    <li>STORM-813: Change storm-starter's README so that it explains mvn exec:java cannot run multilang topology</li>
    <li>STORM-853: Fix upload API to handle multi-args properly</li>
    <li>STORM-850: Convert storm-core's logback-test.xml to log4j2-test.xml</li>
    <li>STORM-848: Shade external dependencies</li>
    <li>STORM-849: Add storm-redis to storm binary distribution</li>
    <li>STORM-760: Use JSON for serialized conf</li>
    <li>STORM-833: Logging framework logback -&gt; log4j 2.x</li>
    <li>STORM-842: Drop Support for Java 1.6</li>
    <li>STORM-835: Netty Client hold batch object until io operation complete</li>
    <li>STORM-827: Allow AutoTGT to work with storm-hdfs too.</li>
    <li>STORM-821: Adding connection provider interface to decouple jdbc connector from a single connection pooling implementation.</li>
    <li>STORM-818: storm-eventhubs configuration improvement and refactoring</li>
    <li>STORM-816: maven-gpg-plugin does not work with gpg 2.1</li>
    <li>STORM-811: remove old metastor_db before running tests again.</li>
    <li>STORM-808: allow null to be parsed as null</li>
    <li>STORM-807: quote args to storm.py correctly</li>
    <li>STORM-801: Add Travis CI badge to README</li>
    <li>STORM-797: DisruptorQueueTest has some race conditions in it.</li>
    <li>STORM-796: Add support for "error" command in ShellSpout</li>
    <li>STORM-795: Update the user document for the extlib issue</li>
    <li>STORM-792: Missing documentation in backtype.storm.generated.Nimbus</li>
    <li>STORM-791: Storm UI displays maps in the config incorrectly</li>
    <li>STORM-790: Log "task is null" instead of let worker died when task is null in transfer-fn</li>
    <li>STORM-789: Send more topology context to Multi-Lang components via initial handshake</li>
    <li>STORM-788: UI Fix key for process latencies</li>
    <li>STORM-787: test-ns should announce test failures with 'BUILD FAILURE'</li>
    <li>STORM-786: KafkaBolt should ack tick tuples</li>
    <li>STORM-773: backtype.storm.transactional-test fails periodically with timeout</li>
    <li>STORM-772: Tasts fail periodically with InterruptedException or InterruptedIOException</li>
    <li>STORM-766: Include version info in the service page</li>
    <li>STORM-765: Thrift serialization for local state</li>
    <li>STORM-764: Have option to compress thrift heartbeat</li>
    <li>STORM-762: uptime for worker heartbeats is lost when converted to thrift</li>
    <li>STORM-761: An option for new/updated Redis keys to expire in RedisMapState</li>
    <li>STORM-757: Simulated time can leak out on errors</li>
    <li>STORM-753: Improve RedisStateQuerier to convert List<values> from Redis value</values></li>
    <li>STORM-752: [storm-redis] Clarify Redis*StateUpdater's expire is optional</li>
    <li>STORM-750: Set Config serialVersionUID</li>
    <li>STORM-749: Remove CSRF check from the REST API.</li>
    <li>STORM-747: assignment-version-callback/info-with-version-callback are not fired when assignments change</li>
    <li>STORM-746: Skip ack init when there are no output tasks</li>
    <li>STORM-745: fix storm.cmd to evaluate 'shift' correctly with 'storm jar'</li>
    <li>STORM-741: Allow users to pass a config value to perform impersonation.</li>
    <li>STORM-740: Simple Transport Client cannot configure thrift buffer size</li>
    <li>STORM-737: Check task-&gt;node+port with read lock to prevent sending to closed connection</li>
    <li>STORM-735: [storm-redis] Upgrade Jedis to 2.7.0</li>
    <li>STORM-730: remove extra curly brace</li>
    <li>STORM-729: Include Executors (Window Hint) if the component is of Bolt type</li>
    <li>STORM-728: Put emitted and transferred stats under correct columns</li>
    <li>STORM-727: Storm tests should succeed even if a storm process is running locally.</li>
    <li>STORM-724: Document RedisStoreBolt and RedisLookupBolt which is missed.</li>
    <li>STORM-723: Remove RedisStateSetUpdater / RedisStateSetCountQuerier which didn't tested and have a bug</li>
    <li>STORM-721: Storm UI server should support SSL.</li>
    <li>STORM-715: Add a link to AssignableMetric.java in Metrics.md</li>
    <li>STORM-714: Make CSS more consistent with self, prev release</li>
    <li>STORM-713: Include topic information with Kafka metrics.</li>
    <li>STORM-712: Storm daemons shutdown if OutOfMemoryError occurs in any thread</li>
    <li>STORM-711: All connectors should use collector.reportError and tuple anchoring.</li>
    <li>STORM-708: CORS support for STORM UI.</li>
    <li>STORM-707: Client (Netty): improve logging to help troubleshooting connection woes</li>
    <li>STORM-704: Apply Travis CI to Apache Storm Project</li>
    <li>STORM-703: With hash key option for RedisMapState, only get values for keys in batch</li>
    <li>STORM-699: storm-jdbc should support custom insert queries. </li>
    <li>STORM-696: Single Namespace Test Launching</li>
    <li>STORM-694: java.lang.ClassNotFoundException: backtype.storm.daemon.common.SupervisorInfo</li>
    <li>STORM-693: KafkaBolt exception handling improvement.</li>
    <li>STORM-691: Add basic lookup / persist bolts</li>
    <li>STORM-690: Return Jedis into JedisPool with marking 'broken' if connection is broken</li>
    <li>STORM-689: SimpleACLAuthorizer should provide a way to restrict who can submit topologies.</li>
    <li>STORM-688: update Util to compile under JDK8</li>
    <li>STORM-687: Storm UI does not display up to date information despite refreshes in IE</li>
    <li>STORM-685: wrong output in log when committed offset is too far behind latest offset</li>
    <li>STORM-684: In RichSpoutBatchExecutor: underlying spout is not closed when emitter is closed</li>
    <li>STORM-683: Make false in a conf really evaluate to false in clojure.</li>
    <li>STORM-682: supervisor should handle worker state corruption gracefully.</li>
    <li>STORM-681: Auto insert license header with genthrift.sh</li>
    <li>STORM-675: Allow users to have storm-env.sh under config dir to set custom JAVA_HOME and other env variables.</li>
    <li>STORM-673: Typo 'deamon' in security documentation</li>
    <li>STORM-672: Typo in Trident documentation example</li>
    <li>STORM-670: restore java 1.6 compatibility (storm-kafka)</li>
    <li>STORM-669: Replace links with ones to latest api document</li>
    <li>STORM-667: Incorrect capitalization "SHell" in Multilang-protocol.md</li>
    <li>STORM-663: Create javadocs for BoltDeclarer</li>
    <li>STORM-659: return grep matches each on its own line.</li>
    <li>STORM-657: make the shutdown-worker sleep time before kill -9 configurable</li>
    <li>STORM-656: Document "external" modules and "Committer Sponsors"</li>
    <li>STORM-651: improvements to storm.cmd</li>
    <li>STORM-641: Add total number of topologies to api/v1/cluster/summary.</li>
    <li>STORM-640: Storm UI vulnerable to poodle attack.</li>
    <li>STORM-637: Integrate PartialKeyGrouping into storm API</li>
    <li>STORM-636: Faster, optional retrieval of last component error</li>
    <li>STORM-635: logviewer returns 404 if storm_home/logs is a symlinked dir.</li>
    <li>STORM-634: Storm serialization changed to thrift to support rolling upgrade.</li>
    <li>STORM-632: New grouping for better load balancing</li>
    <li>STORM-630: Support for Clojure 1.6.0</li>
    <li>STORM-629: Place Link to Source Code Repository on Webpage</li>
    <li>STORM-627: Storm-hbase configuration error.</li>
    <li>STORM-626: Add script to print out the merge command for a given pull request.</li>
    <li>STORM-625: Don't leak netty clients when worker moves or reuse netty client. </li>
    <li>STORM-623: Generate latest javadocs</li>
    <li>STORM-620: Duplicate maven plugin declaration</li>
    <li>STORM-616: Storm JDBC Connector.</li>
    <li>STORM-615: Add REST API to upload topology.</li>
    <li>STORM-613: Fix wrong getOffset return value</li>
    <li>STORM-612: Update the contact address in configure.ac</li>
    <li>STORM-611: Remove extra "break"s</li>
    <li>STORM-610: Check the return value of fts_close()</li>
    <li>STORM-609: Add storm-redis to storm external</li>
    <li>STORM-608: Storm UI CSRF escape characters not work correctly.</li>
    <li>STORM-607: storm-hbase HBaseMapState should support user to customize the hbase-key &amp; hbase-qualifier</li>
    <li>STORM-603: Log errors when required kafka params are missing</li>
    <li>STORM-601: Make jira-github-join ignore case.</li>
    <li>STORM-600: upgrade jacoco plugin to support jdk8</li>
    <li>STORM-599: Use nimbus's cached heartbeats rather than fetching again from ZK</li>
    <li>STORM-596: remove config topology.receiver.buffer.size</li>
    <li>STORM-595: storm-hdfs can only work with sequence files that use Writables.</li>
    <li>STORM-586: Trident kafka spout fails instead of updating offset when kafka offset is out of range.</li>
    <li>STORM-585: Performance issue in none grouping</li>
    <li>STORM-583: Add Microsoft Azure Event Hub spout implementations</li>
    <li>STORM-578: Calls to submit-mocked-assignment in supervisor-test use invalid executor-id format</li>
    <li>STORM-577: long time launch worker will block supervisor heartbeat</li>
    <li>STORM-575: Ability to specify Jetty host to bind to</li>
    <li>STORM-572: Storm UI 'favicon.ico'</li>
    <li>STORM-572: Allow Users to pass TEST-TIMEOUT-MS for java</li>
    <li>STORM-571: upgrade clj-time.</li>
    <li>STORM-570: Switch from tablesorter to datatables jquery plugin.</li>
    <li>STORM-569: Add Conf for bolt's outgoing overflow-buffer.</li>
    <li>STORM-567: Move Storm Documentation/Website from SVN to git</li>
    <li>STORM-565: Fix NPE when topology.groups is null.</li>
    <li>STORM-563: Kafka Spout doesn't pick up from the beginning of the queue unless forceFromStart specified.</li>
    <li>STORM-561: Add flux as an external module</li>
    <li>STORM-557: High Quality Images for presentations</li>
    <li>STORM-554: the type of first param "topology" should be ^StormTopology not ^TopologyContext</li>
    <li>STORM-552: Add netty socket backlog config</li>
    <li>STORM-548: Receive Thread Shutdown hook should connect to local hostname but not Localhost</li>
    <li>STORM-541: Build produces maven warnings</li>
    <li>STORM-539: Storm Hive Connector.</li>
    <li>STORM-533: Add in client and server IConnection metrics.</li>
    <li>STORM-527: update worker.clj -- delete "missing-tasks" checking</li>
    <li>STORM-525: Add time sorting function to the 2nd col of bolt exec table</li>
    <li>STORM-512: KafkaBolt doesn't handle ticks properly</li>
    <li>STORM-505: Fix debug string construction</li>
    <li>STORM-495: KafkaSpout retries with exponential backoff</li>
    <li>STORM-487: Remove storm.cmd, no need to duplicate work python runs on windows too.</li>
    <li>STORM-483: provide dedicated directories for classpath extension</li>
    <li>STORM-456: Storm UI: cannot navigate to topology page when name contains spaces.</li>
    <li>STORM-446: Allow superusers to impersonate other users in secure mode.</li>
    <li>STORM-444: Add AutoHDFS like credential fetching for HBase</li>
    <li>STORM-442: multilang ShellBolt/ShellSpout die() can be hang when Exception happened</li>
    <li>STORM-441: Remove bootstrap macro from Clojure codebase</li>
    <li>STORM-410: Add groups support to log-viewer</li>
    <li>STORM-400: Thrift upgrade to thrift-0.9.2</li>
    <li>STORM-329: fix cascading Storm failure by improving reconnection strategy and buffering messages (thanks tedxia)</li>
    <li>STORM-322: Windows script do not handle spaces in JAVA_HOME path</li>
    <li>STORM-248: cluster.xml location is hardcoded for workers</li>
    <li>STORM-243: Record version and revision information in builds</li>
    <li>STORM-188: Allow user to specifiy full configuration path when running storm command</li>
    <li>STORM-130: Supervisor getting killed due to java.io.FileNotFoundException: File '../stormconf.ser' does not exist.</li>
</ul>