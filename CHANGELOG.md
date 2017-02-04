## 2.0.0
 * STORM-2327: Introduce ConfigurableTopology
 * STORM-2323: Precondition for Leader Nimbus should check all topology blobs and also corresponding dependencies.
 * STORM-2305: STORM-2279 calculates task index different from grouper code
 * STORM-1292: port backtype.storm.messaging-test to java
 * STORM-2271: ClosedByInterruptException should be handled in few cases and removing a confusing debug statement
 * STORM-2272: don't leak simulated time
 * STORM-2275: Nimbus crashed during state transition of topology
 * STORM-2243: adds ip address to supervisor id
 * STORM-2214: add in cacheing of the Kerberos Login
 * STORM-2203: Add a getAll method to KeyValueState interface
 * STORM-1239: port backtype.storm.scheduler.IsolationScheduler to java
 * STORM-2217: Finish porting drpc to java
 * STORM-1308: port backtype.storm.tick-tuple-test to java
 * STORM-2245: integration-test constant compilation failure
 * STORM-1607: Add MongoMapState for supporting trident's exactly once semantics
 * STORM-2104: More graceful handling of acked/failed tuples after partition reassignment
 * STORM-1281: LocalCluster, testing4j and testing.clj to java
 * STORM-2226: Fix kafka spout offset lag ui for kerberized kafka
 * STORM-1276: line for line translation of nimbus to java
 * STORM-2224: Exposed a method to override in computing the field from given tuple in FieldSelector
 * STORM-2220: Added config support for each bolt in Cassandra bolts, fixed the bolts to be used also as sinks.
 * STORM-1886: Extend KeyValueState iface with delete
 * STORM-2193: Fix FilterConfiguration parameter order
 * STORM-2209: Update documents adding new integration for some external systems
 * STORM-2212: Remove Redundant Declarations in Maven POM Files
 * STORM-2195: Clean up some of worker-launcher code
 * storm-2205: Racecondition in getting nimbus summaries while ZK connections are reconnected
 * STORM-1278: Port org.apache.storm.daemon.worker to java
 * STORM-2192: Add a new IAutoCredentials plugin to support SSL files
 * STORM-2197: NimbusClient connectins leak due to leakage in ThriftClient
 * STORM-2185: Storm Supervisor doesn't delete directories properly sometimes
 * STORM-2188: Interrupt all executor threads before joining in executor shutdown
 * STORM-203: Adding paths to default java library path
 * STORM-2175: fix double close of workers
 * STORM-1985: Provide a tool for showing and killing corrupted topology
 * STORM-2012: Upgrade Kafka to 0.8.2.2
 * STORM-2142: ReportErrorAndDie runs suicide function only when InterruptedException or InterruptedIOException is thrown
 * STORM-2134: improving the current scheduling strategy for RAS
 * STORM-2131: Add blob command to worker-launcher, make stormdist directory not writeable by topo owner
 * STORM-2144: Fix Storm-sql group-by behavior in standalone mode
 * STORM-1546: Adding Read and Write Aggregations for Pacemaker to make it HA compatible
 * STORM-2124: show requested cpu mem for each component
 * STORM-2109: Treat Supervisor CPU/MEMORY Configs as Numbers
 * STORM-2122: Cache dependency data, and serialize reading of the data
 * STORM-2117: Supervisor V2 with local mode extracts resources directory to the wrong directory
 * STORM-2110: strip out empty String in worker opts
 * STORM-2100: Fix Trident SQL join tests to not rely on ordering
 * STORM-2018: Supervisor V2
 * STORM-2098: DruidBeamBolt: Pass DruidConfig.Builder as constructor argument
 * STORM-2067: Fix "array element type mismatch" from compute-executors in nimbus.clj
 * STORM-2054: DependencyResolver should be aware of relative path and absolute path
 * STORM-2052: Kafka Spout New Client API - Log Improvements and Parameter Tuning for Better Performance
 * STORM-2045: fixed SpoutExecutor NPE
 * STORM-2041: Make Java 8 as minimum requirement for 2.0 release
 * STORM-1256: port backtype.storm.utils.ZookeeperServerCnxnFactory-test to java
 * STORM-1251: port backtype.storm.serialization.SerializationFactory-test to java
 * STORM-1240: port backtype.storm.security.auth.authorizer.DRPCSimpleACLAuthorizer-test to java
 * STORM-1234: port backtype.storm.security.auth.DefaultHttpCredentialsPlugin-test to java
 * STORM-2037: debug operation should be whitelisted in SimpleAclAuthorizer.
 * STORM-2036: Fix minor bug in RAS Tests
 * STORM-2026: Inconsistency between (SpoutExecutor, BoltExecutor) and (spout-transfer-fn, bolt-transfer-fn)
 * STORM-1277: port backtype.storm.daemon.executor to java
 * STORM-2020: Stop using sun internal classes.
 * STORM-2021: Fix license.
 * STORM-2022: fix FieldsTest
 * STORM-1285: port backtype.storm.command.get-errors to java
 * STORM-2010: port org.apache.storm.command.heartbeats.clj to Java
 * STORM-2009: port org.apache.storm.blobstore.clj and org.apache.storm.command.blobstore.clj to Java
 * STORM-1876: Option to build storm-kafka and storm-kafka-client with different kafka client version
 * STORM-2000: Package storm-opentsdb as part of external dir in installation
 * STORM-1962: support python 3 and 2 in multilang
 * STORM-1964: Unexpected behavior when using count window together with timestamp extraction
 * STORM-1890: ensure we refetch static resources after package build
 * STORM-1966: Expand metric having Map type as value into multiple metrics based on entries
 * STORM-1737: storm-kafka-client has compilation errors with Apache Kafka 0.10
 * STORM-1910: One topology cannot use hdfs spout to read from two locations
 * STORM-1916: Add ability for worker-first classpath
 * STORM-1954: Large Trident topologies can cause memory issues due to DefaultResourceDeclarer object reading config
 * STORM-1913: Additions and Improvements for Trident RAS API
 * STORM-1959: Add missing license header to KafkaPartitionOffsetLag
 * STORM-1249: port backtype.storm.security.serialization.BlowfishTupleSerializer-test to java
 * STORM-1238: port backtype.storm.security.auth.ThriftServer-test to java
 * STORM-1237: port backtype.storm.security.auth.ThriftClient-test to java
 * STORM-1236: port backtype.storm.security.auth.SaslTransportPlugin-test to java
 * STORM-1235: port backtype.storm.security.auth.ReqContext-test to java
 * STORM-1229: port backtype.storm.metric.testing to java
 * STORM-1228: port backtype.storm.fields-test to java
 * STORM-1233: Port AuthUtilsTest to java
 * STORM-1920: version of parent pom for storm-kafka-monitor is set 1.0.2-SNAPSHOT in master branch
 * STORM-1896: Remove duplicate code from HDFS spout
 * STORM-1909: Update HDFS spout documentation
 * STORM-1705: Cap number of retries for a failed message
 * STORM-1884: Prioritize pendingPrepare over pendingCommit
 * STORM-1575: fix TwitterSampleSpout NPE on close
 * STORM-1874: Update logger private permissions
 * STORM-1865: update command line client document
 * STORM-1771. HiveState should flushAndClose before closing old or idle Hive connections
 * STORM-1882: Expose TextFileReader public
 * STORM-1873: Implement alternative behaviour for late tuples
 * STORM-1878: Flux can now handle IStatefulBolts
 * STORM-1864: StormSubmitter should throw respective exceptions and log respective errors forregistered submitter hook invocation
 * STORM-1766: A better algorithm server rack selection for RAS
 * STORM-1859: Ack late tuples in windowed mode
 * STORM-1851: Fix default nimbus impersonation authorizer config
 * STORM-1848: Make KafkaMessageId and Partition serializable to support event logging
 * STORM-1862: Flux ShellSpout and ShellBolt can't emit to named streams
 * Storm-1728: TransactionalTridentKafkaSpout error
 * STORM-1850: State Checkpointing Documentation update
 * STORM-1674: Idle KafkaSpout consumes more bandwidth than needed
 * STORM-1842: Forward references in storm.thrift cause tooling issues
 * STORM-1676: Filter null executor stats from worker heartbeat map
 * STORM-1672: Stats not get class cast exception
 * STORM-1739: update the minor JAVA version dependency in 0.10.0 and above
 * STORM-1733: Flush stdout before calling "os.execvp" to prevent log loss
 * STORM-1535: Make sure hdfs key tab login happens only once for multiple components
 * STORM-1544: Document Debug/Sampling of Topologies
 * STORM-1681: Bug in scheduling cyclic topologies when scheduling with RAS
 * STORM-1679: add storm Scheduler documents
 * STORM-1687: divide by zero in StatsUtil
 * STORM-1464: storm-hdfs support for multiple file outputs
 * STORM-515: Clojure documentation and examples
 * STORM-1279: port backtype.storm.daemon.supervisor to java
 * STORM-1668: Flux silently fails while setting a non-existent property.
 * STORM-1271: Port backtype.storm.daemon.task to java
 * STORM-822: Kafka Spout New Consumer API
 * STORM-1663: Stats couldn't handle null worker HB.
 * STORM-1665: Worker cannot instantiate kryo
 * STORM-1666: Kill from the UI fails silently
 * STORM-1610: port pacemaker_state_factory_test.clj to java
 * STORM-1611: port org.apache.storm.pacemaker.pacemaker to java
 * STORM-1268: port builtin-metrics to java
 * STORM-1648: drpc spout reconnect on failure
 * STORM-1631: Storm CGroup bugs
 * STORM-1616: Add RAS API for Trident
 * STORM-1623: nimbus.clj's minor bug
 * STORM-1624: Add maven central status in README
 * STORM-1232: port backtype.storm.scheduler.DefaultScheduler to java
 * STORM-1231: port backtype.storm.scheduler.EvenScheduler to java
 * STORM-1523: util.clj available-port conversion to java
 * STORM-1252: port backtype.storm.stats to java
 * STORM-1250: port backtype.storm.serialization-test to java
 * STORM-1605: use '/usr/bin/env python' to check python version
 * STORM-1618: Add the option of passing config directory
 * STORM-1269: port backtype.storm.daemon.common to java
 * STORM-1270: port drpc to java
 * STORM-1274: port LocalDRPC to java
 * STORM-1590: port defmeters/defgauge/defhistogram... to java for all of our code to use
 * STORM-1529: Change default worker temp directory location for workers
 * STORM-1543: DRPCSpout should always try to reconnect disconnected DRPCInvocationsClient
 * STORM-1528: Fix CsvPreparableReporter log directory
 * STORM-1561: Supervisor should relaunch worker if assignments have changed
 * STORM-1283: port backtype.storm.MockAutoCred to java
 * STORM-1592: clojure code calling into Utils.exitProcess throws ClassCastException
 * STORM-1579: Fix NoSuchFileException when running tests in storm-core
 * STORM-1244: port backtype.storm.command.upload-credentials to java
 * STORM-1245: port backtype.storm.daemon.acker to java
 * STORM-1545: Topology Debug Event Log in Wrong Location
 * STORM-1254: port ui.helper to java
 * STORM-1571: Improvment Kafka Spout Time Metric
 * STORM-1569: Allowing users to specify the nimbus thrift server queue size.
 * STORM-1564: fix wrong package-info in org.apache.storm.utils.staticmocking
 * STORM-1267: Port set_log_level
 * STORM-1266: Port rebalance
 * STORM-1265: Port monitor
 * STORM-1572: throw NPE when parsing the command line arguments by CLI
 * STORM-1273: port backtype.storm.cluster to java
 * STORM-1479: use a simple implemention for IntSerializer
 * STORM-1255: port storm_utils.clj to java and split Time tests into its
 * STORM-1566: Worker exits with error o.a.s.d.worker [ERROR] Error on initialization of server mk-worker
 * STORM-1558: Utils in java breaks component page due to illegal type cast
 * STORM-1553: port event.clj to java
 * STORM-1262: port backtype.storm.command.dev-zookeeper to java.
 * STORM-1243: port backtype.storm.command.healthcheck to java.
 * STORM-1246: port backtype.storm.local-state to java.
 * STORM-1516: Fixed issue in writing pids with distributed cluster mode.
 * STORM-1253: port backtype.storm.timer to java
 * STORM-1258: port thrift.clj to Thrift.java
 * STORM-1336: Evalute/Port JStorm cgroup support and implement cgroup support for RAS
 * STORM-1511: min/max operators implementation in Trident streams API.
 * STROM-1263: port backtype.storm.command.kill-topology to java
 * STORM-1260: port backtype.storm.command.activate to java
 * STORM-1261: port backtype.storm.command.deactivate to java
 * STORM-1264: port backtype.storm.command.list to java
 * STORM-1272: port backtype.storm.disruptor to java
 * STORM-1248: port backtype.storm.messaging.loader to java
 * STORM-1538: Exception being thrown after Utils conversion to java
 * STORM-1242: migrate backtype.storm.command.config-value to java
 * STORM-1226: Port backtype.storm.util to java
 * STORM-1436: Random test failure on BlobStoreTest / HdfsBlobStoreImplTest (occasionally killed)
 * STORM-1476: Filter -c options from args and add them as part of storm.options
 * STORM-1257: port backtype.storm.zookeeper to java
 * STORM-1504: Add Serializer and instruction for AvroGenericRecordBolt
 * STORM-1524: Add Pluggable daemon metrics Reporters
 * STORM-1521: When using Kerberos login from keytab with multiple bolts/executors ticket is not renewed in hbase bolt.
 * STORM-1769: Added a test to check local nimbus with notifier plugin

## 1.1.0
 * STORM-2296: Kafka spout no dup on leader changes
 * STORM-2014: New Kafka spout duplicates checking if failed messages have reached max retries
 * STORM-2324: Fix deployment failure if resources directory is missing in topology jar
 * STORM-1443: [Storm SQL] Support customizing parallelism in StormSQL
 * STORM-2148: [Storm SQL] Trident mode: back to code generate and compile Trident topology
 * STORM-2321: Handle blobstore zk key deletion in KeySequenceNumber.
 * STORM-2336: Close Localizer and AsyncLocalizer when supervisor is shutting down
 * STORM-2335: Fix broken Topology visualization with empty ':transferred' in executor stats
 * STORM-2331: Emitting from JavaScript should work when not anchoring.
 * STORM-2320:  DRPC client printer class reusable for local and remote DRPC.
 * STORM-2225: change spout config to be simpler.
 * STORM-2330: Fix storm sql code generation for UDAF with non standard sql types
 * STORM-2298: Don't kill Nimbus when ClusterMetricsConsumer is failed to initialize
 * STORM-2301: [storm-cassandra] upgrade cassandra driver to 3.1.2
 * STORM-1446: Compile the Calcite logical plan to Storm Trident logical plan
 * STORM-2303: [storm-opentsdb] Fix list invariant issue for JDK 7
 * STORM-2295: KafkaSpoutStreamsNamedTopics should return output fields with predictable ordering
 * STORM-2300: [Flux] support list of references
 * STORM-2297: [storm-opentsdb] Support Flux for OpenTSDBBolt
 * STORM-2294: Send activate and deactivate command to ShellSpout
 * STORM-2236: Kafka Spout with manual partition management.
 * STORM-2280: Upgrade Calcite version to 1.11.0
 * STORM-2278: Allow max number of disruptor queue flusher threads to be configurable
 * STORM-2277: Add shaded jar for Druid connector
 * STORM-2274: Support named output streams in Hdfs Spout
 * STORM-2204: Adding caching capabilities in HBaseLookupBolt
 * STORM-2267: Use user's local maven repo. directory to local repo.
 * STORM-2254: Provide Socket time out for nimbus thrift client
 * STORM-2200: [Storm SQL] Drop Aggregate & Join support on Trident mode
 * STORM-2244: Some shaded jars doesn't exclude dependency signature files
 * STORM-2266: Close NimbusClient instances appropriately
 * STORM-2257: Add built in support for sum function with different types
 * STORM-2082: add sql external module storm-sql-hdfs
 * STORM-2190: reduce contention between submission and scheduling
 * STORM-2239: Handle InterruptException in new Kafka spout
 * STORM-2238: Add Timestamp extractor for windowed bolt
 * STORM-2235: Introduce new option: 'add remote repositories' for dependency resolver
 * STORM-2215: validate blobs are present before submitting
 * STORM-2170: [Storm SQL] Add built-in socket datasource to runtime
 * STORM-2087: storm-kafka-client - tuples not always being replayed.
 * STORM-2182: Refactor Storm Kafka Examples Into Own Modules.
 * STORM-1694: Kafka Spout Trident Implementation Using New Kafka Consumer API.
 * STORM-2173: [SQL] Support CSV as input / output format
 * STORM-2177: [SQL] Support TSV as input / output format
 * STORM-2172: [SQL] Support Avro as input / output format
 * STORM-2103: [SQL] Introduce new sql external module: storm-sql-mongodb
 * STORM-2109: Under supervisor V2 SUPERVISOR_MEMORY_CAPACITY_MB and SUPERVISOR_CPU_CAPACITY must be Doubles
 * STORM-2110: in supervisor v2 filter out empty command line args
 * STORM-2117: Supervisor V2 with local mode extracts resources directory to topology root directory instead of temporary directory
 * STORM-2131: Add blob command to worker-launcher, make stormdist directory not writeable by topo owner
 * STORM-2018: Supervisor V2
 * STORM-2139: Let ShellBolts and ShellSpouts run with scripts from blobs
 * STORM-2072: Add map, flatMap with different outputs (T->V) in Trident
 * STORM-2125: Use Calcite's implementation of Rex Compiler
 * STORM-1444: Support EXPLAIN statement in StormSQL
 * STORM-2099: Introduce new sql external module: storm-sql-redis
 * STORM-2097: Improve logging in trident core and examples
 * STORM-2066: make error message in IsolatedPool.java more descriptive
 * STORM-1870: Allow FluxShellBolt/Spout set custom "componentConfig" via yaml
 * STORM-2126: fix NPE due to race condition in compute-new-sched-assign…
 * STORM-2089: Replace Consumer of ISqlTridentDataSource with SqlTridentConsumer
 * STORM-2118: A few fixes for storm-sql standalone mode
 * STORM-2105: Cluster/Supervisor total and available resources displayed in the UI
 * STORM-2078: enable paging in worker datatable
 * STORM-1664: Allow Java users to start a local cluster with a Nimbus Thrift server.
 * STORM-1872: Release Jedis connection when topology shutdown
 * STORM-1837: Fix complete-topology and prevent message loss
 * STORM-2092: optimize TridentKafkaState batch sending
 * STORM-1979: Storm Druid Connector implementation.
 * STORM-2057: Support JOIN statement in Storm SQL
 * STORM-1970: external project examples refator
 * STORM-2074: fix storm-kafka-monitor NPE bug
 * STORM-1459: Allow not specifying producer properties in read-only Kafka table in StormSQL
 * STORM-2050: [storm-sql] Support User Defined Aggregate Function for Trident mode
 * STORM-1434: Support the GROUP BY clause in StormSQL
 * STORM-2016: Topology submission improvement: support adding local jars and maven artifacts on submission
 * STORM-1994: Add table with per-topology & worker resource usage and components in (new) supervisor and topology pages
 * STORM-2023: Add calcite-core to dependency of storm-sql-runtime
 * STORM-1839: Storm spout implementation for Amazon Kinesis Streams.
 * STORM-1988: Kafka Offset not showing due to bad classpath.
 * STORM-1950: Change response json of "Topology Lag" REST API to keyed by spoutId, topic, partition.
 * STORM-1833: Simple equi-join in storm-sql standalone mode
 * STORM-1866: Update Resource Aware Scheduler Documentation
 * STORM-1930: Kafka New Client API - Support for Topic Wildcards
 * STORM-1924: Adding conf options for Persistent Word Count Topology
 * STORM-1956: Disabling Backpressure by default 
 * STORM-1934: Fix race condition between sync-supervisor and sync-processes
 * STORM-1919: Introduce FilterBolt on storm-redis
 * STORM-1945: Fix NPE bugs on topology spout lag for storm-kafka-monitor
 * STORM-1719: Introduce REST API: Topology metric stats for stream
 * STORM-1941: Nimbus discovery can fail when zookeeper reconnect happens
 * STORM-1888: add description for shell command
 * STORM-1902: add a simple & flexible FileNameFormat for storm-hdfs
 * STORM-1914: Storm Kafka Field Topic Selector
 * STORM-1925: Remove Nimbus thrift call from Nimbus itself
 * STORM-1907: PartitionedTridentSpoutExecutor has incompatible types that cause ClassCastException
 * STORM-1136: Command line module to return kafka spout offsets lag and display in storm ui.
 * STORM-1911: IClusterMetricsConsumer should use seconds to timestamp unit
 * STORM-1893: Support OpenTSDB for storing timeseries data.
 * STORM-1723: Introduce ClusterMetricsConsumer
 * STORM-1700: Introduce 'whitelist' / 'blacklist' option to MetricsConsumer
 * STORM-1698: Asynchronous MetricsConsumerBolt
 * STORM-1887: Fixed help message for set_log_level command
 * STORM-1709: Added group by support in storm sql standalone mode
 * STORM-1720: Support GEO in storm-redis
 * STORM-1868: Modify TridentKafkaWordCount to run in distributed mode

## 1.0.3
 * STORM-2337: Broken documentation generation for storm-metrics-profiling-internal-actions.md and windows-users-guide.md
 * STORM-2325: Logviewer doesn't consider 'storm.local.hostname'
 * STORM-1742: More accurate 'complete latency'
 * STORM-2176: Workers do not shutdown cleanly and worker hooks don't run when a topology is killed
 * STORM-2293: hostname should only refer node's 'storm.local.hostname'
 * STORM-2246: Logviewer download link has urlencoding on part of the URL
 * STORM-1906: Window count/length of zero should be disallowed
 * STORM-1841: Address a few minor issues in windowing and doc
 * STORM-2268: Fix integration test for Travis CI build
 * STORM-2283: Fix DefaultStateHandler kryo multithreading issues
 * STORM-2279: Unable to open bolt page of storm ui
 * STORM-2264: OpaqueTridentKafkaSpout failing after STORM-2216
 * STORM-2276: Remove twitter4j usages due to license issue (JSON.org is catalog X)
 * STORM-2095: remove any remaining files when deleting blobstore directory
 * STORM-2251: Integration test refers specific version of Storm which should be project version
 * STORM-2234: heartBeatExecutorService in shellSpout don't work well with deactivate
 * STORM-2216: Favor JSONValue.parseWithException
 * STORM-2208: HDFS State Throws FileNotFoundException in Azure Data Lake Store file system (adl://)
 * STORM-2213: ShellSpout has race condition when ShellSpout is being inactive longer than heartbeat timeout
 * STORM-2210: remove array shuffle from ShuffleGrouping
 * STORM-2198: perform RotationAction when stopping HdfsBolt
 * STORM-2196: A typo in RAS_Node::consumeCPU
 * STORM-2189: RAS_Node::freeCPU outputs incorrect info
 * STORM-2184: Don't wakeup KafkaConsumer on shutdown
 * STORM-2018: Supervisor V2
 * STORM-2145: Leave leader nimbus's hostname to log when trying to connect leader nimbus
 * STORM-2127: Storm-eventhubs should use latest amqp and eventhubs-client versions 
 * STORM-2040: Fix bug on assert-can-serialize
 * STORM-2017: ShellBolt stops reporting task ids
 * STORM-2119: bug in log message printing to stdout
 * STORM-2120: Emit to _spoutConfig.outputStreamId
 * STORM-2101: fixes npe in compute-executors in nimbus
 * STORM-2090: Add integration test for storm windowing
 * STORM-2003: Make sure config contains TOPIC before get it
 * STORM-1567: in defaults.yaml 'topology.disable.loadaware' should be 'topology.disable.loadaware.messaging'
 * STORM-1987: Fix TridentKafkaWordCount arg handling in distributed mode.
 * STORM-1969: Modify HiveTopology to show usage of non-partition table.
 * STORM-1849: HDFSFileTopology should use the 3rd argument as topologyName
 * STORM-2086: use DefaultTopicSelector instead of creating a new one
 * STORM-2079: Unneccessary readStormConfig operation
 * STORM-2081: create external directory for storm-sql various data sources and move storm-sql-kafka to it
 * STORM-1344: Remove sql command from storm-jdbc build
 * STORM-2070: Fix sigar native binary download link
 * STORM-2056: Bugs in logviewer
 * STORM-1646: Fix ExponentialBackoffMsgRetryManager test
 * STORM-2039: Backpressure refactoring in worker and executor
 * STORM-2064: Add storm name and function, access result and function to log-thrift-access
 * STORM-2063: Add thread name in worker logs
 * STORM-2047: Add note to add logviewer hosts to browser whitelist
 * STORM-2042: Nimbus client connections not closed properly causing connection leaks
 * STORM-2032: removes warning in case more than one metrics tuple is received
 * STORM-1594: org.apache.storm.tuple.Fields can throw NPE if given invalid field in selector
 * STORM-1995: downloadChunk in nimbus.clj should close the input stream

## 1.0.2
 * STORM-1976: Remove cleanup-corrupt-topologies!
 * STORM-1977: Restore logic: give up leadership when elected as leader but doesn't have one or more of topology codes on local
 * STORM-1939: Frequent InterruptedException raised by ShellBoltMessageQueue.poll
 * STORM-1928: ShellSpout should check heartbeat while ShellSpout is waiting for subprocess to sync
 * STORM-1922: Supervisor summary default order by host
 * STORM-1895: blobstore replication-factor argument
 * STORM-118: Docs: typo in transactional-commit-flow.png
 * STORM-1633: Document blobstore to command-line-client.md
 * STORM-1899: Release HBase connection when topology shutdown
 * STORM-1844: Some tests are flaky due to low timeout
 * STORM-1946: initialize lastHeartbeatTimestamp before starting heartbeat task
 * STORM-1937: Fix WindowTridentProcessor cause NullPointerException
 * STORM-1924: Add a config file parameter to HDFS test topologies
 * STORM-1861: Storm submit command returns exit code of 0 even when it fails.
 * STORM-1755: Revert the kafka client version upgrade in storm-kafka module
 * STORM-1853: Replace ClassLoaderObjectInputStream with ObjectInputStream
 * STORM-1730: LocalCluster#shutdown() does not terminate all storm threads/thread pools.
 * STORM-1745: Add partition to log output in PartitionManager
 * STORM-1735: Nimbus should log that replication succeeded when min replicas was reached exactly
 * STORM-1835: add lock info in thread dump
 * STORM-1764: Pacemaker is throwing some stack traces
 * STORM-1761: Storm-Solr Example Throws ArrayIndexOutOfBoundsException in Remote Cluster Mode
 * STORM-1756: Explicitly null KafkaServer reference in KafkaTestBroker to prevent out of memory on large test classes.
 * STORM-1750: Ensure worker dies when report-error-and-die is called.
 * STORM-1715: using Jedis Protocol.DEFAULT_HOST to replace DEFAULT_HOST
 * STORM-1713: Replace NotImplementedException with UnsupportedOperationException
 * STORM-1661: Introduce a config to turn off blobstore acl validation in insecure mode
 * STORM-1773: Utils.javaDeserialize() doesn't work with primitive types
 
## 1.0.1
 * STORM-1725: Kafka Spout New Consumer API - KafkaSpoutRetryExponentialBackoff method should use HashMap instead of TreeMap not to throw Exception 
 * STORM-1749: Fix storm-starter github links
 * STORM-1678: abstract batch processing to common api `BatchHelper`
 * STORM-1704: When logviewer_search.html opens daemon file, next search always show no result
 * STORM-1714: StatefulBolts ends up as normal bolts while using TopologyBuilder.setBolt without parallelism
 * STORM-1683: only check non-system streams by default
 * STORM-1680: Provide configuration to set min fetch size in KafkaSpout
 * STORM-1649: Optimize Kryo instaces creation in trident windowing
 * STORM-1696: status not sync if zk fails in backpressure
 * STORM-1693: Move stats cleanup to executor shutdown
 * STORM-1670: LocalState#get(String) can throw FileNotFoundException which results in not removing worker heartbeats and supervisor is kind of stuck and goes down after some time.
 * STORM-1677: Test resource files are excluded from source distribution, which makes logviewer-test failing
 * STORM-1585: Add DDL support for UDFs in storm-sql

## 1.0.0
 * STORM-2223: PMML Bolt
 * STORM-1671: Enable logviewer to delete a dir without yaml
 * STORM-1673: log4j2/worker.xml refers old package of LoggerMetricsConsumer
 * STORM-1669: Fix SolrUpdateBolt flush bug
 * STORM-1573: Add batch support for MongoInsertBolt
 * STORM-1660: remove flux gitignore file and move rules to top level gitignore
 * STORM-1634: Refactoring of Resource Aware Scheduler
 * STORM-1030: Hive Connector Fixes
 * STORM-676: Storm Trident support for sliding/tumbling windows
 * STORM-1630: Add guide page for Windows users
 * STORM-1655: Flux doesn't set return code to non-zero when there's any exception while deploying topology to remote cluster
 * STORM-1537: Upgrade to kryo3 in master
 * STORM-1654: HBaseBolt creates tick tuples with no interval when we don't set flushIntervalSecs
 * STORM-1625: Move storm-sql dependencies out of lib folder
 * STORM-1556: nimbus.clj/wait-for-desired-code-replication wrong reset for current-replication-count-jar in local mode
 * STORM-1636: Supervisor shutdown with worker id pass in being nil
 * STORM-1602: Blobstore UTs are failed on Windows
 * STORM-1629: Files/move doesn't work properly with non-empty directory in Windows
 * STORM-1549: Add support for resetting tuple timeout from bolts via the OutputCollector
 * STORM-971: Metric for messages lost due to kafka retention
 * STORM-1483: add storm-mongodb connector
 * STORM-1608: Fix stateful topology acking behavior
 * STORM-1609: Netty Client is not best effort delivery on failed Connection
 * STORM-1620: Update curator to fix CURATOR-209
 * STORM-1469: Adding Plain Sasl Transport Plugin
 * STORM-1588: Do not add event logger details if number of event loggers is zero
 * STORM-1606: print the information of testcase which is on failure
 * STORM-1601: Check if /backpressure/storm-id node exists before requesting children
 * STORM-1574: Better handle backpressure exception etc.
 * STORM-1587: Avoid NPE while prining Metrics
 * STORM-1570: Storm SQL support for nested fields and array
 * STORM-1576: fix ConcurrentModificationException in addCheckpointInputs
 * STORM-1488: UI Topology Page component last error timestamp is from 1970
 * STORM-1552: Fix topology event sampling log dir
 * STORM-1542: Remove profile action retry in case of non-zero exit code
 * STORM-1540: Fix Debug/Sampling for Trident
 * STORM-1522: REST API throws invalid worker log links
 * STORM-1541: Change scope of 'hadoop-minicluster' to test
 * STORM-1532: Fix readCommandLineOpts to parse JSON correctly in windows
 * STORM-1539: Improve Storm ACK-ing performance
 * STORM-1519: Storm syslog logging not confirming to RFC5426 3.1
 * STORM-1520: Nimbus Clojure/Zookeeper issue ("stateChanged" method not found)
 * STORM-1531: Junit and mockito dependencies need to have correct scope defined in storm-elasticsearch pom.xml
 * STORM-1526: Improve Storm core performance
 * STORM-1517: Add peek api in trident stream
 * STORM-1455: kafka spout should not reset to the beginning of partition when offsetoutofrange exception occurs
 * STORM-1505: Add map, flatMap and filter functions in trident stream
 * STORM-1518: Backport of STORM-1504
 * STORM-1510: Fix broken nimbus log link
 * STORM-1503: Worker should not crash on failure to send heartbeats to Pacemaker/ZK
 * STORM-1176: Checkpoint window evaluated/expired state
 * STORM-1494: Add link to supervisor log in Storm UI
 * STORM-1496: Nimbus periodically throws blobstore-related exception
 * STORM-1484: ignore subproject .classpath & .project file
 * STORM-1478: make bolts getComponentConfiguration method cleaner/simpler
 * STORM-1499: fix wrong package name for storm trident
 * STORM-1463: added file schema to log4j config files for windows env
 * STORM-1485: DRPC Connectivity Issues
 * STORM-1486: Fix storm-kafa documentation
 * STORM-1214: add javadoc for Trident Streams and Operations
 * STORM-1450: Fix minor bugs and refactor code in ResourceAwareScheduler
 * STORM-1452: Fixes profiling/debugging out of the box
 * STORM-1406: Add MQTT Support
 * STORM-1473: enable log search for daemon logs
 * STORM-1472: Fix the errorTime bug and show the time to be readable
 * STORM-1466: Move the org.apache.thrift7 namespace to something correct/sensible
 * STORM-1470: Applies shading to hadoop-auth, cleaner exclusions
 * STORM-1467: Switch apache-rat plugin off by default, but enable for Travis-CI
 * STORM-1468: move documentation to asf-site branch
 * STORM-1199: HDFS Spout Implementation.
 * STORM-1453: nimbus.clj/wait-for-desired-code-replication prints wrong log message
 * STORM-1419: Solr bolt should handle tick tuples
 * STORM-1175: State store for windowing operations
 * STORM-1202: Migrate APIs to org.apache.storm, but try to provide some form of backwards compatability
 * STORM-468: java.io.NotSerializableException should be explained
 * STORM-1348: refactor API to remove Insert/Update builder in Cassandra connector
 * STORM-1206: Reduce logviewer memory usage through directory stream
 * STORM-1219: Fix HDFS and Hive bolt flush/acking
 * STORM-1150: Fix the authorization of Logviewer in method authorized-log-user?
 * STORM-1418: improve debug logs for some external modules
 * STORM-1415: Some improvements for trident map StateUpdater
 * STORM-1414: Some improvements for multilang JsonSerializer
 * STORM-1408: clean up the build directory created by tests
 * STORM-1425: Tick tuples should be acked like normal tuples
 * STORM-1432: Spurious failure in storm-kafka test 
 * STORM-1449: Fix Kafka spout to maintain backward compatibility
 * STORM-1458: Add check to see if nimbus is already running.
 * STORM-1462: Upgrade HikariCP to 2.4.3
 * STORM-1457: Avoid collecting pending tuples if topology.debug is off
 * STORM-1430: ui worker checkboxes
 * STORM-1423: storm UI in a secure env shows error even when credentials are present
 * STORM-702: Exhibitor support
 * STORM-1160: Add hadoop-auth dependency needed for storm-core
 * STORM-1404: Fix Mockito test failures in storm-kafka.
 * STORM-1379: Removed Redundant Structure
 * STORM-706: Clarify examples README for IntelliJ.
 * STORM-1396: Added backward compatibility method for File Download
 * STORM-695: storm CLI tool reports zero exit code on error scenario
 * STORM-1416: Documentation for state store
 * STORM-1426: keep backtype.storm.tuple.AddressedTuple and delete duplicated backtype.storm.messaging.AddressedTuple
 * STORM-1417: fixed equals/hashCode contract in CoordType
 * STORM-1422: broken example in storm-starter tutorial
 * STORM-1429: LocalizerTest fix
 * STORM-1401: removes multilang-test
 * STORM-1424: Removed unused topology-path variable
 * STORM-1427: add TupleUtils/listHashCode method and delete tuple.clj
 * STORM-1413: remove unused variables for some tests
 * STORM-1412: null check should be done in the first place
 * STORM-1210: Set Output Stream id in KafkaSpout
 * STORM-1397: Merge conflict from Pacemaker merge
 * STORM-1373: Blobstore API sample example usage
 * STORM-1409: StormClientErrorHandler is not used
 * STORM-1411: Some fixes for storm-windowing
 * STORM-1399: Blobstore tests should write data to `target` so it gets removed when running `mvn clean`
 * STORM-1398: Add back in TopologyDetails.getTopology
 * STORM-898: Add priorities and per user resource guarantees to Resource Aware Scheduler
 * STORM-1187: Support windowing based on tuple ts.
 * STORM-1400: Netty Context removeClient() called after term() causes NullPointerException.
 * STORM-1383: Supervisors should not crash if nimbus is unavailable
 * STORM-1381: Client side topology submission hook.
 * STORM-1376: Performance slowdown due excessive zk connections and log-debugging
 * STORM-1395: Move JUnit dependency to top-level pom
 * STORM-1372: Merging design and usage documents for distcache
 * STORM-1393: Update the storm.log.dir function, add doc for logs
 * STORM-1377: nimbus_auth_test: very short timeouts causing spurious failures
 * STORM-1388: Fix url and email links in README file
 * STORM-1389: Removed creation of projection tuples as they are already available
 * STORM-1179: Create Maven Profiles for Integration Tests.
 * STORM-1387: workers-artifacts directory configurable, and default to be under storm.log.dir.
 * STORM-1211: Add trident state and query support for cassandra connector
 * STORM-1359: Change kryo links from google code to github
 * STORM-1385: Divide by zero exception in stats.clj
 * STORM-1370: Bug fixes for MultitenantScheduler
 * STORM-1374: fix random failure on WindowManagerTest
 * STORM-1040: SQL support for Storm.
 * STORM-1364: Log storm version on daemon start
 * STORM-1375: Blobstore broke Pacemaker
 * STORM-876: Blobstore/DistCache Support
 * STORM-1361: Apache License missing from two Cassandra files
 * STORM-756: Handle taskids response as soon as possible
 * STORM-1218: Use markdown for JavaDoc.
 * STORM-1075: Storm Cassandra connector.
 * STORM-965: excessive logging in storm when non-kerberos client tries to connect
 * STORM-1341: Let topology have own heartbeat timeout for multilang subprocess
 * STORM-1207: Added flux support for IWindowedBolt
 * STORM-1352: Trident should support writing to multiple Kafka clusters.
 * STORM-1220: Avoid double copying in the Kafka spout.
 * STORM-1340: Use Travis-CI build matrix to improve test execution times
 * STORM-1126: Allow a configMethod that takes no arguments (Flux)
 * STORM-1203: worker metadata file creation doesn't use storm.log.dir config
 * STORM-1349: [Flux] Allow constructorArgs to take Maps as arguments
 * STORM-126: Add Lifecycle support API for worker nodes
 * STORM-1213: Remove sigar binaries from source tree
 * STORM-885:  Heartbeat Server (Pacemaker)
 * STORM-1221: Create a common interface for all Trident spout.
 * STORM-1198: Web UI to show resource usages and Total Resources on all supervisors
 * STORM-1167: Add windowing support for storm core.
 * STORM-1215: Use Async Loggers to avoid locking  and logging overhead
 * STORM-1204: Logviewer should graceful report page-not-found instead of 500 for bad topo-id etc
 * STORM-831: Add BugTracker and Central Logging URL to UI
 * STORM-1208: UI: NPE seen when aggregating bolt streams stats
 * STORM-1016: Generate trident bolt ids with sorted group names
 * STORM-1190: System Load too high after recent changes
 * STORM-1098: Nimbus hook for topology actions.
 * STORM-1145: Have IConnection push tuples instead of pull them
 * STORM-1191: bump timeout by 50% due to intermittent travis build failures
 * STORM-794: Modify Spout async loop to treat activate/deactivate ASAP
 * STORM-1196: Upgrade to thrift 0.9.3
 * STORM-1155: Supervisor recurring health checks
 * STORM-1189: Maintain wire compatability with 0.10.x versions of storm.
 * STORM-1185: replace nimbus.host with nimbus.seeds
 * STORM-1164: Code cleanup for typos, warnings and conciseness.
 * STORM-902: Simple Log Search.
 * STORM-1052: TridentKafkaState uses new Kafka Producer API.
 * STORM-1182: Removing and wrapping some exceptions in ConfigValidation to make code cleaner
 * STORM-1134. Windows: Fix log4j config.
 * STORM-1127: allow for boolean arguments (Flux)
 * STORM-1138: Storm-hdfs README should be updated with Avro Bolt information
 * STORM-1154: SequenceFileBolt needs unit tests
 * STORM-162: Load Aware Shuffle Grouping
 * STORM-1158: Storm metrics to profile various storm functions
 * STORM-1161: Add License headers and add rat checks to builds
 * STORM-1165: normalize the scales of CPU/Mem/Net when choosing the best node for Resource Aware Scheduler
 * STORM-1163: use rmr rather than rmpath for remove worker-root
 * STORM-1170: Fix the producer alive issue in DisruptorQueueTest
 * STORM-1168: removes noisy log message & a TODO
 * STORM-1143: Validate topology Configs during topology submission
 * STORM-1157: Adding dynamic profiling for worker, restarting worker, jstack, heap dump, and profiling
 * STORM-1123: TupleImpl - Unnecessary variable initialization.
 * STORM-1153: Use static final instead of just static for class members.
 * STORM-817: Kafka Wildcard Topic Support.
 * STORM-40: Turn worker garbage collection and heapdump on by default.
 * STORM-1152: Change map keySet iteration to entrySet iteration for efficiency.
 * STORM-1147: Storm JDBCBolt should add validation to ensure either insertQuery or table name is specified and not both.
 * STORM-1151: Batching in DisruptorQueue
 * STORM-350: Update disruptor to latest version (3.3.2)
 * STORM-697: Support for Emitting Kafka Message Offset and Partition
 * STORM-1074: Add Avro HDFS bolt
 * STORM-566: Improve documentation including incorrect Kryo ser. framework docs
 * STORM-1073: Refactor AbstractHdfsBolt
 * STORM-1128: Make metrics fast
 * STORM-1122: Fix the format issue in Utils.java
 * STORM-1111: Fix Validation for lots of different configs
 * STORM-1125: Adding separate ZK client for read in Nimbus ZK State
 * STORM-1121: Remove method call to avoid overhead during topology submission time
 * STORM-1120: Fix keyword (schema -> scheme) from main-routes
 * STORM-1115: Stale leader-lock key effectively bans all nodes from becoming leaders
 * STORM-1119: Create access logging for all daemons
 * STORM-1117: Adds visualization-init route previously missing
 * STORM-1118: Added test to compare latency vs. throughput in storm.
 * STORM-1110: Fix Component Page for system components
 * STORM-1093: Launching Workers with resources specified in resource-aware schedulers
 * STORM-1102: Add a default flush interval for HiveBolt
 * STORM-1112: Add executor id to the thread name of the executor thread for debug
 * STORM-1079: Batch Puts to HBase
 * STORM-1084: Improve Storm config validation process to use java annotations instead of *_SCHEMA format
 * STORM-1106: Netty should not limit attempts to reconnect
 * STORM-1103: Changes log message to DEBUG from INFO
 * STORM-1104: Nimbus HA fails to find newly downloaded code files
 * STORM-1087: Avoid issues with transfer-queue backpressure.
 * STORM-893: Resource Aware Scheduling (Experimental)
 * STORM-1095: Tuple.getSourceGlobalStreamid() has wrong camel-case naming
 * STORM-1091: Add unit test for tick tuples to HiveBolt and HdfsBolt
 * STORM-1090: Nimbus HA should support `storm.local.hostname`
 * STORM-820: Aggregate topo stats on nimbus, not ui
 * STORM-412: Allow users to modify logging levels of running topologies
 * STORM-1078: Updated RateTracker to be thread safe
 * STORM-1082: fix nits for properties in kafka tests
 * STORM-993: include uptimeSeconds as JSON integer field
 * STORM-1053: Update storm-kafka README for new producer API confs.
 * STORM-1058: create CLI kill_workers to kill workers on a supervisor node
 * STORM-1063: support relative log4j conf dir for both daemons and workers
 * STORM-1059: Upgrade Storm to use Clojure 1.7.0
 * STORM-1069: add check case for external change of 'now' value.
 * STORM-969: HDFS Bolt can end up in an unrecoverable state.
 * STORM-1068: Configure request.required.acks to be 1 in KafkaUtilsTest for sync
 * STORM-1017: If ignoreZkOffsets set true,KafkaSpout will reset zk offset when recover from failure.
 * STORM-1054: Excessive logging ShellBasedGroupsMapping if the user doesn't have any groups.
 * STORM-954: Toplogy Event Inspector
 * STORM-862: Pluggable System Metrics
 * STORM-1032: Add generics to component configuration methods
 * STORM-886: Automatic Back Pressure
 * STORM-1037: do not remove storm-code in supervisor until kill job
 * STORM-1007: Add more metrics to DisruptorQueue
 * STORM-1011: HBaseBolt default mapper should handle null values
 * STORM-1019: Added missing dependency version to use of org.codehaus.mojo:make-maven-plugin
 * STORM-1020: Document exceptions in ITuple & Fields
 * STORM-1025: Invalid links at https://storm.apache.org/about/multi-language.html
 * STORM-1010: Each KafkaBolt could have a specified properties.
 * STORM-1008: Isolate the code for metric collection and retrieval from DisruptorQueue
 * STORM-991: General cleanup of the generics (storm.trident.spout package)
 * STORM-1000: Use static member classes when permitted 
 * STORM-1003: In cluster.clj replace task-id with component-id in the declaration
 * STORM-1013: [storm-elasticsearch] Expose TransportClient configuration Map to EsConfig
 * STORM-1012: Shading jackson.
 * STORM-974: [storm-elasticsearch] Introduces Tuple -> ES document mapper to get rid of constant field mapping (source, index, type)
 * STORM-978: [storm-elasticsearch] Introduces Lookup(or Query)Bolt which emits matched documents from ES
 * STORM-851: Storm Solr connector
 * STORM-854: [Storm-Kafka] KafkaSpout can set the topic name as the output streamid
 * STORM-990: Refactored TimeCacheMap to extend RotatingMap
 * STORM-829: Hadoop dependency confusion
 * STORM-166: Nimbus HA
 * STORM-976: Config storm.logback.conf.dir is specific to previous logging framework
 * STORM-995: Fix excessive logging
 * STORM-837: HdfsState ignores commits
 * STORM-938: storm-hive add a time interval to flush tuples to hive.
 * STORM-964: Add config (with small default value) for logwriter to restrict its memory usage
 * STORM-980: Re-include storm-kafka tests from Travis CI build
 * STORM-960: HiveBolt should ack tuples only after flushing.
 * STORM-951: Storm Hive connector leaking connections.
 * STORM-806: use storm.zookeeper.connection.timeout in storm-kafka ZkState when newCurator
 * STORM-809: topology.message.timeout.secs should not allow for null or <= 0 values
 * STORM-847: Add cli to get the last storm error from the topology
 * STORM-864: Exclude storm-kafka tests from Travis CI build
 * STORM-477: Add warning for invalid JAVA_HOME
 * STORM-826: Update KafkaBolt to use the new kafka producer API
 * STORM-912: Support SSL on Logviewer
 * STORM-934: The current doc for topology ackers is outdated
 * STORM-160: Allow ShellBolt to set env vars (particularly PATH)
 * STORM-937: Changing noisy log level from info to debug
 * STORM-931: Python Scripts to Produce Formatted JIRA and GitHub Join
 * STORM-924: Set the file mode of the files included when packaging release packages
 * STORM-799: Use IErrorReport interface more broadly
 * STORM-926: change pom to use maven-shade-plugin:2.2
 * STORM-942: Add FluxParser method parseInputStream() to eliminate disk usage
 * STORM-67: Provide API for spouts to know how many pending messages there are
 * STORM-918: Storm CLI could validate arguments/print usage
 * STORM-859: Add regression test of STORM-856
 * STORM-913: Use Curator's delete().deletingChildrenIfNeeded() instead of zk/delete-recursive
 * STORM-968: Adding support to generate the id based on names in Trident
 * STORM-845: Storm ElasticSearch connector
 * STORM-988: supervisor.slots.ports should not allow duplicate element values
 * STORM-975: Storm-Kafka trident topology example
 * STORM-958: Add config for init params of group mapping service
 * STORM-949: On the topology summary UI page, last shown error should have the time and date
 * STORM-1142: Some config validators for positive ints need to allow 0
 * STORM-901: Worker Artifacts Directory
 * STORM-1144: Display requested and assigned cpu/mem resources for schedulers in UI
 * STORM-1217: making small fixes in RAS

## 0.10.3
 * STORM-2158: Fix OutOfMemoryError in Nimbus' SimpleTransportPlugin

## 0.10.2
 * STORM-1834: Documentation How to Generate Certificates For Local Testing SSL Setup
 * STORM-1754: Correct java version in 0.10.x storm-starter
 * STORM-1750: Ensure worker dies when report-error-and-die is called.
 * STORM-1739: update the minor JAVA version dependency in 0.10.0 and above
 * STORM-1733: Flush stdout and stderr before calling "os.execvp" to prevent log loss

## 0.10.1
 * STORM-584: Fix logging for LoggingMetricsConsumer metrics.log file
 * STORM-1596: Do not use single Kerberos TGT instance between multiple threads
 * STORM-1481: avoid Math.abs(Integer) get a negative value
 * STORM-1121: Deprecate test only configuraton nimbus.reassign
 * STORM-1180: FLUX logo wasn't appearing quite right
 * STORM-1482: add missing 'break' for RedisStoreBolt

## 0.10.0
 * STORM-1096: Fix some issues with impersonation on the UI
 * STORM-581: Add rebalance params to Storm REST API
 * STORM-1108: Fix NPE in simulated time
 * STORM-1099: Fix worker childopts as arraylist of strings
 * STORM-1094: advance kafka offset when deserializer yields no object
 * STORM-1066: Specify current directory when supervisor launches a worker
 * STORM-1012: Shaded everything that was not already shaded
 * STORM-967: Shaded everything that was not already shaded
 * STORM-922: Shaded everything that was not already shaded
 * STORM-1042: Shaded everything that was not already shaded
 * STORM-1026: Adding external classpath elements does not work
 * STORM-1055: storm-jdbc README needs fixes and context
 * STORM-1044: Setting dop to zero does not raise an error
 * STORM-1050: Topologies with same name run on one cluster
 * STORM-1005: Supervisor do not get running workers after restart.
 * STORM-803: Better CI logs
 * STORM-1027: Use overflow buffer for emitting metrics
 * STORM-1024: log4j changes leaving ${sys:storm.log.dir} under STORM_HOME dir
 * STORM-944: storm-hive pom.xml has a dependency conflict with calcite
 * STORM-994: Connection leak between nimbus and supervisors
 * STORM-1001: Undefined STORM_EXT_CLASSPATH adds '::' to classpath of workers
 * STORM-977: Incorrect signal (-9) when as-user is true
 * STORM-843: [storm-redis] Add Javadoc to storm-redis
 * STORM-866: Use storm.log.dir instead of storm.home in log4j2 config
 * STORM-810: PartitionManager in storm-kafka should commit latest offset before close
 * STORM-928: Add sources->streams->fields map to Multi-Lang Handshake
 * STORM-945: <DefaultRolloverStrategy> element is not a policy,and should not be putted in the <Policies> element.
 * STORM-857: create logs metadata dir when running securely
 * STORM-793: Made change to logviewer.clj in order to remove the invalid http 500 response
 * STORM-139: hashCode does not work for byte[]
 * STORM-860: UI: while topology is transitioned to killed, "Activate" button is enabled but not functioning
 * STORM-966: ConfigValidation.DoubleValidator doesn't really validate whether the type of the object is a double
 * STORM-742: Let ShellBolt treat all messages to update heartbeat
 * STORM-992: A bug in the timer.clj might cause unexpected delay to schedule new event

## 0.10.0-beta1
 * STORM-873: Flux does not handle diamond topologies

## 0.10.0-beta
 * STORM-867: fix bug with mk-ssl-connector
 * STORM-856: use serialized value of delay secs for topo actions
 * STORM-852: Replaced Apache Log4j Logger with SLF4J API
 * STORM-813: Change storm-starter's README so that it explains mvn exec:java cannot run multilang topology
 * STORM-853: Fix upload API to handle multi-args properly
 * STORM-850: Convert storm-core's logback-test.xml to log4j2-test.xml
 * STORM-848: Shade external dependencies
 * STORM-849: Add storm-redis to storm binary distribution
 * STORM-760: Use JSON for serialized conf
 * STORM-833: Logging framework logback -> log4j 2.x
 * STORM-842: Drop Support for Java 1.6
 * STORM-835: Netty Client hold batch object until io operation complete
 * STORM-827: Allow AutoTGT to work with storm-hdfs too.
 * STORM-821: Adding connection provider interface to decouple jdbc connector from a single connection pooling implementation.
 * STORM-818: storm-eventhubs configuration improvement and refactoring
 * STORM-816: maven-gpg-plugin does not work with gpg 2.1
 * STORM-811: remove old metastor_db before running tests again.
 * STORM-808: allow null to be parsed as null
 * STORM-807: quote args to storm.py correctly
 * STORM-801: Add Travis CI badge to README
 * STORM-797: DisruptorQueueTest has some race conditions in it.
 * STORM-796: Add support for "error" command in ShellSpout
 * STORM-795: Update the user document for the extlib issue
 * STORM-792: Missing documentation in backtype.storm.generated.Nimbus
 * STORM-791: Storm UI displays maps in the config incorrectly
 * STORM-790: Log "task is null" instead of let worker died when task is null in transfer-fn
 * STORM-789: Send more topology context to Multi-Lang components via initial handshake
 * STORM-788: UI Fix key for process latencies
 * STORM-787: test-ns should announce test failures with 'BUILD FAILURE'
 * STORM-786: KafkaBolt shoul
