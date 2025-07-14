---
title: Documentation
layout: documentation
documentation: true
---
### Basics of Storm

* [Javadoc](javadocs/index.html)
* [Tutorial](Tutorial.html)
* [Concepts](Concepts.html)
* [Scheduler](Storm-Scheduler.html)
* [Configuration](Configuration.html)
* [Guaranteeing message processing](Guaranteeing-message-processing.html)
* [Daemon Fault Tolerance](Daemon-Fault-Tolerance.html)
* [Command line client](Command-line-client.html)
* [REST API](STORM-UI-REST-API.html)
* [Understanding the parallelism of a Storm topology](Understanding-the-parallelism-of-a-Storm-topology.html)
* [FAQ](FAQ.html)

### Layers on top of Storm

#### Trident

Trident is an alternative interface to Storm. It provides exactly-once processing, "transactional" datastore persistence, and a set of common stream analytics operations.

* [Trident Tutorial](Trident-tutorial.html)     -- basic concepts and walkthrough
* [Trident API Overview](Trident-API-Overview.html) -- operations for transforming and orchestrating data
* [Trident State](Trident-state.html)        -- exactly-once processing and fast, persistent aggregation
* [Trident spouts](Trident-spouts.html)       -- transactional and non-transactional data intake
* [Trident RAS API](Trident-RAS-API.html)     -- using the Resource Aware Scheduler with Trident.

#### Streams API

Stream APIs is another alternative interface to Storm. It provides a typed API for expressing streaming computations and supports functional style operations.

NOTE: Streams API is an `experimental` feature, and further works might break backward compatibility.
We're also notifying it via annotating classes with marker interface `@InterfaceStability.Unstable`. 

* [Streams API](Stream-API.html)

#### SQL

The Storm SQL integration allows users to run SQL queries over streaming data in Storm.  

NOTE: Storm SQL is an `experimental` feature, so the internals of Storm SQL and supported features are subject to change. 
But small change will not affect the user experience. We will notify the user when breaking UX change is introduced.

* [Storm SQL overview](storm-sql.html)
* [Storm SQL example](storm-sql-example.html)
* [Storm SQL reference](storm-sql-reference.html)
* [Storm SQL internal](storm-sql-internal.html)

#### Flux

* [Flux Data Driven Topology Builder](flux.html)

### Setup and Deploying

* [Setting up a Storm cluster](Setting-up-a-Storm-cluster.html)
* [Local mode](Local-mode.html)
* [Troubleshooting](Troubleshooting.html)
* [Running topologies on a production cluster](Running-topologies-on-a-production-cluster.html)
* [Building Storm](Maven.html) with Maven
* [Setting up a Secure Cluster](SECURITY.html)
* [CGroup Enforcement](cgroups_in_storm.html)
* [Pacemaker reduces load on zookeeper for large clusters](Pacemaker.html)
* [Resource Aware Scheduler](Resource_Aware_Scheduler_overview.html)
* [Generic Resources](Generic-resources.html)
* [Daemon Metrics/Monitoring](ClusterMetrics.html)
* [Windows users guide](windows-users-guide.html)
* [Classpath handling](Classpath-handling.html)

### Intermediate

* [Serialization](Serialization.html)
* [Common patterns](Common-patterns.html)
* [DSLs and multilang adapters](DSLs-and-multilang-adapters.html)
* [Using non-JVM languages with Storm](Using-non-JVM-languages-with-Storm.html)
* [Distributed RPC](Distributed-RPC.html)
* [Hooks](Hooks.html)
* [Metrics (Deprecated)](Metrics.html)
* [Metrics V2](metrics_v2.html)
* [State Checkpointing](State-checkpointing.html)
* [Windowing](Windowing.html)
* [Joining Streams](Joins.html)
* [Blobstore(Distcache)](distcache-blobstore.html)

### Debugging
* [Dynamic Log Level Settings](dynamic-log-level-settings.html)
* [Searching Worker Logs](Logs.html)
* [Worker Profiling](dynamic-worker-profiling.html)
* [Event Logging](Eventlogging.html)

### Integration With External Systems, and Other Libraries
* [Apache Kafka Integration](storm-kafka-client.html)
* [Apache HBase Integration](storm-hbase.html)
* [Apache HDFS Integration](storm-hdfs.html)
* [JDBC Integration](storm-jdbc.html)
* [JMS Integration](storm-jms.html)
* [Redis Integration](storm-redis.html)

#### Container, Resource Management System Integration

* [YARN Integration](https://github.com/yahoo/storm-yarn)
* [Mesos Integration](https://github.com/mesos/storm)
* [Docker Integration](https://hub.docker.com/_/storm/)
* [Kubernetes Integration](https://github.com/kubernetes/examples/tree/master/staging/storm)

### Advanced

* [Defining a non-JVM language DSL for Storm](Defining-a-non-jvm-language-dsl-for-storm.html)
* [Multilang protocol](Multilang-protocol.html) (how to provide support for another language)
* [Implementation docs](Implementation-docs.html)
* [Storm Metricstore](storm-metricstore.html)

