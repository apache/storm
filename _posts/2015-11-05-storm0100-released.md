---
layout: post
title: Storm 0.10.0 released
author: P. Taylor Goetz
---

The Apache Storm community is pleased to announce that version 0.10.0 Stable has been released and is available from [the downloads page](/downloads.html).

This release includes a number of improvements and bug fixes identified in the previous beta release. For a description of the new features included in the 0.10.0 release, please [see the previous announcement of 0.10.0-beta1](/2015/06/15/storm0100-beta-released.html).


Thanks
------
Special thanks are due to all those who have contributed to Apache Storm -- whether through direct code contributions, documentation, bug reports, or helping other users on the mailing lists. Your efforts are much appreciated.


Full Changelog
---------

 * STORM-1108: Fix NPE in simulated time
 * STORM-1106: Netty should not limit attempts to reconnect
 * STORM-1099: Fix worker childopts as arraylist of strings
 * STORM-1096: Fix some issues with impersonation on the UI
 * STORM-912: Support SSL on Logviewer
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
 * STORM-803: Cleanup travis-ci build and logs
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
