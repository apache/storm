---
layout: post
title: Storm 0.9.6 released
author: P. Taylor Goetz
---

The Apache Storm community is pleased to announce that version 0.9.6 has been released and is available from [the downloads page](/downloads.html).

This is a maintenance release that includes a number of important bug fixes that improve Storm's stability and fault tolerance. We encourage users of previous versions to upgrade to this latest release.


Thanks
------
Special thanks are due to all those who have contributed to Apache Storm -- whether through direct code contributions, documentation, bug reports, or helping other users on the mailing lists. Your efforts are much appreciated.


Full Changelog
---------

 * STORM-1027: Use overflow buffer for emitting metrics
 * STORM-996: netty-unit-tests/test-batch demonstrates out-of-order delivery
 * STORM-1056: allow supervisor log filename to be configurable via ENV variable
 * STORM-1051: Netty Client.java's flushMessages produces a NullPointerException
 * STORM-763: nimbus reassigned worker A to another machine, but other worker's netty client can't connect to the new worker A
 * STORM-935: Update Disruptor queue version to 2.10.4
 * STORM-503: Short disruptor queue wait time leads to high CPU usage when idle
 * STORM-728: Put emitted and transferred stats under correct columns
 * STORM-643: KafkaUtils repeatedly fetches messages whose offset is out of range
 * STORM-933: NullPointerException during KafkaSpout deactivation
