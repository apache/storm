---
title: Storm Metrics
layout: documentation
documentation: true
---
Storm exposes a metrics interface to report summary statistics across the full topology.
The numbers you see on the UI come from some of these built in metrics, but are reported through the worker heartbeats instead of through the IMetricsConsumer described below.

If you are looking for cluster wide monitoring please see [Cluster Metrics](ClusterMetrics.html).

### Metric Types

Metrics have to implement [`IMetric`]({{page.git-blob-base}}/storm-client/src/jvm/org/apache/storm/metric/api/IMetric.java) which contains just one method, `getValueAndReset` -- do any remaining work to find the summary value, and reset back to an initial state. For example, the MeanReducer divides the running total by its running count to find the mean, then initializes both values back to zero.

Storm gives you these metric types:

* [AssignableMetric]({{page.git-blob-base}}/storm-client/src/jvm/org/apache/storm/metric/api/AssignableMetric.java) -- set the metric to the explicit value you supply. Useful if it's an external value or in the case that you are already calculating the summary statistic yourself.
* [CombinedMetric]({{page.git-blob-base}}/storm-client/src/jvm/org/apache/storm/metric/api/CombinedMetric.java) -- generic interface for metrics that can be updated associatively. 
* [CountMetric]({{page.git-blob-base}}/storm-client/src/jvm/org/apache/storm/metric/api/CountMetric.java) -- a running total of the supplied values. Call `incr()` to increment by one, `incrBy(n)` to add/subtract the given number.
  - [MultiCountMetric]({{page.git-blob-base}}/storm-client/src/jvm/org/apache/storm/metric/api/MultiCountMetric.java) -- a hashmap of count metrics.
* [ReducedMetric]({{page.git-blob-base}}/storm-client/src/jvm/org/apache/storm/metric/api/ReducedMetric.java)
  - [MeanReducer]({{page.git-blob-base}}/storm-client/src/jvm/org/apache/storm/metric/api/MeanReducer.java) -- track a running average of values given to its `reduce()` method. (It accepts `Double`, `Integer` or `Long` values, and maintains the internal average as a `Double`.) Despite his reputation, the MeanReducer is actually a pretty nice guy in person.
  - [MultiReducedMetric]({{page.git-blob-base}}/storm-client/src/jvm/org/apache/storm/metric/api/MultiReducedMetric.java) -- a hashmap of reduced metrics.

Be aware that even though `getValueAndReset` can return an object returning any object makes it very difficult for an `IMetricsConsumer` to know how to translate it into something usable.  Also note that because it is sent to the `IMetricsConsumer` as a part of a tuple the values returned need to be able to be [serialized](Serialization.html) by your topology.

### Metrics Consumer

You can listen and handle the topology metrics via registering Metrics Consumer to your topology. 

To register metrics consumer to your topology, add to your topology's configuration like:

```java
conf.registerMetricsConsumer(org.apache.storm.metric.LoggingMetricsConsumer.class, 1);
```

You can refer [Config#registerMetricsConsumer](javadocs/org/apache/storm/Config.html#registerMetricsConsumer-java.lang.Class-) and overloaded methods from javadoc.

Otherwise edit the storm.yaml config file:

```yaml
topology.metrics.consumer.register:
  - class: "org.apache.storm.metric.LoggingMetricsConsumer"
    parallelism.hint: 1
  - class: "org.apache.storm.metric.HttpForwardingMetricsConsumer"
    parallelism.hint: 1
    argument: "http://example.com:8080/metrics/my-topology/"
```

Storm adds a MetricsConsumerBolt to your topolology for each class in the `topology.metrics.consumer.register` list. Each MetricsConsumerBolt subscribes to receive metrics from all tasks in the topology. The parallelism for each Bolt is set to `parallelism.hint` and `component id` for that Bolt is set to `__metrics_<metrics consumer class name>`. If you register the same class name more than once, postfix `#<sequence number>` is appended to component id.

Storm provides some built-in metrics consumers for you to try out to see which metrics are provided in your topology.

* [`LoggingMetricsConsumer`]({{page.git-blob-base}}/storm-client/src/jvm/org/apache/storm/metric/LoggingMetricsConsumer.java) -- listens for all metrics and dumps them to log file with TSV (Tab Separated Values).
* [`HttpForwardingMetricsConsumer`]({{page.git-blob-base}}/examples/storm-loadgen/src/main/java/org/apache/storm/loadgen/HttpForwardingMetricsConsumer.java) -- listens for all metrics and POSTs them serialized to a configured URL via HTTP. Storm also provides [`HttpForwardingMetricsServer`]({{page.git-blob-base}}/storm-core/src/jvm/org/apache/storm/metric/HttpForwardingMetricsServer.java) as abstract class so you can extend this class and run as a HTTP server, and handle metrics sent by HttpForwardingMetricsConsumer.

Also, Storm exposes the interface [`IMetricsConsumer`]({{page.git-blob-base}}/storm-client/src/jvm/org/apache/storm/metric/api/IMetricsConsumer.java) for implementing Metrics Consumer so you can create custom metrics consumers and attach to their topologies, or use other great implementation of Metrics Consumers provided by Storm community. Some of examples are [versign/storm-graphite](https://github.com/verisign/storm-graphite), and [storm-metrics-statsd](https://github.com/endgameinc/storm-metrics-statsd).

When you implement your own metrics consumer, `argument` is passed to Object when [IMetricsConsumer#prepare](javadocs/org/apache/storm/metric/api/IMetricsConsumer.html#prepare-java.util.Map-java.lang.Object-org.apache.storm.task.TopologyContext-org.apache.storm.task.IErrorReporter-) is called, so you need to infer the Java type of configured value on yaml, and do explicit type casting.

Please keep in mind that MetricsConsumerBolt is just a kind of Bolt, so whole throughput of the topology will go down when registered metrics consumers cannot keep up handling incoming metrics, so you may want to take care of those Bolts like normal Bolts. One of idea to avoid this is making your implementation of Metrics Consumer as `non-blocking` fashion.


### Build your own metric (task level)

You can measure your own metric by registering `IMetric` to Metric Registry. 

Suppose we would like to measure execution count of Bolt#execute. Let's start with defining metric instance. CountMetric seems to fit our use case.

```java
private transient CountMetric countMetric;
```

Notice we define it as transient. IMertic is not Serializable so we defined as transient to avoid any serialization issues.

Next, let's initialize and register the metric instance.

```java
@Override
public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	// other initialization here.
	countMetric = new CountMetric();
	context.registerMetric("execute_count", countMetric, 60);
}
```

The meaning of first and second parameters are straightforward, metric name and instance of IMetric. Third parameter of [TopologyContext#registerMetric](javadocs/org/apache/storm/task/TopologyContext.html#registerMetric-java.lang.String-T-int-) is the period (seconds) to publish and reset the metric.

Last, let's increment the value when Bolt.execute() is executed.

```java
public void execute(Tuple input) {
	countMetric.incr();
	// handle tuple here.	
}
```

Note that sample rate for topology metrics is not applied to custom metrics since we're calling incr() ourselves.

Done! `countMetric.getValueAndReset()` is called every 60 seconds as we registered as period, and pair of ("execute_count", value) will be pushed to MetricsConsumer.

### Build your own metrics (worker level)

You can register your own worker level metrics by adding them to `Config.WORKER_METRICS` for all workers in cluster, or `Config.TOPOLOGY_WORKER_METRICS` for all workers in specific topology.

For example, we can add `worker.metrics` to storm.yaml in cluster,

```yaml
worker.metrics: 
  metricA: "aaa.bbb.ccc.ddd.MetricA"
  metricB: "aaa.bbb.ccc.ddd.MetricB"
  ...
```

or put `Map<String, String>` (metric name, metric class name) with key `Config.TOPOLOGY_WORKER_METRICS` to config map.

There are some restrictions for worker level metric instances: 

A) Metrics for worker level should be kind of gauge since it is initialized and registered from SystemBolt and not exposed to user tasks.

B) Metrics will be initialized with default constructor, and no injection for configuration or object will be performed.

C) Bucket size (seconds) for metrics is fixed to `Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS`.

### Builtin Metrics

The [builtin metrics]({{page.git-blob-base}}/storm-client/src/jvm/org/apache/storm/daemon/metrics/BuiltinMetricsUtil.java) instrument Storm itself.

[BuiltinMetricsUtil.java]({{page.git-blob-base}}/storm-client/src/jvm/org/apache/storm/daemon/metrics/BuiltinMetricsUtil.java) sets up data structures for the built-in metrics, and facade methods that the other framework components can use to update them. The metrics themselves are calculated in the calling code -- see for example [`ackSpoutMsg`]({{page.git-blob-base}}/storm-client/src/jvm/org/apache/storm/executor/Executor.java).

#### Reporting Rate

The rate at which built in metrics are reported is configurable through the `topology.builtin.metrics.bucket.size.secs` config.  If you set this too low it can overload the consumers,
so please use caution when modifying it.

#### Tuple Counting Metrics

There are several different metrics related to counting what a bolt or spout does to a tuple. These include things like emitting, transferring, acking, and failing of tuples.

In general all of these tuple count metrics are randomly sub-sampled unless otherwise stated.  This means that the counts you see both on the UI and from the built in metrics are not necessarily exact.  In fact by default we sample only 5% of the events and estimate the total number of events from that.  The sampling percentage is configurable per topology through the `topology.stats.sample.rate` config.  Setting it to 1.0 will make the counts exact, but be aware that the more events we sample the slower your topology will run (as the metrics are counted in the same code path as tuples are processed).  This is why we have a 5% sample rate as the default.

The tuple counting metric names contain `"${stream_name}"` or `"${upstream_component}:${stream_name}"`.  The former is used for all spout metrics and for outgoing bolt metrics (`__emit-count` and `__transfer-count`).  The latter is used for bolt metrics that deal with incoming tuples.

So for a word count topology the count bolt might show something like the following for the `__ack-count` metric

```
    "__ack-count-split:default": 80080
```

But the spout instead would show something like the following for the `__ack-count` metric.

```
    "__ack-count-default": 12500
```


##### `__ack-count`

For bolts it is the number of incoming tuples that had the `ack` method called on them.  For spouts it is the number of tuples trees that were fully acked. See Guaranteeing Message Processing[](Guaranteeing-message-processing.html) for more information about what a tuple tree is. If acking is disabled this metric is still reported, but it is not really meaningful.

##### `__fail-count`

For bolts this is the number of incoming tuples that had the `fail` method called on them.  For spouts this is the number of tuple trees that failed.  Tuple trees may fail from timing out or because a bolt called fail on it.  The two are not separated out by this metric.

##### `__emit-count`

This is the total number of times the `emit` method was called to send a tuple.  This is the same for both bolts and spouts.

##### `__transfer-count`

This is the total number of tuples transferred to a downstream bolt/spout for processing. This number will not always match `__emit_count`.  If nothing is registered to receive a tuple down stream the number will be 0 even if tuples were emitted.  Similarly if there are multiple down stream consumers it may be a multiple of the number emitted.  The grouping also can play a role if it sends the tuple to multiple instances of a single bolt down stream.

##### `__execute-count`

This count metric is bolt specific.  It counts the number of times that a bolt's `execute` method was called.

#### Tuple Latency Metrics

Similar to the tuple counting metrics storm also collects average latency metrics for bolts and spouts.  These follow the same structure as the bolt/spout maps and are sub-sampled in the same way as well.  In all cases the latency is measured in milliseconds.

##### `__complete-latency`

The complete latency is just for spouts.  It is the average amount of time it took for `ack` or `fail` to be called for a tuple after it was emitted.  If acking is disabled this metric is likely to be blank or 0 for all values, and should be ignored.

##### `__execute-latency`

This is just for bolts.  It is the average amount of time that the bolt spent in the call to the `execute` method.  The higher this gets, the lower the throughput of tuples per bolt instance.

##### `__process-latency`

This is also just for bolts.  It is the average amount of time between when `execute` was called to start processing a tuple, to when it was acked or failed by the bolt.  If your bolt is a very simple bolt and the processing is synchronous then `__process-latency` and `__execute-latency` should be very close to one another, with process latency being slightly smaller.  If you are doing a join or have asynchronous processing then it may take a while for a tuple to be acked so the process latency would be higher than the execute latency.

##### `__skipped-max-spout-ms`

This metric records how much time a spout was idle because more tuples than `topology.max.spout.pending` were still outstanding.  This is the total time in milliseconds, not the average amount of time and is not sub-sampled.


##### `__skipped-backpressure-ms`

This metric records how much time a spout was idle because back-pressure indicated that downstream queues in the topology were too full.  This is the total time in milliseconds, not the average amount of time and is not sub-sampled. This is similar to skipped-throttle-ms in Storm 1.x.

##### `__backpressure-last-overflow-count`

This metric indicates the overflow count last time BP status was sent, with a minimum value of 1 if a task has backpressure on.

##### `skipped-inactive-ms`

This metric records how much time a spout was idle because the topology was deactivated.  This is the total time in milliseconds, not the average amount of time and is not sub-sampled.

#### Error Reporting Metrics

Storm also collects error reporting metrics for bolts and spouts.

##### `__reported-error-count`

This metric records how many errors were reported by a spout/bolt. It is the total number of times the `reportError` method was called.

#### Queue Metrics

Each bolt or spout instance in a topology has a receive queue.  Each worker also has a worker transfer queue for sending messages to other workers.  All of these have metrics that are reported.

The receive queue metrics are reported under the `receive_queue` name.  The metrics for the queue that sends messages to other workers is under the `worker-transfer-queue` metric name for the system bolt (`__system`).

These queues report the following metrics:

```
{
    "arrival_rate_secs": 1229.1195171893523,
    "overflow": 0,
    "sojourn_time_ms": 2.440771591407277,
    "capacity": 1024,
    "population": 19,
    "pct_full": "0.018".
    "insert_failures": "0",
    "dropped_messages": "0"
}
```

`arrival_rate_secs` is an estimation of the number of tuples that are inserted into the queue in one second, although it is actually the dequeue rate.
The `sojourn_time_ms` is calculated from the arrival rate and is an estimate of how many milliseconds each tuple sits in the queue before it is processed.

The queue has a set maximum number of entries.  If the regular queue fills up an overflow queue takes over.  The number of tuples stored in this overflow section are represented by the `overflow` metric.  Note that an overflow queue is only used for executors to receive tuples from remote workers. It doesn't apply to intra-worker tuple transfer.

`capacity` is the maximum number of entries in the queue. `population` is the number of entries currently filled in the queue. 'pct_full' tracks the percentage of capacity in use.

'insert_failures' tracks the number of failures inserting into the queue. 'dropped_messages' tracks messages dropped due to the overflow queue being full.

#### System Bolt (Worker) Metrics

The System Bolt `__system` provides lots of metrics for different worker wide things.  The one metric not described here is the `__transfer` queue metric, because it fits with the other disruptor metrics described above.

Be aware that the `__system` bolt is an actual bolt so regular bolt metrics described above also will be reported for it.

##### Receive (NettyServer)
`__recv-iconnection` reports stats for the netty server on the worker.  This is what gets messages from other workers.  It is of the form

```
{
    "dequeuedMessages": 0,
    "enqueued": {
      "/127.0.0.1:49952": 389951
    }
}
```

`dequeuedMessages` is a throwback to older code where there was an internal queue between the server and the bolts/spouts.  That is no longer the case and the value can be ignored.
`enqueued` is a map between the address of the remote worker and the number of tuples that were sent from it to this worker.

##### Send (Netty Client)

The `__send-iconnection` metrics report information about all of the clients for this worker.  They are named __send-iconnection-METRIC_TYPE-HOST:PORT for a given Client that is
connected to a worker with the given host/port.  These metrics can be disabled by setting topology.enable.send.iconnection.metrics to false.

The metric types reported for each client are:

 * `reconnects` the number of reconnections that have happened.
 * `pending` the number of messages that have not been sent.  (This corresponds to messages, not tuples)
 * `sent` the number of messages that have been sent.  (This is messages not tuples)
 * `lostOnSend`.  This is the number of messages that were lost because of connection issues. (This is messages not tuples). 

##### JVM Memory

JVM memory usage is reported through `memory.non-heap` for off heap memory, `memory.heap` for on heap memory and `memory.total` for combined values.  These values come from the [MemoryUsage](https://docs.oracle.com/javase/8/docs/api/index.html?java/lang/management/MemoryUsage.html) mxbean.  Each of the metrics are reported as a map with the following keys, and values returned by the corresponding java code.

| Key | Corresponding Code |
|--------|--------------------|
| `max` | `memUsage.getMax()` |
| `committed` | `memUsage.getCommitted()` |
| `init` | `memUsage.getInit()` |
| `used` | `memUsage.getUsed()` |
| `usage` | `Ratio.of(memUsage.getUsed(), memUsage.getMax())` |

##### JVM Garbage Collection

The exact GC metric name depends on the garbage collector that your worker uses.  The data is all collected from `ManagementFactory.getGarbageCollectorMXBeans()` and the name of the metrics is `"GC"` followed by the name of the returned bean with white space removed.  The reported metrics are just

* `count` the number of gc events that happened and
* `time` the total number of milliseconds that were spent doing gc.  

Please refer to the [JVM documentation](https://docs.oracle.com/javase/8/docs/api/java/lang/management/ManagementFactory.html#getGarbageCollectorMXBeans--) for more details.

##### JVM Misc

* There are metrics prefixed with `threads` providing the number of threads, daemon threads, blocked and deadlocked threads.

##### Uptime

* `uptimeSecs` reports the number of seconds the worker has been up for
* `newWorkerEvent` is 1 when a worker is first started and 0 all other times.  This can be used to tell when a worker has crashed and is restarted.
* `startTimeSecs` is when the worker started in seconds since the epoch

##### doHeartbeat-calls

* `doHeartbeat-calls` is a meter that indicates the rate the worker is performing heartbeats.

