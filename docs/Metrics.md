---
title: Storm Metrics
layout: documentation
documentation: true
---
Storm exposes a metrics interface to report summary statistics across the full topology.
It's used internally to track the numbers you see in the Nimbus UI console: counts of executes and acks; average process latency per bolt; worker heap usage; and so forth.

### Metric Types

Metrics have to implement [`IMetric`]({{page.git-blob-base}}/storm-core/src/jvm/org/apache/storm/metric/api/IMetric.java) which contains just one method, `getValueAndReset` -- do any remaining work to find the summary value, and reset back to an initial state. For example, the MeanReducer divides the running total by its running count to find the mean, then initializes both values back to zero.

Storm gives you these metric types:

* [AssignableMetric]({{page.git-blob-base}}/storm-core/src/jvm/org/apache/storm/metric/api/AssignableMetric.java) -- set the metric to the explicit value you supply. Useful if it's an external value or in the case that you are already calculating the summary statistic yourself.
* [CombinedMetric]({{page.git-blob-base}}/storm-core/src/jvm/org/apache/storm/metric/api/CombinedMetric.java) -- generic interface for metrics that can be updated associatively. 
* [CountMetric]({{page.git-blob-base}}/storm-core/src/jvm/org/apache/storm/metric/api/CountMetric.java) -- a running total of the supplied values. Call `incr()` to increment by one, `incrBy(n)` to add/subtract the given number.
  - [MultiCountMetric]({{page.git-blob-base}}/storm-core/src/jvm/org/apache/storm/metric/api/MultiCountMetric.java) -- a hashmap of count metrics.
* [ReducedMetric]({{page.git-blob-base}}/storm-core/src/jvm/org/apache/storm/metric/api/ReducedMetric.java)
  - [MeanReducer]({{page.git-blob-base}}/storm-core/src/jvm/org/apache/storm/metric/api/MeanReducer.java) -- track a running average of values given to its `reduce()` method. (It accepts `Double`, `Integer` or `Long` values, and maintains the internal average as a `Double`.) Despite his reputation, the MeanReducer is actually a pretty nice guy in person.
  - [MultiReducedMetric]({{page.git-blob-base}}/storm-core/src/jvm/org/apache/storm/metric/api/MultiReducedMetric.java) -- a hashmap of reduced metrics.


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

Storm appends MetricsConsumerBolt to your topology per each registered metrics consumer internally, and each MetricsConsumerBolt subscribes to receive metrics from all tasks. The parallelism for that Bolt is set to `parallelism.hint` and `component id` for that Bolt is set to `__metrics_<metrics consumer class name>`. If you register same class name more than once, postfix `#<sequence number>` is appended to component id.

Storm provides some built-in metrics consumers for you to try out to see which metrics are provided in your topology.

* [`LoggingMetricsConsumer`]({{page.git-blob-base}}/storm-core/src/jvm/org/apache/storm/metric/LoggingMetricsConsumer.java) -- listens for all metrics and dumps them to log file with TSV (Tab Separated Values).
* [`HttpForwardingMetricsConsumer`]({{page.git-blob-base}}/storm-core/src/jvm/org/apache/storm/metric/HttpForwardingMetricsConsumer.java) -- listens for all metrics and POSTs them serialized to a configured URL via HTTP. Storm also provides [`HttpForwardingMetricsServer`]({{page.git-blob-base}}/storm-core/src/jvm/org/apache/storm/metric/HttpForwardingMetricsServer.java) as abstract class so you can extend this class and run as a HTTP server, and handle metrics sent by HttpForwardingMetricsConsumer.

Also, Storm exposes the interface [`IMetricsConsumer`]({{page.git-blob-base}}/storm-core/src/jvm/org/apache/storm/metric/api/IMetricsConsumer.java) for implementing Metrics Consumer so you can create custom metrics consumers and attach to their topologies, or use other great implementation of Metrics Consumers provided by Storm community. Some of examples are [versign/storm-graphite](https://github.com/verisign/storm-graphite), and [storm-metrics-statsd](https://github.com/endgameinc/storm-metrics-statsd).

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
	// other intialization here.
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

There're some restrictions for worker level metric instances: 

A) Metrics for worker level should be kind of gauge since it is initialized and registered from SystemBolt and not exposed to user tasks.

B) Metrics will be initialized with default constructor, and no injection for configuration or object will be performed.

C) Bucket size (seconds) for metrics is fixed to `Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS`.


### Builtin Metrics

The [builtin metrics]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/daemon/builtin_metrics.clj) instrument Storm itself.

[builtin_metrics.clj]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/daemon/builtin_metrics.clj) sets up data structures for the built-in metrics, and facade methods that the other framework components can use to update them. The metrics themselves are calculated in the calling code -- see for example [`ack-spout-msg`]({{page.git-blob-base}}/storm-core/src/clj/org/apache/storm/daemon/executor.clj#358)  in `clj/b/s/daemon/daemon/executor.clj`

