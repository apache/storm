---
title: Metrics Reporting API v2
layout: documentation
documentation: true
---
Apache Storm version 1.2 introduced a new metrics system for reporting
internal statistics (e.g. acked, failed, emitted, transferred, queue metrics, etc.) as well as a 
new API for user defined metrics.

The new metrics system is based on [Dropwizard Metrics](http://metrics.dropwizard.io).


## User Defined Metrics
To allow users to define custom metrics, the following methods have been added to the `TopologyContext`
class, an instance of which is passed to spout's `open()` method and bolt's `prepare()` method:

    public Timer registerTimer(String name)

    public Histogram registerHistogram(String name)

    public Meter registerMeter(String name)

    public Counter registerCounter(String name)

    public Gauge registerGauge(String name, Gauge gauge)

API documentation: [Timer](http://metrics.dropwizard.io/4.0.0/apidocs/com/codahale/metrics/Timer.html), 
[Histogram](http://metrics.dropwizard.io/4.0.0/apidocs/com/codahale/metrics/Histogram.html),
[Meter](http://metrics.dropwizard.io/4.0.0/apidocs/com/codahale/metrics/Meter.html),
[Counter](http://metrics.dropwizard.io/4.0.0/apidocs/com/codahale/metrics/Counter.html),
[Gauge](http://metrics.dropwizard.io/4.0.0/apidocs/com/codahale/metrics/Gauge.html)

Each of these methods takes a `name` parameter that acts as an identifier. When metrics are 
registered, Storm will add additional information such as hostname, port, topology ID, etc. to form a unique metric
identifier. For example, if we register a metric named `myCounter` as follows:

```java
    Counter myCounter = topologyContext.registerCounter("myCounter");
```
the resulting name sent to metrics reporters will expand to:

```
   storm.topology.{topology ID}.{hostname}.{component ID}.{task ID}.{worker port}-myCounter 
```

The additional information allows for the unique identification of metrics for component instances across the cluster.

*Important Note:* In order to ensure metric names can be reliably parsed, any `.` characters in name components will
be replaced with an underscore (`_`) character. For example, the hostname `storm.example.com` will appear as
`storm_example_com` in the metric name. This character substitution *is not applied to the user-supplied `name` parameter.

### Example: Tuple Counter Bolt
The following example is a simple bolt implementation that will report the running total up tuples received by a bolt:

```java
public class TupleCountingBolt extends BaseRichBolt {
    private Counter tupleCounter;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.tupleCounter = context.registerCounter("tupleCount");
    }

    @Override
    public void execute(Tuple input) {
        this.tupleCounter.inc();
    }
}
```
 
## Metric Reporter Configuration

 For metrics to be useful they must be *reported*, in other words sent somewhere where they can be consumed and analyzed.
 That can be as simple as writing them to a log file, sending them to a time series database, or exposing them via JMX.
 
The following metric reporters are supported
 
  * Console Reporter (`org.apache.storm.metrics2.reporters.ConsoleStormReporter`):
    Reports metrics to `System.out`.
  * CSV Reporter (`org.apache.storm.metrics2.reporters.CsvStormReporter`):
    Reports metrics to a CSV file.
  * Graphite Reporter (`org.apache.storm.metrics2.reporters.GraphiteStormReporter`):
    Reports metrics to a [Graphite](https://graphiteapp.org) server.
  * JMX Reporter (`org.apache.storm.metrics2.reporters.JmxStormReporter`):
    Exposes metrics via JMX.
  
 Custom metrics reporters can be created by implementing `org.apache.storm.metrics2.reporters.StormReporter` interface 
 or extending `org.apache.storm.metrics2.reporters.ScheduledStormReporter` class.
  
 By default, Storm will collect metrics but not "report" or
 send the collected metrics anywhere. To enable metrics reporting, add a `topology.metrics.reporters` section to `storm.yaml`
 or in topology configuration and configure one or more reporters.
 
 The following example configuration sets up two reporters: a Graphite Reporter and a Console Reporter:
 
 ```yaml
topology.metrics.reporters:
  # Graphite Reporter
  - class: "org.apache.storm.metrics2.reporters.GraphiteStormReporter"
    report.period: 60
    report.period.units: "SECONDS"
    graphite.host: "localhost"
    graphite.port: 2003

  # Console Reporter
  - class: "org.apache.storm.metrics2.reporters.ConsoleStormReporter"
    report.period: 10
    report.period.units: "SECONDS"
    filter:
        class: "org.apache.storm.metrics2.filters.RegexFilter"
        expression: ".*my_component.*emitted.*"
```

Each reporter section begins with a `class` parameter representing the fully-qualified class name of the reporter 
implementation. 

Many reporter implementations are *scheduled*, meaning they report metrics at regular intervals. The reporting interval
is determined by the `report.period` and `report.period.units` parameters.

Reporters can also be configured with an optional filter that determines which metrics get reported. Storm includes the
`org.apache.storm.metrics2.filters.RegexFilter` filter which uses a regular expression to determine which metrics get
reported. Custom filters can be created by implementing the `org.apache.storm.metrics2.filters.StormMetricFilter`
interface:

```java
public interface StormMetricsFilter extends MetricFilter {

    /**
     * Called after the filter is instantiated.
     * @param config A map of the properties from the 'filter' section of the reporter configuration.
     */
    void prepare(Map<String, Object> config);
    
   /**
    *  Returns true if the given metric should be reported.
    */
    boolean matches(String name, Metric metric);

}
```

V2 metrics can be reported with a long name (such as storm.topology.mytopologyname-17-1595349167.hostname.__system.-1.6700-memory.pools.Code-Cache.max) or with a short
name and dimensions (such as memory.pools.Code-Cache.max with dimensions task Id of -1 and component Id of __system) if reporters support this.  Each reporter defaults
to using the long metric name, but can report the short name by configuring report.dimensions.enabled to true for the reporter.

## Backwards Compatibility Notes

1. V2 metrics can also be reported to the Metrics Consumers registered with `topology.metrics.consumer.register` by enabling the `topology.enable.v2.metrics.tick` configuration.
The rate that they will reported to Metric Consumers is controlled by `topology.v2.metrics.tick.interval.seconds`, defaulting to every 60 seconds.

2. Starting from storm 2.3, the config `storm.metrics.reporters` is deprecated in favor of `topology.metrics.reporters`.

3. Starting from storm 2.3, the `daemons` section is removed from `topology.metrics.reporters` (or `storm.metrics.reporters`).
   Before storm 2.3, a `daemons` section is required in the reporter conf to determine which daemons the reporters will apply to. 
However, the reporters configured with `topology.metrics.reporters` (or `storm.metrics.reporters`) actually only apply to workers. They are never really used in daemons like nimbus, supervisor and etc. 
   For daemon metrics, please refer to [Cluster Metrics](ClusterMetrics.html).

4. **Backwards Compatibility Breakage**: starting from storm 2.3, the following configs no longer apply to `topology.metrics.reporters`:
   ```yaml
   storm.daemon.metrics.reporter.plugin.locale
   storm.daemon.metrics.reporter.plugin.rate.unit
   storm.daemon.metrics.reporter.plugin.duration.unit
   ```

    They only apply to daemon metric reporters configured via `storm.daemon.metrics.reporter.plugins` for storm daemons.
    The corresponding configs for `topology.metrics.reporters` can be configured in reporter conf with `locale`, `rate.unit`, `duration.unit` respectively, for example,
    ```yaml
    topology.metrics.reporters:
      # Console Reporter
      - class: "org.apache.storm.metrics2.reporters.ConsoleStormReporter"
        report.period: 10
        report.period.units: "SECONDS"
        locale: "en-US"
        rate.unit: "SECONDS"
        duration.unit: "SECONDS"
    ```
   Default values will be used if they are not set or set to `null`.
   
