---
title: Metrics Reporting API v2
layout: documentation
documentation: true
---
Apache Storm version 1.2 introduced a new metrics system for reporting
internal statistics (e.g. acked, failed, emitted, transferred, disruptor queue metrics, etc.) as well as a 
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
 
 As of Storm 1.2.0 the following metric reporters are supported
 
  * Console Reporter (`org.apache.storm.metrics2.reporters.ConsoleStormReporter`):
    Reports metrics to `System.out`.
  * CSV Reporter (`org.apache.storm.metrics2.reporters.CsvReporter`):
    Reports metrics to a CSV file.
  * Graphite Reporter (`org.apache.storm.metrics2.reporters.GraphiteStormReporter`):
    Reports metrics to a [Graphite](https://graphiteapp.org) server.
  * JMX Reporter (`org.apache.storm.metrics2.reporters.JmxStormReporter`):
    Exposes metrics via JMX.
  
  
 Metrics reporters are configured in the `storm.yaml` file. By default, Storm will collect metrics but not "report" or
 send the collected metrics anywhere. To enable metrics reporting, add a `storm.metrics.reporters` section to `storm.yaml`
 and configure one or more reporters.
 
 The following example configuration sets up two reporters: a Graphite Reporter and a Console Reporter:
 
 ```yaml
storm.metrics.reporters:
  # Graphite Reporter
  - class: "org.apache.storm.metrics2.reporters.GraphiteStormReporter"
    daemons:
        - "supervisor"
        - "nimbus"
        - "worker"
    report.period: 60
    report.period.units: "SECONDS"
    graphite.host: "localhost"
    graphite.port: 2003

  # Console Reporter
  - class: "org.apache.storm.metrics2.reporters.ConsoleStormReporter"
    daemons:
        - "worker"
    report.period: 10
    report.period.units: "SECONDS"
    filter:
        class: "org.apache.storm.metrics2.filters.RegexFilter"
        expression: ".*my_component.*emitted.*"

```

Each reporter section begins with a `class` parameter representing the fully-qualified class name of the reporter 
implementation. The `daemons` section determines which daemons the reporter will apply to (in the example Graphite
Reporter is configured to report metrics from all Storm daemons, while the Console reporter will only report worker and
topology metrics).

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

