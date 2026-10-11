# Storm Metrics Prometheus

This module contains two reporters:

* `PrometheusStormReporter`, a [Metrics V2](https://storm.apache.org/releases/current/metrics_v2.html) reporter which serves
  the metrics of each worker over HTTP for Prometheus to scrape.
* `PrometheusPreparableReporter`, which pushes the cluster metrics to a Prometheus Pushgateway.

## Topology metrics

Unlike the generic JMX or Graphite reporters, `PrometheusStormReporter` uses the dimensions Storm keeps for each task as labels,
so a metric reported by several tasks ends up in one family:

```
fetcher_counter_total{topology="crawl",host="worker1",port="6700",component="fetcher",task="3",scope="status_200"} 1234.0
status_count{topology="crawl",host="worker1",port="6700",component="status",task="4",key="FETCHED"} 56789.0
storm_ack_count_total{topology="crawl",host="worker1",port="6700",component="status",task="4",source_component="spout",stream="default"} 42.0
```

### Usage

Add the jar of this module and its dependencies to the topology or to the `lib` directory of your Storm installation, then set
the following in `storm.yaml` or in the topology configuration:

```yaml
topology.metrics.reporters:
  - class: "org.apache.storm.metrics.prometheus.PrometheusStormReporter"
    # each worker listens on its port + offset, e.g. 7700 for 6700
    port.offset: 1000
```

| Key | Default | Description |
|---|---|---|
| `port` | | Port to listen on, takes precedence over `port.offset`. Used as is in local mode, where it defaults to 9400. |
| `port.offset` | 1000 | Added to the worker port to get the port to listen on. |
| `host` | all interfaces | Address to bind to. |
| `jvm.metrics` | true | Also expose the JVM metrics (memory, GC, threads, `process_start_time_seconds`). |
| `filter` | | Metrics filter, as for the other V2 reporters. |

The workers of a local cluster share a single endpoint.

### Naming

* Scoped metrics (`name.scope`) become the family `name` with a `scope` label. Only the first dot separates the scope, so
  `fetcher_counter.robots.fetched` has `scope="robots.fetched"`.
* Gauges whose value is a map become one family named after the whole metric (`status.count` becomes `status_count`) with a
  `key` label per entry.
* Storm's built-in metrics (`__ack-count-spout:default`) become `storm_ack_count` with `source_component` and `stream` labels.
  The `.m1_rate` gauges Storm registers next to its counters are skipped, use `rate()` on the counter instead.
* Counters and meters are exposed as counters, so `rate()` gives their rate per second.
* Histograms and timers are exposed as summaries plus a `_max` and a `_mean` gauge. Timers are in seconds. Dropwizard
  histograms do not track a sum, so the summaries have no `_sum`; use the `_mean` gauge instead.

### Prometheus

Scrape every worker port of every supervisor, e.g. with 4 slots per supervisor:

```yaml
scrape_configs:
  - job_name: storm
    static_configs:
      - targets: ["supervisor1:7700", "supervisor1:7701", "supervisor1:7702", "supervisor1:7703"]
```

A target is only up while a worker runs on the corresponding slot.

## Cluster metrics

`PrometheusPreparableReporter` pushes [Cluster Metrics](https://storm.apache.org/releases/current/ClusterMetrics.html) to
a [Prometheus Pushgateway](https://github.com/prometheus/pushgateway), where it can be scraped by a Prometheus instance.

### Usage

To use, edit your `storm.yaml` config file:

```yaml
storm.daemon.metrics.reporter.plugins:
  - "org.apache.storm.metrics.prometheus.PrometheusPreparableReporter"
storm.daemon.metrics.reporter.interval.secs: 10

# Configuration for the Prometheus Pushgateway
storm.daemon.metrics.reporter.plugin.prometheus.job: "job_name"
storm.daemon.metrics.reporter.plugin.prometheus.endpoint: "localhost:9091"
storm.daemon.metrics.reporter.plugin.prometheus.scheme: "http"
storm.daemon.metrics.reporter.plugin.prometheus.basic_auth_user: ""
storm.daemon.metrics.reporter.plugin.prometheus.basic_auth_password: ""
storm.daemon.metrics.reporter.plugin.prometheus.skip_tls_validation: false
```

In addition, ensure to put this jar as well as the required transient
dependencies for prometheus into `/lib` of your Storm installation.
