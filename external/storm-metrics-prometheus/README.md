# Storm Metrics Prometheus

This module contains a reporter to push [Cluster Metrics](https://storm.apache.org/releases/current/ClusterMetrics.html) to
a [Prometheus Pushgateway](https://github.com/prometheus/pushgateway), where it can be scraped by a Prometheus instance.

## Usage

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
