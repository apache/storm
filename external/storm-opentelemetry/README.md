# Storm OpenTelemetry

This module traces Storm [tuple trees](https://storm.apache.org/releases/current/Guaranteeing-message-processing.html)
with [OpenTelemetry](https://opentelemetry.io/). Anchored tuples carry the trace context of their tree, so one trace
follows a tuple tree across bolts, threads and workers, and spans created during `execute()` join it.

## Usage

Add the module to the topology jar, or put it with its dependencies in the `lib-worker` directory of each Storm
installation:

```xml
<dependency>
    <groupId>org.apache.storm</groupId>
    <artifactId>storm-opentelemetry</artifactId>
    <version>${storm.version}</version>
</dependency>
```

Then set `topology.tracing.tracer` to `org.apache.storm.opentelemetry.OpenTelemetryTupleTracer` and register an
OpenTelemetry SDK on the workers, for example with the OpenTelemetry Java agent.

The [Tracing](https://storm.apache.org/releases/current/Tracing.html) page covers the setup, the spans and their
attributes, sampling, costs and limits.
