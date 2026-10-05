---
title: Tracing
layout: documentation
documentation: true
---
Storm can carry a trace context with every tuple. A trace then follows a
[tuple tree](Guaranteeing-message-processing.html) across bolts and workers: the spout emit and each bolt `execute()`
it caused. For reliable spouts, the trace also shows how the tree ended.

The tracer is pluggable: the class named in `topology.tracing.tracer` decides what a context is, how it is encoded
for other workers and what is recorded. Storm provides one tracer, for [OpenTelemetry](https://opentelemetry.io/), in
the `storm-opentelemetry` module. With it, spans that the application creates during `execute()`, and spans of
instrumented clients called there, join the tuple's trace. The rest of this page describes that tracer.

## Enabling tracing

Tracing is off by default. To turn it on:

1. Put `storm-opentelemetry` and its dependencies (`opentelemetry-api`, `opentelemetry-context` and
   `opentelemetry-common`) on the worker classpath: either in the `lib-worker` directory of each Storm installation, or
   in the topology jar:

   ```xml
   <dependency>
       <groupId>org.apache.storm</groupId>
       <artifactId>storm-opentelemetry</artifactId>
       <version>${storm.version}</version>
   </dependency>
   ```

2. Set `topology.tracing.tracer` to `org.apache.storm.opentelemetry.OpenTelemetryTupleTracer`.
3. Register an OpenTelemetry SDK as the global instance on the workers. The
   [OpenTelemetry Java agent](https://opentelemetry.io/docs/zero-code/java/agent/), version 2.23.0 or later, is one
   way to do it:

   ```yaml
   topology.tracing.tracer: org.apache.storm.opentelemetry.OpenTelemetryTupleTracer
   topology.worker.childopts: >-
     -javaagent:/opt/otel/opentelemetry-javaagent.jar
     -Dotel.service.name=my-topology
     -Dotel.exporter.otlp.endpoint=http://collector:4318
     -Dotel.traces.sampler=parentbased_traceidratio
     -Dotel.traces.sampler.arg=0.01
   ```

The tracer only calls the OpenTelemetry API; the SDK samples the spans and exports them to the backend it is
configured for, such as an OpenTelemetry Collector or any service that accepts OTLP. An SDK that the application
registers as the global instance works as well; the tracer starts recording once it is registered. Until then, the
tracer starts no traces and records no spans, and the worker logs one warning.

Workers put `lib-worker` before the topology jar on one classpath, so when both carry `opentelemetry-api`, the copy in
`lib-worker` is the one loaded. The agent bridges the API from either place, but not a copy relocated to another
package: when shading the topology jar, do not relocate `io.opentelemetry`.

## What is recorded

| Span | Parent | Recorded when |
|------|--------|---------------|
| `<spout> emit` | none, it starts a trace | a spout emits a tuple, except checkpoint tuples of stateful bolts |
| `<bolt> execute` | the context of the input tuple | `execute()` runs for a tuple that carries a context; the span is current on the executor thread during the call |
| `<bolt> emit` | the first traced anchor's span when all traced anchors belong to one trace, linked to the others' spans; otherwise none, linked to each anchor's span | a bolt emits a tuple whose anchors carry different spans |
| `<spout> ack`, `<spout> fail`, `<spout> timeout` | the `<spout> emit` span | the tuple tree is acked, fails or times out; only for emits with a message id when the topology has ackers; fail and timeout have status ERROR |
| `<bolt> fail` | the execute span of the tuple | a bolt calls `fail()`; status ERROR |

A recording execute span has these attributes: `storm.topology.name`, `storm.topology.id`, `storm.component.id`,
`storm.task.id`, `storm.source.component.id`, `storm.source.stream.id`, `storm.worker.port` and, when the host name
resolves, `storm.worker.host`.

The spout's ack, fail and timeout spans start at the emit and end when the spout executor handles the outcome, before
the spout's `ack()` or `fail()` runs; their `storm.tuple.latency_ms` attribute holds that duration in milliseconds. The
emit spans and the bolt fail span are started and ended at once.

## How the context moves

An emitted tuple takes its context from its [anchors](Guaranteeing-message-processing.html), on whatever thread the
emit runs. When the traced anchors carry one span, the tuple carries that span as parent. When they carry different
spans of one trace, the tuple carries a new span that is a child of the first traced anchor's span and linked to the
others, so the trace continues. When the anchors belong to different traces, the tuple carries a new root span linked
to each of them, so the spans of such a tree fall into several linked traces. An unanchored emit carries no context,
and the work downstream of it is not traced. Tick and other system tuples carry no context.

Between workers, the context travels in the serialized tuple, after the values. Workers of one topology run the same
Storm version; a worker of an earlier version would read such a tuple and ignore the extra bytes. Tuples that stateful
bolts save in their state do not keep their context.

## Sampling

The SDK's sampler decides whether the trace that a spout emit starts is sampled. The tracer passes every context on,
sampled or not. With a parent-based sampler (the default), every span of a tuple tree therefore follows that decision,
including the emit span of an emit whose anchors belong to one trace. The root sampler alone decides whether the new
root of an emit with anchors from different traces is sampled, because the built-in samplers ignore links.

## Continuing a trace on other threads

`StormTracing.context(tuple)`, in `org.apache.storm.opentelemetry`, returns the context to run work for a tuple under,
or an empty context (`Context.root()`) when the tuple carries none. Make it current where work for the tuple runs
outside `execute()`:

```java
import io.opentelemetry.context.Context;
import org.apache.storm.opentelemetry.StormTracing;

Context context = StormTracing.context(input);
pool.submit(context.wrap(() -> {
    Object page = fetch(input); // an instrumented HTTP client called here joins the input's trace
    collector.emit(input, new Values(page));
    collector.ack(input);
}));
```

Emits themselves do not need this: an anchored emit takes its parent from the anchor on any thread.

## Costs and limits

- The Java agent carries the current context into tasks submitted to `java.util.concurrent` executors, so while an
  execute span is current it wraps each task the bolt submits. When no code on those threads needs the context (spans,
  instrumented clients, correlated logs, baggage), or that code makes the context current itself,
  `-Dotel.instrumentation.executors.enabled=false` turns this off for the whole JVM.
- An execute span covers the `execute()` call only. In a bolt that processes the tuple on another thread, the span can
  end before that processing does.
- At high tuple rates with a high sampling ratio, the SDK's batch span processor drops spans once its queue is full and
  logs how many it dropped. Lower the sampling ratio, or tune the processor with the `otel.bsp.*` settings.
- Execute span attributes are set after the span starts, so a sampler cannot use them in its decision.
- A span keeps up to 128 links by default, so an emit whose anchors carry more different spans keeps only part of them.
- On a worker without an SDK, a tuple with one traced anchor passes its context on, but an emit whose anchors carry
  different spans carries none.
- Each spout emit starts a new trace, so a spout cannot continue a trace that arrives with its messages, for example
  in Kafka or JMS headers. Baggage does not travel with tuples.
- In Trident topologies, the master batch coordinator's emits on the `$batch`, `$commit` and `$success` streams are
  spout emits, so each starts its own trace.
- The trace shows how a tuple tree ended, not which bolt held a tuple that timed out.
- An exception thrown by `execute()` is not recorded on the span.

## Writing another tracer

`topology.tracing.tracer` accepts any implementation of `org.apache.storm.tracing.TupleTracer` with a zero-arg
constructor. Each worker creates one instance and shares it between its executors and the threads that serialize and
deserialize tuples. The interface's Javadoc says when Storm calls each method and what it expects back.
