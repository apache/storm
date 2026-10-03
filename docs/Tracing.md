---
title: Tracing
layout: documentation
documentation: true
---
Storm can carry an [OpenTelemetry](https://opentelemetry.io/) trace context with every tuple. A trace then follows a
[tuple tree](Guaranteeing-message-processing.html) across bolts and workers: the spout emit and each bolt `execute()`
it caused. For reliable spouts, the trace also shows how the tree ended. Spans that the application creates during
`execute()`, and spans of instrumented clients called there, join the same trace.

## Enabling tracing

Tracing is off by default; set `topology.tracing.enabled` to `true` to turn it on. Storm calls only the OpenTelemetry
API; the OpenTelemetry SDK registered as the global instance records the spans. The
[OpenTelemetry Java agent](https://opentelemetry.io/docs/zero-code/java/agent/) attached to the workers is one way to
register it:

```yaml
topology.tracing.enabled: true
topology.worker.childopts: >-
  -javaagent:/opt/otel/opentelemetry-javaagent.jar
  -Dotel.service.name=my-topology
  -Dotel.exporter.otlp.endpoint=http://collector:4318
  -Dotel.traces.sampler=parentbased_traceidratio
  -Dotel.traces.sampler.arg=0.01
```

The SDK exports the spans to the backend it is configured for, such as an OpenTelemetry Collector or any service that
accepts OTLP. An SDK that the application registers as the global instance works as well; Storm starts recording once
it is registered. A worker without an SDK records nothing.

## What is recorded

| Span | Parent | Recorded when |
|------|--------|---------------|
| `<spout> emit` | none, it starts a trace | a spout emits a tuple, except checkpoint tuples of stateful bolts |
| `<bolt> execute` | the context of the input tuple | `execute()` runs for a tuple that carries a context; the span is current on the executor thread during the call |
| `<bolt> emit` | none, linked to each anchor's span | a bolt emits a tuple whose anchors carry different spans |
| `<spout> ack`, `<spout> fail`, `<spout> timeout` | the `<spout> emit` span | the tuple tree is acked, fails or times out; only for emits with a message id when the topology has ackers; fail and timeout have status ERROR |
| `<bolt> fail` | the execute span of the tuple | a bolt calls `fail()`; status ERROR |

A recording execute span has these attributes: `storm.topology.name`, `storm.topology.id`, `storm.component.id`,
`storm.task.id`, `storm.source.component.id`, `storm.source.stream.id`, `storm.worker.port` and, when the host name
resolves, `storm.worker.host`.

## How the context moves

An emitted tuple takes its context from its [anchors](Guaranteeing-message-processing.html), on whatever thread the
emit runs. When the traced anchors carry one span, the tuple carries that span as parent. When they carry different
spans, the tuple carries a new root span linked to each of them, so a tree with joins spans several linked traces. An
unanchored emit carries no context, and the work downstream of it is not traced. Tick and other system tuples carry no
context.

Between workers, the context travels in the serialized tuple, after the values. Workers of one topology run the same
Storm version; a worker of an earlier version would read such a tuple and ignore the extra bytes.

## Sampling

The SDK's sampler decides whether the trace that a spout emit starts is sampled. Storm passes every context on, sampled
or not. With a parent-based sampler (the default), every span of a tuple tree therefore follows that decision. The root
sampler alone decides whether the new root of an emit with several anchors is sampled, because the built-in samplers
ignore links.

## Continuing a trace on other threads

`TupleUtils.traceContext(tuple)` returns the context to run work for a tuple under, or an empty context
(`Context.root()`) when the tuple carries none. Make it current where work for the tuple runs outside `execute()`:

```java
import io.opentelemetry.context.Context;

Context context = TupleUtils.traceContext(input);
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
- On a worker without an SDK, a tuple with one traced anchor passes its context on, but an emit with several traced
  anchors carries none.
- The trace shows how a tuple tree ended, not which bolt held a tuple that timed out.
- An exception thrown by `execute()` is not recorded on the span.
- Workers put Storm's libraries before the topology jar on the classpath, so a topology runs against the
  `opentelemetry-api` version Storm ships, not one bundled in its jar. Build the topology against that version and
  declare the dependency as `provided`.
