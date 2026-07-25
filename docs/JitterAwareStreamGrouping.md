---
title: JitterAwareStreamGrouping
layout: documentation
documentation: true
---

`JitterAwareStreamGrouping` is a stream grouping that selects downstream tasks based on observed per-task execution jitter. It steers traffic away from tasks experiencing backpressure or execution variance, producing a more even distribution of work over time.

## Background: the jitter metric

Storm 3.0.0 introduces a per-task jitter metric based on the EWMA algorithm in [RFC 1889 Appendix A](https://www.rfc-editor.org/rfc/rfc1889#appendix-A). The metric tracks inter-arrival time variance in each task's processing loop. A lightweight control loop propagates these measurements to upstream components at regular intervals without touching the data path.

A task with a steady processing rate accumulates near-zero jitter. A task that stalls, GC-pauses, or falls behind its input queue accumulates a rising jitter estimate, which the grouping uses as a signal to route tuples elsewhere.

## Usage

Use `customGrouping` with a `JitterAwareStreamGrouping` instance in place of `shuffleGrouping`:

```java
import org.apache.storm.grouping.JitterAwareStreamGrouping;

TopologyBuilder builder = new TopologyBuilder();
builder.setSpout("spout", new MySpout(), 1);
builder.setBolt("bolt", new MyBolt(), 4)
       .customGrouping("spout", new JitterAwareStreamGrouping());
```

`JitterAwareStreamGrouping` is serializable and safe across workers. Before any jitter data arrives (e.g., at topology startup) it falls back to random task selection.

## Benchmarking

The `storm-perf` module includes a dedicated benchmark topology:

```
storm jar storm-perf/target/storm-perf-*.jar \
  org.apache.storm.perf.JitterAwareGroupingTopology
```

Run it alongside the equivalent shuffle-grouping topology to measure the effect in your cluster. The [local development cluster](Local-dev-cluster.html) provides Grafana dashboards with per-task jitter visibility.

## When to use it

- Bolts with variable execution time (external I/O, cache misses, non-uniform key distributions) where a subset of tasks may consistently lag.
- Topologies where a single hot task creates cascading backpressure upstream.
- As a drop-in replacement for `shuffleGrouping` on any stream where tasks are interchangeable and execution uniformity matters.

## When not to use it

- **Data-locality requirements**: use `fieldsGrouping` where tuples must reach a specific task.
- **Low parallelism** (2 tasks): the grouping has fewer candidates to choose between and the benefit diminishes.
- **Order-sensitive streams**: like any non-deterministic grouping, this does not preserve tuple order across tasks.
- **Intra-worker traffic**: for streams where both components run in the same worker, `localOrShuffleGrouping` avoids serialization entirely and is preferable.

The jitter control loop runs on all topologies regardless of whether `JitterAwareStreamGrouping` is in use. The grouping simply reads measurements the loop already produces; enabling it carries no additional overhead on the data path.
