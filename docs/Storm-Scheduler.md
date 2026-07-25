---
title: Scheduler
layout: documentation
documentation: true
---

Storm now has 4 kinds of built-in schedulers: [DefaultScheduler]({{page.git-blob-base}}/storm-server/src/main/java/org/apache/storm/scheduler/DefaultScheduler.java), [IsolationScheduler]({{page.git-blob-base}}/storm-server/src/main/java/org/apache/storm/scheduler/IsolationScheduler.java), [MultitenantScheduler]({{page.git-blob-base}}/storm-server/src/main/java/org/apache/storm/scheduler/multitenant/MultitenantScheduler.java), [ResourceAwareScheduler](Resource_Aware_Scheduler_overview.html). 

## Pluggable scheduler
You can implement your own scheduler to replace the default scheduler to assign executors to workers. You configure the class to use the "storm.scheduler" config in your storm.yaml, and your scheduler must implement the [IScheduler]({{page.git-blob-base}}/storm-client/src/jvm/org/apache/storm/scheduler/IScheduler.java) interface.

## Isolation Scheduler
The isolation scheduler makes it easy and safe to share a cluster among many topologies. The isolation scheduler lets you specify which topologies should be "isolated", meaning that they run on a dedicated set of machines within the cluster where no other topologies will be running. These isolated topologies are given priority on the cluster, so resources will be allocated to isolated topologies if there's competition with non-isolated topologies, and resources will be taken away from non-isolated topologies if necessary to get resources for an isolated topology. Once all isolated topologies are allocated, the remaining machines on the cluster are shared among all non-isolated topologies.

You can configure the isolation scheduler in the Nimbus configuration by setting "storm.scheduler" to "org.apache.storm.scheduler.IsolationScheduler". Then, use the "isolation.scheduler.machines" config to specify how many machines each topology should get. This configuration is a map from topology name to the number of isolated machines allocated to this topology. For example:

```
isolation.scheduler.machines: 
    "my-topology": 8
    "tiny-topology": 1
    "some-other-topology": 3
```

Any topologies submitted to the cluster not listed there will not be isolated. Note that there is no way for a user of Storm to affect their isolation settings – this is only allowed by the administrator of the cluster (this is very much intentional).

The isolation scheduler solves the multi-tenancy problem – avoiding resource contention between topologies – by providing full isolation between topologies. The intention is that "productionized" topologies should be listed in the isolation config, and test or in-development topologies should not. The remaining machines on the cluster serve the dual role of failover for isolated topologies and for running the non-isolated topologies.

When choosing which hosts to assign an isolated topology to, the scheduler sorts eligible hosts by three criteria in order:

1. **Assignable slots (descending)** — prefer hosts with greater total slot capacity.
2. **Free slots (descending)** — among hosts of equal capacity, prefer the one with more currently free slots, minimising the number of worker evictions needed.
3. **Hostname (ascending)** — alphabetical tiebreaker for deterministic assignments.

The secondary and tertiary sorts were added in Storm 3.0.0. Previously only the primary sort applied, which could cause unnecessary evictions when two hosts had the same total capacity but different numbers of occupied slots.

## EvenScheduler

`EvenScheduler` (used directly or via `DefaultScheduler`) distributes workers as evenly as possible across available supervisors. It is the default when no custom scheduler is configured and `IsolationScheduler` is not in use.

### Idle supervisor rebalance

*Available since Storm 3.0.0. Disabled by default.*

When a supervisor returns from maintenance, its slots are normally left idle: each topology's desired worker count is already satisfied on the surviving supervisors, so `needsScheduling` reports nothing to do. Workers only migrate back if an operator manually rebalances or restarts every affected topology.

Setting `nimbus.even.rebalance.idle.supervisor.enabled: true` adds an opt-in pass that relocates workers onto the returned supervisor automatically in a single scheduling round, distributed across topologies in round-robin order.

| Key | Default | Description |
|-----|---------|-------------|
| `nimbus.even.rebalance.idle.supervisor.enabled` | `false` | Master switch. When false the pass never runs. |
| `nimbus.even.rebalance.max.free.per.topology` | `0` | Per-topology cap on workers relocated per round. `0` means no additional cap beyond the even-distribution budget. |
| `nimbus.even.rebalance.idle.supervisor.min.stable.rounds` | `3` | Number of consecutive monitoring rounds a supervisor must be seen as stable before it is eligible as a relocation target. Set to `0` to disable the flap guard. |

A relocation is a worker JVM restart, involving brief tuple replay, JIT re-warmup, and possible windowed or stateful bolt state restore. Keep this off for topologies with stateful or windowed bolts that have a non-trivial restore cost, latency-sensitive topologies sensitive to JIT re-warmup, and clusters where supervisors are prone to flapping.

**Scope:** `EvenScheduler` and `DefaultScheduler` only. `ResourceAwareScheduler` is unaffected.

