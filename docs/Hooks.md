---
title: Hooks
layout: documentation
documentation: true
---
## Task hooks
Storm provides hooks with which you can insert custom code to run on any number of events within Storm. You create a hook by extending the [BaseTaskHook](javadocs/org/apache/storm/hooks/BaseTaskHook.html) class and overriding the appropriate method for the event you want to catch. There are two ways to register your hook:

1. In the open method of your spout or prepare method of your bolt using the [TopologyContext](javadocs/org/apache/storm/task/TopologyContext.html#addTaskHook) method.
2. Through the Storm configuration using the ["topology.auto.task.hooks"](javadocs/org/apache/storm/Config.html#TOPOLOGY_AUTO_TASK_HOOKS) config. These hooks are automatically registered in every spout or bolt, and are useful for doing things like integrating with a custom monitoring system.

## Worker hooks
Storm also provides worker-level hooks that are called during worker startup, before any bolts or spouts are prepared/opened. You can create such a hook by extending [BaseWorkerHook](javadocs/org/apache/storm/hooks/BaseWorkerHook) (an implementation of [IWorkerHook](javadocs/org/apache/storm/hooks/IWorkerHook.html)) and overriding the methods you want to implement. You can register your hook via `TopologyBuilder.addWorkerHook`.  
The `IWorkerHook#start(Map, WorkerUserContext)` lifecycle method exposes [WorkerUserContext](javadocs/org/apache/storm/hooks/IWorkerHook.html) which provides a way to set application-level common resources via `setResource(String, Object)` method. This resource can then be retrieved by tasks, both spouts (via `open(Map, TopologyContext, SpoutOutputCollector`) and bolts (via `prepare(Map, TopologyContext, OutputCollector`), by calling `TopologyContext#getResource(String)`.

## Shared State amongst components and hooks
Storm provides ways to share resources across different components via the following ways:
1. taskData: this pertains to the task level data and can be written and read by task and task hooks in their corresponding lifecycle methods (`open` for spout and `prepare` for bolt and task hook).
   1. write access: `TopologyContext#setTaskData(String, Object)`
   2. read access: `TopologyContext#getTask(String)`
2. executorData: this pertains to executor level data and is shared across tasks and task hooks which are managed by the concerned executor. Similar to above it is accessible to spouts via `open` and to bolts and task hooks via `prepare` lifecycle method.
   1. write access: `TopologyContext#setExecutorData`
   2. read access: `TopologyContext#getExecutorData(String)`
3. userResources: this pertains to worker level data and is shared across executors, tasks, worker hooks and task hooks which are managed by the concerned worker. Unlike others it can only be written by worker hooks.  
   1. write access: `WorkerUserContext#setResource(String, Object)`
   2. read access: `WorkerTopologyContext#getResouce(String)` or `TopologyContext#getResource(String)`
