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
Storm also provides worker-level hooks that are called during worker startup, before any bolts or spouts are prepared/opened. You can create such a hook by extending [BaseWorkerHook](javadocs/org/apache/storm/hooks/BaseWorkerHook) and overriding the methods you want to implement. You can register your hook via `TopologyBuilder.addWorkerHook`.
