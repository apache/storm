---
title: Message Passing Implementation
layout: documentation
documentation: true
---

(Note: this walkthrough has been updated for v2.8.7. The message passing infrastructure was rewritten in 0.8.0 (originally Disruptor-based) and later moved to JCQueue for better performance.)

This page walks through how emitting and transferring tuples works in Storm.

- Worker is responsible for message transfer
   - Connection management is handled by the `WorkerState` class which manages connections to other workers and maintains a mapping from task -> worker. Connection refresh is triggered every "task.refresh.poll.secs" or whenever assignment in ZK changes. [code](https://github.com/apache/storm/blob/v2.8.7/storm-client/src/jvm/org/apache/storm/daemon/worker/WorkerState.java)
   - Provides a "transfer function" that is used by tasks to send tuples to other tasks. The transfer function takes in a task id and a tuple, and it serializes the tuple and puts it onto a "transfer queue". There is a single transfer queue for each worker. [code](https://github.com/apache/storm/blob/v2.8.7/storm-client/src/jvm/org/apache/storm/daemon/worker/WorkerTransfer.java)
   - Tuple serialization uses `KryoTupleSerializer`. [code](https://github.com/apache/storm/blob/v2.8.7/storm-client/src/jvm/org/apache/storm/serialization/KryoTupleSerializer.java)
   - The worker drains the transfer queue and sends the messages to other workers [code](https://github.com/apache/storm/blob/v2.8.7/storm-client/src/jvm/org/apache/storm/daemon/worker/Worker.java)
   - Message sending happens through this interface: [code](https://github.com/apache/storm/blob/v2.8.7/storm-client/src/jvm/org/apache/storm/messaging/IConnection.java)
   - The implementation for distributed mode uses Netty [code](https://github.com/apache/storm/blob/v2.8.7/storm-client/src/jvm/org/apache/storm/messaging/netty/Context.java)
   - The implementation for local mode uses in memory Java queues (so that it's easy to use Storm locally without needing external messaging dependencies) [code](https://github.com/apache/storm/blob/v2.8.7/storm-client/src/jvm/org/apache/storm/messaging/local/Context.java#L38)
- Receiving messages in tasks works differently in local mode and distributed mode
   - In local mode, the tuple is sent directly to an in-memory queue for the receiving task [code](https://github.com/apache/storm/blob/v2.8.7/storm-client/src/jvm/org/apache/storm/messaging/local/Context.java#L127)
   - In distributed mode, each worker listens on a single TCP port for incoming messages and then routes those messages in-memory to tasks. The TCP port receives [task id, message] and then routes it to the actual task. [code](https://github.com/apache/storm/blob/v2.8.7/storm-client/src/jvm/org/apache/storm/messaging/netty/StormServerHandler.java)
      - The message routing implementation is here: [code](https://github.com/apache/storm/blob/v2.8.7/storm-client/src/jvm/org/apache/storm/messaging/netty/Server.java)
      - Executors consume from their receive queue (a `JCQueue`) [code](https://github.com/apache/storm/blob/v2.8.7/storm-client/src/jvm/org/apache/storm/executor/Executor.java)
        - Bolts listen here: [code](https://github.com/apache/storm/blob/v2.8.7/storm-client/src/jvm/org/apache/storm/executor/bolt/BoltExecutor.java#L154)
        - Spouts listen here: [code](https://github.com/apache/storm/blob/v2.8.7/storm-client/src/jvm/org/apache/storm/executor/spout/SpoutExecutor.java#L165)
- Tasks are responsible for message routing. A tuple is emitted either to a direct stream (where the task id is specified) or a regular stream. In direct streams, the message is only sent if that bolt subscribes to that direct stream. In regular streams, the stream grouping functions are used to determine the task ids to send the tuple to.
  - Tasks have a routing map from {stream id} -> {component id} -> {stream grouping function}; the grouping functions determine the task ids to send the tuples to for either regular stream emit or direct stream emit. [code](https://github.com/apache/storm/blob/v2.8.7/storm-client/src/jvm/org/apache/storm/daemon/Task.java)
  - After getting the output task ids, bolts and spouts use the transfer function provided by the worker to actually transfer the tuples
      - Bolt transfer code here: [code](https://github.com/apache/storm/blob/v2.8.7/storm-client/src/jvm/org/apache/storm/executor/bolt/BoltExecutor.java#L127)
      - Spout transfer code here: [code](https://github.com/apache/storm/blob/v2.8.7/storm-client/src/jvm/org/apache/storm/executor/spout/SpoutExecutor.java#L133)

