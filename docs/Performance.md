---
title: Performance Tuning
layout: documentation
documentation: true
---

Latency, throughput and resource consumption are the three key dimensions involved in performance tuning.
In the following sections we discuss the settings that can used to tune along these dimension and understand the trade-offs.

It is important to understand that these settings can vary depending on the topology, the type of hardware and the number of hosts used by the topology.

## 1. Buffer Size
Spouts and Bolts operate asynchronously using message passing. Message queues used for this purpose are of fixed but configurable size. Buffer size
refers to the size of these queues. Every consumer has its own receive queue. The messages wait in the queue until the consumer is ready to process them.
The queue will typically be almost empty or almost full depending whether the consumer is operating faster or slower than the rate at which producers
are generating messages for it. Storm queues always have a single consumer and potentially multiple producers. There are two buffer size settings
of interest:

- `topology.executor.receive.buffer.size` : This is the size of the message queue employed for each spout and bolt executor.
- `topology.transfer.buffer.size` : This is the size of the outbound message queue used for inter-worker messaging. This queue is referred to as
the *Worker Transfer Queue*.

**Note:** If the specified buffer size is not a power of 2, it is internally rounded up to the next power of 2.

#### Guidance
Very small message queues (size < 1024) are likely to hamper throughput by not providing enough isolation between the consumer and producer. This
can affect the asynchronous nature of the processing as the producers are likely to find the downstream queue to be full.

Very large message queues are also not desirable to deal with slow consumers. Better to employ more consumers (i.e. bolts) on additional CPU cores instead. If queues
are large and often full, the messages will end up waiting longer in these queues at each step of the processing, leading to poor latency being
reported on the Storm UI. Large queues also imply higher memory consumption especially if the queues are typically full.


## 2. Batch Size
Producers can either write a batch of messages to the consumer's queue or write each message individually. This batch size can be configured.
Inserting messages in batches to downstream queues helps reduce the number of synchronization operations required for the inserts. Consequently this helps achieve higher throughput. However,
sometimes it may take a little time for the buffer to fill up, before it is flushed into the downstream queue. This implies that the buffered messages
will take longer to become visible to the downstream consumer who is waiting to process them. This can increase the average end-to-end latency for
these messages. The latency can get very bad if the batch sizes are large and the topology is not experiencing high traffic.

- `topology.producer.batch.size` : The batch size for writes into the receive queue of any spout/bolt is controlled via this setting. This setting
impacts the communication within a worker process. Each upstream producer maintains a separate batch to a component's receive queue. So if two spout
instances are writing to the same downstream bolt instance, each of the spout instances will have maintain a separate batch.

-  `topology.transfer.batch.size` : Messages that are destined to a spout/bolt running on a different worker process, are sent to a queue called
the **Worker Transfer Queue**. The Worker Transfer Thread is responsible for draining the messages in this queue and send them to the appropriate
worker process over the network. This setting controls the batch size for writes into the Worker Transfer Queue.  This impacts the communication
between worker processes.

#### Guidance

**For Low latency:** Set batch size to 1. This basically disables batching. This is likely to reduce peak sustainable throughput under heavy traffic, but
not likely to impact throughput much under low/medium traffic situations.

**For High throughput:** Set batch size > 1. Try values like 10, 100, 1000 or even higher and see what yields the best throughput for the topology.
Beyond a certain point the throughput is likely to get worse.

**Varying throughput:** Topologies often experience fluctuating amounts of incoming traffic over the day. Other topos may experience higher traffic in some
paths and lower throughput in other paths simultaneously. If latency is not a concern, a small bach size (e.g. 10) and in conjunction with the right flush
frequency may provide a reasonable compromise for such scenarios. For meeting stricter latency SLAs, consider setting it to 1.


## 3. Flush Tuple Frequency
In low/medium traffic situations or when batch size is too large, the batches may take too long to fill up and consequently the messages could take unacceptably
long time to become visible to downstream components. In such case, periodic flushing of batches is necessary to keep the messages moving and avoid compromising
latencies when batching is enabled.

When batching has been enabled, special messages called *flush tuples* are inserted periodically into the receive queues of all spout and bolt instances.
This causes each spout/bolt instance to flush all its outstanding batches to their respective downstream components.

`topology.flush.tuple.freq.millis` : This setting controls how often the flush tuples are generated. Flush tuples are not generated if this configuration is
set to 0 or if (`topology.producer.batch.size`=1 and `topology.transfer.batch.size`=1).


#### Guidance
Flushing interval can be used as tool to retain the higher throughput benefits of batching and avoid batched messages getting stuck for too long waiting for their.
batch to fill. Preferably this value should be larger than the average execute latencies of the bolts in the topology. Trying to flush the queues more frequently than
the amount of time it takes to produce the messages may hurt performance. Understanding the average execute latencies of each bolt will help determine the average
number of messages in the queues between two flushes.

**For Low latency:** A smaller value helps achieve tighter latency SLAs.

**For High throughput:**  When trying to maximize throughput under high traffic situations, the batches are likely to get filled and flushed automatically.
To optimize for such cases, this value can be set to a higher number.

**Varying throughput:** If latency is not a concern, a larger value will optimize for high traffic situations. For meeting tighter SLAs set this to lower
values.


## 4. Wait Strategy
Wait strategies are used to conserve CPU usage by trading off some latency and throughput. They are applied for the following situations:

4.1 **Spout Wait:**  In low/no traffic situations, Spout's nextTuple() may not produce any new emits. To prevent invoking the Spout's nextTuple too often,
this wait strategy is used between nextTuple() calls, allowing the spout's executor thread to idle and conserve CPU. Spout wait strategy is also used
when the `topology.max.spout.pending` limit has been reached when ACKers are enabled. Select a strategy using `topology.spout.wait.strategy`. Configure the
chosen wait strategy using one of the `topology.spout.wait.*` settings.

4.2 **Bolt Wait:** : When a bolt polls it's receive queue for new messages to process, it is possible that the queue is empty. This typically happens
in case of low/no traffic situations or when the upstream spout/bolt is inherently slower. This wait strategy is used in such cases. It avoids high CPU usage
due to the bolt continuously checking on a typically empty queue. Select a strategy using `topology.bolt.wait.strategy`. The chosen strategy can be further configured
using the `topology.bolt.wait.*` settings.

4.3 **Backpressure Wait** : Select a strategy using `topology.backpressure.wait.strategy`. When a spout/bolt tries to write to a downstream component's receive queue,
there is a possibility that the queue is full. In such cases the write needs to be retried. This wait strategy is used to induce some idling in-between re-attempts for
conserving CPU. The chosen strategy can be further configured using the `topology.backpressure.wait.*` settings.


#### Built-in wait strategies:
These wait strategies are available for use with all of the above mentioned wait situations.

- **ProgressiveWaitStrategy** : This strategy can be used for Bolt Wait or Backpressure Wait situations. Set the strategy to 'org.apache.storm.policy.WaitStrategyProgressive' to
select this wait strategy. This is a dynamic wait strategy that enters into progressively deeper states of CPU conservation if the Backpressure Wait or Bolt Wait situations persist.
It has 3 levels of idling and allows configuring how long to stay at each level :

  1. Level1 / No Waiting - The first few times it will return immediately. This does not conserve any CPU. The number of times it remains in this state is configured using
  `topology.spout.wait.progressive.level1.count` or `topology.bolt.wait.progressive.level1.count` or `topology.backpressure.wait.progressive.level1.count` depending which
  situation it is being used.

  2. Level 2 / Park Nanos - In this state it disables the current thread for thread scheduling purposes, for 1 nano second using LockSupport.parkNanos(). This puts the CPU in a minimal
  conservation state. It remains in this state for `topology.spout.wait.progressive.level2.count` or `topology.bolt.wait.progressive.level2.count` or
  `topology.backpressure.wait.progressive.level2.count` iterations.

  3. Level 3 / Thread.sleep() - In this level it calls Thread.sleep() with the value specified in `topology.spout.wait.progressive.level3.sleep.millis` or
  `topology.bolt.wait.progressive.level3.sleep.millis` or `topology.backpressure.wait.progressive.level3.sleep.millis`. This is the most CPU conserving level and it remains in
  this level for the remaining iterations.


- **ParkWaitStrategy** : This strategy can be used for Bolt Wait or Backpressure Wait situations. Set the strategy to `org.apache.storm.policy.WaitStrategyPark` to use this.
This strategy disables the current thread for thread scheduling purposes by calling LockSupport.parkNanos(). The amount of park time is configured using either
`topology.bolt.wait.park.microsec` or `topology.backpressure.wait.park.microsec` based on the wait situation it is used. Setting the park time to 0, effectively disables
invocation of LockSupport.parkNanos and this mode can be used to achieve busy polling (which at the cost of high CPU utilization even when idle, may improve latency and/or throughput).


## 5. Max.spout.pending
The setting `topology.max.spout.pending` limits the number of un-ACKed tuples at the spout level. Once a spout reaches this limit, the spout's nextTuple()
method will not be called until some ACKs are received for the outstanding emits. This setting does not have any affect if ACKing is disabled. It
is a spout throttling mechanism which can impact throughput and latency. Setting it to null disables it for storm-core topologies. Impact on throughput
is dependent on the topology and its concurrency (workers/executors), so experimentation is necessary to determine optimal setting. Latency and memory consumption
is expected to typically increase with higher and higher values for this.


## 6. Load Aware messaging
When load aware messaging is enabled (default), shuffle grouping takes additional factors into consideration for message routing.
Impact of this on performance is dependent on the topology and its deployment footprint (i.e. distribution over process and machines).
Consequently it is useful to assess the impact of setting `topology.disable.loadaware.messaging` to `true` or `false` for your
specific case.


## 7. Sampling Rate
Sampling rate is used to control how often certain metrics are computed on the Spout and Bolt executors. This is configured using `topology.stats.sample.rate`
Setting it to 1 means, the stats are computed for every emitted message. As an example, to sample once every 1000 messages it can be set to  0.001. It may be
possible to improve throughput and latency by reducing the sampling rate.


## 8. Budgeting CPU cores for Executors
There are three main types of executors (i.e threads) to take into account when budgeting CPU cores for them. Spout Executors, Bolt Executors, Worker Transfer (handles outbound
messages) and NettyWorker (handles inbound messages).
The first two are used to run spout, bolt and acker instances. The Worker Transfer thread is used to serialize and send messages to other workers (in multi-worker mode).

Executors that are expected to remain busy, either because they are handling a lot of messages, or because their processing is inherently CPU intensive, should be allocated
1 physical core each. Allocating logical cores (instead of physical) or less than 1 physical core for CPU intensive executors increases CPU contention and performance can suffer.
Executors that are not expected to be busy can be allocated a smaller fraction of the physical core (or even logical cores). It maybe not be economical to allocate a full physical
core for executors that are not likely to saturate the CPU.

The *system bolt* generally processes very few messages per second, and so requires very little cpu (typically less than 10% of a physical core).


## 9. Garbage Collection
Choice of GC is an important concern for topologies that are latency or throughput sensitive. It is recommended to try the both the CMS and G1 collectors. Performance characteristics
of the collectors can change between single and multiworker modes and is dependent on hardware characteristics such as number of CPUs and memory localities. Number of GC threads can
also affect performance. Sometimes fewer GC threads can yield better performance. It is advisable to select a collector and tune it by mimicking anticipated peak data rates on hardware
similar to what is used in production.


## 10. Scaling out with Single Worker mode
Communication between executors within a worker process is very fast as there is neither a need to serialize and deserialize messages nor does it involve communicating over the network
stack. In multiworker mode, messages often cross worker process boundaries. For performance sensitive cases, if it is possible to configure a topology to run as many single-worker
instances (for ex. one worker per input partition) rather than one multiworker instance, it may yield significantly better throughput and latency on the same hardware.
The downside to this approach is that it adds the overhead of monitoring and managing many instances rather than one multiworker instance.
