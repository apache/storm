---
title: Locality Awareness In LoadAwareShuffleGrouping
layout: documentation
documentation: true
---

# Locality Awareness In LoadAwareShuffleGrouping

### Motivation

Apache Storm has introduced locality awareness to LoadAwareShuffleGrouping based on Bang-Bang control theory. 
It aims to keep traffic to closer downstream executors to avoid network latency when those executors are not under heavy load. 
It can also avoid serialization/deserialization overhead if the traffic happens in the same worker.  

### How it works

An executor (say `E`) which has LoadAwareShuffleGrouping to downstream executors views them in four `scopes` based on their locations relative to the executor `E` itself. 
The four scopes are:

* `WORKER_LOCAL`: every downstream executor located on the same worker as this executor `E`
* `HOST_LOCAL`: every downstream executor located on the same host as this executor `E`
* `RACK_LOCAL`: every downstream executor located on the same rack as this executor `E`
* `EVERYTHING`: every downstream executor of the executor `E`

It starts with sending tuples to the downstream executors in the scope of `WORKER_LOCAL`. 
The downstream executors in the scope are chosen based on their load. Executors with lower load are more likely to be chosen.
Once the average load of these `WORKER_LOCAL` executors reaches `topology.localityaware.higher.bound`, 
it switches to the higher scope which is `HOST_LOCAL` and starts sending tuples in that scope. 
And if the average load is still higher than the `higher bound`, it switches to a higher scope.

On the other hand, it switches to a lower scope if the average load of the lower scope is less than `topology.localityaware.lower.bound`. 


### How is Load calculated

The load of an downstream executor is the maximum of the following two:

* The population percentage of the receive queue
* Math.min(pendingMessages, 1024) / 1024. 

`pendingMessages`: The upstream executor `E` sends messages to the downstream executor through Netty and the `pendingMessages` is the number of messages that haven't got through to the server.

If the downstream executor located on the same worker as the executor `E`, the load of that downstream executor is:
* The population percentage of the receive queue

### Relationship between Load and Capacity

The capacity of a bolt executor on Storm UI is calculated as:
  * (number executed * average execute latency) / measurement time

It basically means how busy this executor is. If this is around 1.0, the corresponding Bolt is running as fast as it can. A `__capacity` metric exists to track this value for each executor.

The `Capacity` is not related to the `Load`:

* If the `Load` of the executor `E1` is high, 
    * the `Capacity` of `E1` could be high: population of the receive queue of `E1` could be high and it means the executor `E` has more work to do.
    * the `Capacity` could also be low: `pendingMessage` could be high because other executors share the netty connection between the two workers and they are sending too many messages. 
    But the actual population of the receive queue of `E1` might be low.
* If the `Load` is low,
    * the `Capacity` could be low: lower `Load` means less work to do. 
    * the `Capacity` could also be high: because the executor could be receiving tuples and executing tuples at the similar average rate.
* If the `Capacity` is high,
    * the `Load` could be high: high `Capacity` means the executor is busy. It could be because it's receiving too many tuples.
    * the `Load` could also be low: because the executor could be receiving tuples and executing tuples at the similar average rate.
* If the `Capacity` is low,
    * the `Load` could be low: if the `pendingMessage` is low
    * the `Load` could also be high: because the `pendingMessage` might be very high.


### Troubleshooting

#### I am seeing high capacity (close to 1.0) on some executors and low capacity (close to 0) on other executors

1. It could mean that you could reduce parallelism. Your executors are able to keep up and the load never gets to a very high point.

2. You can try to adjust `topology.localityaware.higher.bound` and `topology.localityaware.lower.bound`

3. You can try to enable `topology.ras.order.executors.by.proximity.needs`. With this config, unassigned executors will be sorted by topological order 
with network proximity needs before being scheduled. This is a best-effort to split the topology to slices and allocate executors in each slice to as closest physical location as possible.


#### I just want the capacity on every downstream executor to be even

You can turn off LoadAwareShuffleGrouping by setting `topology.disable.loadaware.messaging` to `true`.
