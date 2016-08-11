---
title: Troubleshooting
layout: documentation
documentation: true
---

This page lists issues people have run into when using Storm along with their solutions.

### Worker processes are crashing on startup with no stack trace

Possible symptoms:
 
 * Topologies work with one node, but workers crash with multiple nodes

Solutions:

 * You may have a misconfigured subnet, where nodes can't locate other nodes based on their hostname. ZeroMQ sometimes crashes the process when it can't resolve a host. There are two solutions:
  * Make a mapping from hostname to IP address in /etc/hosts
  * Set up an internal DNS so that nodes can locate each other based on hostname.
  
### Nodes are unable to communicate with each other

Possible symptoms:

 * Every spout tuple is failing
 * Processing is not working

Solutions:

 * Storm doesn't work with ipv6. You can force ipv4 by adding `-Djava.net.preferIPv4Stack=true` to the supervisor child options and restarting the supervisor. 
 * You may have a misconfigured subnet. See the solutions for `Worker processes are crashing on startup with no stack trace`

### Topology stops processing tuples after awhile

Symptoms:

 * Processing works fine for awhile, and then suddenly stops and spout tuples start failing en masse. 
 
Solutions:

 * This is a known issue with ZeroMQ 2.1.10. Downgrade to ZeroMQ 2.1.7.
 
### Not all supervisors appear in Storm UI

Symptoms:
 
 * Some supervisor processes are missing from the Storm UI
 * List of supervisors in Storm UI changes on refreshes

Solutions:

 * Make sure the supervisor local dirs are independent (e.g., not sharing a local dir over NFS)
 * Try deleting the local dirs for the supervisors and restarting the daemons. Supervisors create a unique id for themselves and store it locally. When that id is copied to other nodes, Storm gets confused. 

### "Multiple defaults.yaml found" error

Symptoms:

 * When deploying a topology with "storm jar", you get this error

Solution:

 * You're most likely including the Storm jars inside your topology jar. When packaging your topology jar, don't include the Storm jars as Storm will put those on the classpath for you.

### "NoSuchMethodError" when running storm jar

Symptoms:

 * When running storm jar, you get a cryptic "NoSuchMethodError"

Solution:

 * You're deploying your topology with a different version of Storm than you built your topology against. Make sure the storm client you use comes from the same version as the version you compiled your topology against.


### Kryo ConcurrentModificationException

Symptoms:

 * At runtime, you get a stack trace like the following:

```
java.lang.RuntimeException: java.util.ConcurrentModificationException
	at org.apache.storm.utils.DisruptorQueue.consumeBatchToCursor(DisruptorQueue.java:84)
	at org.apache.storm.utils.DisruptorQueue.consumeBatchWhenAvailable(DisruptorQueue.java:55)
	at org.apache.storm.disruptor$consume_batch_when_available.invoke(disruptor.clj:56)
	at org.apache.storm.disruptor$consume_loop_STAR_$fn__1597.invoke(disruptor.clj:67)
	at org.apache.storm.util$async_loop$fn__465.invoke(util.clj:377)
	at clojure.lang.AFn.run(AFn.java:24)
	at java.lang.Thread.run(Thread.java:679)
Caused by: java.util.ConcurrentModificationException
	at java.util.LinkedHashMap$LinkedHashIterator.nextEntry(LinkedHashMap.java:390)
	at java.util.LinkedHashMap$EntryIterator.next(LinkedHashMap.java:409)
	at java.util.LinkedHashMap$EntryIterator.next(LinkedHashMap.java:408)
	at java.util.HashMap.writeObject(HashMap.java:1016)
	at sun.reflect.GeneratedMethodAccessor17.invoke(Unknown Source)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:616)
	at java.io.ObjectStreamClass.invokeWriteObject(ObjectStreamClass.java:959)
	at java.io.ObjectOutputStream.writeSerialData(ObjectOutputStream.java:1480)
	at java.io.ObjectOutputStream.writeOrdinaryObject(ObjectOutputStream.java:1416)
	at java.io.ObjectOutputStream.writeObject0(ObjectOutputStream.java:1174)
	at java.io.ObjectOutputStream.writeObject(ObjectOutputStream.java:346)
	at org.apache.storm.serialization.SerializableSerializer.write(SerializableSerializer.java:21)
	at com.esotericsoftware.kryo.Kryo.writeClassAndObject(Kryo.java:554)
	at com.esotericsoftware.kryo.serializers.CollectionSerializer.write(CollectionSerializer.java:77)
	at com.esotericsoftware.kryo.serializers.CollectionSerializer.write(CollectionSerializer.java:18)
	at com.esotericsoftware.kryo.Kryo.writeObject(Kryo.java:472)
	at org.apache.storm.serialization.KryoValuesSerializer.serializeInto(KryoValuesSerializer.java:27)
```

Solution: 

 * This means that you're emitting a mutable object as an output tuple. Everything you emit into the output collector must be immutable. What's happening is that your bolt is modifying the object while it is being serialized to be sent over the network.
