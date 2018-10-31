---
title: Storm RocketMQ
layout: documentation
documentation: true
---

Storm/Trident integration for [RocketMQ](https://rocketmq.incubator.apache.org/). This package includes the core spout, bolt and trident states that allows a storm topology to either write storm tuples into a topic or read from topics in a storm topology.


## Read from Topic
The spout included in this package for reading data from a topic.

### RocketMqSpout
To use the `RocketMqSpout`,  you construct an instance of it by specifying a Properties instance which including rocketmq configs.
RocketMqSpout uses RocketMQ MQPushConsumer as the default implementation. PushConsumer is a high level consumer API, wrapping the pulling details. Looks like broker push messages to consumer.
RocketMqSpout will retry 3(use `SpoutConfig.DEFAULT_MESSAGES_MAX_RETRY` to change the value) times when messages are failed.

 ```java
        Properties properties = new Properties();
        properties.setProperty(SpoutConfig.NAME_SERVER_ADDR, nameserverAddr);
        properties.setProperty(SpoutConfig.CONSUMER_GROUP, group);
        properties.setProperty(SpoutConfig.CONSUMER_TOPIC, topic);

        RocketMqSpout spout = new RocketMqSpout(properties);
 ```


## Write into Topic
The bolt and trident state included in this package for write data into a topic.

### TupleToMessageMapper
The main API for mapping Storm tuple to a RocketMQ Message is the `org.apache.storm.rocketmq.common.mapper.TupleToMessageMapper` interface:

```java
public interface TupleToMessageMapper extends Serializable {
    String getKeyFromTuple(ITuple tuple);
    byte[] getValueFromTuple(ITuple tuple);
}
```

### FieldNameBasedTupleToMessageMapper
`storm-rocketmq` includes a general purpose `TupleToMessageMapper` implementation called `FieldNameBasedTupleToMessageMapper`.

### TopicSelector
The main API for selecting topic and tags is the `org.apache.storm.rocketmq.common.selector.TopicSelector` interface:

```java
public interface TopicSelector extends Serializable {
    String getTopic(ITuple tuple);
    String getTag(ITuple tuple);
}
```

### DefaultTopicSelector/FieldNameBasedTopicSelector
`storm-rocketmq` includes general purpose `TopicSelector` implementations called `DefaultTopicSelector` and `FieldNameBasedTopicSelector`.


### RocketMqBolt
To use the `RocketMqBolt`, you construct an instance of it by specifying TupleToMessageMapper, TopicSelector and Properties instances.
RocketMqBolt send messages async by default. You can change this by invoking `withAsync(false)`.

 ```java
        TupleToMessageMapper mapper = new FieldNameBasedTupleToMessageMapper("word", "count");
        TopicSelector selector = new DefaultTopicSelector(topic);

        properties = new Properties();
        properties.setProperty(RocketMqConfig.NAME_SERVER_ADDR, nameserverAddr);

        RocketMqBolt insertBolt = new RocketMqBolt()
                .withMapper(mapper)
                .withSelector(selector)
                .withProperties(properties);
 ```

### Trident State
We support trident persistent state that can be used with trident topologies. To create a RocketMQ persistent trident state you need to initialize it with the TupleToMessageMapper, TopicSelector, Properties instances. See the example below:

 ```java
        TupleToMessageMapper mapper = new FieldNameBasedTupleToMessageMapper("word", "count");
        TopicSelector selector = new DefaultTopicSelector(topic);

        Properties properties = new Properties();
        properties.setProperty(RocketMqConfig.NAME_SERVER_ADDR, nameserverAddr);

        RocketMqState.Options options = new RocketMqState.Options()
                .withMapper(mapper)
                .withSelector(selector)
                .withProperties(properties);

        StateFactory factory = new RocketMqStateFactory(options);

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1", spout);

        stream.partitionPersist(factory, fields,
                new RocketMqStateUpdater(), new Fields());
 ```

