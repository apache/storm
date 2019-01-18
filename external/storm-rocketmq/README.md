# Storm RocketMQ

Storm/Trident integration for [RocketMQ](https://rocketmq.apache.org/). This package includes the core spout, bolt and trident states that allows a storm topology to either write storm tuples into a topic or read from topics in a storm topology.


## Read from Topic
The spout included in this package for reading data from a topic.

### RocketMqSpout
To use the `RocketMqSpout`,  you construct an instance of it by specifying a Properties instance which including rocketmq configs.
RocketMqSpout uses RocketMQ MQPushConsumer as the default implementation. PushConsumer is a high level consumer API, wrapping the pulling details. Looks like broker push messages to consumer.
RocketMqSpout's messages retrying depends on RocketMQ's push mode retry policy.

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

## Configurations

### Producer Configurations
| NAME        | DESCRIPTION           | DEFAULT  |
| ------------- |:-------------:|:------:|
| nameserver.address      | name server address *Required* | null |
| nameserver.poll.interval      | name server poll topic info interval     |   30000 |
| brokerserver.heartbeat.interval | broker server heartbeat interval      |    30000 |
| producer.group | producer group      |    $UUID |
| producer.retry.times | producer send messages retry times      |    3 |
| producer.timeout | producer send messages timeout      |    3000 |


### Consumer Configurations
| NAME        | DESCRIPTION           | DEFAULT  |
| ------------- |:-------------:|:------:|
| nameserver.address      | name server address *Required* | null |
| nameserver.poll.interval      | name server poll topic info interval     |   30000 |
| brokerserver.heartbeat.interval | broker server heartbeat interval      |    30000 |
| consumer.group | consumer group *Required*     |    null |
| consumer.topic | consumer topic *Required*       |    null |
| consumer.tag | consumer topic tag      |    * |
| consumer.offset.reset.to | what to do when there is no initial offset on the server      |   latest/earliest/timestamp |
| consumer.offset.from.timestamp | the timestamp when `consumer.offset.reset.to=timestamp` was set   |   $TIMESTAMP |
| consumer.messages.orderly | if the consumer topic is ordered      |    false |
| consumer.offset.persist.interval | auto commit offset interval      |    5000 |
| consumer.min.threads | consumer min threads      |    20 |
| consumer.max.threads | consumer max threads      |    64 |
| consumer.callback.executor.threads | client callback executor threads      |    $availableProcessors |
| consumer.batch.size | consumer messages batch size      |    32 |
| consumer.batch.process.timeout | consumer messages batch process timeout      |   $TOPOLOGY_MESSAGE_TIMEOUT_SECS + 10s|


## License

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

## Committer Sponsors

 * Xin Wang ([xinwang@apache.org](mailto:xinwang@apache.org))

