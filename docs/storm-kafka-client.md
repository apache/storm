---
title: Storm Kafka Integration (0.10.x+)
layout: documentation
documentation: true
---
# Storm Apache Kafka integration using the kafka-client jar
This includes the new Apache Kafka consumer API.

## Compatibility

Apache Kafka versions 0.10.1.0 onwards. Please be aware that [KAFKA-7044](https://issues.apache.org/jira/browse/KAFKA-7044) can cause crashes in the spout, so you should upgrade Kafka if you are using an affected version (1.1.0, 1.1.1 or 2.0.0).

## Writing to Kafka as part of your topology
You can create an instance of org.apache.storm.kafka.bolt.KafkaBolt and attach it as a component to your topology or if you
are using trident you can use org.apache.storm.kafka.trident.TridentState, org.apache.storm.kafka.trident.TridentStateFactory and
org.apache.storm.kafka.trident.TridentKafkaUpdater.

You need to provide implementations for the following 2 interfaces

### TupleToKafkaMapper and TridentTupleToKafkaMapper
These interfaces have 2 methods defined:

```java
K getKeyFromTuple(Tuple/TridentTuple tuple);
V getMessageFromTuple(Tuple/TridentTuple tuple);
```

As the name suggests, these methods are called to map a tuple to a Kafka key and a Kafka message. If you just want one field
as key and one field as value, then you can use the provided FieldNameBasedTupleToKafkaMapper.java
implementation. In the KafkaBolt, the implementation always looks for a field with field name "key" and "message" if you
use the default constructor to construct FieldNameBasedTupleToKafkaMapper for backward compatibility
reasons. Alternatively you could also specify a different key and message field by using the non default constructor.
In the TridentKafkaState you must specify what is the field name for key and message as there is no default constructor.
These should be specified while constructing an instance of FieldNameBasedTupleToKafkaMapper.

### KafkaTopicSelector and trident KafkaTopicSelector
This interface has only one method

```java
public interface KafkaTopicSelector {
    String getTopics(Tuple/TridentTuple tuple);
}
```

The implementation of this interface should return the topic to which the tuple's key/message mapping needs to be published
You can return a null and the message will be ignored. If you have one static topic name then you can use
DefaultTopicSelector.java and set the name of the topic in the constructor.
`FieldNameTopicSelector` and `FieldIndexTopicSelector` can be used to select the topic should to publish a tuple to.
A user just needs to specify the field name or field index for the topic name in the tuple itself.
When the topic is name not found , the `Field*TopicSelector` will write messages into default topic .
Please make sure the default topic has been created .

### Specifying Kafka producer properties
You can provide all the producer properties in your Storm topology by calling `KafkaBolt.withProducerProperties()` and `TridentKafkaStateFactory.withProducerProperties()`. Please see  http://kafka.apache.org/documentation.html#newproducerconfigs
Section "Important configuration properties for the producer" for more details.
These are also defined in `org.apache.kafka.clients.producer.ProducerConfig`

### Using wildcard kafka topic match
You can do a wildcard topic match by adding the following config

```java
Config config = new Config();
config.put("kafka.topic.wildcard.match",true);
```

After this you can specify a wildcard topic for matching e.g. clickstream.*.log.  This will match all streams matching clickstream.my.log, clickstream.cart.log etc


### Putting it all together

For the bolt :

```java
TopologyBuilder builder = new TopologyBuilder();

Fields fields = new Fields("key", "message");
FixedBatchSpout spout = new FixedBatchSpout(fields, 4,
            new Values("storm", "1"),
            new Values("trident", "1"),
            new Values("needs", "1"),
            new Values("javadoc", "1")
);
spout.setCycle(true);
builder.setSpout("spout", spout, 5);
//set producer properties.
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("acks", "1");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

KafkaBolt bolt = new KafkaBolt()
        .withProducerProperties(props)
        .withTopicSelector(new DefaultTopicSelector("test"))
        .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
builder.setBolt("forwardToKafka", bolt, 8).shuffleGrouping("spout");

Config conf = new Config();

StormSubmitter.submitTopology("kafkaboltTest", conf, builder.createTopology());
```

For Trident:

```java
Fields fields = new Fields("word", "count");
FixedBatchSpout spout = new FixedBatchSpout(fields, 4,
        new Values("storm", "1"),
        new Values("trident", "1"),
        new Values("needs", "1"),
        new Values("javadoc", "1")
);
spout.setCycle(true);

TridentTopology topology = new TridentTopology();
Stream stream = topology.newStream("spout1", spout);

//set producer properties.
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("acks", "1");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

TridentKafkaStateFactory stateFactory = new TridentKafkaStateFactory()
        .withProducerProperties(props)
        .withKafkaTopicSelector(new DefaultTopicSelector("test"))
        .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("word", "count"));
stream.partitionPersist(stateFactory, fields, new TridentKafkaStateUpdater(), new Fields());

Config conf = new Config();
StormSubmitter.submitTopology("kafkaTridentTest", conf, topology.build());
```

## Reading From kafka (Spouts)

### Configuration

The spout implementations are configured by use of the `KafkaSpoutConfig` class.  This class uses a Builder pattern and can be started either by calling one of
the Builders constructors or by calling the static method builder in the KafkaSpoutConfig class.

The Constructor or static method to create the builder require a few key values (that can be changed later on) but are the minimum config needed to start
a spout.

`bootstrapServers` is the same as the Kafka Consumer Property "bootstrap.servers".
`topics` The topics the spout will consume can either be a `Collection` of specific topic names (1 or more) or a regular expression `Pattern`, which specifies
that any topics that match that regular expression will be consumed.

If you are using the Builder Constructors instead of one of the `builder` methods, you will also need to specify a key deserializer and a value deserializer.  This is to help guarantee type safety through the use
of Java generics.  The deserializers can be specified via the consumer properties set with `setProp`. See the KafkaConsumer configuration documentation for details.

There are a few key configs to pay attention to.

`setFirstPollOffsetStrategy` allows you to set where to start consuming data from.  This is used both in case of failure recovery and starting the spout
for the first time. The allowed values are listed in the [FirstPollOffsetStrategy javadocs](javadocs/org/apache/storm/kafka/spout/KafkaSpoutConfig.FirstPollOffsetStrategy.html).

`setProcessingGuarantee` lets you configure what processing guarantees the spout will provide. This affects how soon consumed offsets can be committed, and the frequency of commits. See the [ProcessingGuarantee javadoc](javadocs/org/apache/storm/kafka/spout/KafkaSpoutConfig.ProcessingGuarantee.html) for details.

`setRecordTranslator` allows you to modify how the spout converts a Kafka Consumer Record into a Tuple, and which stream that tuple will be published into.
By default the "topic", "partition", "offset", "key", and "value" will be emitted to the "default" stream.  If you want to output entries to different
streams based on the topic, storm provides `ByTopicRecordTranslator`.  See below for more examples on how to use these.

`setProp` and `setProps` can be used to set KafkaConsumer properties. The list of these properties can be found in the KafkaConsumer configuration documentation on the [Kafka website](http://kafka.apache.org/documentation.html#consumerconfigs). Note that KafkaConsumer autocommit is unsupported. The KafkaSpoutConfig constructor will throw an exception if the "enable.auto.commit" property is set, and the consumer used by the spout will always have that property set to false. You can configure similar behavior to autocommit through the `setProcessingGuarantee` method on the KafkaSpoutConfig builder.

### Usage Examples

#### Create a Simple Insecure Spout
The following will consume all events published to "topic" and send them to MyBolt with the fields "topic", "partition", "offset", "key", "value".

```java

final TopologyBuilder tp = new TopologyBuilder();
tp.setSpout("kafka_spout", new KafkaSpout<>(KafkaSpoutConfig.builder("127.0.0.1:" + port, "topic").build()), 1);
tp.setBolt("bolt", new myBolt()).shuffleGrouping("kafka_spout");
...
```

#### Wildcard Topics
Wildcard topics will consume from all topics that exist in the specified brokers list and match the pattern.  So in the following example
"topic", "topic_foo" and "topic_bar" will all match the pattern "topic.*", but "not_my_topic" would not match. 

```java

final TopologyBuilder tp = new TopologyBuilder();
tp.setSpout("kafka_spout", new KafkaSpout<>(KafkaSpoutConfig.builder("127.0.0.1:" + port, Pattern.compile("topic.*")).build()), 1);
tp.setBolt("bolt", new myBolt()).shuffleGrouping("kafka_spout");
...
```

#### Multiple Streams

```java

final TopologyBuilder tp = new TopologyBuilder();

//By default all topics not covered by another rule, but consumed by the spout will be emitted to "STREAM_1" as "topic", "key", and "value"
ByTopicRecordTranslator<String, String> byTopic = new ByTopicRecordTranslator<>(
    (r) -> new Values(r.topic(), r.key(), r.value()),
    new Fields("topic", "key", "value"), "STREAM_1");
//For topic_2 all events will be emitted to "STREAM_2" as just "key" and "value"
byTopic.forTopic("topic_2", (r) -> new Values(r.key(), r.value()), new Fields("key", "value"), "STREAM_2");

tp.setSpout("kafka_spout", new KafkaSpout<>(KafkaSpoutConfig.builder("127.0.0.1:" + port, "topic_1", "topic_2", "topic_3").build()), 1);
tp.setBolt("bolt", new myBolt()).shuffleGrouping("kafka_spout", "STREAM_1");
tp.setBolt("another", new myOtherBolt()).shuffleGrouping("kafka_spout", "STREAM_2");
...
```

#### Trident

```java
final TridentTopology tridentTopology = new TridentTopology();
final Stream spoutStream = tridentTopology.newStream("kafkaSpout",
    new KafkaTridentSpoutOpaque<>(KafkaSpoutConfig.builder("127.0.0.1:" + port, Pattern.compile("topic.*")).build()))
      .parallelismHint(1)
...
```

Trident does not support multiple streams and will ignore any streams set for output.  If however the Fields are not identical for each
output topic it will throw an exception and not continue.

#### Example topologies
Example topologies using storm-kafka-client can be found in the examples/storm-kafka-client-examples directory included in the Storm source or binary distributions.

### Custom RecordTranslators (ADVANCED)

In most cases the built in SimpleRecordTranslator and ByTopicRecordTranslator should cover your use case.  If you do run into a situation where you need a custom one
then this documentation will describe how to do this properly, and some of the less than obvious classes involved.

The point of `apply` is to take a ConsumerRecord and turn it into a `List<Object>` that can be emitted.  What is not obvious is how to tell the spout to emit it to a
specific stream.  To do this you will need to return an instance of `org.apache.storm.kafka.spout.KafkaTuple`.  This provides a method `routedTo` that will say which
specific stream the tuple should go to.

For Example:

```java
return new KafkaTuple(1, 2, 3, 4).routedTo("bar");
```

Will cause the tuple to be emitted on the "bar" stream.

Be careful when writing custom record translators because just like in a storm spout it needs to be self consistent.  The `streams` method should return
a full set of streams that this translator will ever try to emit on.  Additionally `getFieldsFor` should return a valid Fields object for each of those
streams.  If you are doing this for Trident a value must be in the List returned by `apply` for every field in the Fields object for that stream,
otherwise trident can throw exceptions.


### Manual Partition Assignment (ADVANCED)

By default the KafkaSpout instances will be assigned partitions using a round robin strategy. If you need to customize partition assignment, you must implement the `ManualPartitioner` interface. You can pass your implementation to the `KafkaSpoutConfig.Builder` constructor. Please take care when supplying a custom implementation, since an incorrect `ManualPartitioner` implementation could leave some partitions unread, or concurrently read by multiple spout instances. See the `RoundRobinManualPartitioner` for an example of how to implement this functionality.

### Manual partition discovery

You can customize how the spout discovers existing partitions, by implementing the `TopicFilter` interface. Storm-kafka-client ships with a few implementations. Like `ManualPartitioner`, you can pass your implementation to the `KafkaSpoutConfig.Builder` constructor. Note that the `TopicFilter` is only responsible for discovering partitions, deciding which of the discovered partitions to subscribe to is the responsibility of `ManualPartitioner`.

## Using storm-kafka-client with different versions of kafka

Storm-kafka-client's Kafka dependency is defined as `provided` scope in maven, meaning it will not be pulled in
as a transitive dependency. This allows you to use a version of Kafka dependency compatible with your kafka cluster.

When building a project with storm-kafka-client, you must explicitly add the Kafka clients dependency. For example, to
use Kafka-clients 0.10.0.0, you would use the following dependency in your `pom.xml`:

```xml
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>0.10.0.0</version>
        </dependency>
```

You can also override the kafka clients version while building from maven, with parameter `storm.kafka.client.version`
e.g. `mvn clean install -Dstorm.kafka.client.version=0.10.0.0`

When selecting a kafka client version, you should ensure -
 1. The Kafka api must be compatible. The storm-kafka-client module only supports Kafka **0.10 or newer**. For older versions,
 you can use the storm-kafka module (https://github.com/apache/storm/tree/1.x-branch/external/storm-kafka).  
 2. The Kafka client version selected by you should be wire compatible with the broker. Please see the [Kafka compatibility matrix](https://cwiki.apache.org/confluence/display/KAFKA/Compatibility+Matrix).

# Kafka Spout Performance Tuning

The Kafka spout provides two internal parameters to control its performance. The parameters can be set using the [setOffsetCommitPeriodMs](javadocs/org/apache/storm/kafka/spout/KafkaSpoutConfig.Builder.html#setOffsetCommitPeriodMs-long-) and [setMaxUncommittedOffsets](javadocs/org/apache/storm/kafka/spout/KafkaSpoutConfig.Builder.html#setMaxUncommittedOffsets-int-) methods. 

* "offset.commit.period.ms" controls how often the spout commits to Kafka
* "max.uncommitted.offsets" controls how many offsets can be pending commit before another poll can take place
<br/>

The [Kafka consumer config] (http://kafka.apache.org/documentation.html#consumerconfigs) parameters may also have an impact on the performance of the spout. The following Kafka parameters are likely the most influential in the spout performance: 

* “fetch.min.bytes”
* “fetch.max.wait.ms”
* [Kafka Consumer](http://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html) instance poll timeout, which is specified for each Kafka spout using the [setPollTimeoutMs](javadocs/org/apache/storm/kafka/spout/KafkaSpoutConfig.Builder.html#setPollTimeoutMs-long-) method.
<br/>

Depending on the structure of your Kafka cluster, distribution of the data, and availability of data to poll, these parameters will have to be configured appropriately. Please refer to the Kafka documentation on Kafka parameter tuning.

### Default values

Currently the Kafka spout has has the following default values, which have been shown to give good performance in the test environment as described in this [blog post] (https://hortonworks.com/blog/microbenchmarking-storm-1-0-performance/)

* poll.timeout.ms = 200
* offset.commit.period.ms = 30000   (30s)
* max.uncommitted.offsets = 10000000
<br/>

# Tuple Tracking

By default the spout only tracks emitted tuples when the processing guarantee is AT_LEAST_ONCE. It may be necessary to track
emitted tuples with other processing guarantees to benefit from Storm features such as showing complete latency in the UI,
or enabling backpressure with Config.TOPOLOGY_MAX_SPOUT_PENDING.

```java
KafkaSpoutConfig<String, String> kafkaConf = KafkaSpoutConfig
  .builder(String bootstrapServers, String ... topics)
  .setProcessingGuarantee(ProcessingGuarantee.AT_MOST_ONCE)
  .setTupleTrackingEnforced(true)
```

Note: This setting has no effect with AT_LEAST_ONCE processing guarantee, where tuple tracking is required and therefore always enabled.

# Mapping from `storm-kafka` to `storm-kafka-client` spout properties

This may not be an exhaustive list because the `storm-kafka` configs were taken from Storm 0.9.6
[SpoutConfig](https://svn.apache.org/repos/asf/storm/site/releases/0.9.6/javadocs/storm/kafka/SpoutConfig.html) and
[KafkaConfig](https://svn.apache.org/repos/asf/storm/site/releases/0.9.6/javadocs/storm/kafka/KafkaConfig.html).
`storm-kafka-client` spout configurations were taken from Storm 1.0.6
[KafkaSpoutConfig](https://storm.apache.org/releases/1.0.6/javadocs/org/apache/storm/kafka/spout/KafkaSpoutConfig.html) 
and Kafka 0.10.1.0 [ConsumerConfig](https://kafka.apache.org/0101/javadoc/index.html?org/apache/kafka/clients/consumer/ConsumerConfig.html).

| SpoutConfig   | KafkaSpoutConfig/ConsumerConfig | KafkaSpoutConfig Usage |
| ------------- | ------------------------------- | ---------------------- |
| **Setting:** `startOffsetTime`<br><br> **Default:** `EarliestTime`<br>________________________________________________ <br> **Setting:** `forceFromStart` <br><br> **Default:** `false` <br><br> `startOffsetTime` & `forceFromStart` together determine the starting offset. `forceFromStart` determines whether the Zookeeper offset is ignored. `startOffsetTime` sets the timestamp that determines the beginning offset, in case there is no offset in Zookeeper, or the Zookeeper offset is ignored | **Setting:** [`FirstPollOffsetStrategy`](javadocs/org/apache/storm/kafka/spout/KafkaSpoutConfig.FirstPollOffsetStrategy.html)<br><br> **Default:** `UNCOMMITTED_EARLIEST` <br><br> [Refer to the helper table](#helper-table-for-setting-firstpolloffsetstrategy) for picking `FirstPollOffsetStrategy` based on your `startOffsetTime` & `forceFromStart` settings | [`<KafkaSpoutConfig-Builder>.setFirstPollOffsetStrategy(<strategy-name>)`](javadocs/org/apache/storm/kafka/spout/KafkaSpoutConfig.Builder.html#setFirstPollOffsetStrategy-org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy-)|
| **Setting:** `scheme`<br><br> The interface that specifies how a `ByteBuffer` from a Kafka topic is transformed into Storm tuple <br>**Default:** `RawMultiScheme` | **Setting:** [`Deserializers`](https://kafka.apache.org/11/javadoc/org/apache/kafka/common/serialization/Deserializer.html)| [`<KafkaSpoutConfig-Builder>.setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, <deserializer-class>)`](javadocs/org/apache/storm/kafka/spout/KafkaSpoutConfig.Builder.html#setProp-java.lang.String-java.lang.Object-)<br><br> [`<KafkaSpoutConfig-Builder>.setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, <deserializer-class>)`](javadocs/org/apache/storm/kafka/spout/KafkaSpoutConfig.Builder.html#setProp-java.lang.String-java.lang.Object-)|
| **Setting:** `fetchSizeBytes`<br><br> Message fetch size -- the number of bytes to attempt to fetch in one request to a Kafka server <br> **Default:** `1MB` | **Setting:** [`max.partition.fetch.bytes`](http://kafka.apache.org/10/documentation.html#newconsumerconfigs) | [`<KafkaSpoutConfig-Builder>.setProp(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, <int-value>)`](javadocs/org/apache/storm/kafka/spout/KafkaSpoutConfig.Builder.html#setProp-java.lang.String-java.lang.Object-)|
| **Setting:** `bufferSizeBytes`<br><br> Buffer size (in bytes) for network requests. The buffer size which consumer has for pulling data from producer <br> **Default:** `1MB`| **Setting:** [`receive.buffer.bytes`](http://kafka.apache.org/10/documentation.html#newconsumerconfigs) | [`<KafkaSpoutConfig-Builder>.setProp(ConsumerConfig.RECEIVE_BUFFER_CONFIG, <int-value>)`](javadocs/org/apache/storm/kafka/spout/KafkaSpoutConfig.Builder.html#setProp-java.lang.String-java.lang.Object-)|
| **Setting:** `socketTimeoutMs`<br><br> **Default:** `10000` | **N/A** ||
| **Setting:** `useStartOffsetTimeIfOffsetOutOfRange`<br><br> **Default:** `true` | **Setting:** [`auto.offset.reset`](http://kafka.apache.org/10/documentation.html#newconsumerconfigs) <br><br> **Default:** Note that the default value for `auto.offset.reset` is `earliest` if you have [`ProcessingGuarantee`](javadocs/org/apache/storm/kafka/spout/KafkaSpoutConfig.ProcessingGuarantee.html) set to `AT_LEAST_ONCE`, but the default value is `latest` otherwise.| [`<KafkaSpoutConfig-Builder>.setProp(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, <String>)`](javadocs/org/apache/storm/kafka/spout/KafkaSpoutConfig.Builder.html#setProp-java.lang.String-java.lang.Object-)|
| **Setting:** `fetchMaxWait`<br><br> Maximum time in ms to wait for the response <br> **Default:** `10000` | **Setting:** [`fetch.max.wait.ms`](http://kafka.apache.org/10/documentation.html#newconsumerconfigs) | [`<KafkaSpoutConfig-Builder>.setProp(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, <value>)`](javadocs/org/apache/storm/kafka/spout/KafkaSpoutConfig.Builder.html#setProp-java.lang.String-java.lang.Object-)|
| **Setting:** `maxOffsetBehind`<br><br> Specifies how long a spout attempts to retry the processing of a failed tuple. One of the scenarios is when a failing tuple's offset is more than `maxOffsetBehind` behind the acked offset, the spout stops retrying the tuple.<br>**Default:** `LONG.MAX_VALUE`| **N/A** ||
| **Setting:** `clientId`| **Setting:** [`client.id`](http://kafka.apache.org/10/documentation.html#newconsumerconfigs)| [`<KafkaSpoutConfig-Builder>.setProp(ConsumerConfig.CLIENT_ID_CONFIG, <String>)`](javadocs/org/apache/storm/kafka/spout/KafkaSpoutConfig.Builder.html#setProp-java.lang.String-java.lang.Object-)|

If you are using this table to upgrade your topology to use `storm-kafka-client` instead of `storm-kafka`, then you will also need to migrate the consumer offsets from ZooKeeper to Kafka broker. Use [`storm-kafka-migration`](https://github.com/apache/storm/tree/master/external/storm-kafka-migration) tool to migrate the Kafka consumer offsets.

#### Helper table for setting `FirstPollOffsetStrategy`

Pick and set `FirstPollOffsetStrategy` based on `startOffsetTime` & `forceFromStart` settings:

| `startOffsetTime`    | `forceFromStart` | `FirstPollOffsetStrategy` |
| -------------------- | ---------------- | ------------------------- |
| `EarliestTime` | `true` | `EARLIEST` |
| `EarliestTime` | `false` | `UNCOMMITTED_EARLIEST` |
| `LatestTime` | `true` | `LATEST` |
| `LatestTime` | `false` | `UNCOMMITTED_LATEST` |
