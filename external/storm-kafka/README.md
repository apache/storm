Storm Kafka
====================

Provides core storm and Trident spout implementations for consuming data from Apache Kafka 0.8.x.

##Spouts
We support both trident and core storm spouts.
 
###KafkaConfig
To initialize the kafka spout you need to construct an instance of the kafka config.
 
```java
    public KafkaConfig(List<Broker> seedBrokerHosts, String topic)
```

Following are the available configuration options:
    
| name | default | description | Applicable to Trident?
|--- | --- | --- | --- |
| seedBrokers | must be supplied | Partial list of kafka broker hosts that will be contacted to get topic partition information. | Yes |
| topic | must be supplied | The topic from which this spout will consume messages. | Yes |
| clientId | kafka.api.OffsetRequest.DefaultClientId() | Kafka simple consumer client Id. | Yes |
| fetchSizeBytes | 1024 * 1024 | The number of byes of messages to attempt to fetch for each topic-partition in each fetch request. | Yes|
| socketTimeoutMs | 10000 | Socket timeout for kafka network requests. | Yes |
| fetchMaxWait | 10000 |  MaxTime to wait before considering a fetch request as failed. | Yes |
| bufferSizeBytes | 1024 * 1024 | Kafka consumer buffer size in bytes. | Yes |
| scheme | RawMultiScheme | An interface that dictates how to covert a kafka key-message into storm tuple. See below for more details. | Yes |
| forceFromStart | false | flag to control if the consumer should consume messages from the beginning of a partition. | Yes |
| startOffsetTime | kafka.api.OffsetRequest.EarliestTime() | when forceFromStart is true, this is configuration will be used to get the kafka offset from which the consumer will start consuming. | Yes |
| maxOffsetBehind | Long.MAX_VALUE | Controls how far behind this spout is allowed to get. | No |
| useStartOffsetTimeIfOffsetOutOfRange | true | If the spout receives an offset_out_of_range error, should it reset the consuming offset to startOffsetTime. | Yes |
| metricsTimeBucketSizeInSecs | 60 | Number of seconds the each metrics bucket represents. | Yes |
| stateUpdateIntervalMs | 2000 | setting for how often to save the current kafka offset to ZooKeeper. | No |
| retryInitialDelayMs | 0 | how long should the spout wait before retrying a failed message for the firs time. | No |
| retryDelayMultiplier | 1.0 | Exponential back-off retry setting, the multiplier to be used while calculating back off sleep time. | No |
| retryDelayMaxMs | 60 * 1000 | Max exponential back-off sleep time. | No |

###MultiScheme
MultiScheme is an interface that dictates how the byte[] consumed from kafka gets transformed into a storm tuple. It
also controls the naming of your output field.

```java
  public Iterable<List<Object>> deserialize(byte[] ser);
  public Fields getOutputFields();
```

The default RawMultiScheme just takes the byte[] and returns a tuple with byte[] as is. The name of the outputField is
"bytes". There are alternative implementation like SchemeAsMultiScheme and KeyValueSchemeAsMultiScheme which can convert
the byte[] to String. 
### Examples
####Core Spout
```java
List<Broker> seedBrokers = Lists.newArrayList(new Broker("localhost", 9092));
String topic = "test";
KafkaConfig spoutConfig = new KafkaConfig(seedBrokers, topic);
spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
```
####Trident Spout
```java
List<Broker> seedBrokers = Lists.newArrayList(new Broker("localhost", 9092));
String topic = "test";
KafkaConfig spoutConfig = new KafkaConfig(seedBrokers, topic);
spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);
```

## Using storm-kafka with different versions of Scala

Storm-kafka's Kafka dependency is defined as `provided` scope in maven, meaning it will not be pulled in
as a transitive dependency. This allows you to use a version of Kafka built against a specific Scala version.

When building a project with storm-kafka, you must explicitly add the Kafka dependency. For example, to
use Kafka 0.8.1.1 built against Scala 2.10, you would use the following dependency in your `pom.xml`:

```xml
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka_2.10</artifactId>
            <version>0.8.1.1</version>
            <exclusions>
                <exclusion>
                    <groupId>org.apache.zookeeper</groupId>
                    <artifactId>zookeeper</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
```

Note that the ZooKeeper and log4j dependencies are excluded to prevent version conflicts with Storm's dependencies.

##Writing to Kafka as part of your topology
You can create an instance of storm.kafka.bolt.KafkaBolt and attach it as a component to your topology or if you 
are using trident you can use storm.kafka.trident.TridentState, storm.kafka.trident.TridentStateFactory and
storm.kafka.trident.TridentKafkaUpdater.

You need to provide implementation of following 2 interfaces

###TupleToKafkaMapper
These interfaces have 2 methods defined:

```java
    K getKeyFromTuple(ITuple tuple);
    V getMessageFromTuple(ITuple tuple);
```

as the name suggests these methods are called to map a tuple to kafka key and kafka message. If you just want one field
as key and one field as value then you can use the provided FieldNameBasedTupleToKafkaMapper.java 
implementation. By default the implementation always looks for a field with field name "key" and "message" if you 
use the default constructor to construct FieldNameBasedTupleToKafkaMapper for backward compatibility 
reasons. Alternatively you could also specify a different key and message field by using the non default constructor.

###KafkaTopicSelector
This interface has only one method
```java
public interface KafkaTopicSelector {
    String getTopics(ITuple tuple);
}
```
The implementation of this interface should return topic to which the tuple's key/message mapping needs to be published 
You can return a null and the message will be ignored. If you have one static topic name then you can use 
DefaultTopicSelector.java and set the name of the topic in the constructor.

### Specifying kafka producer properties
You can provide all the produce properties , see http://kafka.apache.org/documentation.html#producerconfigs 
section "Important configuration properties for the producer", in your storm topology config by setting the properties
map with key kafka.broker.properties.

###Putting it all together

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
        KafkaBolt bolt = new KafkaBolt()
                .withKafkaTopicSelector(new DefaultTopicSelector("test"))
                .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
        builder.setBolt("forwardToKafka", bolt, 8).shuffleGrouping("spout");
        
        Config conf = new Config();
        //set producer properties.
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("request.required.acks", "1");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        conf.put(TridentKafkaState.KAFKA_BROKER_PROPERTIES, props);
        
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

        TridentKafkaStateFactory stateFactory = new TridentKafkaStateFactory()
                .withKafkaTopicSelector(new DefaultTopicSelector("test"))
                .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("word", "count"));
        stream.partitionPersist(stateFactory, fields, new TridentKafkaUpdater(), new Fields());

        Config conf = new Config();
        //set producer properties.
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("request.required.acks", "1");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        conf.put(TridentKafkaState.KAFKA_BROKER_PROPERTIES, props);
        StormSubmitter.submitTopology("kafkaTridentTest", conf, topology.build());
```

## Committer Sponsors

 * P. Taylor Goetz ([ptgoetz@apache.org](mailto:ptgoetz@apache.org))
