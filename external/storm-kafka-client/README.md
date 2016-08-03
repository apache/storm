#Storm Kafka Spout with New Kafka Consumer API

Apache Storm Spout implementation to consume data from Apache Kafka 0.9.x. It allows 
clients to consume data from Kafka starting at offsets as defined by the offset strategy specified in `FirstPollOffsetStrategy`. 
In case of failure, the Kafka Spout will re-start consuming messages from the offset that matches the chosen `FirstPollOffsetStrategy`.

The Kafka Spout implementation allows you to specify the stream (`KafkaSpoutStream`) associated with each topic. `KafkaSpoutStream` represents the stream and output fields used by a topic.

The `KafkaSpoutTuplesBuilder` wraps all the logic that builds `Tuple`s from `ConsumerRecord`s. The logic is provided by the user by implementing the appropriate number of `KafkaSpoutTupleBuilder` instances.

Multiple topics can use the same `KafkaSpoutTupleBuilder` implementation, as long as the logic to build `Tuple`s from `ConsumerRecord`s is identical.

# Usage Examples

### Create a Kafka Spout:

The code snippet bellow is extracted from the example in the module [test] (https://github.com/apache/storm/tree/master/external/storm-kafka-client/src/test/java/org/apache/storm/kafka/spout/test). Please refer to this module for more detail

```java
KafkaSpout<String,String> kafkaSpout = new KafkaSpout<>(kafkaSpoutConfig);

KafkaSpoutConfig kafkaSpoutConfig = new KafkaSpoutConfig.Builder<String, String>(kafkaConsumerProps, kafkaSpoutStreams, tuplesBuilder, retryService)
                                                    .setOffsetCommitPeriodMs(10_000)
                                                    .setFirstPollOffsetStrategy(EARLIEST)
                                                    .setMaxUncommittedOffsets(250)
                                                    .build();


KafkaSpoutStreams kafkaSpoutStreams = new KafkaSpoutStreams.Builder(outputFields, STREAMS[0], new String[]{TOPICS[0], TOPICS[1]})
                .addStream(outputFields, STREAMS[0], new String[]{TOPICS[2]})  // contents of topic test2 sent to test_stream
                .addStream(outputFields1, STREAMS[2], new String[]{TOPICS[2]})  // contents of topic test2 sent to test2_stream
                .build();

Map<String, Object> kafkaConsumerProps = new HashMap<>();
        props.put(KafkaSpoutConfig.Consumer.BOOTSTRAP_SERVERS, "127.0.0.1:9092");
        props.put(KafkaSpoutConfig.Consumer.GROUP_ID, "kafkaSpoutTestGroup");
        props.put(KafkaSpoutConfig.Consumer.KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(KafkaSpoutConfig.Consumer.VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");

KafkaSpoutTuplesBuilder<String, String> tuplesBuilder = new KafkaSpoutTuplesBuilder.Builder<>(
                new TopicsTest0Test1TupleBuilder<String, String>(TOPICS[0], TOPICS[1]),
                new TopicTest2TupleBuilder<String, String>(TOPICS[2]))
                .build();

KafkaSpoutRetryService retryService = new KafkaSpoutRetryExponentialBackoff(TimeInterval.microSeconds(500),
                    TimeInterval.milliSeconds(2), Integer.MAX_VALUE, TimeInterval.seconds(10));
```
 
### Create a simple Toplogy using the Kafka Spout:

```java
TopologyBuilder tp = new TopologyBuilder();
tp.setSpout("kafka_spout", new KafkaSpout<>(getKafkaSpoutConfig(getKafkaSpoutStreams())), 1);
tp.setBolt("kafka_bolt", new KafkaSpoutTestBolt()).shuffleGrouping("kafka_spout", STREAMS[0]);
tp.setBolt("kafka_bolt_1", new KafkaSpoutTestBolt()).shuffleGrouping("kafka_spout", STREAMS[2]);
tp.createTopology();
```

# Build And Run Bundled Examples  
To be able to run the examples you must first build the java code in the package `storm-kafka-client`, 
and then generate an uber jar with all the dependencies.

## Use the Maven Shade Plugin to Build the Uber Jar

Add the following to `REPO_HOME/storm/external/storm-kafka-client/pom.xml`
```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-shade-plugin</artifactId>
    <version>2.4.1</version>
    <executions>
        <execution>
            <phase>package</phase>
            <goals>
                <goal>shade</goal>
            </goals>
            <configuration>
                <transformers>
                    <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                        <mainClass>org.apache.storm.kafka.spout.test.KafkaSpoutTopologyMain</mainClass>
                    </transformer>
                </transformers>
            </configuration>
        </execution>
    </executions>
</plugin>
```

create the uber jar by running the commmand:

`mvn package -f REPO_HOME/storm/external/storm-kafka-client/pom.xml`

This will create the uber jar file with the name and location matching the following pattern:
 
`REPO_HOME/storm/external/storm-kafka-client/target/storm-kafka-client-1.0.x.jar`

### Run Storm Topology

Copy the file `REPO_HOME/storm/external/storm-kafka-client/target/storm-kafka-client-1.0.x.jar` to `STORM_HOME/extlib`

Using the Kafka command line tools create three topics [test, test1, test2] and use the Kafka console producer to populate the topics with some data 

Execute the command `STORM_HOME/bin/storm jar REPO_HOME/storm/external/storm/target/storm-kafka-client-1.0.x.jar org.apache.storm.kafka.spout.test.KafkaSpoutTopologyMain`

With the debug level logs enabled it is possible to see the messages of each topic being redirected to the appropriate Bolt as defined 
by the streams defined and choice of shuffle grouping.

## Using storm-kafka-client with different versions of kafka

Storm-kafka-client's Kafka dependency is defined as `provided` scope in maven, meaning it will not be pulled in
as a transitive dependency. This allows you to use a version of Kafka dependency compatible with your kafka cluster.

When building a project with storm-kafka-client, you must explicitly add the Kafka clients dependency. For example, to
use Kafka-clients 0.9.0.1, you would use the following dependency in your `pom.xml`:

```xml
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>0.9.0.1</version>
        </dependency>
```

You can also override the kafka clients version while building from maven, with parameter `storm.kafka.client.version`
e.g. `mvn clean install -Dstorm.kafka.client.version=0.9.0.1`

When selecting a kafka client version, you should ensure -
 1. kafka api is compatible. storm-kafka-client module only supports **0.9 or newer** kafka client API. For older versions,
 you can use storm-kafka module (https://github.com/apache/storm/tree/master/external/storm-kafka).
 2. The kafka client selected by you should be wire compatible with the broker. e.g. 0.9.x client will not work with
 0.8.x broker.

#Kafka Spout Performance Tuning

The Kafka spout provides two internal parameters to control its performance. The parameters can be set using the [KafkaSpoutConfig] (https://github.com/apache/storm/blob/1.0.x-branch/external/storm-kafka-client/src/main/java/org/apache/storm/kafka/spout/KafkaSpoutConfig.java) methods [setOffsetCommitPeriodMs] (https://github.com/apache/storm/blob/1.0.x-branch/external/storm-kafka-client/src/main/java/org/apache/storm/kafka/spout/KafkaSpoutConfig.java#L189-L193) and [setMaxUncommittedOffsets] (https://github.com/apache/storm/blob/1.0.x-branch/external/storm-kafka-client/src/main/java/org/apache/storm/kafka/spout/KafkaSpoutConfig.java#L211-L217). 

* "offset.commit.period.ms" controls how often the spout commits to Kafka
* "max.uncommitted.offsets" controls how many offsets can be pending commit before another poll can take place
<br/>

The [Kafka consumer config] (http://kafka.apache.org/documentation.html#consumerconfigs) parameters may also have an impact on the performance of the spout. The following Kafka parameters are likely the most influential in the spout performance: 

* “fetch.min.bytes”
* “fetch.max.wait.ms”
* [Kafka Consumer] (http://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html) instance poll timeout, which is specified for each Kafka spout using the [KafkaSpoutConfig] (https://github.com/apache/storm/blob/1.0.x-branch/external/storm-kafka-client/src/main/java/org/apache/storm/kafka/spout/KafkaSpoutConfig.java) method [setPollTimeoutMs] (https://github.com/apache/storm/blob/1.0.x-branch/external/storm-kafka-client/src/main/java/org/apache/storm/kafka/spout/KafkaSpoutConfig.java#L180-L184)
<br/>

Depending on the structure of your Kafka cluster, distribution of the data, and availability of data to poll, these parameters will have to be configured appropriately. Please refer to the Kafka documentation on Kafka parameter tuning.

###Default values

Currently the Kafka spout has has the following default values, which have shown to give good performance in the test environment as described in this [blog post] (https://hortonworks.com/blog/microbenchmarking-storm-1-0-performance/)

* poll.timeout.ms = 200
* offset.commit.period.ms = 30000   (30s)
* max.uncommitted.offsets = 10000000
<br/>

There will be a blog post coming soon analyzing the trade-offs of this tuning parameters, and comparing the performance of the Kafka Spouts using the Kafka client API introduced in 0.9 (new implementation) and in prior versions (prior implementation)

#Future Work
Trident spout implementation, support for topic patterns, and comprehensive metrics
