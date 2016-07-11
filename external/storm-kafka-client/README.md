#Storm Kafka Spout with New Kafaka Consumer API

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

KafkaSpoutRetryService retryService = new KafkaSpoutRetryExponentialBackoff(new TimeInterval(500, TimeUnit.MICROSECONDS),
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

#Future Work
Trident spout implementation, support for topic patterns, and comprehensive metrics