#Storm Kinesis Spout
Provides core storm spout for consuming data from a stream in Amazon Kinesis Streams. It stores the sequence numbers that can be committed in zookeeper and 
starts consuming records after that sequence number on restart by default. Below is the code sample to create a sample topology that uses the spout. Each 
object used in configuring the spout is explained below. Ideally, the number of spout tasks should be equal to number of shards in kinesis. However each task 
can read from more than one shard.

```java
public class KinesisSpoutTopology {
    public static void main (String args[]) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        String topologyName = args[0];
        RecordToTupleMapper recordToTupleMapper = new TestRecordToTupleMapper();
        KinesisConnectionInfo kinesisConnectionInfo = new KinesisConnectionInfo(new CredentialsProviderChain(), new ClientConfiguration(), Regions.US_WEST_2,
                1000);
        ZkInfo zkInfo = new ZkInfo("localhost:2181", "/kinesisOffsets", 20000, 15000, 10000L, 3, 2000);
        KinesisConfig kinesisConfig = new KinesisConfig(args[1], ShardIteratorType.TRIM_HORIZON,
                recordToTupleMapper, new Date(), new ExponentialBackoffRetrier(), zkInfo, kinesisConnectionInfo, 10000L);
        KinesisSpout kinesisSpout = new KinesisSpout(kinesisConfig);
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("spout", kinesisSpout, 3);
        topologyBuilder.setBolt("bolt", new KinesisBoltTest(), 1).shuffleGrouping("spout");
        Config topologyConfig = new Config();
        topologyConfig.setDebug(true);
        topologyConfig.setNumWorkers(3);
        StormSubmitter.submitTopology(topologyName, topologyConfig, topologyBuilder.createTopology());
    }
}
```
As you can see above the spout takes an object of KinesisConfig in its constructor. The constructor of KinesisConfig takes 8 objects as explained below.

#### `String` streamName
name of kinesis stream to consume data from

#### `ShardIteratorType` shardIteratorType
3 types are supported - TRIM_HORIZON(beginning of shard), LATEST and AT_TIMESTAMP. By default this argument is ignored if state for shards 
is found in zookeeper. Hence they will apply the first time a topology is started. If you want to use any of these in subsequent runs of the topology, you 
will need to clear the state of zookeeper node used for storing sequence numbers

#### `RecordToTupleMapper` recordToTupleMapper
an implementation of `RecordToTupleMapper` interface that spout will call to convert a kinesis record to a storm tuple. It has two methods. getOutputFields 
tells the spout the fields that will be present in the tuple emitted from the getTuple method. If getTuple returns null, the record will be acked
```java
    Fields getOutputFields ();
    List<Object> getTuple (Record record);
```

#### `Date` timestamp
used in conjunction with the AT_TIMESTAMP shardIteratorType argument. This will make the spout fetch records from kinesis starting at that time or later. The
time used by kinesis is the server side time associated to the record by kinesis

#### `FailedMessageRetryHadnler` failedMessageRetryHandler 
an implementation of the `FailedMessageRetryHandler` interface. By default this module provides an implementation that supports a exponential backoff retry
mechanism for failed messages. That implementation has two constructors. Default no args constructor will configure first retry at 100 milliseconds and 
subsequent retires at Math.pow(2, i-1) where i is the retry number in the range 2 to LONG.MAX_LONG. 2 represents the base for exponential function in seconds. 
Other constructor takes retry interval in millis for first retry as first argument, base for exponential function in seconds as second argument and number of 
retries as third argument. The methods of this interface and its working in accord with the spout is explained below
```java
    boolean failed (KinesisMessageId messageId);
    KinesisMessageId getNextFailedMessageToRetry ();
    void failedMessageEmitted (KinesisMessageId messageId);
    void acked (KinesisMessageId messageId);
```
failed method will be called on every tuple that failed in the spout. It should return true if that failed message is scheduled to be retried, false otherwise.

getNextFailedMessageToRetry method will be called the first thing every time a spout wants to emit a tuple. It should return a message that should be retried
if any or null otherwise. Note that it can return null in the case it does not have any message to retry as of that moment. However, it should eventually 
return every message for which it returned true when failed method was called for that message

failedMessageEmitted will be called if spout successfully manages to get the record from kinesis and emit it. If not, the implementation should return the same 
message when getNextFailedMessageToRetry is called again

acked will be called once the failed message was re-emitted and successfully acked by the spout. If it was failed by the spout failed will be called again

#### `ZkInfo` zkInfo
an object encapsulating information for zookeeper interaction. The constructor takes zkUrl as first argument which is a comma separated string of zk host and
port, zkNode as second that will be used as the root node for storing committed sequence numbers, session timeout as third in milliseconds, connection timeout
as fourth in milliseconds, commit interval as fifth in milliseconds for committing sequence numbers to zookeeper, retry attempts as sixth for zk client
connection retry attempts, retry interval as seventh in milliseconds for time to wait before retrying to connect. 

#### `KinesisConnectionInfo` kinesisConnectionInfo
an object that captures arguments for connecting to kinesis using kinesis client. It has a constructor that takes an implementation of `AWSCredentialsProvider`
as first argument. This module provides an implementation called `CredentialsProviderChain` that allows the spout to authenticate with kinesis using one of 
the 5 mechanisms in this order - `EnvironmentVariableCredentialsProvider`, `SystemPropertiesCredentialsProvider`, `ClasspathPropertiesFileCredentialsProvider`, 
`InstanceProfileCredentialsProvider`, `ProfileCredentialsProvider`. It takes an object of `ClientConfiguration` as second argument for configuring the kinesis 
client, `Regions` as third argument that sets the region to connect to on the client and recordsLimit as the fourth argument which represents the maximum number
of records kinesis client will retrieve for every GetRecords request. This limit should be carefully chosen based on the size of the record, kinesis 
throughput rate limits and per tuple latency in storm for the topology. Also if one task will be reading from more than one shards then that will also affect
the choice of limit argument

#### `Long` maxUncommittedRecords
this represents the maximum number of uncommitted sequence numbers allowed per task. Once this number is reached spout will not fetch any new records from 
kinesis. Uncommited sequence numbers are defined as the sum of all the messages for a task that have not been committed to zookeeper. This is different from 
topology level max pending messages. For example if this value is set to 10, and the spout emitted sequence numbers from 1 to 10. Sequence number 1 is pending 
and 2 to 10 acked. In that case the number of uncommitted sequence numbers is 10 since no sequence number in the range 1 to 10 can be committed to zk. 
However, storm can still call next tuple on the spout because there is only 1 pending message
 
### Maven dependencies
Aws sdk version that this was tested with is 1.10.77

```xml
 <dependencies>
        <dependency>
            <groupId>com.amazonaws</groupId>
            <artifactId>aws-java-sdk</artifactId>
            <version>${aws-java-sdk.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.storm</groupId>
            <artifactId>storm-client</artifactId>
            <version>${project.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.curator</groupId>
            <artifactId>curator-framework</artifactId>
            <version>${curator.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>slf4j-log4j12</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>com.googlecode.json-simple</groupId>
            <artifactId>json-simple</artifactId>
        </dependency>
 </dependencies>
```

#Future Work
Handle merging or splitting of shards in kinesis, Trident spout implementation and metrics

## Committer Sponsors

 * Sriharsha Chintalapani ([sriharsha@apache.org](mailto:sriharsha@apache.org))
