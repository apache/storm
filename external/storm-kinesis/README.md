# Amazon Kinesis Storm Spout

This is the first take on Amazon Kinesis Streams spout taken from  (https://github.com/awslabs/kinesis-storm-spout). It is a work in progress and expected to change significantly in near future

The **Amazon Kinesis Storm spout** helps Java developers integrate [Amazon Kinesis][aws-kinesis] with [Storm](http://storm.apache.org).

## Requirements

 + [AWS SDK for Java](http://aws.amazon.com/sdkforjava)
 + [Java 1.7 (Java SE 7)](http://www.oracle.com/technetwork/java/javase/overview/index.html) or later
 + [Apache Commons Lang](http://commons.apache.org/proper/commons-lang/) 3.0 or later
 + [Google Guava](https://code.google.com/p/guava-libraries/) 13.0 or later
 + And, of course, [Amazon Kinesis](aws-kinesis) and [Storm](http://storm-project.net/)

## Overview

The Amazon Kinesis Storm spout fetches data records from Amazon Kinesis and emits them as tuples. The spout stores checkpoint state in [ZooKeeper](http://zookeeper.apache.org/) to track the current position in the stream.

The Amazon Kinesis Storm spout can be configured to retry failed records. By default, it retries a failed record 3 times. If a record fails and the retry limit has been reached, the spout will log an error and skip over the record. The spout buffers pending records in memory, so it can re-emit a failed record without having to re-fetch the record from Amazon Kinesis. The spout sets the checkpoint to the highest sequence number that has been ack'ed (or exhausted retry attempts).

To use the spout, you'll need to add it to your Storm topology. 

+ **KinesisSpout**: Constructs an instance of the spout, using your AWS credentials and the configuration specified in KinesisSpoutConfig (as well as com.amazonaws.ClientConfiguration, via the AWS SDK). Each task executed by the spout operates on a distinct set of Amazon Kinesis shards. Shard states are periodically committed to ZooKeeper. When the spout is deactivated, it will disconnect from ZooKeeper, but the spout will continue monitoring its local state so you can activate it again later.
+ **KinesisSpoutConfig**: Configures the spout, including the Storm topology name, the Amazon Kinesis stream name, the endpoint for connecting to ZooKeeper, and the prefix for the ZooKeeper paths where the spout state is stored. See the samples folder for configuration examples.
+ **DefaultKinesisRecordScheme**: This default scheme, used by the sample topology, emits a tuple of `(partitionKey, record)`. If you want to emit more structured data, you can provide your own implementation of IKinesisRecordScheme.

The samples folder includes a sample topology and sample bolt, using the number of Amazon Kinesis shards as the parallelism hint for the spout. For more information about Storm topologies and bolts, see the [Storm documentation](http://storm.apache.org/documentation/Home.html).

## Using the Sample

1. Edit the *.properties file to configure your Storm topology, Amazon Kinesis stream, and ZooKeeper details. For your AWS Credentials, we recommend using IAM roles on Amazon EC2 when possible. You can also specify your credentials using system properties, environment variables, or AwsCredentials.properties.
2. Package the spout and the sample (including all dependencies but excluding Storm itself) into one JAR file.
3. Deploy the package to Storm via the JAR file, e.g., `storm jar my-spout-sample.jar SampleTopology sample.properties RemoteMode` 

## Related Resources

[Amazon Kinesis Developer Guide](http://docs.aws.amazon.com/kinesis/latest/dev/introduction.html)  
[Amazon Kinesis API Reference](http://docs.aws.amazon.com/kinesis/latest/APIReference/Welcome.html)  

[Amazon Kinesis Client Library](https://github.com/awslabs/amazon-kinesis-client)  
[Amazon Kinesis Connector Library](https://github.com/awslabs/amazon-kinesis-connectors)

[aws-kinesis]: http://aws.amazon.com/kinesis/
