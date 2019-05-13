## Usage
This module contains example topologies demonstrating storm-kafka-client spout and Trident usage. Please ensure you have a Kafka instance running at localhost:9092 before you deploy the topologies.

The module is built by running `mvn clean package -Dstorm.kafka.client.version=<kafka_broker_version>`, where the property should match the Kafka version you want to use. For example, for Kafka 0.11.0.0 the `<kafka_broker_version>` would be `0.11.0.0`. This will generate the `target/storm-kafka-client-examples-VERSION.jar` file. The jar contains all dependencies and can be submitted to Storm via the Storm CLI, e.g.
```
storm jar storm-kafka-client-examples-2.0.0-SNAPSHOT.jar org.apache.storm.kafka.spout.KafkaSpoutTopologyMainNamedTopics
```
will submit the topologies set up by KafkaSpoutTopologyMainNamedTopics to Storm.

Note that this example produces a jar containing all dependencies for ease of use. When you deploy your own topologies in a production environment you may want to reduce the jar size by extracting some dependencies (e.g. `org.apache.kafka:kafka-clients`) from the jar. You can do this by setting the dependencies you don't want to include in the jars to `provided` scope, and then using the `--artifacts` flag for the `storm jar` command to fetch the dependencies when submitting the topology. See the [CLI documentation](http://storm.apache.org/releases/2.0.0-SNAPSHOT/Command-line-client.html) for syntax.