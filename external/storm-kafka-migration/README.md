## Setup
Put the storm-kafka-migration jar in a directory alongside the version of org.apache.kafka:kafka-clients that matches your broker version. You will also need to write a configuration file, an example file can be found in src/main/conf. Put the configuration file in the same directory as the two jars. You should also stop the topology you want to migrate offsets for.

## Migrating non-Trident storm-kafka offsets to storm-kafka-client
Run `java -cp "*" org.apache.storm.kafka.migration.KafkaSpoutMigration your-config-file.yaml`. The tool will print the migrated offsets to console. The offsets will be migrated into Kafka, belonging to the consumer group you set in the configuration. You need to set the same consumer group when setting up your storm-kafka-client spout.

## Migrating Trident storm-kafka offsets to storm-kafka-client
Run `java -cp "*" org.apache.storm.kafka.migration.KafkaTridentSpoutMigration your-config-file.yaml`. The tool will print the migrated offsets to console. The offsets will be migrated to the Zookeeper path you specify in the configuration. When you configure your storm-kafka-client topology, you need to use the same Zookeeper path, and the TridentDataSource.newStream txid must be the same as the "new.topology.txid" setting you specified in the configuration.