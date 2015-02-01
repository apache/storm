package storm.kafka;


import backtype.storm.task.TopologyContext;
import storm.kafka.trident.IBrokerReader;

import java.io.Serializable;
import java.util.Map;

public interface KafkaFactory extends Serializable {

    public IBrokerReader brokerReader(Map stormConf, KafkaConfig conf);

    public PartitionCoordinator partitionCoordinator(DynamicPartitionConnections connections, ZkState state,
                                                     Map conf, final TopologyContext context, int totalTasks,
                                                     SpoutConfig kafkaConfig, String uuid);

}
