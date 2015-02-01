package storm.kafka;

import backtype.storm.task.TopologyContext;
import storm.kafka.trident.IBrokerReader;
import storm.kafka.trident.ZkBrokerReader;

import java.util.Map;

public class ZkKafkaFactory implements KafkaFactory {

    private final ZkHosts hosts;

    public ZkKafkaFactory(ZkHosts hosts){
        this.hosts = hosts;
    }

    @Override
    public IBrokerReader brokerReader(Map stormConf, KafkaConfig conf) {
        return new ZkBrokerReader(stormConf, conf.topic, hosts);
    }

    @Override
    public PartitionCoordinator partitionCoordinator(DynamicPartitionConnections connections, ZkState state, Map conf,
                                                     TopologyContext context, int totalTasks,
                                                     SpoutConfig kafkaConfig, String uuid) {
        return new ZkCoordinator(connections, conf, kafkaConfig, state, context.getThisTaskIndex(), totalTasks, uuid, hosts);
    }
}
