package storm.kafka;

import backtype.storm.task.TopologyContext;
import storm.kafka.trident.IBrokerReader;
import storm.kafka.trident.StaticBrokerReader;

import java.util.Map;

public class StaticKafkaFactory implements KafkaFactory {

    private final StaticHosts hosts;

    public StaticKafkaFactory(StaticHosts hosts) {
        this.hosts = hosts;
    }

    @Override
    public IBrokerReader brokerReader(Map stormConf, KafkaConfig conf) {
        return new StaticBrokerReader(hosts.getPartitionInformation());
    }

    @Override
    public PartitionCoordinator partitionCoordinator(DynamicPartitionConnections connections, ZkState state, Map conf,
                                                     final TopologyContext context, int totalTasks,
                                                     SpoutConfig kafkaConfig, String uuid) {
        return new StaticCoordinator(connections, conf, kafkaConfig, state, context.getThisTaskIndex(), totalTasks, uuid, hosts);
    }
}
