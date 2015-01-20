package storm.kafka.spout.partition;

import com.google.common.collect.Lists;
import storm.kafka.spout.helper.ConsumerConnectionCache;
import storm.kafka.spout.KafkaConfig;
import storm.kafka.spout.helper.KafkaUtils;
import storm.kafka.spout.helper.ZkState;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class KafkaPartitionCoordinator implements PartitionCoordinator {

    //default scope so we can access these values during test.
    long REFRESH_INTERVAL_MILLS = 60 * 60 * 1000; //1 hour

    final int taskIndex;

    private long lastRefreshTimeinMillis;

    private final int totalTasks;

    private KafkaConfig spoutConfig;

    private ConsumerConnectionCache connectionCache;

    private String topologyInstanceId;

    private ZkState state;

    private Map<String, Object> stormConf;

    private Map<Partition, PartitionManager> partitionManagerMap;

    public KafkaPartitionCoordinator(ConsumerConnectionCache consumerConnectionCache, Map conf, KafkaConfig spoutConfig, ZkState state, int taskIndex, int totalTasks, String topologyInstanceId) {
        this.connectionCache = consumerConnectionCache;
        this.stormConf = conf;
        this.spoutConfig = spoutConfig;
        this.state = state;
        this.taskIndex = taskIndex;
        this.totalTasks = totalTasks;
        this.topologyInstanceId = topologyInstanceId;
        this.partitionManagerMap = new TreeMap<Partition, PartitionManager>();
        refresh();
    }

    @Override
    public List<PartitionManager> getMyManagedPartitions() {
        if((System.currentTimeMillis() - lastRefreshTimeinMillis) >= REFRESH_INTERVAL_MILLS) {
            refresh();
        }
        return Lists.newArrayList(partitionManagerMap.values());
    }

    @Override
    public PartitionManager getManager(Partition partition) {
        if((System.currentTimeMillis() - lastRefreshTimeinMillis) >= REFRESH_INTERVAL_MILLS) {
            refresh();
        }
        return partitionManagerMap.get(partition);
    }

    @Override
    public void refresh() {
        partitionManagerMap.clear();

        GlobalPartitionInformation topicPartitionInfo = KafkaUtils.getTopicPartitionInfo(this.spoutConfig.seedBrokers, this.spoutConfig.topic);

        List<Partition> partitionsForTask = KafkaUtils.calculatePartitionsForTask(topicPartitionInfo, totalTasks, taskIndex);

        for(Partition partition : partitionsForTask) {
            PartitionManager partitionManager = new PartitionManager(connectionCache, topologyInstanceId, state, stormConf, spoutConfig, partition);
            partitionManagerMap.put(partition, partitionManager);
        }

        this.lastRefreshTimeinMillis = System.currentTimeMillis();
    }

}
