package storm.kafka;

import backtype.storm.Config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static storm.kafka.SpoutConfig.STATE_STORE_KAFKA;
import static storm.kafka.SpoutConfig.STATE_STORE_ZOOKEEPER;

public class PartitionStateManagerFactory {

    private ZkStateStore sharedZkStateStore;

    private Map _stormConf;
    private SpoutConfig _spoutConfig;

    private ZkStateStore createZkStateStore(Map conf, SpoutConfig spoutConfig) {
        Map _zkStateStoreConf = new HashMap(conf);
        List<String> zkServers = _spoutConfig.zkServers;
        if (zkServers == null) {
            zkServers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        }
        Integer zkPort = _spoutConfig.zkPort;
        if (zkPort == null) {
            zkPort = ((Number) conf.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
        }
        _zkStateStoreConf.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, zkServers);
        _zkStateStoreConf.put(Config.TRANSACTIONAL_ZOOKEEPER_PORT, zkPort);
        _zkStateStoreConf.put(Config.TRANSACTIONAL_ZOOKEEPER_ROOT, _spoutConfig.zkRoot);
        return new ZkStateStore(_zkStateStoreConf, _spoutConfig);
    }

    public PartitionStateManagerFactory(Map stormConf, SpoutConfig spoutConfig) {
        this._stormConf = stormConf;
        this._spoutConfig = spoutConfig;

        // default to original storm storage format
        if (_spoutConfig.stateStore == null || STATE_STORE_ZOOKEEPER.equals(_spoutConfig.stateStore)) {
            sharedZkStateStore = createZkStateStore(_stormConf, _spoutConfig);
        }
    }

    public PartitionStateManager getInstance(Partition partition) {

        if (_spoutConfig.stateStore == null || STATE_STORE_ZOOKEEPER.equals(_spoutConfig.stateStore)) {
            return new PartitionStateManager(_stormConf,_spoutConfig,  partition, sharedZkStateStore);

        } else if (STATE_STORE_KAFKA.equals(_spoutConfig.stateStore)) {
            KafkaStateStore kafkaStateStore = new KafkaStateStore(_stormConf, _spoutConfig, partition);
            return new PartitionStateManager(_stormConf, _spoutConfig, partition, kafkaStateStore);

        } else {
            throw new RuntimeException(String.format("Invalid value defined for _spoutConfig.stateStore: %s. "
                            + "Valid values are %s, %s. Default to %s",
                    _spoutConfig.stateStore, STATE_STORE_ZOOKEEPER, STATE_STORE_KAFKA, STATE_STORE_ZOOKEEPER));
        }
    }

    public void close() {
        if (sharedZkStateStore != null) {
            sharedZkStateStore.close();
        }
    }
}
