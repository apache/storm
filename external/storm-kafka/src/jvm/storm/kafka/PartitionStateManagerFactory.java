package storm.kafka;

import backtype.storm.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static storm.kafka.SpoutConfig.STATE_STORE__KAFKA;
import static storm.kafka.SpoutConfig.STATE_STORE_ZOOKEEPER;

public class PartitionStateManagerFactory {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionStateManagerFactory.class);

    private ZkDataStore sharedZkDataStore;

    private Map _stormConf;
    private SpoutConfig _spoutConfig;


    private ZkDataStore createZkDataStore(Map conf, SpoutConfig spoutConfig) {
        Map _zkDataStoreConf = new HashMap(conf);
        List<String> zkServers = _spoutConfig.zkServers;
        if (zkServers == null) {
            zkServers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        }
        Integer zkPort = _spoutConfig.zkPort;
        if (zkPort == null) {
            zkPort = ((Number) conf.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
        }
        _zkDataStoreConf.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, zkServers);
        _zkDataStoreConf.put(Config.TRANSACTIONAL_ZOOKEEPER_PORT, zkPort);
        _zkDataStoreConf.put(Config.TRANSACTIONAL_ZOOKEEPER_ROOT, _spoutConfig.zkRoot);
        return new ZkDataStore(_zkDataStoreConf);

    }

    public PartitionStateManagerFactory(Map stormConf, SpoutConfig spoutConfig) {
        this._stormConf = stormConf;
        this._spoutConfig = spoutConfig;

        // default to original storm storage format
        if (_spoutConfig.stateStore == null || STATE_STORE_ZOOKEEPER.equals(_spoutConfig.stateStore)) {
            sharedZkDataStore = createZkDataStore(_stormConf, _spoutConfig);
        }
    }

    public PartitionStateManager getInstance(Partition partition) {

        if (_spoutConfig.stateStore == null || STATE_STORE_ZOOKEEPER.equals(_spoutConfig.stateStore)) {
            return new ZKBackedPartitionStateManager(_stormConf,_spoutConfig,  partition, sharedZkDataStore);

        } else if (STATE_STORE__KAFKA.equals(_spoutConfig.stateStore)) {
            KafkaDataStore kafkaDataStore = new KafkaDataStore(_stormConf, _spoutConfig, partition);
            return new KafkaBackedPartitionStateManager(_stormConf, _spoutConfig, partition, kafkaDataStore);

        } else {
            throw new RuntimeException(String.format("Invalid value defined for _spoutConfig.stateStore: %s. "
                            + "Valid values are %s, %s. Default to %s",
                    _spoutConfig.stateStore, STATE_STORE_ZOOKEEPER, STATE_STORE__KAFKA, STATE_STORE_ZOOKEEPER));
        }
    }

    public void close() {
        if (sharedZkDataStore != null) {
            sharedZkDataStore.close();
        }
    }
}
