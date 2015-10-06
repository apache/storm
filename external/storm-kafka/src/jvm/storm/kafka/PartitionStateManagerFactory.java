package storm.kafka;

import org.apache.commons.io.IOUtils;

import java.util.Map;

import static storm.kafka.SpoutConfig.STATE_STORE_KAFKA;
import static storm.kafka.SpoutConfig.STATE_STORE_ZOOKEEPER;

public class PartitionStateManagerFactory {

    private StateStore _stateStore;

    private StateStore createZkStateStore(Map conf, SpoutConfig spoutConfig) {
        return new ZkStateStore(conf, spoutConfig);
    }

    private StateStore createKafkaStateStore(Map conf, SpoutConfig spoutConfig) {
        return new KafkaStateStore(conf, spoutConfig);
    }

    public PartitionStateManagerFactory(Map stormConf, SpoutConfig spoutConfig) {
        // default to original storm storage format
        if (spoutConfig.stateStore == null || STATE_STORE_ZOOKEEPER.equals(spoutConfig.stateStore)) {
            _stateStore = createZkStateStore(stormConf, spoutConfig);

        } else if (STATE_STORE_KAFKA.equals(spoutConfig.stateStore)) {
            _stateStore = createKafkaStateStore(stormConf, spoutConfig);

        } else {
            throw new RuntimeException(String.format("Invalid value defined for _spoutConfig.stateStore: %s. "
                            + "Valid values are %s, %s. Default to %s",
                    spoutConfig.stateStore, STATE_STORE_ZOOKEEPER, STATE_STORE_KAFKA, STATE_STORE_ZOOKEEPER));
        }
    }

    public PartitionStateManager getInstance(Partition partition) {
        return new PartitionStateManager(partition, _stateStore);
    }

    public void close() {
        if (_stateStore != null) {
            IOUtils.closeQuietly(_stateStore);
        }
    }
}
