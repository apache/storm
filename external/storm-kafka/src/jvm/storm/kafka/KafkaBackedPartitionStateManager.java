package storm.kafka;

import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KafkaBackedPartitionStateManager implements PartitionStateManager {

    public static final Logger LOG = LoggerFactory.getLogger(KafkaBackedPartitionStateManager.class);

    private KafkaDataStore _dataStore;

    public KafkaBackedPartitionStateManager(Map stormConf, SpoutConfig spoutConfig, Partition partition, KafkaDataStore dataStore) {
        this._dataStore = dataStore;
    }

    @Override
    public Map<Object, Object> getState() {
        return  (Map<Object, Object>) JSONValue.parse(_dataStore.read());
    }

    @Override
    public void writeState(Map<Object, Object> data) {
        assert data.containsKey("offset");

        Long offsetOfPartition = (Long)data.get("offset");
        String stateData = JSONValue.toJSONString(data);
        _dataStore.write(offsetOfPartition, stateData);
    }
}