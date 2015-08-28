package storm.kafka;

import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Map;

public class ZKBackedPartitionStateManager implements PartitionStateManager  {

    public static final Logger LOG = LoggerFactory.getLogger(ZKBackedPartitionStateManager.class);

    private SpoutConfig _spoutConfig;
    private Partition _partition;
    private ZkDataStore _zkDataStore;

    public ZKBackedPartitionStateManager(Map stormConfig, SpoutConfig spoutConfig, Partition partition, ZkDataStore zkDataStore) {
        this._spoutConfig = spoutConfig;
        this._partition = partition;
        this._zkDataStore = zkDataStore;
    }

    private String committedPath() {
        return _spoutConfig.zkRoot + "/" + _spoutConfig.id + "/" + _partition.getId();
    }

    @Override
    public Map<Object, Object> getState() {
        LOG.debug("Reading from " + committedPath() + " for state data");
        try {
            byte[] b = _zkDataStore.read(committedPath());
            if (b == null) {
                return null;
            }
            return (Map<Object, Object>) JSONValue.parse(new String(b, "UTF-8"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeState(Map<Object, Object> data) {
        LOG.debug("Writing to " + committedPath() + " with stat data " + data.toString());
        _zkDataStore.write(committedPath(), JSONValue.toJSONString(data).getBytes(Charset.forName("UTF-8")));
    }
}
