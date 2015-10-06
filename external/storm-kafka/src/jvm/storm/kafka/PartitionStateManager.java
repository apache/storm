package storm.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * A partition state manager that simply encapsulates a partition in itself. Each instance of this manager keeps
 * the state of its corresponding partition.
 */
public class PartitionStateManager implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaStateStore.class);

    private Partition _partition;
    private StateStore _stateStore;

    public PartitionStateManager(Partition partition, StateStore stateStore) {
        this._partition = partition;
        this._stateStore = stateStore;
    }

    public Map<Object, Object> getState() {
        return _stateStore.readState(_partition);
    }

    public void writeState(Map<Object, Object> state) {
        _stateStore.writeState(_partition, state);
    }

    @Override
    public void close() throws IOException {
        if (_stateStore != null) {
            _stateStore.close();
        }
        LOG.info("State store closed.");
    }
}