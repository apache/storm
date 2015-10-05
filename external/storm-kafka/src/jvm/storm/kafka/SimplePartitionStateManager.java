package storm.kafka;

import java.util.Map;

/**
 * A partition state manager that simply encapsulates a partition in itself. Each instance of this manager keeps
 * the state of its corresponding partition.
 */
public class SimplePartitionStateManager implements PartitionStateManager  {

    private SpoutConfig _spoutConfig;
    private Partition _partition;
    private StateStore _stataStore;

    public SimplePartitionStateManager(Map stormConfig, SpoutConfig spoutConfig, Partition partition, StateStore stateStore) {
        this._spoutConfig = spoutConfig;
        this._partition = partition;
        this._stataStore = stateStore;
    }

    @Override
    public Map<Object, Object> getState() {
        return _stataStore.readState(_partition);
    }

    @Override
    public void writeState(Map<Object, Object> state) {
        _stataStore.writeState(_partition, state);
    }
}