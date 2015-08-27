package storm.kafka;

import java.util.Map;

public interface PartitionStateManager {

    Map<Object, Object> getState();

    void writeState(Map<Object, Object> data);
}