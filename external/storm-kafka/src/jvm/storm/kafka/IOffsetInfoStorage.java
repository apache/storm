package storm.kafka;

import java.util.Map;

/**
 * Created by olgagorun on 5/25/15.
 */
public interface IOffsetInfoStorage {

    public void set(String spoutId, String partitionId, Map<Object,Object> data);

    public Map<Object,Object> get(String spoutId, String partitionId);

    public void close();
}
