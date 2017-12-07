package org.apache.storm.metricstore.rocksdb;

import org.apache.storm.metricstore.AggLevel;
import org.junit.Assert;
import org.junit.Test;

public class RocksDbKeyTest {

    @Test
    public void testConstructors() {
        byte[] raw = new byte[RocksDbKey.KEY_SIZE];
        raw[0] = KeyType.COMPONENT_STRING.getValue();
        raw[2] = 0x01;
        raw[3] = 0x02;
        raw[4] = 0x03;
        raw[5] = 0x04;
        RocksDbKey rawKey = new RocksDbKey(raw);

        RocksDbKey metadataKey = new RocksDbKey(KeyType.COMPONENT_STRING, 0x01020304);
        Assert.assertEquals(0, metadataKey.compareTo(rawKey));
        Assert.assertEquals(KeyType.COMPONENT_STRING, metadataKey.getType());

        metadataKey = new RocksDbKey(KeyType.TOPOLOGY_STRING, 0x01020304);
        Assert.assertTrue(metadataKey.compareTo(rawKey) < 0);
        Assert.assertEquals(KeyType.TOPOLOGY_STRING, metadataKey.getType());

        metadataKey = new RocksDbKey(KeyType.COMPONENT_STRING, 0x01020305);
        Assert.assertTrue(metadataKey.compareTo(rawKey) > 0);

        Assert.assertEquals(new Integer(0x01020304), rawKey.getTopologyId());
        Assert.assertEquals(KeyType.COMPONENT_STRING, rawKey.getType());
    }

    @Test
    public void testMetricKey() {
        AggLevel aggLevel = AggLevel.AGG_LEVEL_10_MIN;
        Integer topologyId = 0x45665;
        long timestamp = System.currentTimeMillis();
        Integer metricId = 0xF3916034;
        Integer componentId = 0x82915031;
        Integer executorId = 0x434738;
        Integer hostId = 0x4348394;
        Integer port = 3456;
        Integer streamId = 0x84221956;
        RocksDbKey key = RocksDbKey.createMetricKey(aggLevel, topologyId, timestamp, metricId,
                componentId, executorId, hostId, port, streamId);
        Assert.assertEquals(topologyId, key.getTopologyId());
        Assert.assertEquals(timestamp, key.getTimestamp());
        Assert.assertEquals(metricId, key.getMetricId());
        Assert.assertEquals(componentId, key.getComponentId());
        Assert.assertEquals(executorId, key.getExecutorId());
        Assert.assertEquals(hostId, key.getHostnameId());
        Assert.assertEquals(port, key.getPort());
        Assert.assertEquals(streamId, key.getStreamId());
    }
}
