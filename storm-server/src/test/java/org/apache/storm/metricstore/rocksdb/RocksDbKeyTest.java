/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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

        Assert.assertEquals(0x01020304, rawKey.getTopologyId());
        Assert.assertEquals(KeyType.COMPONENT_STRING, rawKey.getType());
    }

    @Test
    public void testMetricKey() {
        AggLevel aggLevel = AggLevel.AGG_LEVEL_10_MIN;
        int topologyId = 0x45665;
        long timestamp = System.currentTimeMillis();
        int metricId = 0xF3916034;
        int componentId = 0x82915031;
        int executorId = 0x434738;
        int hostId = 0x4348394;
        int port = 3456;
        int streamId = 0x84221956;
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
