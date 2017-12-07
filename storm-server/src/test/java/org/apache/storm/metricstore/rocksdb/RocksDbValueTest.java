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
import org.apache.storm.metricstore.Metric;
import org.apache.storm.metricstore.MetricException;
import org.junit.Assert;
import org.junit.Test;

public class RocksDbValueTest {

    @Test
    public void testMetadataConstructor() {
        long timestamp = System.currentTimeMillis();
        String s = "MyTopology123";
        RocksDbValue value = new RocksDbValue(timestamp, s);
        Assert.assertEquals(timestamp, value.getLastTimestamp());
        Assert.assertEquals(s, value.getMetdataString());

        RocksDbValue value2 = new RocksDbValue(value.getRaw());
        Assert.assertEquals(timestamp, value2.getLastTimestamp());
        Assert.assertEquals(s, value2.getMetdataString());

        int stringId = 0x509;
        RocksDbKey key = new RocksDbKey(KeyType.EXEC_ID_STRING, stringId);
        StringMetadata metadata = value2.getStringMetadata(key);
        Assert.assertEquals(stringId, metadata.getStringId());
        Assert.assertEquals(timestamp, metadata.getLastTimestamp());
        Assert.assertEquals(1, metadata.getMetadataTypes().size());
        Assert.assertEquals(KeyType.EXEC_ID_STRING, metadata.getMetadataTypes().get(0));
    }

    @Test
    public void testMetricConstructor() throws MetricException {
        Metric m = new Metric("cpu", 1L,"myTopologyId123", 1,
                "componentId1", "executorId1", "hostname1", "streamid1",
                7777, AggLevel.AGG_LEVEL_NONE);
        Metric m2 = new Metric(m);
        Metric m3 = new Metric(m);

        m.addValue(238);

        RocksDbValue value = new RocksDbValue(m);
        value.populateMetric(m2);
        Assert.assertEquals(m.getValue(), m2.getValue(), 0x001);
        Assert.assertEquals(m.getCount(), m2.getCount(), 0x001);
        Assert.assertEquals(m.getSum(), m2.getSum(), 0x001);
        Assert.assertEquals(m.getMin(), m2.getMin(), 0x001);
        Assert.assertEquals(m.getMax(), m2.getMax(), 0x001);

        RocksDbValue value2 = new RocksDbValue(value.getRaw());
        value2.populateMetric(m3);
        Assert.assertEquals(m.getValue(), m3.getValue(), 0x001);
        Assert.assertEquals(m.getCount(), m3.getCount(), 0x001);
        Assert.assertEquals(m.getSum(), m3.getSum(), 0x001);
        Assert.assertEquals(m.getMin(), m3.getMin(), 0x001);
        Assert.assertEquals(m.getMax(), m3.getMax(), 0x001);
    }
}
