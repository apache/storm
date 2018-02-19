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

package org.apache.storm.hbasemetricstore;

import java.nio.ByteBuffer;
import java.util.Map;
import javax.xml.bind.DatatypeConverter;
import org.apache.storm.metricstore.AggLevel;
import org.apache.storm.metricstore.Metric;
import org.apache.storm.metricstore.MetricException;

/**
 * Class implementing the key used by an HBaseMetricStore.
 */
public class HBaseMetricKey {
    private static final int METRIC_KEY_LENGTH = 29;
    private static final int NUM_INTEGER_FIELDS = 7;
    private static final byte FUZZY_MATCH = 0;
    private static final byte FUZZY_WILDCARD = 1;
    private byte[] key = new byte[METRIC_KEY_LENGTH];

    HBaseMetricKey(AggLevel aggLevel, int topologyId, int metricNameId, int componentId, int executorId, int hostId,
                   int port, int streamId) {
        ByteBuffer bb = ByteBuffer.wrap(this.key);
        bb.put(aggLevel.getValue());
        bb.putInt(topologyId);
        bb.putInt(metricNameId);
        bb.putInt(componentId);
        bb.putInt(executorId);
        bb.putInt(hostId);
        bb.putInt(port);
        bb.putInt(streamId);
    }

    HBaseMetricKey(byte[] rawKey) {
        this.key = rawKey;
    }

    private HBaseMetricKey() {
    }

    byte[] getRawKey() {
        return this.key;
    }

    /**
     * Create a fuzzy match key for a FuzzyRowFilter from an existing key to be used to perform a scan.
     *
     * @return  the fuzzy match key
     */
    HBaseMetricKey createScanKey() {
        HBaseMetricKey fuzzyInfo = new HBaseMetricKey();
        ByteBuffer bb = ByteBuffer.wrap(this.key);
        ByteBuffer fuzzyBb = ByteBuffer.wrap(fuzzyInfo.key);

        // Agg Level should always match
        bb.get();
        fuzzyBb.put(FUZZY_MATCH);

        for (int i = 0 ; i < NUM_INTEGER_FIELDS; i++) {
            int value = bb.getInt();
            byte val = (value != HBaseStore.INVALID_METADATA_STRING_ID) ? FUZZY_MATCH : FUZZY_WILDCARD;
            for (int j = 0; j < 4; j++) {
                fuzzyBb.put(val);
            }
        }

        return fuzzyInfo;
    }

    /**
     * Create a metric from the key.
     *
     * @return  the metric corresponding to the key
     */
    Metric createMetric(Map<HBaseMetadataKeyType, HBaseMetadataCache> metadataCacheMap) throws MetricException {
        ByteBuffer bb = ByteBuffer.wrap(this.key);
        byte aggLevelValue = bb.get();
        AggLevel aggLevel = AggLevel.getAggLevel(aggLevelValue);
        if (aggLevel == null) {
            throw new MetricException("Invalid aggLevel " + aggLevelValue);
        }

        int topologyId = bb.getInt();
        String topology = metadataCacheMap.get(HBaseMetadataKeyType.TOPOLOGY).lookupMetadataStringId(topologyId);
        if (topology == null) {
            throw new MetricException("Invalid topologyId " + topologyId);
        }

        int metricNameId = bb.getInt();
        String metricName = metadataCacheMap.get(HBaseMetadataKeyType.METRIC_NAME).lookupMetadataStringId(metricNameId);
        if (metricName == null) {
            throw new MetricException("Invalid metricNameId " + metricNameId);
        }

        int componentId = bb.getInt();
        String component = metadataCacheMap.get(HBaseMetadataKeyType.COMPONENT_ID).lookupMetadataStringId(componentId);
        if (component == null) {
            throw new MetricException("Invalid componentId " + componentId);
        }

        int executorId = bb.getInt();
        String executor = metadataCacheMap.get(HBaseMetadataKeyType.EXECUTOR_ID).lookupMetadataStringId(executorId);
        if (executor == null) {
            throw new MetricException("Invalid executorId " + executorId);
        }

        int hostId = bb.getInt();
        String hostname = metadataCacheMap.get(HBaseMetadataKeyType.HOST_ID).lookupMetadataStringId(hostId);
        if (hostname == null) {
            throw new MetricException("Invalid hostId " + hostId);
        }

        int port = bb.getInt();

        int streamId = bb.getInt();
        String stream = metadataCacheMap.get(HBaseMetadataKeyType.STREAM_ID).lookupMetadataStringId(streamId);
        if (stream == null) {
            throw new MetricException("Invalid streamId " + streamId);
        }

        return new Metric(metricName, 0L, topology, 0, component, executor,
                hostname, stream, port, aggLevel);
    }

    @Override
    public String toString() {
        return "[0x" + DatatypeConverter.printHexBinary(this.key) + "]";
    }
}
