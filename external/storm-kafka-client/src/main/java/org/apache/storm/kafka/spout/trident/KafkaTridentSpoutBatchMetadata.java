/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.kafka.spout.trident;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps transaction batch information.
 */
public class KafkaTridentSpoutBatchMetadata implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTridentSpoutBatchMetadata.class);

    public static final String FIRST_OFFSET_KEY = "firstOffset";
    public static final String LAST_OFFSET_KEY = "lastOffset";
    public static final String TOPOLOGY_ID_KEY = "topologyId";

    // first offset of this batch
    private final long firstOffset;
    // last offset of this batch
    private final long lastOffset;
    //The unique topology id for the topology that created this metadata
    private final String topologyId;

    /**
     * Builds a metadata object.
     *
     * @param firstOffset The first offset for the batch
     * @param lastOffset The last offset for the batch
     */
    public KafkaTridentSpoutBatchMetadata(long firstOffset, long lastOffset, String topologyId) {
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
        this.topologyId = topologyId;
        LOG.debug("Created {}", this.toString());
    }

    public long getFirstOffset() {
        return firstOffset;
    }

    public long getLastOffset() {
        return lastOffset;
    }

    public String getTopologyId() {
        return topologyId;
    }

    /**
     * Constructs a metadata object from a Map in the format produced by {@link #toMap() }.
     *
     * @param map The source map
     * @return A new metadata object
     */
    public static KafkaTridentSpoutBatchMetadata fromMap(Map<String, Object> map) {
        return new KafkaTridentSpoutBatchMetadata(
            ((Number) map.get(FIRST_OFFSET_KEY)).longValue(),
            ((Number) map.get(LAST_OFFSET_KEY)).longValue(),
            (String) map.get(TOPOLOGY_ID_KEY)
        );
    }

    /**
     * Writes this metadata object to a Map so Trident can read/write it to Zookeeper.
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put(FIRST_OFFSET_KEY, firstOffset);
        map.put(LAST_OFFSET_KEY, lastOffset);
        map.put(TOPOLOGY_ID_KEY, topologyId);
        return map;
    }

    @Override
    public final String toString() {
        return "KafkaTridentSpoutBatchMetadata{"
            + "firstOffset=" + firstOffset
            + ", lastOffset=" + lastOffset
            + ", topologyId=" + topologyId
            + '}';
    }
}
