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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

/**
 * Wraps transaction batch information
 */
public class KafkaTridentSpoutBatchMetadata implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTridentSpoutBatchMetadata.class);
    private static final TopicPartitionSerializer TP_SERIALIZER = new TopicPartitionSerializer();
    
    public static final String TOPIC_PARTITION_KEY = "tp";
    public static final String FIRST_OFFSET_KEY = "firstOffset";
    public static final String LAST_OFFSET_KEY = "lastOffset";
    
    // topic partition of this batch
    private final TopicPartition topicPartition;  
    // first offset of this batch
    private final long firstOffset;               
    // last offset of this batch
    private final long lastOffset;

    /**
     * Builds a metadata object.
     * @param topicPartition The topic partition
     * @param firstOffset The first offset for the batch
     * @param lastOffset The last offset for the batch
     */
    public KafkaTridentSpoutBatchMetadata(TopicPartition topicPartition, long firstOffset, long lastOffset) {
        this.topicPartition = topicPartition;
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
    }

    /**
     * Builds a metadata object from a non-empty set of records.
     * @param topicPartition The topic partition the records belong to.
     * @param consumerRecords The non-empty set of records.
     */
    public <K, V> KafkaTridentSpoutBatchMetadata(TopicPartition topicPartition, ConsumerRecords<K, V> consumerRecords) {
        Validate.notNull(consumerRecords.records(topicPartition));
        List<ConsumerRecord<K, V>> records = consumerRecords.records(topicPartition);
        Validate.isTrue(!records.isEmpty(), "There must be at least one record in order to build metadata");
        
        this.topicPartition = topicPartition;
        firstOffset = records.get(0).offset();
        lastOffset = records.get(records.size() - 1).offset();
        LOG.debug("Created {}", this.toString());
    }

    public long getFirstOffset() {
        return firstOffset;
    }

    public long getLastOffset() {
        return lastOffset;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }
    
    /**
     * Constructs a metadata object from a Map in the format produced by {@link #toMap() }.
     * @param map The source map
     * @return A new metadata object
     */
    public static KafkaTridentSpoutBatchMetadata fromMap(Map<String, Object> map) {
        Map<String, Object> topicPartitionMap = (Map<String, Object>)map.get(TOPIC_PARTITION_KEY);
        TopicPartition tp = TP_SERIALIZER.fromMap(topicPartitionMap);
        return new KafkaTridentSpoutBatchMetadata(tp, ((Number)map.get(FIRST_OFFSET_KEY)).longValue(),
            ((Number)map.get(LAST_OFFSET_KEY)).longValue());
    }
    
    /**
     * Writes this metadata object to a Map so Trident can read/write it to Zookeeper.
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put(TOPIC_PARTITION_KEY, TP_SERIALIZER.toMap(topicPartition));
        map.put(FIRST_OFFSET_KEY, firstOffset);
        map.put(LAST_OFFSET_KEY, lastOffset);
        return map;
    }

    @Override
    public final String toString() {
        return super.toString() +
                "{topicPartition=" + topicPartition +
                ", firstOffset=" + firstOffset +
                ", lastOffset=" + lastOffset +
                '}';
    }
}
