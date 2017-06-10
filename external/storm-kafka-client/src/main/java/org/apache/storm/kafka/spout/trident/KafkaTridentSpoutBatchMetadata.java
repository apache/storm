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
public class KafkaTridentSpoutBatchMetadata<K,V> implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTridentSpoutBatchMetadata.class);

    private TopicPartition topicPartition;  // topic partition of this batch
    private long firstOffset;               // first offset of this batch
    private long lastOffset;                // last offset of this batch

    public KafkaTridentSpoutBatchMetadata(TopicPartition topicPartition, long firstOffset, long lastOffset) {
        this.topicPartition = topicPartition;
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
    }

    public KafkaTridentSpoutBatchMetadata(TopicPartition topicPartition, ConsumerRecords<K, V> consumerRecords, KafkaTridentSpoutBatchMetadata<K, V> lastBatch) {
        this.topicPartition = topicPartition;

        List<ConsumerRecord<K, V>> records = consumerRecords.records(topicPartition);

        if (records != null && !records.isEmpty()) {
            firstOffset = records.get(0).offset();
            lastOffset = records.get(records.size() - 1).offset();
        } else {
            if (lastBatch != null) {
                firstOffset = lastBatch.firstOffset;
                lastOffset = lastBatch.lastOffset;
            }
        }
        LOG.debug("Created {}", this);
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

    @Override
    public String toString() {
        return super.toString() +
                "{topicPartition=" + topicPartition +
                ", firstOffset=" + firstOffset +
                ", lastOffset=" + lastOffset +
                '}';
    }
}
