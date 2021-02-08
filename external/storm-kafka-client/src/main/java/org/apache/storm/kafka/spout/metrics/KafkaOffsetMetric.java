/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.kafka.spout.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.storm.kafka.spout.internal.OffsetManager;
import org.apache.storm.metric.api.IMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used compute the partition and topic level offset metrics.
 * <p>
 * Partition level metrics are:
 * topicName/partition_{number}/earliestTimeOffset //gives beginning offset of the partition
 * topicName/partition_{number}/latestTimeOffset //gives end offset of the partition
 * topicName/partition_{number}/latestEmittedOffset //gives latest emitted offset of the partition from the spout
 * topicName/partition_{number}/latestCompletedOffset //gives latest committed offset of the partition from the spout
 * topicName/partition_{number}/spoutLag // the delta between the latest Offset and latestCompletedOffset
 * topicName/partition_{number}/recordsInPartition // total number of records in the partition
 * </p>
 * <p>
 * Topic level metrics are:
 * topicName/totalEarliestTimeOffset //gives the total beginning offset of all the associated partitions of this spout
 * topicName/totalLatestTimeOffset //gives the total end offset of all the associated partitions of this spout
 * topicName/totalLatestEmittedOffset //gives the total latest emitted offset of all the associated partitions of this spout
 * topicName/totalLatestCompletedOffset //gives the total latest committed offset of all the associated partitions of this spout
 * topicName/spoutLag // total spout lag of all the associated partitions of this spout
 * topicName/totalRecordsInPartitions //total number of records in all the associated partitions of this spout
 * </p>
 */
public class KafkaOffsetMetric<K, V> implements IMetric {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetMetric.class);
    private final Supplier<Map<TopicPartition, OffsetManager>> offsetManagerSupplier;
    private final Supplier<Consumer<K, V>> consumerSupplier;

    public KafkaOffsetMetric(Supplier<Map<TopicPartition, OffsetManager>> offsetManagerSupplier,
        Supplier<Consumer<K, V>> consumerSupplier) {
        this.offsetManagerSupplier = offsetManagerSupplier;
        this.consumerSupplier = consumerSupplier;
    }

    @Override
    public Object getValueAndReset() {

        Map<TopicPartition, OffsetManager> offsetManagers = offsetManagerSupplier.get();
        Consumer<K, V> consumer = consumerSupplier.get();

        if (offsetManagers == null || offsetManagers.isEmpty() || consumer == null) {
            LOG.debug("Metrics Tick: offsetManagers or kafkaConsumer is null.");
            return null;
        }

        Map<String, TopicMetrics> topicMetricsMap = new HashMap<>();
        Set<TopicPartition> topicPartitions = offsetManagers.keySet();

        Map<TopicPartition, Long> beginningOffsets;
        Map<TopicPartition, Long> endOffsets;

        try {
            beginningOffsets = consumer.beginningOffsets(topicPartitions);
            endOffsets = consumer.endOffsets(topicPartitions);
        } catch (RetriableException e) {
            LOG.warn("Failed to get offsets from Kafka! Will retry on next metrics tick.", e);
            return null;
        }

        //map to hold partition level and topic level metrics
        Map<String, Long> result = new HashMap<>();

        for (Map.Entry<TopicPartition, OffsetManager> entry : offsetManagers.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            OffsetManager offsetManager = entry.getValue();

            long latestTimeOffset = endOffsets.get(topicPartition);
            long earliestTimeOffset = beginningOffsets.get(topicPartition);

            long latestEmittedOffset = offsetManager.getLatestEmittedOffset();
            long latestCompletedOffset = offsetManager.getCommittedOffset();
            long spoutLag = latestTimeOffset - latestCompletedOffset;
            long recordsInPartition =  latestTimeOffset - earliestTimeOffset;

            String metricPath = topicPartition.topic()  + "/partition_" + topicPartition.partition();
            result.put(metricPath + "/" + "spoutLag", spoutLag);
            result.put(metricPath + "/" + "earliestTimeOffset", earliestTimeOffset);
            result.put(metricPath + "/" + "latestTimeOffset", latestTimeOffset);
            result.put(metricPath + "/" + "latestEmittedOffset", latestEmittedOffset);
            result.put(metricPath + "/" + "latestCompletedOffset", latestCompletedOffset);
            result.put(metricPath + "/" + "recordsInPartition", recordsInPartition);

            TopicMetrics topicMetrics = topicMetricsMap.get(topicPartition.topic());
            if (topicMetrics == null) {
                topicMetrics = new TopicMetrics();
                topicMetricsMap.put(topicPartition.topic(), topicMetrics);
            }

            topicMetrics.totalSpoutLag += spoutLag;
            topicMetrics.totalEarliestTimeOffset += earliestTimeOffset;
            topicMetrics.totalLatestTimeOffset += latestTimeOffset;
            topicMetrics.totalLatestEmittedOffset += latestEmittedOffset;
            topicMetrics.totalLatestCompletedOffset += latestCompletedOffset;
            topicMetrics.totalRecordsInPartitions += recordsInPartition;
        }

        for (Map.Entry<String, TopicMetrics> e : topicMetricsMap.entrySet()) {
            String topic = e.getKey();
            TopicMetrics topicMetrics = e.getValue();
            result.put(topic + "/" + "totalSpoutLag", topicMetrics.totalSpoutLag);
            result.put(topic + "/" + "totalEarliestTimeOffset", topicMetrics.totalEarliestTimeOffset);
            result.put(topic + "/" + "totalLatestTimeOffset", topicMetrics.totalLatestTimeOffset);
            result.put(topic + "/" + "totalLatestEmittedOffset", topicMetrics.totalLatestEmittedOffset);
            result.put(topic + "/" + "totalLatestCompletedOffset", topicMetrics.totalLatestCompletedOffset);
            result.put(topic + "/" + "totalRecordsInPartitions", topicMetrics.totalRecordsInPartitions);
        }

        LOG.debug("Metrics Tick: value : {}", result);
        return result;
    }

    private class TopicMetrics {
        long totalSpoutLag = 0;
        long totalEarliestTimeOffset = 0;
        long totalLatestTimeOffset = 0;
        long totalLatestEmittedOffset = 0;
        long totalLatestCompletedOffset = 0;
        long totalRecordsInPartitions = 0;
    }
}
