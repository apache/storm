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

package org.apache.storm.kafka.spout.metrics2;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.storm.kafka.spout.internal.OffsetManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Partition level metrics.
 * <p>
 * topicName/partition_{number}/earliestTimeOffset //gives beginning offset of the partition
 * topicName/partition_{number}/latestTimeOffset //gives end offset of the partition
 * topicName/partition_{number}/latestEmittedOffset //gives latest emitted offset of the partition from the spout
 * topicName/partition_{number}/latestCompletedOffset //gives latest committed offset of the partition from the spout
 * topicName/partition_{number}/spoutLag // the delta between the latest Offset and latestCompletedOffset
 * topicName/partition_{number}/recordsInPartition // total number of records in the partition
 * </p>
 */
public class KafkaOffsetPartitionMetrics<K, V> implements MetricSet {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetPartitionMetrics.class);
    private final Supplier<Map<TopicPartition, OffsetManager>> offsetManagerSupplier;
    private final Supplier<Consumer<K, V>> consumerSupplier;

    private TopicPartition topicPartition;
    private KafkaOffsetTopicMetrics topicMetrics;

    public KafkaOffsetPartitionMetrics(Supplier<Map<TopicPartition, OffsetManager>> offsetManagerSupplier,
                                       Supplier<Consumer<K, V>> consumerSupplier,
                                       TopicPartition topicPartition,
                                       KafkaOffsetTopicMetrics topicMetrics) {
        this.offsetManagerSupplier = offsetManagerSupplier;
        this.consumerSupplier = consumerSupplier;
        this.topicPartition = topicPartition;
        this.topicMetrics = topicMetrics;

        LOG.info("Running KafkaOffsetMetricSet");
    }

    @Override
    public Map<String, Metric> getMetrics() {
        Map<String, Metric> metrics = new HashMap();

        String metricPath = topicPartition.topic()  + "/partition_" + topicPartition.partition();
        Gauge<Long> spoutLagGauge = new Gauge<Long>() {
            @Override
            public Long getValue() {
                Map<TopicPartition, Long> endOffsets = getEndOffsets(Collections.singleton(topicPartition));
                if (endOffsets == null || endOffsets.isEmpty()) {
                    LOG.error("Failed to get endOffsets from Kafka for topic partitions: {}.", topicPartition);
                    return 0L;
                }
                // add value to topic level metric
                OffsetManager offsetManager = offsetManagerSupplier.get().get(topicPartition);
                Long ret = endOffsets.get(topicPartition) - offsetManager.getCommittedOffset();
                topicMetrics.totalSpoutLag += ret;
                return ret;
            }
        };

        Gauge<Long> earliestTimeOffsetGauge = new Gauge<Long>() {
            @Override
            public Long getValue() {
                Map<TopicPartition, Long> beginningOffsets = getBeginningOffsets(Collections.singleton(topicPartition));
                if (beginningOffsets == null || beginningOffsets.isEmpty()) {
                    LOG.error("Failed to get beginningOffsets from Kafka for topic partitions: {}.", topicPartition);
                    return 0L;
                }
                // add value to topic level metric
                Long ret = beginningOffsets.get(topicPartition);
                topicMetrics.totalEarliestTimeOffset += beginningOffsets.get(topicPartition);
                return ret;
            }
        };

        Gauge<Long> latestTimeOffsetGauge = new Gauge<Long>() {
            @Override
            public Long getValue() {
                Map<TopicPartition, Long> endOffsets = getEndOffsets(Collections.singleton(topicPartition));
                if (endOffsets == null || endOffsets.isEmpty()) {
                    LOG.error("Failed to get endOffsets from Kafka for topic partitions: {}.", topicPartition);
                    return 0L;
                }
                // add value to topic level metric
                Long ret = endOffsets.get(topicPartition);
                topicMetrics.totalLatestTimeOffset += ret;
                return ret;
            }
        };

        Gauge<Long> latestEmittedOffsetGauge = new Gauge<Long>() {
            @Override
            public Long getValue() {
                // add value to topic level metric
                OffsetManager offsetManager = offsetManagerSupplier.get().get(topicPartition);
                Long ret = offsetManager.getLatestEmittedOffset();
                topicMetrics.totalLatestEmittedOffset += ret;
                return ret;
            }
        };

        Gauge<Long> latestCompletedOffsetGauge = new Gauge<Long>() {
            @Override
            public Long getValue() {
                // add value to topic level metric
                OffsetManager offsetManager = offsetManagerSupplier.get().get(topicPartition);
                Long ret = offsetManager.getCommittedOffset();
                topicMetrics.totalLatestCompletedOffset += ret;
                return ret;
            }
        };

        Gauge<Long> recordsInPartitionGauge = new Gauge<Long>() {
            @Override
            public Long getValue() {
                Map<TopicPartition, Long> endOffsets = getEndOffsets(Collections.singleton(topicPartition));
                if (endOffsets == null || endOffsets.isEmpty()) {
                    LOG.error("Failed to get endOffsets from Kafka for topic partitions: {}.", topicPartition);
                    return 0L;
                }
                Map<TopicPartition, Long> beginningOffsets = getBeginningOffsets(Collections.singleton(topicPartition));
                if (beginningOffsets == null || beginningOffsets.isEmpty()) {
                    LOG.error("Failed to get beginningOffsets from Kafka for topic partitions: {}.", topicPartition);
                    return 0L;
                }
                // add value to topic level metric
                Long ret = endOffsets.get(topicPartition) - beginningOffsets.get(topicPartition);
                topicMetrics.totalRecordsInPartitions += ret;
                return ret;
            }
        };

        metrics.put(metricPath + "/" + "spoutLag", spoutLagGauge);
        metrics.put(metricPath + "/" + "earliestTimeOffset", earliestTimeOffsetGauge);
        metrics.put(metricPath + "/" + "latestTimeOffset", latestTimeOffsetGauge);
        metrics.put(metricPath + "/" + "latestEmittedOffset", latestEmittedOffsetGauge);
        metrics.put(metricPath + "/" + "latestCompletedOffset", latestCompletedOffsetGauge);
        metrics.put(metricPath + "/" + "recordsInPartition", recordsInPartitionGauge);

        return metrics;
    }

    private Map<TopicPartition, Long> getBeginningOffsets(Set<TopicPartition> topicPartitions) {
        Consumer<K, V> consumer = consumerSupplier.get();
        if (consumer == null) {
            LOG.error("Kafka consumer object is null, returning 0.");
            return Collections.EMPTY_MAP;
        }

        Map<TopicPartition, Long> beginningOffsets;
        try {
            beginningOffsets = consumer.beginningOffsets(topicPartitions);
        } catch (RetriableException e) {
            LOG.error("Failed to get offset from Kafka for topic partitions: {}.", topicPartition, e);
            return Collections.EMPTY_MAP;
        }
        return beginningOffsets;
    }

    private Map<TopicPartition, Long> getEndOffsets(Set<TopicPartition> topicPartitions) {
        Consumer<K, V> consumer = consumerSupplier.get();
        if (consumer == null) {
            LOG.error("Kafka consumer object is null, returning 0.");
            return Collections.EMPTY_MAP;
        }

        Map<TopicPartition, Long> endOffsets;
        try {
            endOffsets = consumer.endOffsets(topicPartitions);
        } catch (RetriableException e) {
            LOG.error("Failed to get offset from Kafka for topic partitions: {}.", topicPartition, e);
            return Collections.EMPTY_MAP;
        }
        return endOffsets;
    }
}
