/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.kafka.spout.metrics2;

import static org.apache.storm.kafka.spout.metrics2.KafkaOffsetUtil.getBeginningOffsets;
import static org.apache.storm.kafka.spout.metrics2.KafkaOffsetUtil.getEndOffsets;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.TopicPartition;
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
    private final Supplier<Admin> adminSupplier;

    private TopicPartition topicPartition;

    public KafkaOffsetPartitionMetrics(Supplier<Map<TopicPartition, OffsetManager>> offsetManagerSupplier,
                                       Supplier<Admin> adminSupplier,
                                       TopicPartition topicPartition) {
        this.offsetManagerSupplier = offsetManagerSupplier;
        this.adminSupplier = adminSupplier;
        this.topicPartition = topicPartition;

        LOG.info("Running KafkaOffsetMetricSet");
    }

    @Override
    public Map<String, Metric> getMetrics() {
        Map<String, Metric> metrics = new HashMap();

        String metricPath = topicPartition.topic() + "/partition_" + topicPartition.partition();
        Gauge<Long> spoutLagGauge = () -> {
            Map<TopicPartition, Long> endOffsets = getEndOffsets(Collections.singleton(topicPartition), adminSupplier);
            if (endOffsets == null || endOffsets.isEmpty()) {
                LOG.error("Failed to get endOffsets from Kafka for topic partitions: {}.", topicPartition);
                return 0L;
            }
            OffsetManager offsetManager = offsetManagerSupplier.get().get(topicPartition);
            return endOffsets.get(topicPartition) - offsetManager.getCommittedOffset();
        };

        Gauge<Long> earliestTimeOffsetGauge = () -> {
            Map<TopicPartition, Long> beginningOffsets = getBeginningOffsets(Collections.singleton(topicPartition), adminSupplier);
            if (beginningOffsets == null || beginningOffsets.isEmpty()) {
                LOG.error("Failed to get beginningOffsets from Kafka for topic partitions: {}.", topicPartition);
                return 0L;
            }
            return beginningOffsets.get(topicPartition);
        };

        Gauge<Long> latestTimeOffsetGauge = () -> {
            Map<TopicPartition, Long> endOffsets = getEndOffsets(Collections.singleton(topicPartition), adminSupplier);
            if (endOffsets == null || endOffsets.isEmpty()) {
                LOG.error("Failed to get endOffsets from Kafka for topic partitions: {}.", topicPartition);
                return 0L;
            }
            return endOffsets.get(topicPartition);
        };

        Gauge<Long> latestEmittedOffsetGauge = () -> {
            OffsetManager offsetManager = offsetManagerSupplier.get().get(topicPartition);
            return offsetManager.getLatestEmittedOffset();
        };

        Gauge<Long> latestCompletedOffsetGauge = () -> {
            OffsetManager offsetManager = offsetManagerSupplier.get().get(topicPartition);
            return offsetManager.getCommittedOffset();
        };

        Gauge<Long> recordsInPartitionGauge = () -> {
            Map<TopicPartition, Long> endOffsets = getEndOffsets(Collections.singleton(topicPartition), adminSupplier);
            if (endOffsets == null || endOffsets.isEmpty()) {
                LOG.error("Failed to get endOffsets from Kafka for topic partitions: {}.", topicPartition);
                return 0L;
            }
            Map<TopicPartition, Long> beginningOffsets = getBeginningOffsets(Collections.singleton(topicPartition), adminSupplier);
            if (beginningOffsets == null || beginningOffsets.isEmpty()) {
                LOG.error("Failed to get beginningOffsets from Kafka for topic partitions: {}.", topicPartition);
                return 0L;
            }
            return endOffsets.get(topicPartition) - beginningOffsets.get(topicPartition);
        };

        metrics.put(metricPath + "/" + "spoutLag", spoutLagGauge);
        metrics.put(metricPath + "/" + "earliestTimeOffset", earliestTimeOffsetGauge);
        metrics.put(metricPath + "/" + "latestTimeOffset", latestTimeOffsetGauge);
        metrics.put(metricPath + "/" + "latestEmittedOffset", latestEmittedOffsetGauge);
        metrics.put(metricPath + "/" + "latestCompletedOffset", latestCompletedOffsetGauge);
        metrics.put(metricPath + "/" + "recordsInPartition", recordsInPartitionGauge);

        return metrics;
    }
}
