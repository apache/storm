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
import java.util.Set;
import java.util.function.Supplier;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.internal.OffsetManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Topic level metrics.
 * <p>
 * topicName/totalEarliestTimeOffset //gives the total beginning offset of all the associated partitions of this spout
 * topicName/totalLatestTimeOffset //gives the total end offset of all the associated partitions of this spout
 * topicName/totalLatestEmittedOffset //gives the total latest emitted offset of all the associated partitions of this spout
 * topicName/totalLatestCompletedOffset //gives the total latest committed offset of all the associated partitions of this spout
 * topicName/spoutLag // total spout lag of all the associated partitions of this spout
 * topicName/totalRecordsInPartitions //total number of records in all the associated partitions of this spout
 * </p>
 */
public class KafkaOffsetTopicMetrics implements MetricSet {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetTopicMetrics.class);

    private String topic;
    Set<TopicPartition> assignment;
    Supplier<Map<TopicPartition, OffsetManager>> offsetManagerSupplier;
    Supplier<Admin> adminSupplier;

    public KafkaOffsetTopicMetrics(String topic, Supplier<Map<TopicPartition,
                                           OffsetManager>> offsetManagerSupplier,
                                   Supplier<Admin> adminSupplier,
                                   Set<TopicPartition> newAssignment) {
        this.topic = topic;
        this.assignment = newAssignment;
        this.offsetManagerSupplier = offsetManagerSupplier;
        this.adminSupplier = adminSupplier;
        LOG.info("Create KafkaOffsetTopicMetrics for topic: {}", topic);
    }

    @Override
    public Map<String, Metric> getMetrics() {
        Map<String, Metric> metrics = new HashMap();

        Gauge<Long> totalSpoutLagGauge = () -> {
            Long totalSpoutLag = 0L;
            for (TopicPartition topicPartition : assignment) {
                String topicOfPartition = topicPartition.topic();
                if (topicOfPartition.equals(topic)) {
                    Map<TopicPartition, Long> endOffsets = getEndOffsets(Collections.singleton(topicPartition), adminSupplier);
                    if (endOffsets == null || endOffsets.isEmpty()) {
                        LOG.error("Failed to get endOffsets from Kafka for topic partitions: {}.", topicPartition);
                        return 0L;
                    }
                    // add value to topic level metric
                    OffsetManager offsetManager = offsetManagerSupplier.get().get(topicPartition);
                    Long ret = endOffsets.get(topicPartition) - offsetManager.getCommittedOffset();
                    totalSpoutLag += ret;
                }
            }
            return totalSpoutLag;
        };

        Gauge<Long> totalEarliestTimeOffsetGauge = () -> {

            Long totalEarliestTimeOffset = 0L;

            for (TopicPartition topicPartition : assignment) {
                String topicOfPartition = topicPartition.topic();
                if (topicOfPartition.equals(topic)) {
                    Map<TopicPartition, Long> beginningOffsets = getBeginningOffsets(Collections.singleton(topicPartition), adminSupplier);
                    if (beginningOffsets == null || beginningOffsets.isEmpty()) {
                        LOG.error("Failed to get beginningOffsets from Kafka for topic partitions: {}.", topicPartition);
                        return 0L;
                    }
                    // add value to topic level metric
                    Long ret = beginningOffsets.get(topicPartition);
                    totalEarliestTimeOffset += ret;
                }

            }
            return totalEarliestTimeOffset;
        };

        Gauge<Long> totalLatestTimeOffsetGauge = () -> {

            Long totalLatestTimeOffset = 0L;
            for (TopicPartition topicPartition : assignment) {
                String topicOfPartition = topicPartition.topic();
                if (topicOfPartition.equals(topic)) {
                    Map<TopicPartition, Long> endOffsets = getEndOffsets(Collections.singleton(topicPartition), adminSupplier);
                    if (endOffsets == null || endOffsets.isEmpty()) {
                        LOG.error("Failed to get endOffsets from Kafka for topic partitions: {}.", topicPartition);
                        return 0L;
                    }
                    // add value to topic level metric
                    Long ret = endOffsets.get(topicPartition);
                    totalLatestTimeOffset += ret;
                }
            }
            return totalLatestTimeOffset;
        };

        Gauge<Long> totalLatestEmittedOffsetGauge = () -> {

            Long totalLatestEmittedOffset = 0L;
            for (TopicPartition topicPartition : assignment) {
                String topicOfPartition = topicPartition.topic();
                if (topicOfPartition.equals(topic)) {
                    OffsetManager offsetManager = offsetManagerSupplier.get().get(topicPartition);
                    Long ret = offsetManager.getLatestEmittedOffset();
                    totalLatestEmittedOffset += ret;
                }

            }
            return totalLatestEmittedOffset;
        };

        Gauge<Long> totalLatestCompletedOffsetGauge = () -> {

            Long totalLatestCompletedOffset = 0L;
            for (TopicPartition topicPartition : assignment) {
                String topicOfPartition = topicPartition.topic();
                if (topicOfPartition.equals(topic)) {
                    // add value to topic level metric
                    OffsetManager offsetManager = offsetManagerSupplier.get().get(topicPartition);
                    Long ret = offsetManager.getCommittedOffset();
                    totalLatestCompletedOffset += ret;
                }
            }

            return totalLatestCompletedOffset;
        };

        Gauge<Long> totalRecordsInPartitionsGauge = () -> {
            Long totalRecordsInPartitions = 0L;
            for (TopicPartition topicPartition : assignment) {
                String topicOfPartition = topicPartition.topic();
                if (topicOfPartition.equals(topic)) {
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
                    // add value to topic level metric
                    Long ret = endOffsets.get(topicPartition) - beginningOffsets.get(topicPartition);
                    totalRecordsInPartitions += ret;
                }
            }
            return totalRecordsInPartitions;
        };

        metrics.put(topic + "/" + "totalSpoutLag", totalSpoutLagGauge);
        metrics.put(topic + "/" + "totalEarliestTimeOffset", totalEarliestTimeOffsetGauge);
        metrics.put(topic + "/" + "totalLatestTimeOffset", totalLatestTimeOffsetGauge);
        metrics.put(topic + "/" + "totalLatestEmittedOffset", totalLatestEmittedOffsetGauge);
        metrics.put(topic + "/" + "totalLatestCompletedOffset", totalLatestCompletedOffsetGauge);
        metrics.put(topic + "/" + "totalRecordsInPartitions", totalRecordsInPartitionsGauge);
        return metrics;
    }
}
