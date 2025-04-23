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

import java.util.function.Supplier;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.internal.OffsetManager;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is used to manage both the partition and topic level offset metrics.
 */
public class KafkaOffsetMetricManager<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetMetricManager.class);
    private final Supplier<Map<TopicPartition, OffsetManager>> offsetManagerSupplier;
    private final Supplier<Admin> adminSupplier;
    private TopologyContext topologyContext;

    private Map<String, KafkaOffsetTopicMetrics> topicMetricsMap;
    private Map<TopicPartition, KafkaOffsetPartitionMetrics> topicPartitionMetricsMap;

    public KafkaOffsetMetricManager(Supplier<Map<TopicPartition, OffsetManager>> offsetManagerSupplier,
                                    Supplier<Admin> adminSupplier,
                                    TopologyContext topologyContext) {
        this.offsetManagerSupplier = offsetManagerSupplier;
        this.adminSupplier = adminSupplier;
        this.topologyContext = topologyContext;

        this.topicMetricsMap = new HashMap<>();
        this.topicPartitionMetricsMap = new HashMap<>();
        LOG.info("Running KafkaOffsetMetricManager");
    }

    public void registerPartitionAndTopicLevelMetrics(Set<TopicPartition> newAssignment) {

        KafkaOffsetPartitionAndTopicMetrics topicPartitionMetricSet
                = new KafkaOffsetPartitionAndTopicMetrics(offsetManagerSupplier, adminSupplier, newAssignment);
        topologyContext.registerMetricSet("kafkaOffset", topicPartitionMetricSet);
    }

    public Map<TopicPartition, KafkaOffsetPartitionMetrics> getTopicPartitionMetricsMap() {
        return topicPartitionMetricsMap;
    }

    public Map<String, KafkaOffsetTopicMetrics> getTopicMetricsMap() {
        return topicMetricsMap;
    }
}
