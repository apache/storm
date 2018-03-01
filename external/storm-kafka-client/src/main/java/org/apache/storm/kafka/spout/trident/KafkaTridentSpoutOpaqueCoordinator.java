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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.internal.KafkaConsumerFactory;
import org.apache.storm.kafka.spout.internal.KafkaConsumerFactoryDefault;
import org.apache.storm.kafka.spout.internal.Timer;
import org.apache.storm.trident.spout.IOpaquePartitionedTridentSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTridentSpoutOpaqueCoordinator<K,V> implements IOpaquePartitionedTridentSpout.Coordinator<List<Map<String, Object>>>,
        Serializable {
    //Initial delay for the assignment refresh timer
    public static final long TIMER_DELAY_MS = 500;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTridentSpoutOpaqueCoordinator.class);

    private final TopicPartitionSerializer tpSerializer = new TopicPartitionSerializer();
    private final KafkaSpoutConfig<K, V> kafkaSpoutConfig;
    private final Timer refreshAssignmentTimer;
    private final KafkaConsumer<K, V> consumer;
    
    private Set<TopicPartition> partitionsForBatch;

    /**
     * Creates a new coordinator based on the given spout config.
     * @param kafkaSpoutConfig The spout config to use
     */
    public KafkaTridentSpoutOpaqueCoordinator(KafkaSpoutConfig<K, V> kafkaSpoutConfig) {
        this(kafkaSpoutConfig, new KafkaConsumerFactoryDefault<>());
    }
    
    KafkaTridentSpoutOpaqueCoordinator(KafkaSpoutConfig<K, V> kafkaSpoutConfig, KafkaConsumerFactory<K, V> consumerFactory) {
        this.kafkaSpoutConfig = kafkaSpoutConfig;
        this.refreshAssignmentTimer = new Timer(TIMER_DELAY_MS, kafkaSpoutConfig.getPartitionRefreshPeriodMs(), TimeUnit.MILLISECONDS);
        this.consumer = consumerFactory.createConsumer(kafkaSpoutConfig);
        LOG.debug("Created {}", this.toString());
    }

    @Override
    public boolean isReady(long txid) {
        LOG.debug("isReady = true");
        return true;    // the "old" trident kafka spout always returns true, like this
    }

    @Override
    public List<Map<String, Object>> getPartitionsForBatch() {
        if (refreshAssignmentTimer.isExpiredResetOnTrue() || partitionsForBatch == null) {
            partitionsForBatch = kafkaSpoutConfig.getTopicFilter().getAllSubscribedPartitions(consumer);
        }
        LOG.debug("TopicPartitions for batch {}", partitionsForBatch);
        return partitionsForBatch.stream()
            .map(tp -> tpSerializer.toMap(tp))
            .collect(Collectors.toList());
    }

    @Override
    public void close() {
        this.consumer.close();
        LOG.debug("Closed");
    }

    @Override
    public final String toString() {
        return super.toString()
                + "{kafkaSpoutConfig=" + kafkaSpoutConfig
                + '}';
    }
}
