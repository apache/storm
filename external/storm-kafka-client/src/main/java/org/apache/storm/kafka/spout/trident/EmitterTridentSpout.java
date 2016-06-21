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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutTuplesBuilder;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IOpaquePartitionedTridentSpout;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class EmitterTridentSpout<K,V> implements IOpaquePartitionedTridentSpout.Emitter<List<TopicPartition>, TopicPartitionTridentSpout, MetadataTridentSpout<K,V>>, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(EmitterTridentSpout.class);

    private final KafkaSpoutConfig<K, V> kafkaSpoutConfig;
    private KafkaManagerTridentSpout<K, V> kafkaManager;
    private final KafkaConsumer<K, V> kafkaConsumer;
    private final KafkaSpoutTuplesBuilder<K, V> tuplesBuilder;

    public EmitterTridentSpout(KafkaManagerTridentSpout<K,V> kafkaManager) {
        this.kafkaManager = kafkaManager;
        this.kafkaManager.subscribeKafkaConsumer();
        kafkaConsumer = kafkaManager.getKafkaConsumer();
        tuplesBuilder = kafkaManager.getTuplesBuilder();
        kafkaSpoutConfig = kafkaManager.getKafkaSpoutConfig();
        LOG.debug("Created {}", this);
    }

    private KafkaOpaquePartitionedTridentSpout kafkaOpaquePartitionedTridentSpout;

    @Override
    public MetadataTridentSpout<K,V> emitPartitionBatch(TransactionAttempt tx, TridentCollector collector, TopicPartitionTridentSpout partition, MetadataTridentSpout<K,V> lastPartitionMeta) {
        LOG.debug("Emitting batch for partition: [partition = {}], [transaction = {}], [collector = {}], [lastMetadata = {}]", partition, tx, collector, lastPartitionMeta);

        MetadataTridentSpout<K,V> currentPartitionMeta = lastPartitionMeta;

        final Set<TopicPartition> assignedTopicPartitions  = new HashSet<>(kafkaConsumer.assignment());
        LOG.debug("Currently assigned topic partitions [{}]", assignedTopicPartitions);
        assignedTopicPartitions.remove(partition.getTopicPartition());

        final TopicPartition[] pausedTopicPartitions = new TopicPartition[assignedTopicPartitions.size()];

        try {
            kafkaConsumer.pause(assignedTopicPartitions.toArray(pausedTopicPartitions));
            LOG.trace("Paused topic partitions [{}]", Arrays.toString(pausedTopicPartitions));

            final ConsumerRecords<K, V> records = kafkaConsumer.poll(kafkaSpoutConfig.getPollTimeoutMs());
            LOG.debug("Polled [{}] records from Kafka.", records.count());

            currentPartitionMeta = new MetadataTridentSpout<>(records);

            for (ConsumerRecord<K, V> record : records) {
                final List<Object> tuple = tuplesBuilder.buildTuple(record);
                collector.emit(tuple);
                LOG.debug("Emitted tuple [{}] for record: [{}]", tuple, record);
            }
        } finally {
            kafkaConsumer.resume(pausedTopicPartitions);
            LOG.trace("Resumed topic partitions [{}]", Arrays.toString(pausedTopicPartitions));
        }
        LOG.debug("Current metadata {}", currentPartitionMeta);
        return currentPartitionMeta;
    }
    @Override
    public void refreshPartitions(List<TopicPartitionTridentSpout> partitionResponsibilities) {

    }

    @Override
    public List<TopicPartitionTridentSpout> getOrderedPartitions(List<TopicPartition> allPartitionInfo) {
        final List<TopicPartitionTridentSpout> topicPartitionsTrident = new ArrayList<>(allPartitionInfo == null ? 0 : allPartitionInfo.size());
        if (allPartitionInfo != null) {
            for (TopicPartition topicPartition : allPartitionInfo) {
                topicPartitionsTrident.add(new TopicPartitionTridentSpout(topicPartition));
            }
        }
        LOG.debug("OrderedPartitions = {}", topicPartitionsTrident);
        return topicPartitionsTrident;
    }

    @Override
    public void close() {

    }

    @Override
    public String toString() {
        return "MyEmitter{" +
                "kafkaSpoutConfig=" + kafkaSpoutConfig +
                ", kafkaManager=" + kafkaManager +
                ", kafkaConsumer=" + kafkaConsumer +
                ", tuplesBuilder=" + tuplesBuilder +
                ", kafkaOpaquePartitionedTridentSpout=" + kafkaOpaquePartitionedTridentSpout +
                '}';
    }
}
