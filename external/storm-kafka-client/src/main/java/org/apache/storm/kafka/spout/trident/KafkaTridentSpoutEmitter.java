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

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;
import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.LATEST;
import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST;
import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.kafka.spout.internal.Timer;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IOpaquePartitionedTridentSpout;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTridentSpoutEmitter<K,V> implements IOpaquePartitionedTridentSpout.Emitter<List<TopicPartition>, KafkaTridentSpoutTopicPartition, KafkaTridentSpoutBatchMetadata<K,V>>, Serializable {
    private static final long serialVersionUID = -7343927794834130435L;

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTridentSpoutEmitter.class);

    // Kafka
    private final KafkaConsumer<K, V> kafkaConsumer;

    // Bookkeeping
    private final KafkaTridentSpoutManager<K, V> kafkaManager;
    // Declare some KafkaTridentSpoutManager references for convenience
    private final long pollTimeoutMs;
    private final KafkaSpoutConfig.FirstPollOffsetStrategy firstPollOffsetStrategy;
    private final RecordTranslator<K, V> translator;
    private final Timer refreshSubscriptionTimer;

    public KafkaTridentSpoutEmitter(KafkaTridentSpoutManager<K,V> kafkaManager, TopologyContext context) {
        this.kafkaManager = kafkaManager;
        this.kafkaManager.subscribeKafkaConsumer(context);
        refreshSubscriptionTimer = new Timer(500, kafkaManager.getKafkaSpoutConfig().getPartitionRefreshPeriodMs(), TimeUnit.MILLISECONDS);

        //must subscribeKafkaConsumer before this line
        kafkaConsumer = kafkaManager.getKafkaConsumer();
        translator = kafkaManager.getKafkaSpoutConfig().getTranslator();

        final KafkaSpoutConfig<K, V> kafkaSpoutConfig = kafkaManager.getKafkaSpoutConfig();
        pollTimeoutMs = kafkaSpoutConfig.getPollTimeoutMs();
        firstPollOffsetStrategy = kafkaSpoutConfig.getFirstPollOffsetStrategy();
        LOG.debug("Created {}", this);
    }

    @Override
    public KafkaTridentSpoutBatchMetadata<K, V> emitPartitionBatch(TransactionAttempt tx, TridentCollector collector,
            KafkaTridentSpoutTopicPartition partitionTs, KafkaTridentSpoutBatchMetadata<K, V> lastBatch) {
        LOG.debug("Emitting batch: [transaction = {}], [partition = {}], [collector = {}], [lastBatchMetadata = {}]",
                tx, partitionTs, collector, lastBatch);

        final TopicPartition topicPartition = partitionTs.getTopicPartition();
        KafkaTridentSpoutBatchMetadata<K, V> currentBatch = lastBatch;
        Collection<TopicPartition> pausedTopicPartitions = Collections.emptySet();

        try {
            // pause other topic partitions to only poll from current topic partition
            pausedTopicPartitions = pauseTopicPartitions(topicPartition);

            seek(topicPartition, lastBatch);

            // poll
            if (refreshSubscriptionTimer.isExpiredResetOnTrue()) {
                kafkaManager.getKafkaSpoutConfig().getSubscription().refreshAssignment();
            }
            final ConsumerRecords<K, V> records = kafkaConsumer.poll(pollTimeoutMs);
            LOG.debug("Polled [{}] records from Kafka.", records.count());

            if (!records.isEmpty()) {
                emitTuples(collector, records);
                // build new metadata
                currentBatch = new KafkaTridentSpoutBatchMetadata<>(topicPartition, records, lastBatch);
            }
        } finally {
            kafkaConsumer.resume(pausedTopicPartitions);
            LOG.trace("Resumed topic partitions [{}]", pausedTopicPartitions);
        }
        LOG.debug("Current batch metadata {}", currentBatch);
        return currentBatch;
    }

    private void emitTuples(TridentCollector collector, ConsumerRecords<K, V> records) {
        for (ConsumerRecord<K, V> record : records) {
            final List<Object> tuple = translator.apply(record);
            collector.emit(tuple);
            LOG.debug("Emitted tuple [{}] for record: [{}]", tuple, record);
        }
    }

    /**
     * Determines the offset of the next fetch. For failed batches lastBatchMeta is not null and contains the fetch
     * offset of the failed batch. In this scenario the next fetch will take place at the offset of the failed batch.
     * When the previous batch is successful, lastBatchMeta is null, and the offset of the next fetch is either the
     * offset of the last commit to kafka, or if no commit was yet made, the offset dictated by
     * {@link KafkaSpoutConfig.FirstPollOffsetStrategy}
     *
     * @return the offset of the next fetch
     */
    private long seek(TopicPartition tp, KafkaTridentSpoutBatchMetadata<K, V> lastBatchMeta) {
        if (lastBatchMeta != null) {
            kafkaConsumer.seek(tp, lastBatchMeta.getLastOffset() + 1);  // seek next offset after last offset from previous batch
            LOG.debug("Seeking fetch offset to next offset after last offset from previous batch");

        } else {
            LOG.debug("Seeking fetch offset from firstPollOffsetStrategy and last commit to Kafka");
            final OffsetAndMetadata committedOffset = kafkaConsumer.committed(tp);
            if (committedOffset != null) {             // offset was committed for this TopicPartition
                if (firstPollOffsetStrategy.equals(EARLIEST)) {
                    kafkaConsumer.seekToBeginning(Collections.singleton(tp));
                } else if (firstPollOffsetStrategy.equals(LATEST)) {
                    kafkaConsumer.seekToEnd(Collections.singleton(tp));
                } else {
                    // By default polling starts at the last committed offset. +1 to point fetch to the first uncommitted offset.
                    kafkaConsumer.seek(tp, committedOffset.offset() + 1);
                }
            } else {    // no commits have ever been done, so start at the beginning or end depending on the strategy
                if (firstPollOffsetStrategy.equals(EARLIEST) || firstPollOffsetStrategy.equals(UNCOMMITTED_EARLIEST)) {
                    kafkaConsumer.seekToBeginning(Collections.singleton(tp));
                } else if (firstPollOffsetStrategy.equals(LATEST) || firstPollOffsetStrategy.equals(UNCOMMITTED_LATEST)) {
                    kafkaConsumer.seekToEnd(Collections.singleton(tp));
                }
            }
        }
        final long fetchOffset = kafkaConsumer.position(tp);
        LOG.debug("Set [fetchOffset = {}]", fetchOffset);
        return fetchOffset;
    }

    // returns paused topic partitions
    private Collection<TopicPartition> pauseTopicPartitions(TopicPartition excludedTp) {
        final Set<TopicPartition> pausedTopicPartitions  = new HashSet<>(kafkaConsumer.assignment());
        LOG.debug("Currently assigned topic partitions [{}]", pausedTopicPartitions);
        pausedTopicPartitions.remove(excludedTp);
        kafkaConsumer.pause(pausedTopicPartitions);
        LOG.trace("Paused topic partitions [{}]", pausedTopicPartitions);
        return pausedTopicPartitions;
    }

    @Override
    public void refreshPartitions(List<KafkaTridentSpoutTopicPartition> partitionResponsibilities) {
        LOG.debug("Refreshing topic partitions [{}]", partitionResponsibilities);
    }

    @Override
    public List<KafkaTridentSpoutTopicPartition> getOrderedPartitions(List<TopicPartition> allPartitionInfo) {
        final List<KafkaTridentSpoutTopicPartition> topicPartitionsTrident = new ArrayList<>(allPartitionInfo == null ? 0 : allPartitionInfo.size());
        if (allPartitionInfo != null) {
            for (TopicPartition topicPartition : allPartitionInfo) {
                topicPartitionsTrident.add(new KafkaTridentSpoutTopicPartition(topicPartition));
            }
        }
        LOG.debug("OrderedPartitions = {}", topicPartitionsTrident);
        return topicPartitionsTrident;
    }

    @Override
    public void close() {
        kafkaConsumer.close();
        LOG.debug("Closed");
    }

    @Override
    public String toString() {
        return "KafkaTridentSpoutEmitter{" +
                ", kafkaManager=" + kafkaManager +
                '}';
    }
}
