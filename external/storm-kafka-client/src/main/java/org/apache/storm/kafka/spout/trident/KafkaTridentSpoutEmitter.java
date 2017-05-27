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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
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

public class KafkaTridentSpoutEmitter<K, V> implements IOpaquePartitionedTridentSpout.Emitter<
        List<TopicPartition>,
        KafkaTridentSpoutTopicPartition,
        KafkaTridentSpoutBatchMetadata<K, V>>,
        Serializable {

    private static final long serialVersionUID = -7343927794834130435L;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTridentSpoutEmitter.class);

    // Kafka
    private final KafkaConsumer<K, V> kafkaConsumer;

    // Bookkeeping
    private final KafkaTridentSpoutManager<K, V> kafkaManager;
    private Set<TopicPartition> firstPoll = new HashSet<>();        // set of topic-partitions for which first poll has already occurred

    // Declare some KafkaTridentSpoutManager and Storm internal references for convenience
    private final long pollTimeoutMs;
    private final KafkaSpoutConfig.FirstPollOffsetStrategy firstPollOffsetStrategy;
    private final RecordTranslator<K, V> translator;
    private final Timer refreshSubscriptionTimer;
    private final KafkaSpoutConfig<K, V> kafkaSpoutConfig;
    private final TopologyContext topologyContext;

    private boolean transactionInProgress;
    private Collection<TopicPartition> pausedTopicPartitions;
    private boolean replayTransaction;
    private KafkaSpoutConsumerRebalanceListener kafkaConsListener;

    public KafkaTridentSpoutEmitter(KafkaTridentSpoutManager<K, V> kafkaManager,
            TopologyContext topologyContext, Timer refreshSubscriptionTimer) {
        this.kafkaManager = kafkaManager;
        this.topologyContext = topologyContext;
        this.refreshSubscriptionTimer = refreshSubscriptionTimer;
        kafkaSpoutConfig = kafkaManager.getKafkaSpoutConfig();
        kafkaConsListener = new KafkaSpoutConsumerRebalanceListener();
        kafkaConsumer = kafkaManager.createAndSubscribeKafkaConsumer(topologyContext, kafkaConsListener);
        translator = kafkaSpoutConfig.getTranslator();
        pollTimeoutMs = kafkaSpoutConfig.getPollTimeoutMs();
        firstPollOffsetStrategy = kafkaSpoutConfig.getFirstPollOffsetStrategy();
        pausedTopicPartitions = Collections.emptySet();
        LOG.debug("Created {}", this);
    }

    /**
     * Creates instance of this class with default 500 millisecond refresh subscription timer.
     */
    public KafkaTridentSpoutEmitter(KafkaTridentSpoutManager<K, V> kafkaManager, TopologyContext topologyContext) {
        this(kafkaManager, topologyContext, new Timer(500,
                kafkaManager.getKafkaSpoutConfig().getPartitionRefreshPeriodMs(), TimeUnit.MILLISECONDS));
    }

    private class KafkaSpoutConsumerRebalanceListener implements ConsumerRebalanceListener {
        TopicPartition currBatchTp;     // Topic Partition being processed in current batch

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log("Partitions revoked", partitions);
            KafkaTridentSpoutTopicPartitionRegistry.INSTANCE.removeAll(partitions);

            if (transactionInProgress) {
                resumeTopicPartitions();
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log("Partitions reassignment", partitions);
            KafkaTridentSpoutTopicPartitionRegistry.INSTANCE.addAll(partitions);

            if (transactionInProgress) {
                if (!partitions.contains(currBatchTp)) {
                    replayTransaction = true;
                    LOG.warn("Partitions reassignment. Current batch's topic-partition [{}] "
                            + "no longer assigned to consumer={} of consumer-group={}. Replaying transaction.",
                            currBatchTp, kafkaConsumer);
                } else {
                    pauseTopicPartitions(partitions, currBatchTp);   // pause topic-partitions other than current batch's tp
                }
            }
        }

        private void log(String msg, Collection<TopicPartition> partitions) {  // tip - transactionInProgresses
            LOG.info("{}. [transaction-in-progress={}, currBatchTp={}, paused-topic-partitions={}, "
                    + "topic-partitions={}, consumer-group={}, consumer={}]",
                    msg, transactionInProgress, currBatchTp, pausedTopicPartitions, partitions,
                    kafkaSpoutConfig.getConsumerGroupId(), kafkaConsumer);
        }
    }

    @Override
    public KafkaTridentSpoutBatchMetadata<K, V> emitPartitionBatch(TransactionAttempt tx, TridentCollector collector,
            KafkaTridentSpoutTopicPartition currBatchPartition, KafkaTridentSpoutBatchMetadata<K, V> lastBatch) {

        KafkaTridentSpoutBatchMetadata<K, V> currentBatch = lastBatch;
        try {
            LOG.debug("Processing batch: [transaction = {}], [currBatchPartition = {}], [lastBatchMetadata = {}], [collector = {}]",
                    tx, currBatchPartition, lastBatch, collector);

            transactionInProgress = true;
            final TopicPartition currBatchTp = currBatchPartition.getTopicPartition();
            kafkaConsListener.currBatchTp = currBatchTp;

            final Set<TopicPartition> assignments = kafkaConsumer.assignment();

            if (assignments == null || !assignments.contains(currBatchPartition.getTopicPartition())) {
                LOG.warn("SKIPPING processing batch [transaction = {}], [currBatchPartition = {}], [lastBatchMetadata = {}], "
                        + "[collector = {}] because it is not part of the assignments {} of consumer instance [{}] "
                        + "of consumer group [{}]", tx, currBatchPartition, lastBatch, collector, assignments,
                            kafkaConsumer, kafkaSpoutConfig.getConsumerGroupId());
            } else {
                // pause other topic-partitions to poll only from current topic-partition
                pauseTopicPartitions(assignments, currBatchTp);

                seek(currBatchTp, lastBatch);

                if (refreshSubscriptionTimer.isExpiredResetOnTrue()) {
                    kafkaSpoutConfig.getSubscription().refreshAssignment();
                }

                // Consumer rebalance listener is called during this poll
                final ConsumerRecords<K, V> records = kafkaConsumer.poll(pollTimeoutMs);

                if (replayTransaction) {
                    replayTransaction();
                } else {
                    LOG.debug("Polled [{}] records from Kafka.", records.count());
                    if (!records.isEmpty()) {
                        emitTuples(collector, records);
                        // build new metadata
                        currentBatch = new KafkaTridentSpoutBatchMetadata<>(currBatchTp, records, lastBatch);
                    }
                }
            }
        } finally {
            resumeTopicPartitions();
            transactionInProgress = false;
            replayTransaction = false;
            kafkaConsListener.currBatchTp = null;
        }
        LOG.debug("Emitted batch: [transaction = {}], [currBatchPartition = {}], [lastBatchMetadata = {}], "
            + "[currBatchMetadata = {}], [collector = {}]", tx, currBatchPartition, lastBatch, currentBatch, collector);
        return currentBatch;
    }

    // Logs msg. The metadata state is not updated, hence left as the previous transaction's metadata, which is equivalent to a replay
    private void replayTransaction() {
        LOG.debug("Replaying transaction due to Kafka consumer rebalance. [consumer-group={}, consumer={}",
                kafkaSpoutConfig.getConsumerGroupId(), kafkaConsumer);
    }

    private void resumeTopicPartitions() {
        final Collection<TopicPartition> resumedTps = pausedTopicPartitions;
        kafkaConsumer.resume(pausedTopicPartitions);
        pausedTopicPartitions = Collections.emptySet();
        LOG.trace("Resumed topic-partitions {}", resumedTps);
    }

    private void pauseTopicPartitions(Collection<TopicPartition> assigned, TopicPartition tpNotToPause) {
        final Set<TopicPartition> topicPartitionsToPause = new HashSet<>(assigned);
        LOG.debug("Currently assigned topic-partitions {}", topicPartitionsToPause);
        if (tpNotToPause != null) {
            topicPartitionsToPause.remove(tpNotToPause);
        } else {
            LOG.warn("Attempted to pause null topic-partition");
        }
        kafkaConsumer.pause(topicPartitionsToPause);
        LOG.debug("Paused topic-partitions {}", topicPartitionsToPause);
        pausedTopicPartitions = topicPartitionsToPause;
    }

    private void emitTuples(TridentCollector collector, ConsumerRecords<K, V> records) {
        for (ConsumerRecord<K, V> record : records) {
            final List<Object> tuple = translator.apply(record);
            collector.emit(tuple);
            LOG.debug("Emitted tuple {} for record [{}]", tuple, record);
        }
    }

    /**
     * Determines the offset of the next fetch. For failed batches lastBatchMeta is not null and contains the fetch
     * offset of the failed batch. In this scenario the next fetch will take place at offset of the failed batch + 1.
     * When the previous batch is successful, lastBatchMeta is null, and the offset of the next fetch is, for the first poll,
     * the offset of the last commit to kafka, or if no commit was yet made, the offset dictated by
     * {@link KafkaSpoutConfig.FirstPollOffsetStrategy}. For the polls after the first, it is the fetch offset of where the
     * kafka consumer instance left off, as dictated by Kafka when no seek happens.
     *
     * @return the offset of the next fetch
     */
    private long seek(TopicPartition tp, KafkaTridentSpoutBatchMetadata<K, V> lastBatchMeta) {
        if (lastBatchMeta != null) {
            kafkaConsumer.seek(tp, lastBatchMeta.getLastOffset() + 1);  // seek next offset after last offset from previous batch
            LOG.debug("Seeking fetch offset to next offset after last offset from previous batch for topic-partition [{}]", tp);
        } else if (isFirstPoll(tp)) {
            LOG.debug("Seeking fetch offset from firstPollOffsetStrategy and last commit to Kafka for topic-partition [{}]", tp);
            firstPoll.add(tp);
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

    private boolean isFirstPoll(TopicPartition tp) {
        return !firstPoll.contains(tp);
    }

    @Override
    public void refreshPartitions(List<KafkaTridentSpoutTopicPartition> partitionResponsibilities) {
        LOG.trace("Refreshing of topic-partitions handled by Kafka. "
            + "No action taken by this method for topic-partitions {}", partitionResponsibilities);
    }

    /**
     * Computes ordered list of topic-partitions for this task taking into consideration that topic-partitions
     * for this task must be assigned to the Kafka consumer running on this task.
     *
     * @param allPartitionInfo list of all partitions as returned by {@link KafkaTridentSpoutOpaqueCoordinator}
     * @return ordered list of topic partitions for this task
     */
    @Override
    public List<KafkaTridentSpoutTopicPartition> getOrderedPartitions(final List<TopicPartition> allPartitionInfo) {
        final List<KafkaTridentSpoutTopicPartition> allPartitions = newKafkaTridentSpoutTopicPartitions(allPartitionInfo);
        LOG.debug("Returning all topic-partitions {} across all tasks. Current task index [{}]. Total tasks [{}] ",
                allPartitions, topologyContext.getThisTaskIndex(), getNumTasks());
        return allPartitions;
    }

    @Override
    public List<KafkaTridentSpoutTopicPartition> getPartitionsForTask(int taskId, int numTasks, List<TopicPartition> allPartitionInfo) {
        final Set<TopicPartition> assignedTps = kafkaConsumer.assignment();
        LOG.debug("Consumer [{}], running on task with index [{}], has assigned topic-partitions {}", kafkaConsumer, taskId, assignedTps);
        final List<KafkaTridentSpoutTopicPartition> taskTps = newKafkaTridentSpoutTopicPartitions(assignedTps);
        LOG.debug("Returning topic-partitions {} for task with index [{}]", taskTps, taskId);
        return taskTps;
    }

    private List<KafkaTridentSpoutTopicPartition> newKafkaTridentSpoutTopicPartitions(Collection<TopicPartition> tps) {
        final List<KafkaTridentSpoutTopicPartition> kttp = new ArrayList<>(tps == null ? 0 : tps.size());
        if (tps != null) {
            for (TopicPartition tp : tps) {
                LOG.trace("Added topic-partition [{}]", tp);
                kttp.add(new KafkaTridentSpoutTopicPartition(tp));
            }
        }
        return kttp;
    }

    private int getNumTasks() {
        return topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size();
    }

    @Override
    public void close() {
        kafkaConsumer.close();
        LOG.debug("Closed");
    }

    @Override
    public String toString() {
        return super.toString()
            + "{kafkaManager=" + kafkaManager
            + ", transactionInProgress=" + transactionInProgress
            + ", pausedTopicPartitions=" + pausedTopicPartitions
            + ", replayTransaction=" + replayTransaction
            + ", currBatchTp=" + kafkaConsListener.currBatchTp
            + '}';
    }
}
