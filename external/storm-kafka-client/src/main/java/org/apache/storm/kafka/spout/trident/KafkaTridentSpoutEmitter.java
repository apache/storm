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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;
import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.LATEST;
import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST;
import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST;

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

    // Declare some KafkaTridentSpoutManager references for convenience
    private final long pollTimeoutMs;
    private final KafkaSpoutConfig.FirstPollOffsetStrategy firstPollOffsetStrategy;
    private final RecordTranslator<K, V> translator;
    private final Timer refreshSubscriptionTimer;

    private TopologyContext topologyContext;

    public KafkaTridentSpoutEmitter(KafkaTridentSpoutManager<K,V> kafkaManager, TopologyContext topologyContext, Timer refreshSubscriptionTimer) {
        this.kafkaConsumer = kafkaManager.createAndSubscribeKafkaConsumer(topologyContext);
        this.kafkaManager = kafkaManager;
        this.topologyContext = topologyContext;
        this.refreshSubscriptionTimer = refreshSubscriptionTimer;
        this.translator = kafkaManager.getKafkaSpoutConfig().getTranslator();

        final KafkaSpoutConfig<K, V> kafkaSpoutConfig = kafkaManager.getKafkaSpoutConfig();
        this.pollTimeoutMs = kafkaSpoutConfig.getPollTimeoutMs();
        this.firstPollOffsetStrategy = kafkaSpoutConfig.getFirstPollOffsetStrategy();
        LOG.debug("Created {}", this);
    }

    /**
     * Creates instance of this class with default 500 millisecond refresh subscription timer
     */
    public KafkaTridentSpoutEmitter(KafkaTridentSpoutManager<K,V> kafkaManager, TopologyContext topologyContext) {
        this(kafkaManager, topologyContext, new Timer(500,
                kafkaManager.getKafkaSpoutConfig().getPartitionRefreshPeriodMs(), TimeUnit.MILLISECONDS));
    }

    @Override
    public KafkaTridentSpoutBatchMetadata<K, V> emitPartitionBatch(TransactionAttempt tx, TridentCollector collector,
            KafkaTridentSpoutTopicPartition currBatchPartition, KafkaTridentSpoutBatchMetadata<K, V> lastBatch) {

        LOG.debug("Processing batch: [transaction = {}], [currBatchPartition = {}], [lastBatchMetadata = {}], [collector = {}]",
                tx, currBatchPartition, lastBatch, collector);

        final TopicPartition currBatchTp = currBatchPartition.getTopicPartition();
        final Set<TopicPartition> assignments = kafkaConsumer.assignment();
        KafkaTridentSpoutBatchMetadata<K, V> currentBatch = lastBatch;
        Collection<TopicPartition> pausedTopicPartitions = Collections.emptySet();

        if (assignments == null || !assignments.contains(currBatchPartition.getTopicPartition())) {
            LOG.warn("SKIPPING processing batch: [transaction = {}], [currBatchPartition = {}], [lastBatchMetadata = {}], " +
                            "[collector = {}] because it is not assigned {} to consumer instance [{}] of consumer group [{}]",
                    tx, currBatchPartition, lastBatch, collector, assignments, kafkaConsumer,
                    kafkaManager.getKafkaSpoutConfig().getConsumerGroupId());
        } else {
            try {
                // pause other topic-partitions to only poll from current topic-partition
                pausedTopicPartitions = pauseTopicPartitions(currBatchTp);

                seek(currBatchTp, lastBatch);

                // poll
                if (refreshSubscriptionTimer.isExpiredResetOnTrue()) {
                    kafkaManager.getKafkaSpoutConfig().getSubscription().refreshAssignment();
                }

                final ConsumerRecords<K, V> records = kafkaConsumer.poll(pollTimeoutMs);
                LOG.debug("Polled [{}] records from Kafka.", records.count());

                if (!records.isEmpty()) {
                    emitTuples(collector, records);
                    // build new metadata
                    currentBatch = new KafkaTridentSpoutBatchMetadata<>(currBatchTp, records, lastBatch);
                }
            } finally {
                kafkaConsumer.resume(pausedTopicPartitions);
                LOG.trace("Resumed topic-partitions {}", pausedTopicPartitions);
            }
            LOG.debug("Emitted batch: [transaction = {}], [currBatchPartition = {}], [lastBatchMetadata = {}], " +
                    "[currBatchMetadata = {}], [collector = {}]", tx, currBatchPartition, lastBatch, currentBatch, collector);
        }

        return currentBatch;
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

    // returns paused topic-partitions.
    private Collection<TopicPartition> pauseTopicPartitions(TopicPartition excludedTp) {
        final Set<TopicPartition> pausedTopicPartitions = new HashSet<>(kafkaConsumer.assignment());
        LOG.debug("Currently assigned topic-partitions {}", pausedTopicPartitions);
        pausedTopicPartitions.remove(excludedTp);
        kafkaConsumer.pause(pausedTopicPartitions);
        LOG.debug("Paused topic-partitions {}", pausedTopicPartitions);
        return pausedTopicPartitions;
    }

    @Override
    public void refreshPartitions(List<KafkaTridentSpoutTopicPartition> partitionResponsibilities) {
        LOG.trace("Refreshing of topic-partitions handled by Kafka. " +
                "No action taken by this method for topic partitions {}", partitionResponsibilities);
    }

    /**
     * Computes ordered list of topic-partitions for this task taking into consideration that topic-partitions
     * for this task must be assigned to the Kafka consumer running on this task.
     * @param allPartitionInfo list of all partitions as returned by {@link KafkaTridentSpoutOpaqueCoordinator}
     * @return ordered list of topic partitions for this task
     */
    @Override
    public List<KafkaTridentSpoutTopicPartition> getOrderedPartitions(final List<TopicPartition> allPartitionInfo) {
        final int numTopicPartitions = allPartitionInfo == null ? 0 : allPartitionInfo.size();
        final int taskIndex = topologyContext.getThisTaskIndex();
        final int numTasks = topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size();

        LOG.debug("Computing task ordered list of topic-partitions from all partitions list {}, " +
                "for task with index [{}] of total tasks [{}] ", allPartitionInfo, taskIndex, numTasks);

        final Set<TopicPartition> assignment = kafkaConsumer.assignment();
        LOG.debug("Consumer [{}] has assigned topic-partitions {}", kafkaConsumer, assignment);

        List<KafkaTridentSpoutTopicPartition> taskOrderedTps = new ArrayList<>(numTopicPartitions);

        if (numTopicPartitions > 0) {
            final KafkaTridentSpoutTopicPartition[] tps = new KafkaTridentSpoutTopicPartition[numTopicPartitions];
            int tpTaskComputedIdx = taskIndex;
            /*
             * Put this task's Kafka consumer assigned topic-partitions in the right index locations such
             * that distribution by OpaquePartitionedTridentSpoutExecutor can be done correctly. This algorithm
             * does the distribution in exactly the same way as the one used in OpaquePartitionedTridentSpoutExecutor
             */
            for (TopicPartition assignedTp : assignment) {
                if (tpTaskComputedIdx >= numTopicPartitions) {
                    LOG.warn("Ignoring attempt to add consumer [{}] assigned topic-partition [{}] to index [{}], " +
                            "out of bounds [{}]. ", kafkaConsumer, assignedTp, tpTaskComputedIdx, numTopicPartitions);
                    break;
                }
                tps[tpTaskComputedIdx] = new KafkaTridentSpoutTopicPartition(assignedTp);
                LOG.debug("Added consumer assigned topic-partition [{}] to position [{}] for task with index [{}]",
                        assignedTp, tpTaskComputedIdx, taskIndex);
                tpTaskComputedIdx += numTasks;
            }

            // Put topic-partitions assigned to consumer instances running in different tasks in the empty slots
            int i = 0;
            for (TopicPartition tp : allPartitionInfo) {
                /*
                 * Topic-partition not assigned to the Kafka consumer associated with this emitter task, hence not yet
                 * added to the list of task ordered partitions. To be processed next.
                 */
                if (!assignment.contains(tp)) {
                    for (; i < numTopicPartitions; i++) {
                        if (tps[i] == null) {   // find empty slot to put the topic-partition
                            tps[i] = new KafkaTridentSpoutTopicPartition(tp);
                            LOG.debug("Added to position [{}] topic-partition [{}], which is assigned to a consumer " +
                                    "running on a task other than task with index [{}] ", i, tp, taskIndex);
                            i++;
                            break;
                        }
                    }
                }
            }
            taskOrderedTps = Arrays.asList(tps);
        }
        LOG.debug("Returning ordered list of topic-partitions {} for task with index [{}], of total tasks [{}] ",
                taskOrderedTps, taskIndex, numTasks);
        return taskOrderedTps;
    }

    @Override
    public void close() {
        kafkaConsumer.close();
        LOG.debug("Closed");
    }

    @Override
    public String toString() {
        return super.toString() +
                "{kafkaManager=" + kafkaManager +
                '}';
    }
}
