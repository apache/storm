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

package org.apache.storm.kafka.spout;

import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.EARLIEST;
import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.LATEST;
import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST;
import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.UNCOMMITTED_LATEST;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.ProcessingGuarantee;
import org.apache.storm.kafka.spout.internal.CommitMetadataManager;
import org.apache.storm.kafka.spout.internal.ConsumerFactory;
import org.apache.storm.kafka.spout.internal.ConsumerFactoryDefault;
import org.apache.storm.kafka.spout.internal.OffsetManager;
import org.apache.storm.kafka.spout.internal.Timer;
import org.apache.storm.kafka.spout.metrics.KafkaOffsetMetric;
import org.apache.storm.kafka.spout.subscription.TopicAssigner;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSpout<K, V> extends BaseRichSpout {

    private static final long serialVersionUID = 4151921085047987154L;
    //Initial delay for the commit and assignment refresh timers
    public static final long TIMER_DELAY_MS = 500;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);

    // Storm
    protected SpoutOutputCollector collector;

    // Kafka
    private final KafkaSpoutConfig<K, V> kafkaSpoutConfig;
    private final ConsumerFactory<K, V> kafkaConsumerFactory;
    private final TopicAssigner topicAssigner;
    private transient Consumer<K, V> consumer;

    // Bookkeeping
    // Strategy to determine the fetch offset of the first realized by the spout upon activation
    private transient FirstPollOffsetStrategy firstPollOffsetStrategy;
    // Class that has the logic to handle tuple failure.
    private transient KafkaSpoutRetryService retryService;
    // Handles tuple events (emit, ack etc.)
    private transient KafkaTupleListener tupleListener;
    // timer == null only if the processing guarantee is at-most-once
    private transient Timer commitTimer;
    // Initialization is only complete after the first call to  KafkaSpoutConsumerRebalanceListener.onPartitionsAssigned()

    // Tuples that were successfully acked/emitted. These tuples will be committed periodically when the commit timer expires,
    // or after a consumer rebalance, or during close/deactivate. Always empty if processing guarantee is none or at-most-once.
    private transient Map<TopicPartition, OffsetManager> offsetManagers;
    // Tuples that have been emitted but that are "on the wire", i.e. pending being acked or failed.
    // Always empty if processing guarantee is none or at-most-once
    private transient Set<KafkaSpoutMessageId> emitted;
    // Records that have been polled and are queued to be emitted in the nextTuple() call. One record is emitted per nextTuple()
    private transient Map<TopicPartition, List<ConsumerRecord<K, V>>> waitingToEmit;
    // Triggers when an assignment should be refreshed
    private transient Timer refreshAssignmentTimer;
    private transient TopologyContext context;
    private transient CommitMetadataManager commitMetadataManager;
    private transient KafkaOffsetMetric<K, V> kafkaOffsetMetric;
    private transient KafkaSpoutConsumerRebalanceListener rebalanceListener;

    public KafkaSpout(KafkaSpoutConfig<K, V> kafkaSpoutConfig) {
        this(kafkaSpoutConfig, new ConsumerFactoryDefault<>(), new TopicAssigner());
    }

    @VisibleForTesting
    KafkaSpout(KafkaSpoutConfig<K, V> kafkaSpoutConfig, ConsumerFactory<K, V> kafkaConsumerFactory, TopicAssigner topicAssigner) {
        this.kafkaConsumerFactory = kafkaConsumerFactory;
        this.topicAssigner = topicAssigner;
        this.kafkaSpoutConfig = kafkaSpoutConfig;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.context = context;

        // Spout internals
        this.collector = collector;

        // Offset management
        firstPollOffsetStrategy = kafkaSpoutConfig.getFirstPollOffsetStrategy();

        // Retries management
        retryService = kafkaSpoutConfig.getRetryService();

        tupleListener = kafkaSpoutConfig.getTupleListener();

        if (kafkaSpoutConfig.getProcessingGuarantee() != KafkaSpoutConfig.ProcessingGuarantee.AT_MOST_ONCE) {
            // In at-most-once mode the offsets are committed after every poll, and not periodically as controlled by the timer
            commitTimer = new Timer(TIMER_DELAY_MS, kafkaSpoutConfig.getOffsetsCommitPeriodMs(), TimeUnit.MILLISECONDS);
        }
        refreshAssignmentTimer = new Timer(TIMER_DELAY_MS, kafkaSpoutConfig.getPartitionRefreshPeriodMs(), TimeUnit.MILLISECONDS);

        offsetManagers = new HashMap<>();
        emitted = new HashSet<>();
        waitingToEmit = new HashMap<>();
        commitMetadataManager = new CommitMetadataManager(context, kafkaSpoutConfig.getProcessingGuarantee());

        rebalanceListener = new KafkaSpoutConsumerRebalanceListener();

        consumer = kafkaConsumerFactory.createConsumer(kafkaSpoutConfig.getKafkaProps());

        tupleListener.open(conf, context);
        if (canRegisterMetrics()) {
            registerMetric();
        }

        LOG.info("Kafka Spout opened with the following configuration: {}", kafkaSpoutConfig);
    }

    private void registerMetric() {
        LOG.info("Registering Spout Metrics");
        kafkaOffsetMetric = new KafkaOffsetMetric<>(() -> Collections.unmodifiableMap(offsetManagers), () -> consumer);
        context.registerMetric("kafkaOffset", kafkaOffsetMetric, kafkaSpoutConfig.getMetricsTimeBucketSizeInSecs());
    }

    private boolean canRegisterMetrics() {
        try {
            KafkaConsumer.class.getDeclaredMethod("beginningOffsets", Collection.class);
        } catch (NoSuchMethodException e) {
            LOG.warn("Minimum required kafka-clients library version to enable metrics is 0.10.1.0. Disabling spout metrics.");
            return false;
        }
        return true;
    }

    private boolean isAtLeastOnceProcessing() {
        return kafkaSpoutConfig.getProcessingGuarantee() == KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE;
    }

    // =========== Consumer Rebalance Listener - On the same thread as the caller ===========
    private class KafkaSpoutConsumerRebalanceListener implements ConsumerRebalanceListener {

        private Collection<TopicPartition> previousAssignment = new HashSet<>();

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            previousAssignment = partitions;

            LOG.info("Partitions revoked. [consumer-group={}, consumer={}, topic-partitions={}]",
                kafkaSpoutConfig.getConsumerGroupId(), consumer, partitions);

            if (isAtLeastOnceProcessing()) {
                commitOffsetsForAckedTuples();
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            LOG.info("Partitions reassignment. [task-ID={}, consumer-group={}, consumer={}, topic-partitions={}]",
                context.getThisTaskId(), kafkaSpoutConfig.getConsumerGroupId(), consumer, partitions);

            initialize(partitions);
            tupleListener.onPartitionsReassigned(partitions);
        }

        private void initialize(Collection<TopicPartition> partitions) {
            if (isAtLeastOnceProcessing()) {
                // remove offsetManagers for all partitions that are no longer assigned to this spout
                offsetManagers.keySet().retainAll(partitions);
                retryService.retainAll(partitions);

                /*
                 * Emitted messages for partitions that are no longer assigned to this spout can't be acked and should not be retried, hence
                 * remove them from emitted collection.
                 */
                emitted.removeIf(msgId -> !partitions.contains(msgId.getTopicPartition()));
            }
            waitingToEmit.keySet().retainAll(partitions);

            Set<TopicPartition> newPartitions = new HashSet<>(partitions);
            // If this partition was previously assigned to this spout,
            // leave the acked offsets and consumer position as they were to resume where it left off
            newPartitions.removeAll(previousAssignment);
            for (TopicPartition newTp : newPartitions) {
                final OffsetAndMetadata committedOffset = consumer.committed(newTp);
                final long fetchOffset = doSeek(newTp, committedOffset);
                LOG.debug("Set consumer position to [{}] for topic-partition [{}] with [{}] and committed offset [{}]",
                    fetchOffset, newTp, firstPollOffsetStrategy, committedOffset);
                if (isAtLeastOnceProcessing() && !offsetManagers.containsKey(newTp)) {
                    offsetManagers.put(newTp, new OffsetManager(newTp, fetchOffset));
                }
            }
            LOG.info("Initialization complete");
        }

        /**
         * Sets the cursor to the location dictated by the first poll strategy and returns the fetch offset.
         */
        private long doSeek(TopicPartition newTp, OffsetAndMetadata committedOffset) {
            LOG.trace("Seeking offset for topic-partition [{}] with [{}] and committed offset [{}]",
                newTp, firstPollOffsetStrategy, committedOffset);

            if (committedOffset != null) {
                // offset was previously committed for this consumer group and topic-partition, either by this or another topology.
                if (commitMetadataManager.isOffsetCommittedByThisTopology(newTp,
                    committedOffset,
                    Collections.unmodifiableMap(offsetManagers))) {
                    // Another KafkaSpout instance (of this topology) already committed, therefore FirstPollOffsetStrategy does not apply.
                    consumer.seek(newTp, committedOffset.offset());
                } else {
                    // offset was not committed by this topology, therefore FirstPollOffsetStrategy applies
                    // (only when the topology is first deployed).
                    if (firstPollOffsetStrategy.equals(EARLIEST)) {
                        consumer.seekToBeginning(Collections.singleton(newTp));
                    } else if (firstPollOffsetStrategy.equals(LATEST)) {
                        consumer.seekToEnd(Collections.singleton(newTp));
                    } else {
                        // Resume polling at the last committed offset, i.e. the first offset that is not marked as processed.
                        consumer.seek(newTp, committedOffset.offset());
                    }
                }
            } else {
                // no offset commits have ever been done for this consumer group and topic-partition,
                // so start at the beginning or end depending on FirstPollOffsetStrategy
                if (firstPollOffsetStrategy.equals(EARLIEST) || firstPollOffsetStrategy.equals(UNCOMMITTED_EARLIEST)) {
                    consumer.seekToBeginning(Collections.singleton(newTp));
                } else if (firstPollOffsetStrategy.equals(LATEST) || firstPollOffsetStrategy.equals(UNCOMMITTED_LATEST)) {
                    consumer.seekToEnd(Collections.singleton(newTp));
                }
            }
            return consumer.position(newTp);
        }
    }

    // ======== Next Tuple =======
    @Override
    public void nextTuple() {
        try {
            if (refreshAssignmentTimer.isExpiredResetOnTrue()) {
                refreshAssignment();
            }

            if (commitTimer != null && commitTimer.isExpiredResetOnTrue()) {
                if (isAtLeastOnceProcessing()) {
                    commitOffsetsForAckedTuples();
                } else if (kafkaSpoutConfig.getProcessingGuarantee() == ProcessingGuarantee.NO_GUARANTEE) {
                    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit =
                        createFetchedOffsetsMetadata(consumer.assignment());
                    consumer.commitAsync(offsetsToCommit, null);
                    LOG.debug("Committed offsets {} to Kafka", offsetsToCommit);
                }
            }

            PollablePartitionsInfo pollablePartitionsInfo = getPollablePartitionsInfo();
            if (pollablePartitionsInfo.shouldPoll()) {
                try {
                    setWaitingToEmit(pollKafkaBroker(pollablePartitionsInfo));
                } catch (RetriableException e) {
                    LOG.error("Failed to poll from kafka.", e);
                }
            }

            emitIfWaitingNotEmitted();
        } catch (InterruptException e) {
            throwKafkaConsumerInterruptedException();
        }
    }

    private void throwKafkaConsumerInterruptedException() {
        //Kafka throws their own type of exception when interrupted.
        //Throw a new Java InterruptedException to ensure Storm can recognize the exception as a reaction to an interrupt.
        throw new RuntimeException(new InterruptedException("Kafka consumer was interrupted"));
    }

    private PollablePartitionsInfo getPollablePartitionsInfo() {
        if (isWaitingToEmit()) {
            LOG.debug("Not polling. Tuples waiting to be emitted.");
            return new PollablePartitionsInfo(Collections.emptySet(), Collections.emptyMap());
        }

        Set<TopicPartition> assignment = consumer.assignment();
        if (!isAtLeastOnceProcessing()) {
            return new PollablePartitionsInfo(assignment, Collections.emptyMap());
        }

        Map<TopicPartition, Long> earliestRetriableOffsets = retryService.earliestRetriableOffsets();
        Set<TopicPartition> pollablePartitions = new HashSet<>();
        final int maxUncommittedOffsets = kafkaSpoutConfig.getMaxUncommittedOffsets();
        for (TopicPartition tp : assignment) {
            OffsetManager offsetManager = offsetManagers.get(tp);
            int numUncommittedOffsets = offsetManager.getNumUncommittedOffsets();
            if (numUncommittedOffsets < maxUncommittedOffsets) {
                //Allow poll if the partition is not at the maxUncommittedOffsets limit
                pollablePartitions.add(tp);
            } else {
                long offsetAtLimit = offsetManager.getNthUncommittedOffsetAfterCommittedOffset(maxUncommittedOffsets);
                Long earliestRetriableOffset = earliestRetriableOffsets.get(tp);
                if (earliestRetriableOffset != null && earliestRetriableOffset <= offsetAtLimit) {
                    //Allow poll if there are retriable tuples within the maxUncommittedOffsets limit
                    pollablePartitions.add(tp);
                } else {
                    LOG.debug("Not polling on partition [{}]. It has [{}] uncommitted offsets, which exceeds the limit of [{}]. ", tp,
                        numUncommittedOffsets, maxUncommittedOffsets);
                }
            }
        }
        return new PollablePartitionsInfo(pollablePartitions, earliestRetriableOffsets);
    }

    private boolean isWaitingToEmit() {
        return waitingToEmit.values().stream()
            .anyMatch(list -> !list.isEmpty());
    }

    private void setWaitingToEmit(ConsumerRecords<K, V> consumerRecords) {
        for (TopicPartition tp : consumerRecords.partitions()) {
            waitingToEmit.put(tp, new LinkedList<>(consumerRecords.records(tp)));
        }
    }

    // ======== poll =========
    private ConsumerRecords<K, V> pollKafkaBroker(PollablePartitionsInfo pollablePartitionsInfo) {
        doSeekRetriableTopicPartitions(pollablePartitionsInfo.pollableEarliestRetriableOffsets);
        Set<TopicPartition> pausedPartitions = new HashSet<>(consumer.assignment());
        pausedPartitions.removeIf(pollablePartitionsInfo.pollablePartitions::contains);
        try {
            consumer.pause(pausedPartitions);
            final ConsumerRecords<K, V> consumerRecords = consumer.poll(kafkaSpoutConfig.getPollTimeoutMs());
            ackRetriableOffsetsIfCompactedAway(pollablePartitionsInfo.pollableEarliestRetriableOffsets, consumerRecords);
            final int numPolledRecords = consumerRecords.count();
            LOG.debug("Polled [{}] records from Kafka",
                numPolledRecords);
            if (kafkaSpoutConfig.getProcessingGuarantee() == KafkaSpoutConfig.ProcessingGuarantee.AT_MOST_ONCE) {
                //Commit polled records immediately to ensure delivery is at-most-once.
                Map<TopicPartition, OffsetAndMetadata> offsetsToCommit =
                    createFetchedOffsetsMetadata(consumer.assignment());
                consumer.commitSync(offsetsToCommit);
                LOG.debug("Committed offsets {} to Kafka", offsetsToCommit);
            }
            return consumerRecords;
        } finally {
            consumer.resume(pausedPartitions);
        }
    }

    private void doSeekRetriableTopicPartitions(Map<TopicPartition, Long> pollableEarliestRetriableOffsets) {
        for (Entry<TopicPartition, Long> retriableTopicPartitionAndOffset : pollableEarliestRetriableOffsets.entrySet()) {
            //Seek directly to the earliest retriable message for each retriable topic partition
            consumer.seek(retriableTopicPartitionAndOffset.getKey(), retriableTopicPartitionAndOffset.getValue());
        }
    }

    private void ackRetriableOffsetsIfCompactedAway(Map<TopicPartition, Long> earliestRetriableOffsets,
        ConsumerRecords<K, V> consumerRecords) {
        for (Entry<TopicPartition, Long> entry : earliestRetriableOffsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            List<ConsumerRecord<K, V>> records = consumerRecords.records(tp);
            if (!records.isEmpty()) {
                ConsumerRecord<K, V> record = records.get(0);
                long seekOffset = entry.getValue();
                long earliestReceivedOffset = record.offset();
                if (seekOffset < earliestReceivedOffset) {
                    //Since we asked for tuples starting at seekOffset, some retriable records must have been compacted away.
                    //Ack up to the first offset received if the record is not already acked or currently in the topology
                    for (long i = seekOffset; i < earliestReceivedOffset; i++) {
                        KafkaSpoutMessageId msgId = retryService.getMessageId(tp, i);
                        if (!offsetManagers.get(tp).contains(msgId) && !emitted.contains(msgId)) {
                            LOG.debug("Record at offset [{}] appears to have been compacted away from topic [{}], marking as acked", i, tp);
                            retryService.remove(msgId);
                            emitted.add(msgId);
                            ack(msgId);
                        }
                    }
                }
            }
        }
    }

    // ======== emit  =========
    private void emitIfWaitingNotEmitted() {
        Iterator<List<ConsumerRecord<K, V>>> waitingToEmitIter = waitingToEmit.values().iterator();
        outerLoop:
        while (waitingToEmitIter.hasNext()) {
            List<ConsumerRecord<K, V>> waitingToEmitForTp = waitingToEmitIter.next();
            while (!waitingToEmitForTp.isEmpty()) {
                final boolean emittedTuple = emitOrRetryTuple(waitingToEmitForTp.remove(0));
                if (emittedTuple) {
                    break outerLoop;
                }
            }
            waitingToEmitIter.remove();
        }
    }

    /**
     * Creates a tuple from the kafka record and emits it if it was never emitted or it is ready to be retried.
     *
     * @param record to be emitted
     * @return true if tuple was emitted. False if tuple has been acked or has been emitted and is pending ack or fail
     */
    private boolean emitOrRetryTuple(ConsumerRecord<K, V> record) {
        final TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        final KafkaSpoutMessageId msgId = retryService.getMessageId(tp, record.offset());

        if (offsetManagers.containsKey(tp) && offsetManagers.get(tp).contains(msgId)) {   // has been acked
            LOG.trace("Tuple for record [{}] has already been acked. Skipping", record);
        } else if (emitted.contains(msgId)) {   // has been emitted and it is pending ack or fail
            LOG.trace("Tuple for record [{}] has already been emitted. Skipping", record);
        } else {
            final List<Object> tuple = kafkaSpoutConfig.getTranslator().apply(record);
            if (isEmitTuple(tuple)) {
                final boolean isScheduled = retryService.isScheduled(msgId);
                // not scheduled <=> never failed (i.e. never emitted), or scheduled and ready to be retried
                if (!isScheduled || retryService.isReady(msgId)) {
                    final String stream = tuple instanceof KafkaTuple ? ((KafkaTuple) tuple).getStream() : Utils.DEFAULT_STREAM_ID;

                    if (!isAtLeastOnceProcessing()) {
                        if (kafkaSpoutConfig.isTupleTrackingEnforced()) {
                            collector.emit(stream, tuple, msgId);
                            LOG.trace("Emitted tuple [{}] for record [{}] with msgId [{}]", tuple, record, msgId);
                        } else {
                            collector.emit(stream, tuple);
                            LOG.trace("Emitted tuple [{}] for record [{}]", tuple, record);
                        }
                    } else {
                        emitted.add(msgId);
                        offsetManagers.get(tp).addToEmitMsgs(msgId.offset());
                        if (isScheduled) {  // Was scheduled for retry and re-emitted, so remove from schedule.
                            retryService.remove(msgId);
                        }
                        collector.emit(stream, tuple, msgId);
                        tupleListener.onEmit(tuple, msgId);
                        LOG.trace("Emitted tuple [{}] for record [{}] with msgId [{}]", tuple, record, msgId);
                    }
                    return true;
                }
            } else {
                /*
                 * if a null tuple is not configured to be emitted, it should be marked as emitted and acked immediately to allow its offset
                 * to be commited to Kafka
                 */
                LOG.debug("Not emitting null tuple for record [{}] as defined in configuration.", record);
                if (isAtLeastOnceProcessing()) {
                    msgId.setNullTuple(true);
                    offsetManagers.get(tp).addToEmitMsgs(msgId.offset());
                    ack(msgId);
                }
            }
        }
        return false;
    }

    /**
     * Emits a tuple if it is not a null tuple, or if the spout is configured to emit null tuples.
     */
    private boolean isEmitTuple(List<Object> tuple) {
        return tuple != null || kafkaSpoutConfig.isEmitNullTuples();
    }

    private Map<TopicPartition, OffsetAndMetadata> createFetchedOffsetsMetadata(Set<TopicPartition> assignedPartitions) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (TopicPartition tp : assignedPartitions) {
            offsetsToCommit.put(tp, new OffsetAndMetadata(consumer.position(tp), commitMetadataManager.getCommitMetadata()));
        }
        return offsetsToCommit;
    }

    private void commitOffsetsForAckedTuples() {
        final Map<TopicPartition, OffsetAndMetadata> nextCommitOffsets = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetManager> tpOffset : offsetManagers.entrySet()) {
            final OffsetAndMetadata nextCommitOffset = tpOffset.getValue().findNextCommitOffset(commitMetadataManager.getCommitMetadata());
            if (nextCommitOffset != null) {
                nextCommitOffsets.put(tpOffset.getKey(), nextCommitOffset);
            }
        }

        // Commit offsets that are ready to be committed for every topic partition
        if (!nextCommitOffsets.isEmpty()) {
            consumer.commitSync(nextCommitOffsets);
            LOG.debug("Offsets successfully committed to Kafka [{}]", nextCommitOffsets);
            // Instead of iterating again, it would be possible to commit and update the state for each TopicPartition
            // in the prior loop, but the multiple network calls should be more expensive than iterating twice over a small loop
            for (Map.Entry<TopicPartition, OffsetAndMetadata> tpOffset : nextCommitOffsets.entrySet()) {
                //Update the OffsetManager for each committed partition, and update numUncommittedOffsets
                final TopicPartition tp = tpOffset.getKey();
                long position = consumer.position(tp);
                long committedOffset = tpOffset.getValue().offset();
                if (position < committedOffset) {
                    /*
                     * The position is behind the committed offset. This can happen in some cases, e.g. if a message failed, lots of (more
                     * than max.poll.records) later messages were acked, and the failed message then gets acked. The consumer may only be
                     * part way through "catching up" to where it was when it went back to retry the failed tuple. Skip the consumer forward
                     * to the committed offset.
                     */
                    LOG.debug("Consumer fell behind committed offset. Catching up. Position was [{}], skipping to [{}]",
                        position, committedOffset);
                    consumer.seek(tp, committedOffset);
                }
                /**
                 * In some cases the waitingToEmit list may contain tuples that have just been committed. Drop these.
                 */
                List<ConsumerRecord<K, V>> waitingToEmitForTp = waitingToEmit.get(tp);
                if (waitingToEmitForTp != null) {
                    //Discard the pending records that are already committed
                    waitingToEmit.put(tp, waitingToEmitForTp.stream()
                        .filter(record -> record.offset() >= committedOffset)
                        .collect(Collectors.toCollection(LinkedList::new)));
                }

                final OffsetManager offsetManager = offsetManagers.get(tp);
                offsetManager.commit(tpOffset.getValue());
                LOG.debug("[{}] uncommitted offsets for partition [{}] after commit", offsetManager.getNumUncommittedOffsets(), tp);
            }
        } else {
            LOG.trace("No offsets to commit. {}", this);
        }
    }

    // ======== Ack =======
    @Override
    public void ack(Object messageId) {
        if (!isAtLeastOnceProcessing()) {
            return;
        }

        // Only need to keep track of acked tuples if commits to Kafka are controlled by
        // tuple acks, which happens only for at-least-once processing semantics
        final KafkaSpoutMessageId msgId = (KafkaSpoutMessageId) messageId;

        if (msgId.isNullTuple()) {
            //a null tuple should be added to the ack list since by definition is a direct ack
            offsetManagers.get(msgId.getTopicPartition()).addToAckMsgs(msgId);
            LOG.debug("Received direct ack for message [{}], associated with null tuple", msgId);
            tupleListener.onAck(msgId);
            return;
        }

        if (!emitted.contains(msgId)) {
            LOG.debug("Received ack for message [{}], associated with tuple emitted for a ConsumerRecord that "
                + "came from a topic-partition that this consumer group instance is no longer tracking "
                + "due to rebalance/partition reassignment. No action taken.", msgId);
        } else {
            Validate.isTrue(!retryService.isScheduled(msgId), "The message id " + msgId + " is queued for retry while being acked."
                + " This should never occur barring errors in the RetryService implementation or the spout code.");
            offsetManagers.get(msgId.getTopicPartition()).addToAckMsgs(msgId);
            emitted.remove(msgId);
        }
        tupleListener.onAck(msgId);
    }

    // ======== Fail =======
    @Override
    public void fail(Object messageId) {
        if (!isAtLeastOnceProcessing()) {
            return;
        }
        // Only need to keep track of failed tuples if commits to Kafka are controlled by
        // tuple acks, which happens only for at-least-once processing semantics
        final KafkaSpoutMessageId msgId = (KafkaSpoutMessageId) messageId;
        if (!emitted.contains(msgId)) {
            LOG.debug("Received fail for tuple this spout is no longer tracking."
                + " Partitions may have been reassigned. Ignoring message [{}]", msgId);
            return;
        }
        Validate.isTrue(!retryService.isScheduled(msgId), "The message id " + msgId + " is queued for retry while being failed."
            + " This should never occur barring errors in the RetryService implementation or the spout code.");

        msgId.incrementNumFails();

        if (!retryService.schedule(msgId)) {
            LOG.debug("Reached maximum number of retries. Message [{}] being marked as acked.", msgId);
            // this tuple should be removed from emitted only inside the ack() method. This is to ensure
            // that the OffsetManager for that TopicPartition is updated and allows commit progression
            tupleListener.onMaxRetryReached(msgId);
            ack(msgId);
        } else {
            tupleListener.onRetry(msgId);
            emitted.remove(msgId);
        }
    }

    // ======== Activate / Deactivate / Close / Declare Outputs =======
    @Override
    public void activate() {
        try {
            refreshAssignment();
        } catch (InterruptException e) {
            throwKafkaConsumerInterruptedException();
        }
    }

    private void refreshAssignment() {
        Set<TopicPartition> allPartitions = kafkaSpoutConfig.getTopicFilter().getAllSubscribedPartitions(consumer);
        List<TopicPartition> allPartitionsSorted = new ArrayList<>(allPartitions);
        Collections.sort(allPartitionsSorted, TopicPartitionComparator.INSTANCE);
        Set<TopicPartition> assignedPartitions = kafkaSpoutConfig.getTopicPartitioner()
            .getPartitionsForThisTask(allPartitionsSorted, context);
        topicAssigner.assignPartitions(consumer, assignedPartitions, rebalanceListener);
    }

    @Override
    public void deactivate() {
        try {
            commitIfNecessary();
        } catch (InterruptException e) {
            throwKafkaConsumerInterruptedException();
        }
    }

    @Override
    public void close() {
        try {
            shutdown();
        } catch (InterruptException e) {
            throwKafkaConsumerInterruptedException();
        }
    }

    private void commitIfNecessary() {
        if (isAtLeastOnceProcessing()) {
            commitOffsetsForAckedTuples();
        }
    }

    private void shutdown() {
        try {
            commitIfNecessary();
        } finally {
            //remove resources
            consumer.close();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        RecordTranslator<K, V> translator = kafkaSpoutConfig.getTranslator();
        for (String stream : translator.streams()) {
            declarer.declareStream(stream, translator.getFieldsFor(stream));
        }
    }

    @Override
    public String toString() {
        return "KafkaSpout{"
            + "offsetManagers =" + offsetManagers
            + ", emitted=" + emitted
            + "}";
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> configuration = super.getComponentConfiguration();
        if (configuration == null) {
            configuration = new HashMap<>();
        }
        String configKeyPrefix = "config.";

        configuration.put(configKeyPrefix + "topics", getTopicsString());

        configuration.put(configKeyPrefix + "groupid", kafkaSpoutConfig.getConsumerGroupId());
        for (Entry<String, Object> conf : kafkaSpoutConfig.getKafkaProps().entrySet()) {
            if (conf.getValue() != null && isPrimitiveOrWrapper(conf.getValue().getClass())) {
                configuration.put(configKeyPrefix + conf.getKey(), conf.getValue());
            } else {
                LOG.debug("Dropping Kafka prop '{}' from component configuration", conf.getKey());
            }
        }
        return configuration;
    }

    private boolean isPrimitiveOrWrapper(Class<?> type) {
        if (type == null) {
            return false;
        }
        return type.isPrimitive() || isWrapper(type);
    }

    private boolean isWrapper(Class<?> type) {
        return type == Double.class || type == Float.class || type == Long.class
            || type == Integer.class || type == Short.class || type == Character.class
            || type == Byte.class || type == Boolean.class || type == String.class;
    }

    private String getTopicsString() {
        return kafkaSpoutConfig.getTopicFilter().getTopicsString();
    }

    private static class PollablePartitionsInfo {

        private final Set<TopicPartition> pollablePartitions;
        //The subset of earliest retriable offsets that are on pollable partitions
        private final Map<TopicPartition, Long> pollableEarliestRetriableOffsets;

        PollablePartitionsInfo(Set<TopicPartition> pollablePartitions, Map<TopicPartition, Long> earliestRetriableOffsets) {
            this.pollablePartitions = pollablePartitions;
            this.pollableEarliestRetriableOffsets = earliestRetriableOffsets.entrySet().stream()
                .filter(entry -> pollablePartitions.contains(entry.getKey()))
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
        }

        public boolean shouldPoll() {
            return !this.pollablePartitions.isEmpty();
        }
    }

    @VisibleForTesting
    KafkaOffsetMetric<K, V> getKafkaOffsetMetric() {
        return kafkaOffsetMetric;
    }
}
