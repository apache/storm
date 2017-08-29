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

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;
import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.LATEST;
import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST;
import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST;

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
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.internal.KafkaConsumerFactory;
import org.apache.storm.kafka.spout.internal.KafkaConsumerFactoryDefault;
import org.apache.storm.kafka.spout.internal.OffsetManager;
import org.apache.storm.kafka.spout.internal.Timer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSpout<K, V> extends BaseRichSpout {
    private static final long serialVersionUID = 4151921085047987154L;
    //Initial delay for the commit and subscription refresh timers
    public static final long TIMER_DELAY_MS = 500;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);

    // Storm
    protected SpoutOutputCollector collector;

    // Kafka
    private final KafkaSpoutConfig<K, V> kafkaSpoutConfig;
    private KafkaConsumerFactory kafkaConsumerFactory;
    private transient KafkaConsumer<K, V> kafkaConsumer;
    private transient boolean consumerAutoCommitMode;

    // Bookkeeping
    // Strategy to determine the fetch offset of the first realized by the spout upon activation
    private transient FirstPollOffsetStrategy firstPollOffsetStrategy;  
    // Class that has the logic to handle tuple failure
    private transient KafkaSpoutRetryService retryService;
    // Handles tuple events (emit, ack etc.)
    private transient KafkaTupleListener tupleListener;
    // timer == null for auto commit mode
    private transient Timer commitTimer;                                
    // Flag indicating that the spout is still undergoing initialization process.
    private transient boolean initialized;                              
    // Initialization is only complete after the first call to  KafkaSpoutConsumerRebalanceListener.onPartitionsAssigned()

    // Tuples that were successfully acked/emitted. These tuples will be committed periodically when the commit timer expires,
    //or after a consumer rebalance, or during close/deactivate
    private transient Map<TopicPartition, OffsetManager> offsetManagers;
    // Tuples that have been emitted but that are "on the wire", i.e. pending being acked or failed. Not used if it's AutoCommitMode
    private transient Set<KafkaSpoutMessageId> emitted;                 
    // Records that have been polled and are queued to be emitted in the nextTuple() call. One record is emitted per nextTuple()
    private transient Iterator<ConsumerRecord<K, V>> waitingToEmit;     
    // Number of offsets that have been polled and emitted but not yet been committed. Not used if auto commit mode is enabled.
    private transient long numUncommittedOffsets;                       
    // Triggers when a subscription should be refreshed
    private transient Timer refreshSubscriptionTimer;                   
    private transient TopologyContext context;


    public KafkaSpout(KafkaSpoutConfig<K, V> kafkaSpoutConfig) {
        this(kafkaSpoutConfig, new KafkaConsumerFactoryDefault<>());
    }

    //This constructor is here for testing
    KafkaSpout(KafkaSpoutConfig<K, V> kafkaSpoutConfig, KafkaConsumerFactory<K, V> kafkaConsumerFactory) {
        this.kafkaConsumerFactory = kafkaConsumerFactory;
        this.kafkaSpoutConfig = kafkaSpoutConfig;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        initialized = false;
        this.context = context;

        // Spout internals
        this.collector = collector;
        numUncommittedOffsets = 0;

        // Offset management
        firstPollOffsetStrategy = kafkaSpoutConfig.getFirstPollOffsetStrategy();
        // with AutoCommitMode, offset will be periodically committed in the background by Kafka consumer
        consumerAutoCommitMode = kafkaSpoutConfig.isConsumerAutoCommitMode();

        // Retries management
        retryService = kafkaSpoutConfig.getRetryService();

        tupleListener = kafkaSpoutConfig.getTupleListener();

        if (!consumerAutoCommitMode) {     // If it is auto commit, no need to commit offsets manually
            commitTimer = new Timer(TIMER_DELAY_MS, kafkaSpoutConfig.getOffsetsCommitPeriodMs(), TimeUnit.MILLISECONDS);
        }
        refreshSubscriptionTimer = new Timer(TIMER_DELAY_MS, kafkaSpoutConfig.getPartitionRefreshPeriodMs(), TimeUnit.MILLISECONDS);

        offsetManagers = new HashMap<>();
        emitted = new HashSet<>();
        waitingToEmit = Collections.emptyListIterator();

        tupleListener.open(conf, context);

        LOG.info("Kafka Spout opened with the following configuration: {}", kafkaSpoutConfig);
    }

    // =========== Consumer Rebalance Listener - On the same thread as the caller ===========

    private class KafkaSpoutConsumerRebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            LOG.info("Partitions revoked. [consumer-group={}, consumer={}, topic-partitions={}]",
                    kafkaSpoutConfig.getConsumerGroupId(), kafkaConsumer, partitions);
            if (!consumerAutoCommitMode && initialized) {
                initialized = false;
                commitOffsetsForAckedTuples();
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            LOG.info("Partitions reassignment. [task-ID={}, consumer-group={}, consumer={}, topic-partitions={}]",
                    context.getThisTaskId(), kafkaSpoutConfig.getConsumerGroupId(), kafkaConsumer, partitions);

            tupleListener.onPartitionsReassigned(partitions);
            initialize(partitions);
        }

        private void initialize(Collection<TopicPartition> partitions) {
            if (!consumerAutoCommitMode) {
                // remove from acked all partitions that are no longer assigned to this spout
                offsetManagers.keySet().retainAll(partitions);
            }

            retryService.retainAll(partitions);

            /*
             * Emitted messages for partitions that are no longer assigned to this spout can't
             * be acked and should not be retried, hence remove them from emitted collection.
            */
            Set<TopicPartition> partitionsSet = new HashSet<>(partitions);
            Iterator<KafkaSpoutMessageId> msgIdIterator = emitted.iterator();
            while (msgIdIterator.hasNext()) {
                KafkaSpoutMessageId msgId = msgIdIterator.next();
                if (!partitionsSet.contains(msgId.getTopicPartition())) {
                    msgIdIterator.remove();
                }
            }

            for (TopicPartition tp : partitions) {
                final OffsetAndMetadata committedOffset = kafkaConsumer.committed(tp);
                final long fetchOffset = doSeek(tp, committedOffset);
                setAcked(tp, fetchOffset);
            }
            initialized = true;
            LOG.info("Initialization complete");
        }

        /**
         * sets the cursor to the location dictated by the first poll strategy and returns the fetch offset.
         */
        private long doSeek(TopicPartition tp, OffsetAndMetadata committedOffset) {
            long fetchOffset;
            if (committedOffset != null) {             // offset was committed for this TopicPartition
                if (firstPollOffsetStrategy.equals(EARLIEST)) {
                    kafkaConsumer.seekToBeginning(Collections.singleton(tp));
                    fetchOffset = kafkaConsumer.position(tp);
                } else if (firstPollOffsetStrategy.equals(LATEST)) {
                    kafkaConsumer.seekToEnd(Collections.singleton(tp));
                    fetchOffset = kafkaConsumer.position(tp);
                } else {
                    // By default polling starts at the last committed offset. +1 to point fetch to the first uncommitted offset.
                    fetchOffset = committedOffset.offset() + 1;
                    kafkaConsumer.seek(tp, fetchOffset);
                }
            } else {    // no commits have ever been done, so start at the beginning or end depending on the strategy
                if (firstPollOffsetStrategy.equals(EARLIEST) || firstPollOffsetStrategy.equals(UNCOMMITTED_EARLIEST)) {
                    kafkaConsumer.seekToBeginning(Collections.singleton(tp));
                } else if (firstPollOffsetStrategy.equals(LATEST) || firstPollOffsetStrategy.equals(UNCOMMITTED_LATEST)) {
                    kafkaConsumer.seekToEnd(Collections.singleton(tp));
                }
                fetchOffset = kafkaConsumer.position(tp);
            }
            return fetchOffset;
        }
    }

    private void setAcked(TopicPartition tp, long fetchOffset) {
        // If this partition was previously assigned to this spout, leave the acked offsets as they were to resume where it left off
        if (!consumerAutoCommitMode && !offsetManagers.containsKey(tp)) {
            offsetManagers.put(tp, new OffsetManager(tp, fetchOffset));
        }
    }

    // ======== Next Tuple =======

    @Override
    public void nextTuple() {
        try {
            if (initialized) {
                if (commit()) {
                    commitOffsetsForAckedTuples();
                }

                if (poll()) {
                    try {
                        setWaitingToEmit(pollKafkaBroker());
                    } catch (RetriableException e) {
                        LOG.error("Failed to poll from kafka.", e);
                    }
                }

                if (waitingToEmit()) {
                    emit();
                }
            } else {
                LOG.debug("Spout not initialized. Not sending tuples until initialization completes");
            }
        } catch (InterruptException e) {
            throwKafkaConsumerInterruptedException();
        }
    }

    private void throwKafkaConsumerInterruptedException() {
        //Kafka throws their own type of exception when interrupted.
        //Throw a new Java InterruptedException to ensure Storm can recognize the exception as a reaction to an interrupt.
        throw new RuntimeException(new InterruptedException("Kafka consumer was interrupted"));
    }

    private boolean commit() {
        return !consumerAutoCommitMode && commitTimer.isExpiredResetOnTrue();    // timer != null for non auto commit mode
    }

    private boolean poll() {
        final int maxUncommittedOffsets = kafkaSpoutConfig.getMaxUncommittedOffsets();
        final int readyMessageCount = retryService.readyMessageCount();
        final boolean poll = !waitingToEmit()
            //Check that the number of uncommitted, nonretriable tuples is less than the maxUncommittedOffsets limit
            //Accounting for retriable tuples this way still guarantees that the limit is followed on a per partition basis,
            //and prevents locking up the spout when there are too many retriable tuples
            && (numUncommittedOffsets - readyMessageCount < maxUncommittedOffsets
            || consumerAutoCommitMode);
        
        if (!poll) {
            if (waitingToEmit()) {
                LOG.debug("Not polling. Tuples waiting to be emitted."
                    + " [{}] uncommitted offsets across all topic partitions", numUncommittedOffsets);
            }

            if (numUncommittedOffsets >= maxUncommittedOffsets && !consumerAutoCommitMode) {
                LOG.debug("Not polling. [{}] uncommitted offsets across all topic partitions has reached the threshold of [{}]",
                    numUncommittedOffsets, maxUncommittedOffsets);
            }
        }
        return poll;
    }

    private boolean waitingToEmit() {
        return waitingToEmit != null && waitingToEmit.hasNext();
    }

    private void setWaitingToEmit(ConsumerRecords<K, V> consumerRecords) {
        List<ConsumerRecord<K, V>> waitingToEmitList = new LinkedList<>();
        for (TopicPartition tp : consumerRecords.partitions()) {
            waitingToEmitList.addAll(consumerRecords.records(tp));
        }
        waitingToEmit = waitingToEmitList.iterator();
    }

    // ======== poll =========
    private ConsumerRecords<K, V> pollKafkaBroker() {
        doSeekRetriableTopicPartitions();
        if (refreshSubscriptionTimer.isExpiredResetOnTrue()) {
            kafkaSpoutConfig.getSubscription().refreshAssignment();
        }
        final ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(kafkaSpoutConfig.getPollTimeoutMs());
        final int numPolledRecords = consumerRecords.count();
        LOG.debug("Polled [{}] records from Kafka. [{}] uncommitted offsets across all topic partitions",
            numPolledRecords, numUncommittedOffsets);
        return consumerRecords;
    }

    private void doSeekRetriableTopicPartitions() {
        final Map<TopicPartition, Long> retriableTopicPartitions = retryService.earliestRetriableOffsets();

        for (Entry<TopicPartition, Long> retriableTopicPartitionAndOffset : retriableTopicPartitions.entrySet()) {
            //Seek directly to the earliest retriable message for each retriable topic partition
            kafkaConsumer.seek(retriableTopicPartitionAndOffset.getKey(), retriableTopicPartitionAndOffset.getValue());
        }
    }

    // ======== emit  =========
    private void emit() {
        while (!emitTupleIfNotEmitted(waitingToEmit.next()) && waitingToEmit.hasNext()) {
            waitingToEmit.remove();
        }
    }

    /**
     * Creates a tuple from the kafka record and emits it if it was not yet emitted
     *
     * @param record to be emitted
     * @return true if tuple was emitted. False if tuple has been acked or has been emitted and is pending ack or fail
     */
    private boolean emitTupleIfNotEmitted(ConsumerRecord<K, V> record) {
        final TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        final KafkaSpoutMessageId msgId = retryService.getMessageId(record);
        if (offsetManagers.containsKey(tp) && offsetManagers.get(tp).contains(msgId)) {   // has been acked
            LOG.trace("Tuple for record [{}] has already been acked. Skipping", record);
        } else if (emitted.contains(msgId)) {   // has been emitted and it's pending ack or fail
            LOG.trace("Tuple for record [{}] has already been emitted. Skipping", record);
        } else {
            final List<Object> tuple = kafkaSpoutConfig.getTranslator().apply(record);
            if (isEmitTuple(tuple)) {
                final boolean isScheduled = retryService.isScheduled(msgId);
                // not scheduled <=> never failed (i.e. never emitted), or scheduled and ready to be retried
                if (!isScheduled || retryService.isReady(msgId)) {
                    if (consumerAutoCommitMode) {
                        if (tuple instanceof KafkaTuple) {
                            collector.emit(((KafkaTuple) tuple).getStream(), tuple);
                        } else {
                            collector.emit(tuple);
                        }
                    } else {
                        emitted.add(msgId);
                        offsetManagers.get(tp).addToEmitMsgs(msgId.offset());
                        if (isScheduled) {  // Was scheduled for retry and re-emitted, so remove from schedule.
                            retryService.remove(msgId);
                        } else {            //New tuple, hence increment the uncommitted offset counter
                            numUncommittedOffsets++;
                        }

                        if (tuple instanceof KafkaTuple) {
                            collector.emit(((KafkaTuple) tuple).getStream(), tuple, msgId);
                        } else {
                            collector.emit(tuple, msgId);
                        }
                        tupleListener.onEmit(tuple, msgId);
                    }
                    LOG.trace("Emitted tuple [{}] for record [{}] with msgId [{}]", tuple, record, msgId);
                    return true;
                }
            } else {
                LOG.debug("Not emitting null tuple for record [{}] as defined in configuration.", record);
                msgId.setEmitted(false);
                ack(msgId);
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

    private void commitOffsetsForAckedTuples() {
        // Find offsets that are ready to be committed for every topic partition
        final Map<TopicPartition, OffsetAndMetadata> nextCommitOffsets = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetManager> tpOffset : offsetManagers.entrySet()) {
            final OffsetAndMetadata nextCommitOffset = tpOffset.getValue().findNextCommitOffset();
            if (nextCommitOffset != null) {
                nextCommitOffsets.put(tpOffset.getKey(), nextCommitOffset);
            }
        }

        // Commit offsets that are ready to be committed for every topic partition
        if (!nextCommitOffsets.isEmpty()) {
            kafkaConsumer.commitSync(nextCommitOffsets);
            LOG.debug("Offsets successfully committed to Kafka [{}]", nextCommitOffsets);
            // Instead of iterating again, it would be possible to commit and update the state for each TopicPartition
            // in the prior loop, but the multiple network calls should be more expensive than iterating twice over a small loop
            for (Map.Entry<TopicPartition, OffsetAndMetadata> tpOffset : nextCommitOffsets.entrySet()) {
                //Update the OffsetManager for each committed partition, and update numUncommittedOffsets
                final TopicPartition tp = tpOffset.getKey();
                final OffsetManager offsetManager = offsetManagers.get(tp);
                long numCommittedOffsets = offsetManager.commit(tpOffset.getValue());
                numUncommittedOffsets -= numCommittedOffsets;
                LOG.debug("[{}] uncommitted offsets across all topic partitions",
                        numUncommittedOffsets);
            }
        } else {
            LOG.trace("No offsets to commit. {}", this);
        }
    }

    // ======== Ack =======

    @Override
    public void ack(Object messageId) {
        final KafkaSpoutMessageId msgId = (KafkaSpoutMessageId) messageId;
        if (!emitted.contains(msgId)) {
            if (msgId.isEmitted()) {
                LOG.debug("Received ack for message [{}], associated with tuple emitted for a ConsumerRecord that "
                        + "came from a topic-partition that this consumer group instance is no longer tracking "
                        + "due to rebalance/partition reassignment. No action taken.", msgId);
            } else {
                LOG.debug("Received direct ack for message [{}], associated with null tuple", msgId);
            }
        } else {
            if (!consumerAutoCommitMode) {  // Only need to keep track of acked tuples if commits are not done automatically
                offsetManagers.get(msgId.getTopicPartition()).addToAckMsgs(msgId);
            }
            emitted.remove(msgId);
        }
        tupleListener.onAck(msgId);
    }

    // ======== Fail =======

    @Override
    public void fail(Object messageId) {
        final KafkaSpoutMessageId msgId = (KafkaSpoutMessageId) messageId;
        if (!emitted.contains(msgId)) {
            LOG.debug("Received fail for tuple this spout is no longer tracking."
                + " Partitions may have been reassigned. Ignoring message [{}]", msgId);
            return;
        }
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
            subscribeKafkaConsumer();
        } catch (InterruptException e) {
            throwKafkaConsumerInterruptedException();
        }
    }

    private void subscribeKafkaConsumer() {
        kafkaConsumer = kafkaConsumerFactory.createConsumer(kafkaSpoutConfig);

        kafkaSpoutConfig.getSubscription().subscribe(kafkaConsumer, new KafkaSpoutConsumerRebalanceListener(), context);
    }

    @Override
    public void deactivate() {
        try {
            shutdown();
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

    private void shutdown() {
        try {
            if (!consumerAutoCommitMode) {
                commitOffsetsForAckedTuples();
            }
        } finally {
            //remove resources
            kafkaConsumer.close();
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
        configuration.put(configKeyPrefix + "bootstrap.servers", kafkaSpoutConfig.getKafkaProps().get("bootstrap.servers"));
        configuration.put(configKeyPrefix + "security.protocol", kafkaSpoutConfig.getKafkaProps().get("security.protocol"));
        return configuration;
    }

    private String getTopicsString() {
        return kafkaSpoutConfig.getSubscription().getTopicsString();
    }
}



