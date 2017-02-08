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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
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
import org.apache.storm.kafka.spout.internal.Timer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSpout<K, V> extends BaseRichSpout {
    private static final long serialVersionUID = 4151921085047987154L;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);
    private static final Comparator<KafkaSpoutMessageId> OFFSET_COMPARATOR = new OffsetComparator();

    // Storm
    protected SpoutOutputCollector collector;

    // Kafka
    private final KafkaSpoutConfig<K, V> kafkaSpoutConfig;
    private final KafkaConsumerFactory kafkaConsumerFactory;
    private transient KafkaConsumer<K, V> kafkaConsumer;
    private transient boolean consumerAutoCommitMode;


    // Bookkeeping
    private transient FirstPollOffsetStrategy firstPollOffsetStrategy;  // Strategy to determine the fetch offset of the first realized by the spout upon activation
    private transient KafkaSpoutRetryService retryService;              // Class that has the logic to handle tuple failure
    private transient Timer commitTimer;                                // timer == null for auto commit mode
    private transient boolean initialized;                              // Flag indicating that the spout is still undergoing initialization process.
    // Initialization is only complete after the first call to  KafkaSpoutConsumerRebalanceListener.onPartitionsAssigned()

    transient Map<TopicPartition, OffsetEntry> acked;           // Tuples that were successfully acked. These tuples will be committed periodically when the commit timer expires, after consumer rebalance, or on close/deactivate
    private transient Set<KafkaSpoutMessageId> emitted;                 // Tuples that have been emitted but that are "on the wire", i.e. pending being acked or failed
    private transient Iterator<ConsumerRecord<K, V>> waitingToEmit;         // Records that have been polled and are queued to be emitted in the nextTuple() call. One record is emitted per nextTuple()
    private transient long numUncommittedOffsets;                       // Number of offsets that have been polled and emitted but not yet been committed
    private transient TopologyContext context;
    private transient Timer refreshSubscriptionTimer;                   // Used to say when a subscription should be refreshed


    public KafkaSpout(KafkaSpoutConfig<K, V> kafkaSpoutConfig) {
        this(kafkaSpoutConfig, new KafkaConsumerFactoryDefault());
    }
    
    //This constructor is here for testing
    KafkaSpout(KafkaSpoutConfig<K, V> kafkaSpoutConfig, KafkaConsumerFactory<K, V> kafkaConsumerFactory) {
        this.kafkaSpoutConfig = kafkaSpoutConfig;                 // Pass in configuration
        this.kafkaConsumerFactory = kafkaConsumerFactory;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        initialized = false;
        this.context = context;

        // Spout internals
        this.collector = collector;
        numUncommittedOffsets = 0;

        // Offset management
        firstPollOffsetStrategy = kafkaSpoutConfig.getFirstPollOffsetStrategy();
        consumerAutoCommitMode = kafkaSpoutConfig.isConsumerAutoCommitMode();

        // Retries management
        retryService = kafkaSpoutConfig.getRetryService();

        if (!consumerAutoCommitMode) {     // If it is auto commit, no need to commit offsets manually
            commitTimer = new Timer(500, kafkaSpoutConfig.getOffsetsCommitPeriodMs(), TimeUnit.MILLISECONDS);
        }
        refreshSubscriptionTimer = new Timer(500, kafkaSpoutConfig.getPartitionRefreshPeriodMs(), TimeUnit.MILLISECONDS);

        acked = new HashMap<>();
        emitted = new HashSet<>();
        waitingToEmit = Collections.emptyListIterator();

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
            LOG.info("Partitions reassignment. [consumer-group={}, consumer={}, topic-partitions={}]",
                    kafkaSpoutConfig.getConsumerGroupId(), kafkaConsumer, partitions);

            initialize(partitions);
        }

        private void initialize(Collection<TopicPartition> partitions) {
            if (!consumerAutoCommitMode) {
                acked.keySet().retainAll(partitions);   // remove from acked all partitions that are no longer assigned to this spout
            }

            retryService.retainAll(partitions);
            
            //Emitted messages for partitions that are no longer assigned to this spout can't be acked, and they shouldn't be retried. Remove them from emitted.
            Set<TopicPartition> partitionsSet = new HashSet<>(partitions);
            emitted.removeIf((msgId) -> !partitionsSet.contains(msgId.getTopicPartition()));

            for (TopicPartition tp : partitions) {
                final OffsetAndMetadata committedOffset = kafkaConsumer.committed(tp);
                final long fetchOffset = doSeek(tp, committedOffset);
                setAcked(tp, fetchOffset);
            }
            initialized = true;
            LOG.info("Initialization complete");
        }

        /**
         * sets the cursor to the location dictated by the first poll strategy and returns the fetch offset
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
        if (!consumerAutoCommitMode && !acked.containsKey(tp)) {
            acked.put(tp, new OffsetEntry(tp, fetchOffset));
        }
    }

    // ======== Next Tuple =======

    @Override
    public void nextTuple() {
        try{
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
        final boolean poll = !waitingToEmit() && numUncommittedOffsets < maxUncommittedOffsets;

        if (!poll) {
            if (waitingToEmit()) {
                LOG.debug("Not polling. Tuples waiting to be emitted. [{}] uncommitted offsets across all topic partitions", numUncommittedOffsets);
            }

            if (numUncommittedOffsets >= maxUncommittedOffsets) {
                LOG.debug("Not polling. [{}] uncommitted offsets across all topic partitions has reached the threshold of [{}]", numUncommittedOffsets, maxUncommittedOffsets);
            }
        }
        return poll;
    }

    private boolean waitingToEmit() {
        return waitingToEmit != null && waitingToEmit.hasNext();
    }

    public void setWaitingToEmit(ConsumerRecords<K,V> consumerRecords) {
        List<ConsumerRecord<K,V>> waitingToEmitList = new LinkedList<>();
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
        LOG.debug("Polled [{}] records from Kafka. [{}] uncommitted offsets across all topic partitions", numPolledRecords, numUncommittedOffsets);
        return consumerRecords;
    }

    private void doSeekRetriableTopicPartitions() {
        final Set<TopicPartition> retriableTopicPartitions = retryService.retriableTopicPartitions();

        for (TopicPartition rtp : retriableTopicPartitions) {
            final OffsetAndMetadata offsetAndMeta = acked.get(rtp).findNextCommitOffset();
            if (offsetAndMeta != null) {
                kafkaConsumer.seek(rtp, offsetAndMeta.offset() + 1);  // seek to the next offset that is ready to commit in next commit cycle
            } else {
                kafkaConsumer.seek(rtp, acked.get(rtp).committedOffset + 1);    // Seek to last committed offset
            }
        }
    }

    // ======== emit  =========
    private void emit() {
        while(!emitTupleIfNotEmitted(waitingToEmit.next()) && waitingToEmit.hasNext()) {
            waitingToEmit.remove();
        }
    }

    //Emits one tuple per record
    //@return true if tuple was emitted
    private boolean emitTupleIfNotEmitted(ConsumerRecord<K, V> record) {
        final TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        final KafkaSpoutMessageId msgId = new KafkaSpoutMessageId(record);

        if (acked.containsKey(tp) && acked.get(tp).contains(msgId)) {   // has been acked
            LOG.trace("Tuple for record [{}] has already been acked. Skipping", record);
        } else if (emitted.contains(msgId)) {   // has been emitted and it's pending ack or fail
            LOG.trace("Tuple for record [{}] has already been emitted. Skipping", record);
        } else {
            boolean isScheduled = retryService.isScheduled(msgId);
            if (!isScheduled || retryService.isReady(msgId)) {   // not scheduled <=> never failed (i.e. never emitted) or ready to be retried
                final List<Object> tuple = kafkaSpoutConfig.getTranslator().apply(record);
                if (tuple instanceof KafkaTuple) {
                    collector.emit(((KafkaTuple)tuple).getStream(), tuple, msgId);
                } else {
                    collector.emit(tuple, msgId);
                }
                emitted.add(msgId);
                numUncommittedOffsets++;
                if (isScheduled) { // Was scheduled for retry, now being re-emitted. Remove from schedule.
                    retryService.remove(msgId);
                }
                LOG.trace("Emitted tuple [{}] for record [{}]", tuple, record);
                return true;
            }
        }
        return false;
    }

    private void commitOffsetsForAckedTuples() {
        // Find offsets that are ready to be committed for every topic partition
        final Map<TopicPartition, OffsetAndMetadata> nextCommitOffsets = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetEntry> tpOffset : acked.entrySet()) {
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
            for (Map.Entry<TopicPartition, OffsetEntry> tpOffset : acked.entrySet()) {
                final OffsetEntry offsetEntry = tpOffset.getValue();
                offsetEntry.commit(nextCommitOffsets.get(tpOffset.getKey()));
            }
        } else {
            LOG.trace("No offsets to commit. {}", this);
        }
    }

    // ======== Ack =======

    @Override
    public void ack(Object messageId) {
        final KafkaSpoutMessageId msgId = (KafkaSpoutMessageId) messageId;
        if(!emitted.contains(msgId)) {
            LOG.debug("Received ack for tuple this spout is no longer tracking. Partitions may have been reassigned. Ignoring message [{}]", msgId);
            return;
        }
        
        if (!consumerAutoCommitMode) {  // Only need to keep track of acked tuples if commits are not done automatically
            acked.get(msgId.getTopicPartition()).add(msgId);
        }
        emitted.remove(msgId);
    }

    // ======== Fail =======

    @Override
    public void fail(Object messageId) {
        final KafkaSpoutMessageId msgId = (KafkaSpoutMessageId) messageId;
        if(!emitted.contains(msgId)) {
            LOG.debug("Received fail for tuple this spout is no longer tracking. Partitions may have been reassigned. Ignoring message [{}]", msgId);
            return;
        }
        emitted.remove(msgId);
        msgId.incrementNumFails();
        if (!retryService.schedule(msgId)) {
            LOG.debug("Reached maximum number of retries. Message [{}] being marked as acked.", msgId);
            ack(msgId);
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
        for (String stream: translator.streams()) {
            declarer.declareStream(stream, translator.getFieldsFor(stream));
        }
    }

    @Override
    public String toString() {
        return "KafkaSpout{" +
                "acked=" + acked +
                ", emitted=" + emitted +
                "}";
    }

    @Override
    public Map<String, Object> getComponentConfiguration () {
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

    // ======= Offsets Commit Management ==========

    private static class OffsetComparator implements Comparator<KafkaSpoutMessageId> {
        public int compare(KafkaSpoutMessageId m1, KafkaSpoutMessageId m2) {
            return m1.offset() < m2.offset() ? -1 : m1.offset() == m2.offset() ? 0 : 1;
        }
    }

    /**
     * This class is not thread safe
     */
    class OffsetEntry {
        private final TopicPartition tp;
        private final long initialFetchOffset;  /* First offset to be fetched. It is either set to the beginning, end, or to the first uncommitted offset.
                                                 * Initial value depends on offset strategy. See KafkaSpoutConsumerRebalanceListener */
        private long committedOffset;     // last offset committed to Kafka. Initially it is set to fetchOffset - 1
        private final NavigableSet<KafkaSpoutMessageId> ackedMsgs = new TreeSet<>(OFFSET_COMPARATOR);     // acked messages sorted by ascending order of offset

        public OffsetEntry(TopicPartition tp, long initialFetchOffset) {
            this.tp = tp;
            this.initialFetchOffset = initialFetchOffset;
            this.committedOffset = initialFetchOffset - 1;
            LOG.debug("Instantiated {}", this);
        }

        public void add(KafkaSpoutMessageId msgId) {          // O(Log N)
            ackedMsgs.add(msgId);
        }

        /**
         * An offset is only committed when all records with lower offset have
         * been acked. This guarantees that all offsets smaller than the
         * committedOffset have been delivered.
         * @return the next OffsetAndMetadata to commit, or null if no offset is ready to commit.
         */
        public OffsetAndMetadata findNextCommitOffset() {
            boolean found = false;
            long currOffset;
            long nextCommitOffset = committedOffset;
            KafkaSpoutMessageId nextCommitMsg = null;     // this is a convenience variable to make it faster to create OffsetAndMetadata

            for (KafkaSpoutMessageId currAckedMsg : ackedMsgs) {  // complexity is that of a linear scan on a TreeMap
                if ((currOffset = currAckedMsg.offset()) == nextCommitOffset + 1) {            // found the next offset to commit
                    found = true;
                    nextCommitMsg = currAckedMsg;
                    nextCommitOffset = currOffset;
                } else if (currAckedMsg.offset() > nextCommitOffset + 1) {    // offset found is not continuous to the offsets listed to go in the next commit, so stop search
                    LOG.debug("topic-partition [{}] has non-continuous offset [{}]. It will be processed in a subsequent batch.", tp, currOffset);
                    break;
                } else {
                    //Received a redundant ack. Ignore and continue processing.
                    LOG.warn("topic-partition [{}] has unexpected offset [{}]. Current committed Offset [{}]",
                            tp, currOffset,  committedOffset);
                }
            }

            OffsetAndMetadata nextCommitOffsetAndMetadata = null;
            if (found) {
                nextCommitOffsetAndMetadata = new OffsetAndMetadata(nextCommitOffset, nextCommitMsg.getMetadata(Thread.currentThread()));
                LOG.debug("topic-partition [{}] has offsets [{}-{}] ready to be committed",tp, committedOffset + 1, nextCommitOffsetAndMetadata.offset());
            } else {
                LOG.debug("topic-partition [{}] has NO offsets ready to be committed", tp);
            }
            LOG.trace("{}", this);
            return nextCommitOffsetAndMetadata;
        }

        /**
         * Marks an offset has committed. This method has side effects - it sets the internal state in such a way that future
         * calls to {@link #findNextCommitOffset()} will return offsets greater than the offset specified, if any.
         *
         * @param committedOffset offset to be marked as committed
         */
        public void commit(OffsetAndMetadata committedOffset) {
            long numCommittedOffsets = 0;
            if (committedOffset != null) {
                final long oldCommittedOffset = this.committedOffset;
                numCommittedOffsets = committedOffset.offset() - this.committedOffset;
                this.committedOffset = committedOffset.offset();
                for (Iterator<KafkaSpoutMessageId> iterator = ackedMsgs.iterator(); iterator.hasNext(); ) {
                    if (iterator.next().offset() <= committedOffset.offset()) {
                        iterator.remove();
                    } else {
                        break;
                    }
                }
                numUncommittedOffsets-= numCommittedOffsets;
                LOG.debug("Committed offsets [{}-{} = {}] for topic-partition [{}]. [{}] uncommitted offsets across all topic partitions",
                        oldCommittedOffset + 1, this.committedOffset, numCommittedOffsets, tp, numUncommittedOffsets);
            } else {
                LOG.debug("Committed [{}] offsets for topic-partition [{}]. [{}] uncommitted offsets across all topic partitions",
                        numCommittedOffsets, tp, numUncommittedOffsets);
            }
            LOG.trace("{}", this);
        }

        long getCommittedOffset() {
            return committedOffset;
        }

        public boolean isEmpty() {
            return ackedMsgs.isEmpty();
        }

        public boolean contains(ConsumerRecord<K, V> record) {
            return contains(new KafkaSpoutMessageId(record));
        }

        public boolean contains(KafkaSpoutMessageId msgId) {
            return ackedMsgs.contains(msgId);
        }

        @Override
        public String toString() {
            return "OffsetEntry{" +
                    "topic-partition=" + tp +
                    ", fetchOffset=" + initialFetchOffset +
                    ", committedOffset=" + committedOffset +
                    ", ackedMsgs=" + ackedMsgs +
                    '}';
        }
    }
}
