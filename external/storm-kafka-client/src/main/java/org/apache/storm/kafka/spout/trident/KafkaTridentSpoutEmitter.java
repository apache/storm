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

import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.EARLIEST;
import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.LATEST;
import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.TIMESTAMP;
import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST;
import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.UNCOMMITTED_LATEST;
import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.UNCOMMITTED_TIMESTAMP;

import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.kafka.spout.TopicPartitionComparator;
import org.apache.storm.kafka.spout.internal.ConsumerFactory;
import org.apache.storm.kafka.spout.internal.ConsumerFactoryDefault;
import org.apache.storm.kafka.spout.subscription.TopicAssigner;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTridentSpoutEmitter<K, V> implements Serializable {

    private static final long serialVersionUID = -7343927794834130435L;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTridentSpoutEmitter.class);

    // Kafka
    private final Consumer<K, V> consumer;
    private final KafkaTridentSpoutConfig<K, V> kafkaSpoutConfig;
    private final TopicAssigner topicAssigner;

    // The first seek offset for each topic partition, i.e. the offset this spout instance started processing at.
    private final Map<TopicPartition, Long> tpToFirstSeekOffset = new HashMap<>();

    private final long pollTimeoutMs;
    private final FirstPollOffsetStrategy firstPollOffsetStrategy;
    private final RecordTranslator<K, V> translator;
    private final TopicPartitionSerializer tpSerializer = new TopicPartitionSerializer();
    private final TopologyContext topologyContext;
    private final long startTimeStamp;

    /**
     * Create a new Kafka spout emitter.
     *
     * @param kafkaSpoutConfig The kafka spout config
     * @param topologyContext The topology context
     */
    public KafkaTridentSpoutEmitter(KafkaTridentSpoutConfig<K, V> kafkaSpoutConfig, TopologyContext topologyContext) {
        this(kafkaSpoutConfig, topologyContext, new ConsumerFactoryDefault<>(), new TopicAssigner());
    }

    @VisibleForTesting
    KafkaTridentSpoutEmitter(KafkaTridentSpoutConfig<K, V> kafkaSpoutConfig, TopologyContext topologyContext,
        ConsumerFactory<K, V> consumerFactory, TopicAssigner topicAssigner) {
        this.kafkaSpoutConfig = kafkaSpoutConfig;
        this.consumer = consumerFactory.createConsumer(kafkaSpoutConfig.getKafkaProps());
        this.topologyContext = topologyContext;
        this.translator = kafkaSpoutConfig.getTranslator();
        this.topicAssigner = topicAssigner;
        this.pollTimeoutMs = kafkaSpoutConfig.getPollTimeoutMs();
        this.firstPollOffsetStrategy = kafkaSpoutConfig.getFirstPollOffsetStrategy();
        this.startTimeStamp = kafkaSpoutConfig.getStartTimeStamp();
        LOG.debug("Created {}", this.toString());
    }

    /**
     * Emit a batch that has already been emitted.
     */
    public void reEmitPartitionBatch(TransactionAttempt tx, TridentCollector collector,
        KafkaTridentSpoutTopicPartition currBatchPartition, Map<String, Object> currBatch) {

        final TopicPartition currBatchTp = currBatchPartition.getTopicPartition();

        throwIfEmittingForUnassignedPartition(currBatchTp);

        KafkaTridentSpoutBatchMetadata currBatchMeta = KafkaTridentSpoutBatchMetadata.fromMap(currBatch);
        Collection<TopicPartition> pausedTopicPartitions = Collections.emptySet();

        if (!topologyContext.getStormId().equals(currBatchMeta.getTopologyId())
            && isFirstPollOffsetStrategyIgnoringCommittedOffsets()) {
            LOG.debug("Skipping re-emit of batch that was originally emitted by another topology,"
                + " because the current first poll offset strategy ignores committed offsets.");
            return;
        }

        LOG.debug("Re-emitting batch: [transaction= {}], [currBatchPartition = {}], [currBatchMetadata = {}], [collector = {}]",
            tx, currBatchPartition, currBatch, collector);

        try {
            // pause other topic-partitions to only poll from current topic-partition
            pausedTopicPartitions = pauseTopicPartitions(currBatchTp);

            long seekOffset = currBatchMeta.getFirstOffset();
            if (seekOffset < 0 && currBatchMeta.getFirstOffset() == currBatchMeta.getLastOffset()) {
                LOG.debug("Skipping re-emit of batch with negative starting offset."
                    + " The spout may set a negative starting offset for an empty batch that occurs at the start of a partition."
                    + " It is not expected that Trident will replay such an empty batch,"
                    + " but this guard is here in case it tries to do so. See STORM-2990, STORM-3279 for context.");
                return;
            }
            LOG.debug("Seeking to offset [{}] for topic partition [{}]", seekOffset, currBatchTp);
            consumer.seek(currBatchTp, seekOffset);

            final ConsumerRecords<K, V> records = consumer.poll(pollTimeoutMs);
            LOG.debug("Polled [{}] records from Kafka.", records.count());

            for (ConsumerRecord<K, V> record : records) {
                if (record.offset() == currBatchMeta.getLastOffset() + 1) {
                    break;
                }
                if (record.offset() > currBatchMeta.getLastOffset()) {
                    throw new RuntimeException(String.format("Error when re-emitting batch. Overshot the end of the batch."
                        + " The batch end offset was [{%d}], but received [{%d}]."
                        + " Ensure log compaction is disabled in Kafka, since it is incompatible with non-opaque transactional spouts.",
                        currBatchMeta.getLastOffset(), record.offset()));
                }
                emitTuple(collector, record);
            }
        } finally {
            consumer.resume(pausedTopicPartitions);
            LOG.trace("Resumed topic-partitions {}", pausedTopicPartitions);
        }
        LOG.debug("Re-emitted batch: [transaction = {}], [currBatchPartition = {}], [currBatchMetadata = {}], "
            + "[collector = {}]", tx, currBatchPartition, currBatchMeta, collector);
    }

    /**
     * Emit a new batch.
     */
    public Map<String, Object> emitPartitionBatchNew(TransactionAttempt tx, TridentCollector collector,
        KafkaTridentSpoutTopicPartition currBatchPartition, Map<String, Object> lastBatch) {

        LOG.debug("Processing batch: [transaction = {}], [currBatchPartition = {}], [lastBatchMetadata = {}], [collector = {}]",
            tx, currBatchPartition, lastBatch, collector);

        final TopicPartition currBatchTp = currBatchPartition.getTopicPartition();

        throwIfEmittingForUnassignedPartition(currBatchTp);

        KafkaTridentSpoutBatchMetadata lastBatchMeta = lastBatch == null ? null : KafkaTridentSpoutBatchMetadata.fromMap(lastBatch);
        KafkaTridentSpoutBatchMetadata currentBatch = lastBatchMeta;
        Collection<TopicPartition> pausedTopicPartitions = Collections.emptySet();

        try {
            // pause other topic-partitions to only poll from current topic-partition
            pausedTopicPartitions = pauseTopicPartitions(currBatchTp);

            seek(currBatchTp, lastBatchMeta);

            final List<ConsumerRecord<K, V>> records = consumer.poll(pollTimeoutMs).records(currBatchTp);
            LOG.debug("Polled [{}] records from Kafka.", records.size());

            if (!records.isEmpty()) {
                for (ConsumerRecord<K, V> record : records) {
                    emitTuple(collector, record);
                }
                // build new metadata based on emitted records
                currentBatch = new KafkaTridentSpoutBatchMetadata(
                    records.get(0).offset(),
                    records.get(records.size() - 1).offset(),
                    topologyContext.getStormId());
            } else {
                //Build new metadata based on the consumer position.
                //We want the next emit to start at the current consumer position,
                //so make a meta that indicates that position - 1 is the last emitted offset
                //This helps us avoid cases like STORM-3279, and simplifies the seek logic.
                long lastEmittedOffset = consumer.position(currBatchTp) - 1;
                currentBatch = new KafkaTridentSpoutBatchMetadata(lastEmittedOffset, lastEmittedOffset, topologyContext.getStormId());
            }
        } finally {
            consumer.resume(pausedTopicPartitions);
            LOG.trace("Resumed topic-partitions {}", pausedTopicPartitions);
        }
        LOG.debug("Emitted batch: [transaction = {}], [currBatchPartition = {}], [lastBatchMetadata = {}], "
            + "[currBatchMetadata = {}], [collector = {}]", tx, currBatchPartition, lastBatch, currentBatch, collector);

        return currentBatch.toMap();
    }

    private boolean isFirstPollOffsetStrategyIgnoringCommittedOffsets() {
        return firstPollOffsetStrategy == FirstPollOffsetStrategy.EARLIEST
            || firstPollOffsetStrategy == FirstPollOffsetStrategy.LATEST;
    }

    private void throwIfEmittingForUnassignedPartition(TopicPartition currBatchTp) {
        final Set<TopicPartition> assignments = consumer.assignment();
        if (!assignments.contains(currBatchTp)) {
            throw new IllegalStateException("The spout is asked to emit tuples on a partition it is not assigned."
                + " This indicates a bug in the TopicFilter or ManualPartitioner implementations."
                + " The current partition is [" + currBatchTp + "], the assigned partitions are [" + assignments + "].");
        }
    }

    private void emitTuple(TridentCollector collector, ConsumerRecord<K, V> record) {
        final List<Object> tuple = translator.apply(record);
        collector.emit(tuple);
        LOG.debug("Emitted tuple {} for record [{}]", tuple, record);
    }

    /**
     * Determines the offset of the next fetch. Will use the firstPollOffsetStrategy if this is the first poll for the topic partition.
     * Otherwise the next offset will be one past the last batch, based on lastBatchMeta.
     *
     * <p>lastBatchMeta should only be null in the following cases:
     * <ul>
     * <li>This is the first batch for this partition</li>
     * <li>This is a replay of the first batch for this partition</li>
     * </ul>
     *
     * @return the offset of the next fetch
     */
    private long seek(TopicPartition tp, KafkaTridentSpoutBatchMetadata lastBatchMeta) {
        if (isFirstPollSinceExecutorStarted(tp)) {
            boolean isFirstPollSinceTopologyWasDeployed = lastBatchMeta == null 
                || !topologyContext.getStormId().equals(lastBatchMeta.getTopologyId());
            if (firstPollOffsetStrategy == EARLIEST && isFirstPollSinceTopologyWasDeployed) {
                LOG.debug("First poll for topic partition [{}], seeking to partition beginning", tp);
                consumer.seekToBeginning(Collections.singleton(tp));
            } else if (firstPollOffsetStrategy == LATEST && isFirstPollSinceTopologyWasDeployed) {
                LOG.debug("First poll for topic partition [{}], seeking to partition end", tp);
                consumer.seekToEnd(Collections.singleton(tp));
            } else if (firstPollOffsetStrategy == TIMESTAMP && isFirstPollSinceTopologyWasDeployed) {
                LOG.debug("First poll for topic partition [{}], seeking to partition based on startTimeStamp", tp);
                seekOffsetByStartTimeStamp(tp);
            } else if (lastBatchMeta != null) {
                LOG.debug("First poll for topic partition [{}], using last batch metadata", tp);
                consumer.seek(tp, lastBatchMeta.getLastOffset() + 1);  // seek next offset after last offset from previous batch
            } else if (firstPollOffsetStrategy == UNCOMMITTED_EARLIEST) {
                LOG.debug("First poll for topic partition [{}] with no last batch metadata, seeking to partition beginning", tp);
                consumer.seekToBeginning(Collections.singleton(tp));
            } else if (firstPollOffsetStrategy == UNCOMMITTED_LATEST) {
                LOG.debug("First poll for topic partition [{}] with no last batch metadata, seeking to partition end", tp);
                consumer.seekToEnd(Collections.singleton(tp));
            } else if (firstPollOffsetStrategy == UNCOMMITTED_TIMESTAMP) {
                LOG.debug("First poll for topic partition [{}] with no last batch metadata, "
                        + "seeking to partition based on startTimeStamp", tp);
                seekOffsetByStartTimeStamp(tp);
            }
            tpToFirstSeekOffset.put(tp, consumer.position(tp));
        } else if (lastBatchMeta != null) {
            consumer.seek(tp, lastBatchMeta.getLastOffset() + 1);  // seek next offset after last offset from previous batch
            LOG.debug("First poll for topic partition [{}], using last batch metadata", tp);
        } else {
            /*
             * Last batch meta is null, but this is not the first batch emitted for this partition by this emitter instance. This is
             * a replay of the first batch for this partition. Use the offset the consumer started at.
             */
            long initialFetchOffset = tpToFirstSeekOffset.get(tp);
            consumer.seek(tp, initialFetchOffset);
            LOG.debug("First poll for topic partition [{}], no last batch metadata present."
                + " Using stored initial fetch offset [{}]", tp, initialFetchOffset);
        }

        final long fetchOffset = consumer.position(tp);
        LOG.debug("Set [fetchOffset = {}] for partition [{}]", fetchOffset, tp);
        return fetchOffset;
    }

    /**
     * Seek the consumer to offset corresponding to startTimeStamp.
     */
    private void seekOffsetByStartTimeStamp(TopicPartition tp) {
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(Collections.singletonMap(tp, startTimeStamp));
        OffsetAndTimestamp startOffsetAndTimeStamp = offsetsForTimes.get(tp);
        long startTimeStampOffset = startOffsetAndTimeStamp.offset();
        LOG.debug("First poll for topic partition [{}], seeking to partition from startTimeStamp [{}]", tp, startTimeStamp);
        consumer.seek(tp, startTimeStampOffset);
    }

    private boolean isFirstPollSinceExecutorStarted(TopicPartition tp) {
        return !tpToFirstSeekOffset.containsKey(tp);
    }

    // returns paused topic-partitions.
    private Collection<TopicPartition> pauseTopicPartitions(TopicPartition excludedTp) {
        final Set<TopicPartition> pausedTopicPartitions = new HashSet<>(consumer.assignment());
        LOG.debug("Currently assigned topic-partitions {}", pausedTopicPartitions);
        pausedTopicPartitions.remove(excludedTp);
        consumer.pause(pausedTopicPartitions);
        LOG.debug("Paused topic-partitions {}", pausedTopicPartitions);
        return pausedTopicPartitions;
    }

    /**
     * Get the input partitions in sorted order.
     */
    public List<KafkaTridentSpoutTopicPartition> getOrderedPartitions(final List<Map<String, Object>> allPartitionInfo) {
        List<TopicPartition> sortedPartitions = allPartitionInfo.stream()
            .map(map -> tpSerializer.fromMap(map))
            .sorted(TopicPartitionComparator.INSTANCE)
            .collect(Collectors.toList());
        final List<KafkaTridentSpoutTopicPartition> allPartitions = newKafkaTridentSpoutTopicPartitions(sortedPartitions);
        LOG.debug("Returning all topic-partitions {} across all tasks. Current task index [{}]. Total tasks [{}] ",
            allPartitions, topologyContext.getThisTaskIndex(), getNumTasks());
        return allPartitions;
    }

    /**
     * Get the partitions that should be handled by this task.
     */
    public List<KafkaTridentSpoutTopicPartition> getPartitionsForTask(int taskId, int numTasks,
        List<KafkaTridentSpoutTopicPartition> allPartitionInfoSorted) {
        List<TopicPartition> tps = allPartitionInfoSorted.stream()
            .map(kttp -> kttp.getTopicPartition())
            .collect(Collectors.toList());
        final Set<TopicPartition> assignedTps = kafkaSpoutConfig.getTopicPartitioner().getPartitionsForThisTask(tps, topologyContext);
        LOG.debug("Consumer [{}], running on task with index [{}], has assigned topic-partitions {}", consumer, taskId, assignedTps);
        final List<KafkaTridentSpoutTopicPartition> taskTps = newKafkaTridentSpoutTopicPartitions(assignedTps);
        return taskTps;
    }

    /**
     * Prepare the emitter to handle the input partitions.
     */
    public void refreshPartitions(List<KafkaTridentSpoutTopicPartition> partitionResponsibilities) {
        Set<TopicPartition> assignedTps = partitionResponsibilities.stream()
            .map(kttp -> kttp.getTopicPartition())
            .collect(Collectors.toSet());
        topicAssigner.assignPartitions(consumer, assignedTps, new KafkaSpoutConsumerRebalanceListener());
        LOG.debug("Assigned partitions [{}] to this task", assignedTps);
    }

    private List<KafkaTridentSpoutTopicPartition> newKafkaTridentSpoutTopicPartitions(Collection<TopicPartition> tps) {
        final List<KafkaTridentSpoutTopicPartition> kttp = new ArrayList<>(tps.size());
        for (TopicPartition tp : tps) {
            LOG.trace("Added topic-partition [{}]", tp);
            kttp.add(new KafkaTridentSpoutTopicPartition(tp));
        }
        return kttp;
    }

    private int getNumTasks() {
        return topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size();
    }

    public void close() {
        consumer.close();
        LOG.debug("Closed");
    }

    @Override
    public final String toString() {
        return super.toString()
            + "{kafkaSpoutConfig=" + kafkaSpoutConfig
            + '}';
    }

    /**
     * Just logs reassignments.
     */
    private class KafkaSpoutConsumerRebalanceListener implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            LOG.info("Partitions revoked. [consumer={}, topic-partitions={}]",
                consumer, partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            LOG.info("Partitions reassignment. [consumer={}, topic-partitions={}]",
                consumer, partitions);
        }
    }
}
