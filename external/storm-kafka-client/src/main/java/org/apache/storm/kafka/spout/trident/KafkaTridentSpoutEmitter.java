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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.kafka.spout.TopicPartitionComparator;
import org.apache.storm.kafka.spout.internal.KafkaConsumerFactory;
import org.apache.storm.kafka.spout.internal.KafkaConsumerFactoryDefault;
import org.apache.storm.kafka.spout.subscription.TopicAssigner;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IOpaquePartitionedTridentSpout;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTridentSpoutEmitter<K, V> implements IOpaquePartitionedTridentSpout.Emitter<
        List<Map<String, Object>>,
        KafkaTridentSpoutTopicPartition,
        Map<String, Object>>,
        Serializable {

    private static final long serialVersionUID = -7343927794834130435L;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTridentSpoutEmitter.class);

    // Kafka
    private final KafkaConsumer<K, V> kafkaConsumer;
    private final KafkaSpoutConfig<K, V> kafkaSpoutConfig;
    private final TopicAssigner topicAssigner;
    
    // set of topic-partitions for which first poll has already occurred, and the first polled txid
    private final Map<TopicPartition, Long> firstPollTransaction = new HashMap<>(); 

    private final long pollTimeoutMs;
    private final KafkaSpoutConfig.FirstPollOffsetStrategy firstPollOffsetStrategy;
    private final RecordTranslator<K, V> translator;
    private final TopicPartitionSerializer tpSerializer = new TopicPartitionSerializer();
    private final TopologyContext topologyContext;

    /**
     * Create a new Kafka spout emitter.
     * @param kafkaSpoutConfig The kafka spout config
     * @param topologyContext The topology context
     */
    public KafkaTridentSpoutEmitter(KafkaSpoutConfig<K, V> kafkaSpoutConfig, TopologyContext topologyContext) {
        this(kafkaSpoutConfig, topologyContext, new KafkaConsumerFactoryDefault<>(), new TopicAssigner());
    }
    
    KafkaTridentSpoutEmitter(KafkaSpoutConfig<K, V> kafkaSpoutConfig, TopologyContext topologyContext,
        KafkaConsumerFactory<K, V> consumerFactory, TopicAssigner topicAssigner) {
        this.kafkaSpoutConfig = kafkaSpoutConfig;
        this.kafkaConsumer = consumerFactory.createConsumer(kafkaSpoutConfig);
        this.topologyContext = topologyContext;
        this.translator = kafkaSpoutConfig.getTranslator();
        this.topicAssigner = topicAssigner;
        this.pollTimeoutMs = kafkaSpoutConfig.getPollTimeoutMs();
        this.firstPollOffsetStrategy = kafkaSpoutConfig.getFirstPollOffsetStrategy();
        LOG.debug("Created {}", this.toString());
    }

    @Override
    public Map<String, Object> emitPartitionBatch(TransactionAttempt tx, TridentCollector collector,
            KafkaTridentSpoutTopicPartition currBatchPartition, Map<String, Object> lastBatch) {

        LOG.debug("Processing batch: [transaction = {}], [currBatchPartition = {}], [lastBatchMetadata = {}], [collector = {}]",
                tx, currBatchPartition, lastBatch, collector);

        final TopicPartition currBatchTp = currBatchPartition.getTopicPartition();
        final Set<TopicPartition> assignments = kafkaConsumer.assignment();
        KafkaTridentSpoutBatchMetadata lastBatchMeta = lastBatch == null ? null : KafkaTridentSpoutBatchMetadata.fromMap(lastBatch);
        KafkaTridentSpoutBatchMetadata currentBatch = lastBatchMeta;
        Collection<TopicPartition> pausedTopicPartitions = Collections.emptySet();

        if (!assignments.contains(currBatchPartition.getTopicPartition())) {
            throw new IllegalStateException("The spout is asked to emit tuples on a partition it is not assigned."
                + " This indicates a bug in the TopicFilter or ManualPartitioner implementations."
                + " The current partition is [" + currBatchPartition + "], the assigned partitions are [" + assignments + "].");
        } else {
            try {
                // pause other topic-partitions to only poll from current topic-partition
                pausedTopicPartitions = pauseTopicPartitions(currBatchTp);

                seek(currBatchTp, lastBatchMeta, tx.getTransactionId());

                final ConsumerRecords<K, V> records = kafkaConsumer.poll(pollTimeoutMs);
                LOG.debug("Polled [{}] records from Kafka.", records.count());

                if (!records.isEmpty()) {
                    emitTuples(collector, records);
                    // build new metadata
                    currentBatch = new KafkaTridentSpoutBatchMetadata(records.records(currBatchTp));
                }
            } finally {
                kafkaConsumer.resume(pausedTopicPartitions);
                LOG.trace("Resumed topic-partitions {}", pausedTopicPartitions);
            }
            LOG.debug("Emitted batch: [transaction = {}], [currBatchPartition = {}], [lastBatchMetadata = {}], "
                    + "[currBatchMetadata = {}], [collector = {}]", tx, currBatchPartition, lastBatch, currentBatch, collector);
        }

        return currentBatch == null ? null : currentBatch.toMap();
    }

    private void emitTuples(TridentCollector collector, ConsumerRecords<K, V> records) {
        for (ConsumerRecord<K, V> record : records) {
            final List<Object> tuple = translator.apply(record);
            collector.emit(tuple);
            LOG.debug("Emitted tuple {} for record [{}]", tuple, record);
        }
    }

    /**
     * Determines the offset of the next fetch. Will use the firstPollOffsetStrategy if this is the first poll for the topic partition.
     * Otherwise the next offset will be one past the last batch, based on lastBatchMeta.
     * 
     * <p>lastBatchMeta should only be null when the previous txid was not emitted (e.g. new topic),
     * it is the first poll for the spout instance, or it is a replay of the first txid this spout emitted on this partition.
     * In the second case, there are either no previous transactions, or the MBC is still committing them
     * and they will fail because this spout did not emit the corresponding batches. If it had emitted them, the meta could not be null. 
     * In any case, the lastBatchMeta should never be null if this is not the first poll for this spout instance.
     *
     * @return the offset of the next fetch
     */
    private long seek(TopicPartition tp, KafkaTridentSpoutBatchMetadata lastBatchMeta, long transactionId) {
        if (isFirstPoll(tp, transactionId)) {
            if (firstPollOffsetStrategy == EARLIEST) {
                LOG.debug("First poll for topic partition [{}], seeking to partition beginning", tp);
                kafkaConsumer.seekToBeginning(Collections.singleton(tp));
            } else if (firstPollOffsetStrategy == LATEST) {
                LOG.debug("First poll for topic partition [{}], seeking to partition end", tp);
                kafkaConsumer.seekToEnd(Collections.singleton(tp));
            } else if (lastBatchMeta != null) {
                LOG.debug("First poll for topic partition [{}], using last batch metadata", tp);
                kafkaConsumer.seek(tp, lastBatchMeta.getLastOffset() + 1);  // seek next offset after last offset from previous batch
            } else if (firstPollOffsetStrategy == UNCOMMITTED_EARLIEST) {
                LOG.debug("First poll for topic partition [{}] with no last batch metadata, seeking to partition beginning", tp);
                kafkaConsumer.seekToBeginning(Collections.singleton(tp));
            } else if (firstPollOffsetStrategy == UNCOMMITTED_LATEST) {
                LOG.debug("First poll for topic partition [{}] with no last batch metadata, seeking to partition end", tp);
                kafkaConsumer.seekToEnd(Collections.singleton(tp));
            }
            firstPollTransaction.put(tp, transactionId);
        } else {
            kafkaConsumer.seek(tp, lastBatchMeta.getLastOffset() + 1);  // seek next offset after last offset from previous batch
            LOG.debug("First poll for topic partition [{}], using last batch metadata", tp);
        }

        final long fetchOffset = kafkaConsumer.position(tp);
        LOG.debug("Set [fetchOffset = {}] for partition [{}]", fetchOffset, tp);
        return fetchOffset;
    }

    private boolean isFirstPoll(TopicPartition tp, long txid) {
        // The first poll is either the "real" first transaction, or a replay of the first transaction
        return !firstPollTransaction.containsKey(tp) || firstPollTransaction.get(tp) == txid;
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

    @Override
    public List<KafkaTridentSpoutTopicPartition> getPartitionsForTask(int taskId, int numTasks,
        List<KafkaTridentSpoutTopicPartition> allPartitionInfoSorted) {
        List<TopicPartition> tps = allPartitionInfoSorted.stream()
            .map(kttp -> kttp.getTopicPartition())
            .collect(Collectors.toList());
        final Set<TopicPartition> assignedTps = kafkaSpoutConfig.getTopicPartitioner().getPartitionsForThisTask(tps, topologyContext);
        LOG.debug("Consumer [{}], running on task with index [{}], has assigned topic-partitions {}", kafkaConsumer, taskId, assignedTps);
        final List<KafkaTridentSpoutTopicPartition> taskTps = newKafkaTridentSpoutTopicPartitions(assignedTps);
        return taskTps;
    }
    
    
    @Override
    public void refreshPartitions(List<KafkaTridentSpoutTopicPartition> partitionResponsibilities) {
        Set<TopicPartition> assignedTps = partitionResponsibilities.stream()
            .map(kttp -> kttp.getTopicPartition())
            .collect(Collectors.toSet());
        topicAssigner.assignPartitions(kafkaConsumer, assignedTps, new KafkaSpoutConsumerRebalanceListener());
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

    @Override
    public void close() {
        kafkaConsumer.close();
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
            LOG.info("Partitions revoked. [consumer-group={}, consumer={}, topic-partitions={}]",
                    kafkaSpoutConfig.getConsumerGroupId(), kafkaConsumer, partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            LOG.info("Partitions reassignment. [consumer-group={}, consumer={}, topic-partitions={}]",
                    kafkaSpoutConfig.getConsumerGroupId(), kafkaConsumer, partitions);
        }
    }
}
