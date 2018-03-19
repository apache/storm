/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.kafka;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;

import org.apache.storm.Config;
import org.apache.storm.kafka.KafkaSpout.EmitState;
import org.apache.storm.kafka.trident.MaxMetric;
import org.apache.storm.metric.api.CombinedMetric;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.ReducedMetric;
import org.apache.storm.spout.SpoutOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

/**
 * Manages partitions.
 * @author unknown
 * @param <S> the type of spout config required by the
 * {@link FailedMsgRetryManager}
 * @param <F> the type of {@link FailedMsgRetryManager}
 */
public class PartitionManager<S extends SpoutConfig,
        F extends FailedMsgRetryManager<S>> {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionManager.class);

    private final CombinedMetric _fetchAPILatencyMax;
    private final ReducedMetric _fetchAPILatencyMean;
    private final CountMetric _fetchAPICallCount;
    private final CountMetric _fetchAPIMessageCount;
    // Count of messages which could not be emitted or retried because they were deleted from kafka
    private final CountMetric _lostMessageCount;
    // Count of messages which were not retried because failedMsgRetryManager didn't consider offset eligible for
    // retry
    private final CountMetric _messageIneligibleForRetryCount;
    Long emittedToOffset;
    // _pending key = Kafka offset, value = time at which the message was first submitted to the topology
    private SortedMap<Long,Long> pending = new TreeMap<Long,Long>();
    private final F failedMsgRetryManager;

    // retryRecords key = Kafka offset, value = retry info for the given message
    Long committedTo;
    LinkedList<MessageAndOffset> waitingToEmit = new LinkedList<MessageAndOffset>();
    Partition _partition;
    S spoutConfig;
    String _topologyInstanceId;
    SimpleConsumer _consumer;
    DynamicPartitionConnections connections;
    ZkState state;
    Map topoConf;
    long numberFailed, numberAcked;

    /**
     * Creates a new partition manager.
     * @param connectionsArg the connections to use
     * @param topologyInstanceIdArg the topology id
     * @param stateArg the initial state
     * @param topoConfArg the topology configuration
     * @param spoutConfigArg the spout configuration
     * @param idArg the id to use
     * @param previousManager previous partition manager if manager for
     * partition is being recreated
     */
    public PartitionManager(
            final DynamicPartitionConnections connectionsArg,
            final String topologyInstanceIdArg,
            final ZkState stateArg,
            final Map<String, Object> topoConfArg,
            final S spoutConfigArg,
            final Partition idArg,
            final PartitionManager<S, F> previousManager)
    {
        this(connectionsArg,
                topologyInstanceIdArg,
                stateArg,
                topoConfArg,
                spoutConfigArg,
                idArg,
                previousManager.failedMsgRetryManager,
                previousManager.committedTo,
                previousManager.emittedToOffset,
                previousManager.waitingToEmit,
                previousManager.pending,
                false //init
        );
    }

    /**
     * Creates a new partition manager.
     * @param connectionsArg the connections to use
     * @param topologyInstanceIdArg the topology id
     * @param stateArg the initial state
     * @param topoConfArg the topology configuration
     * @param spoutConfigArg the spout configuration
     * @param idArg the id to use
     * @param failedMsgRetryManagerArg the failed message retry manager
     */
    public PartitionManager(
            final DynamicPartitionConnections connectionsArg,
            final String topologyInstanceIdArg,
            final ZkState stateArg,
            final Map<String, Object> topoConfArg,
            final S spoutConfigArg,
            final Partition idArg,
            final F failedMsgRetryManagerArg) {
        this(connectionsArg,
                topologyInstanceIdArg,
                stateArg,
                topoConfArg,
                spoutConfigArg,
                idArg,
                failedMsgRetryManagerArg,
                0l,
                0l,
                new LinkedList<>(),
                new TreeMap<>(),
                true);
    }

    /**
     * Creates a new partition manager.
     * @param connectionsArg the connections to use
     * @param topologyInstanceIdArg the topology id
     * @param stateArg the initial state
     * @param topoConfArg the topology configuration
     * @param spoutConfigArg the spout configuration
     * @param idArg the id to use
     * @param failedMsgRetryManagerArg the failed message retry manager
     * @param committedTo the committed
     * @param emittedToOffset the emitted offset
     * @param waitingToEmit the initial waiting to emit queue
     * @param pending the initial pending queue
     * @param init flag to run the init routine
     */
    public PartitionManager(
            final DynamicPartitionConnections connections,
            final String topologyInstanceId,
            final ZkState state,
            final Map<String, Object> topoConf,
            final S spoutConfig,
            final Partition id,
            final F failedMsgRetryManager,
            final Long committedTo,
            final Long emittedToOffset,
            final LinkedList<MessageAndOffset> waitingToEmit,
            final SortedMap<Long,Long> pending,
            final boolean init) {
        _partition = id;
        this.connections = connections;
        this.spoutConfig = spoutConfig;
        _topologyInstanceId = topologyInstanceId;
        _consumer = connections.register(id.host, id.topic, id.partition);
        this.state = state;
        this.topoConf = topoConf;
        numberAcked = numberFailed = 0;
        this.failedMsgRetryManager = failedMsgRetryManager;
        this.committedTo = committedTo;
        this.emittedToOffset = emittedToOffset;
        this.waitingToEmit = waitingToEmit;
        this.pending = pending;
        LOG.info("Recreating PartitionManager based on previous manager, _waitingToEmit size: {}, _pending size: {}",
                waitingToEmit.size(),
                pending.size());
        if (init) {
            failedMsgRetryManager.prepare(spoutConfig, this.topoConf);

            String jsonTopologyId = null;
            Long jsonOffset = null;
            String path = committedPath();
            try {
                Map<Object, Object> json = this.state.readJSON(path);
                LOG.info("Read partition information from: " + path + "  --> " + json);
                if (json != null) {
                    jsonTopologyId = (String) ((Map<Object, Object>) json.get("topology")).get("id");
                    jsonOffset = (Long) json.get("offset");
                }
            } catch (Throwable e) {
                LOG.warn("Error reading and/or parsing at ZkNode: " + path, e);
            }

            String topic = _partition.topic;
            Long currentOffset = KafkaUtils.getOffset(_consumer, topic, id.partition, spoutConfig);

            if (jsonTopologyId == null || jsonOffset == null) { // failed to parse JSON?
                this.committedTo = currentOffset;
                LOG.info("No partition information found, using configuration to determine offset");
            } else if (!topologyInstanceId.equals(jsonTopologyId) && spoutConfig.ignoreZkOffsets) {
                this.committedTo = KafkaUtils.getOffset(_consumer, topic, id.partition, spoutConfig.startOffsetTime);
                LOG.info("Topology change detected and ignore zookeeper offsets set to true, using configuration to determine offset");
            } else {
                this.committedTo = jsonOffset;
                LOG.info("Read last commit offset from zookeeper: " + this.committedTo + "; old topology_id: " + jsonTopologyId + " - new topology_id: " + topologyInstanceId);
            }

            if (currentOffset - this.committedTo > spoutConfig.maxOffsetBehind || this.committedTo <= 0) {
                LOG.info("Last commit offset from zookeeper: " + this.committedTo);
                Long lastCommittedOffset = this.committedTo;
                this.committedTo = currentOffset;
                LOG.info("Commit offset " + lastCommittedOffset + " is more than " +
                        spoutConfig.maxOffsetBehind + " behind latest offset " + currentOffset + ", resetting to startOffsetTime=" + spoutConfig.startOffsetTime);
            }

            LOG.info("Starting Kafka " + _consumer.host() + " " + id + " from offset " + this.committedTo);
            this.emittedToOffset = this.committedTo;
        }

        _fetchAPILatencyMax = new CombinedMetric(new MaxMetric());
        _fetchAPILatencyMean = new ReducedMetric(new MeanReducer());
        _fetchAPICallCount = new CountMetric();
        _fetchAPIMessageCount = new CountMetric();
        _lostMessageCount = new CountMetric();
        _messageIneligibleForRetryCount = new CountMetric();
    }

    public Map getMetricsDataMap() {
        String metricPrefix = _partition.getId();

        Map<String, Object> ret = new HashMap<>();
        ret.put(metricPrefix + "/fetchAPILatencyMax", _fetchAPILatencyMax.getValueAndReset());
        ret.put(metricPrefix + "/fetchAPILatencyMean", _fetchAPILatencyMean.getValueAndReset());
        ret.put(metricPrefix + "/fetchAPICallCount", _fetchAPICallCount.getValueAndReset());
        ret.put(metricPrefix + "/fetchAPIMessageCount", _fetchAPIMessageCount.getValueAndReset());
        ret.put(metricPrefix + "/lostMessageCount", _lostMessageCount.getValueAndReset());
        ret.put(metricPrefix + "/messageIneligibleForRetryCount", _messageIneligibleForRetryCount.getValueAndReset());
        return ret;
    }

    //returns false if it's reached the end of current batch
    public EmitState next(SpoutOutputCollector collector) {
        if (waitingToEmit.isEmpty()) {
            fill();
        }
        while (true) {
            MessageAndOffset toEmit = waitingToEmit.pollFirst();
            if (toEmit == null) {
                return EmitState.NO_EMITTED;
            }

            Iterable<List<Object>> tups;
            if (spoutConfig.scheme instanceof MessageMetadataSchemeAsMultiScheme) {
                tups = KafkaUtils.generateTuples((MessageMetadataSchemeAsMultiScheme) spoutConfig.scheme, toEmit.message(), _partition, toEmit.offset());
            } else {
                tups = KafkaUtils.generateTuples(spoutConfig, toEmit.message(), _partition.topic);
            }

            if ((tups != null) && tups.iterator().hasNext()) {
               if (!Strings.isNullOrEmpty(spoutConfig.outputStreamId)) {
                    for (List<Object> tup : tups) {
                        collector.emit(spoutConfig.outputStreamId, tup, new KafkaMessageId(_partition, toEmit.offset()));
                    }
                } else {
                    for (List<Object> tup : tups) {
                        collector.emit(tup, new KafkaMessageId(_partition, toEmit.offset()));
                    }
                }
                break;
            } else {
                ack(toEmit.offset());
            }
        }
        if (!waitingToEmit.isEmpty()) {
            return EmitState.EMITTED_MORE_LEFT;
        } else {
            return EmitState.EMITTED_END;
        }
    }


    private void fill() {
        long start = System.currentTimeMillis();
        Long offset;

        // Are there failed tuples? If so, fetch those first.
        offset = this.failedMsgRetryManager.nextFailedMessageToRetry();
        final boolean processingNewTuples = (offset == null);
        if (processingNewTuples) {
            offset = emittedToOffset;
        }

        ByteBufferMessageSet msgs = null;
        try {
            msgs = KafkaUtils.fetchMessages(spoutConfig, _consumer, _partition, offset);
        } catch (TopicOffsetOutOfRangeException e) {
            offset = KafkaUtils.getOffset(_consumer, _partition.topic, _partition.partition, kafka.api.OffsetRequest.EarliestTime());
            // fetch failed, so don't update the fetch metrics

            //fix bug [STORM-643] : remove outdated failed offsets
            if (!processingNewTuples) {
                // For the case of EarliestTime it would be better to discard
                // all the failed offsets, that are earlier than actual EarliestTime
                // offset, since they are anyway not there.
                // These calls to broker API will be then saved.
                Set<Long> omitted = this.failedMsgRetryManager.clearOffsetsBefore(offset);

                // Omitted messages have not been acked and may be lost
                if (null != omitted) {
                    _lostMessageCount.incrBy(omitted.size());
                }

                pending.headMap(offset).clear();

                LOG.warn("Removing the failed offsets for {} that are out of range: {}", _partition, omitted);
            }

            if (offset > emittedToOffset) {
                _lostMessageCount.incrBy(offset - emittedToOffset);
                emittedToOffset = offset;
                LOG.warn("{} Using new offset: {}", _partition, emittedToOffset);
            }

            return;
        }
        long millis = System.currentTimeMillis() - start;
        _fetchAPILatencyMax.update(millis);
        _fetchAPILatencyMean.update(millis);
        _fetchAPICallCount.incr();
        if (msgs != null) {
            int numMessages = 0;

            for (MessageAndOffset msg : msgs) {
                final Long cur_offset = msg.offset();
                if (cur_offset < offset) {
                    // Skip any old offsets.
                    continue;
                }
                if (processingNewTuples || this.failedMsgRetryManager.shouldReEmitMsg(cur_offset)) {
                    numMessages += 1;
                    if (!pending.containsKey(cur_offset)) {
                        pending.put(cur_offset, System.currentTimeMillis());
                    }
                    waitingToEmit.add(msg);
                    emittedToOffset = Math.max(msg.nextOffset(), emittedToOffset);
                    if (failedMsgRetryManager.shouldReEmitMsg(cur_offset)) {
                        this.failedMsgRetryManager.retryStarted(cur_offset);
                    }
                }
            }
            _fetchAPIMessageCount.incrBy(numMessages);
        }
    }

    public void ack(Long offset) {
        if (!pending.isEmpty() && pending.firstKey() < offset - spoutConfig.maxOffsetBehind) {
            // Too many things pending!
            pending.headMap(offset - spoutConfig.maxOffsetBehind).clear();
        }
        pending.remove(offset);
        this.failedMsgRetryManager.acked(offset);
        numberAcked++;
    }

    public void fail(Long offset) {
        if (offset < emittedToOffset - spoutConfig.maxOffsetBehind) {
            LOG.info("Skipping failed tuple at offset={}" +
                        " because it's more than maxOffsetBehind={}" +
                        " behind _emittedToOffset={} for {}",
                offset,
                spoutConfig.maxOffsetBehind,
                emittedToOffset,
                _partition
            );
        } else {
            LOG.debug("Failing at offset={} with _pending.size()={} pending and _emittedToOffset={} for {}", offset, pending.size(), emittedToOffset, _partition);
            numberFailed++;
            if (numberAcked == 0 && numberFailed > spoutConfig.maxOffsetBehind) {
                throw new RuntimeException("Too many tuple failures");
            }

            // Offset may not be considered for retry by failedMsgRetryManager
            if (this.failedMsgRetryManager.retryFurther(offset)) {
                this.failedMsgRetryManager.failed(offset);
            } else {
                // state for the offset should be cleaned up
                LOG.warn("Will not retry failed kafka offset {} further", offset);
                _messageIneligibleForRetryCount.incr();
                this.failedMsgRetryManager.cleanOffsetAfterRetries(_partition, offset);
                pending.remove(offset);
                this.failedMsgRetryManager.acked(offset);
            }
        }
    }

    public void commit() {
        long lastCompletedOffset = lastCompletedOffset();
        if (committedTo != lastCompletedOffset) {
            LOG.debug("Writing last completed offset ({}) to ZK for {} for topology: {}", lastCompletedOffset, _partition, _topologyInstanceId);
            Map<Object, Object> data = (Map<Object, Object>) ImmutableMap.builder()
                    .put("topology", ImmutableMap.of("id", _topologyInstanceId,
                            "name", topoConf.get(Config.TOPOLOGY_NAME)))
                    .put("offset", lastCompletedOffset)
                    .put("partition", _partition.partition)
                    .put("broker", ImmutableMap.of("host", _partition.host.host,
                            "port", _partition.host.port))
                    .put("topic", _partition.topic).build();
            state.writeJSON(committedPath(), data);

            committedTo = lastCompletedOffset;
            LOG.debug("Wrote last completed offset ({}) to ZK for {} for topology: {}", lastCompletedOffset, _partition, _topologyInstanceId);
        } else {
            LOG.debug("No new offset for {} for topology: {}", _partition, _topologyInstanceId);
        }
    }

    protected String committedPath() {
        return spoutConfig.zkRoot + "/" + spoutConfig.id + "/" + _partition.getId();
    }

    public long lastCompletedOffset() {
        if (pending.isEmpty()) {
            return emittedToOffset;
        } else {
            return pending.firstKey();
        }
    }

    public OffsetData getOffsetData() {
        return new OffsetData(emittedToOffset, lastCompletedOffset());
    }

    public Partition getPartition() {
        return _partition;
    }

    public void close() {
        commit();
        connections.unregister(_partition.host, _partition.topic , _partition.partition);
    }

    static class KafkaMessageId implements Serializable {
        public Partition partition;
        public long offset;

        public KafkaMessageId(Partition partition, long offset) {
            this.partition = partition;
            this.offset = offset;
        }
    }

    public static class OffsetData {
        public long latestEmittedOffset;
        public long latestCompletedOffset;

        public OffsetData(long latestEmittedOffset, long latestCompletedOffset) {
            this.latestEmittedOffset = latestEmittedOffset;
            this.latestCompletedOffset = latestCompletedOffset;
        }
    }
}
