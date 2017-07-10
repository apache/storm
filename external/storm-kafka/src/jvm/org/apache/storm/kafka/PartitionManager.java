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

public class PartitionManager {
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
    Long _emittedToOffset;
    // _pending key = Kafka offset, value = time at which the message was first submitted to the topology
    private SortedMap<Long,Long> _pending = new TreeMap<Long,Long>();
    private final FailedMsgRetryManager _failedMsgRetryManager;

    // retryRecords key = Kafka offset, value = retry info for the given message
    Long _committedTo;
    LinkedList<MessageAndOffset> _waitingToEmit = new LinkedList<MessageAndOffset>();
    Partition _partition;
    SpoutConfig _spoutConfig;
    String _topologyInstanceId;
    SimpleConsumer _consumer;
    DynamicPartitionConnections _connections;
    ZkState _state;
    Map _topoConf;
    long numberFailed, numberAcked;

    public PartitionManager(
            DynamicPartitionConnections connections,
            String topologyInstanceId,
            ZkState state,
            Map<String, Object> topoConf,
            SpoutConfig spoutConfig,
            Partition id)
    {
        this(connections, topologyInstanceId, state, topoConf, spoutConfig, id, null);
    }

    /**
     * @param previousManager previous partition manager if manager for partition is being recreated
     */
    public PartitionManager(
            DynamicPartitionConnections connections,
            String topologyInstanceId,
            ZkState state,
            Map<String, Object> topoConf,
            SpoutConfig spoutConfig,
            Partition id,
            PartitionManager previousManager) {
        _partition = id;
        _connections = connections;
        _spoutConfig = spoutConfig;
        _topologyInstanceId = topologyInstanceId;
        _consumer = connections.register(id.host, id.topic, id.partition);
        _state = state;
        _topoConf = topoConf;
        numberAcked = numberFailed = 0;

        if (previousManager != null) {
            _failedMsgRetryManager = previousManager._failedMsgRetryManager;
            _committedTo = previousManager._committedTo;
            _emittedToOffset = previousManager._emittedToOffset;
            _waitingToEmit = previousManager._waitingToEmit;
            _pending = previousManager._pending;
            LOG.info("Recreating PartitionManager based on previous manager, _waitingToEmit size: {}, _pending size: {}",
                    _waitingToEmit.size(),
                    _pending.size());
        } else {
            try {
                _failedMsgRetryManager = (FailedMsgRetryManager) Class.forName(spoutConfig.failedMsgRetryManagerClass).newInstance();
                _failedMsgRetryManager.prepare(spoutConfig, _topoConf);
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                throw new IllegalArgumentException(String.format("Failed to create an instance of <%s> from: <%s>",
                        FailedMsgRetryManager.class,
                        spoutConfig.failedMsgRetryManagerClass), e);
            }

            String jsonTopologyId = null;
            Long jsonOffset = null;
            String path = committedPath();
            try {
                Map<Object, Object> json = _state.readJSON(path);
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
                _committedTo = currentOffset;
                LOG.info("No partition information found, using configuration to determine offset");
            } else if (!topologyInstanceId.equals(jsonTopologyId) && spoutConfig.ignoreZkOffsets) {
                _committedTo = KafkaUtils.getOffset(_consumer, topic, id.partition, spoutConfig.startOffsetTime);
                LOG.info("Topology change detected and ignore zookeeper offsets set to true, using configuration to determine offset");
            } else {
                _committedTo = jsonOffset;
                LOG.info("Read last commit offset from zookeeper: " + _committedTo + "; old topology_id: " + jsonTopologyId + " - new topology_id: " + topologyInstanceId);
            }

            if (currentOffset - _committedTo > spoutConfig.maxOffsetBehind || _committedTo <= 0) {
                LOG.info("Last commit offset from zookeeper: " + _committedTo);
                Long lastCommittedOffset = _committedTo;
                _committedTo = currentOffset;
                LOG.info("Commit offset " + lastCommittedOffset + " is more than " +
                        spoutConfig.maxOffsetBehind + " behind latest offset " + currentOffset + ", resetting to startOffsetTime=" + spoutConfig.startOffsetTime);
            }

            LOG.info("Starting Kafka " + _consumer.host() + " " + id + " from offset " + _committedTo);
            _emittedToOffset = _committedTo;
        }

        _fetchAPILatencyMax = new CombinedMetric(new MaxMetric());
        _fetchAPILatencyMean = new ReducedMetric(new MeanReducer());
        _fetchAPICallCount = new CountMetric();
        _fetchAPIMessageCount = new CountMetric();
        _lostMessageCount = new CountMetric();
        _messageIneligibleForRetryCount = new CountMetric();
    }

    public Map getMetricsDataMap() {
        Map<String, Object> ret = new HashMap<>();
        ret.put(_partition + "/fetchAPILatencyMax", _fetchAPILatencyMax.getValueAndReset());
        ret.put(_partition + "/fetchAPILatencyMean", _fetchAPILatencyMean.getValueAndReset());
        ret.put(_partition + "/fetchAPICallCount", _fetchAPICallCount.getValueAndReset());
        ret.put(_partition + "/fetchAPIMessageCount", _fetchAPIMessageCount.getValueAndReset());
        ret.put(_partition + "/lostMessageCount", _lostMessageCount.getValueAndReset());
        ret.put(_partition + "/messageIneligibleForRetryCount", _messageIneligibleForRetryCount.getValueAndReset());
        return ret;
    }

    //returns false if it's reached the end of current batch
    public EmitState next(SpoutOutputCollector collector) {
        if (_waitingToEmit.isEmpty()) {
            fill();
        }
        while (true) {
            MessageAndOffset toEmit = _waitingToEmit.pollFirst();
            if (toEmit == null) {
                return EmitState.NO_EMITTED;
            }

            Iterable<List<Object>> tups;
            if (_spoutConfig.scheme instanceof MessageMetadataSchemeAsMultiScheme) {
                tups = KafkaUtils.generateTuples((MessageMetadataSchemeAsMultiScheme) _spoutConfig.scheme, toEmit.message(), _partition, toEmit.offset());
            } else {
                tups = KafkaUtils.generateTuples(_spoutConfig, toEmit.message(), _partition.topic);
            }

            if ((tups != null) && tups.iterator().hasNext()) {
               if (!Strings.isNullOrEmpty(_spoutConfig.outputStreamId)) {
                    for (List<Object> tup : tups) {
                        collector.emit(_spoutConfig.outputStreamId, tup, new KafkaMessageId(_partition, toEmit.offset()));
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
        if (!_waitingToEmit.isEmpty()) {
            return EmitState.EMITTED_MORE_LEFT;
        } else {
            return EmitState.EMITTED_END;
        }
    }


    private void fill() {
        long start = System.currentTimeMillis();
        Long offset;

        // Are there failed tuples? If so, fetch those first.
        offset = this._failedMsgRetryManager.nextFailedMessageToRetry();
        final boolean processingNewTuples = (offset == null);
        if (processingNewTuples) {
            offset = _emittedToOffset;
        }

        ByteBufferMessageSet msgs = null;
        try {
            msgs = KafkaUtils.fetchMessages(_spoutConfig, _consumer, _partition, offset);
        } catch (TopicOffsetOutOfRangeException e) {
            offset = KafkaUtils.getOffset(_consumer, _partition.topic, _partition.partition, kafka.api.OffsetRequest.EarliestTime());
            // fetch failed, so don't update the fetch metrics

            //fix bug [STORM-643] : remove outdated failed offsets
            if (!processingNewTuples) {
                // For the case of EarliestTime it would be better to discard
                // all the failed offsets, that are earlier than actual EarliestTime
                // offset, since they are anyway not there.
                // These calls to broker API will be then saved.
                Set<Long> omitted = this._failedMsgRetryManager.clearOffsetsBefore(offset);

                // Omitted messages have not been acked and may be lost
                if (null != omitted) {
                    _lostMessageCount.incrBy(omitted.size());
                }

                _pending.headMap(offset).clear();

                LOG.warn("Removing the failed offsets for {} that are out of range: {}", _partition, omitted);
            }

            if (offset > _emittedToOffset) {
                _lostMessageCount.incrBy(offset - _emittedToOffset);
                _emittedToOffset = offset;
                LOG.warn("{} Using new offset: {}", _partition, _emittedToOffset);
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
                if (processingNewTuples || this._failedMsgRetryManager.shouldReEmitMsg(cur_offset)) {
                    numMessages += 1;
                    if (!_pending.containsKey(cur_offset)) {
                        _pending.put(cur_offset, System.currentTimeMillis());
                    }
                    _waitingToEmit.add(msg);
                    _emittedToOffset = Math.max(msg.nextOffset(), _emittedToOffset);
                    if (_failedMsgRetryManager.shouldReEmitMsg(cur_offset)) {
                        this._failedMsgRetryManager.retryStarted(cur_offset);
                    }
                }
            }
            _fetchAPIMessageCount.incrBy(numMessages);
        }
    }

    public void ack(Long offset) {
        if (!_pending.isEmpty() && _pending.firstKey() < offset - _spoutConfig.maxOffsetBehind) {
            // Too many things pending!
            _pending.headMap(offset - _spoutConfig.maxOffsetBehind).clear();
        }
        _pending.remove(offset);
        this._failedMsgRetryManager.acked(offset);
        numberAcked++;
    }

    public void fail(Long offset) {
        if (offset < _emittedToOffset - _spoutConfig.maxOffsetBehind) {
            LOG.info(
                    "Skipping failed tuple at offset={}" +
                        " because it's more than maxOffsetBehind={}" +
                        " behind _emittedToOffset={} for {}",
                offset,
                _spoutConfig.maxOffsetBehind,
                _emittedToOffset,
                _partition
            );
        } else {
            LOG.debug("Failing at offset={} with _pending.size()={} pending and _emittedToOffset={} for {}", offset, _pending.size(), _emittedToOffset, _partition);
            numberFailed++;
            if (numberAcked == 0 && numberFailed > _spoutConfig.maxOffsetBehind) {
                throw new RuntimeException("Too many tuple failures");
            }

            // Offset may not be considered for retry by failedMsgRetryManager
            if (this._failedMsgRetryManager.retryFurther(offset)) {
                this._failedMsgRetryManager.failed(offset);
            } else {
                // state for the offset should be cleaned up
                LOG.warn("Will not retry failed kafka offset {} further", offset);
                _messageIneligibleForRetryCount.incr();
                this._failedMsgRetryManager.cleanOffsetAfterRetries(_partition, offset);
                _pending.remove(offset);
                this._failedMsgRetryManager.acked(offset);
            }
        }
    }

    public void commit() {
        long lastCompletedOffset = lastCompletedOffset();
        if (_committedTo != lastCompletedOffset) {
            LOG.debug("Writing last completed offset ({}) to ZK for {} for topology: {}", lastCompletedOffset, _partition, _topologyInstanceId);
            Map<Object, Object> data = (Map<Object, Object>) ImmutableMap.builder()
                    .put("topology", ImmutableMap.of("id", _topologyInstanceId,
                            "name", _topoConf.get(Config.TOPOLOGY_NAME)))
                    .put("offset", lastCompletedOffset)
                    .put("partition", _partition.partition)
                    .put("broker", ImmutableMap.of("host", _partition.host.host,
                            "port", _partition.host.port))
                    .put("topic", _partition.topic).build();
            _state.writeJSON(committedPath(), data);

            _committedTo = lastCompletedOffset;
            LOG.debug("Wrote last completed offset ({}) to ZK for {} for topology: {}", lastCompletedOffset, _partition, _topologyInstanceId);
        } else {
            LOG.debug("No new offset for {} for topology: {}", _partition, _topologyInstanceId);
        }
    }

    protected String committedPath() {
        return _spoutConfig.zkRoot + "/" + _spoutConfig.id + "/" + _partition.getId();
    }

    public long lastCompletedOffset() {
        if (_pending.isEmpty()) {
            return _emittedToOffset;
        } else {
            return _pending.firstKey();
        }
    }

    public OffsetData getOffsetData() {
        return new OffsetData(_emittedToOffset, lastCompletedOffset());
    }

    public Partition getPartition() {
        return _partition;
    }

    public void close() {
        commit();
        _connections.unregister(_partition.host, _partition.topic , _partition.partition);
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
