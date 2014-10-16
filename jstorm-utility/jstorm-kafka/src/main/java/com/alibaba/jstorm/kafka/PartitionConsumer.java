package com.alibaba.jstorm.kafka;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.esotericsoftware.minlog.Log;
import com.google.common.collect.ImmutableMap;

import kafka.javaapi.OffsetRequest;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import backtype.storm.Config;
import backtype.storm.spout.RawMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.utils.Utils;

/**
 * 
 * @author feilaoda
 *
 */
public class PartitionConsumer {
    private static Logger LOG = Logger.getLogger(PartitionConsumer.class);

    static enum EmitState {
        EMIT_MORE, EMIT_END, EMIT_NONE
    }

    private Partition partition;
    private MessageConsumer consumer;
    private PartitionCoordinator coordinator;

    private KafkaSpoutConfig config;
    private LinkedList<MessageAndOffset> emittingMessages = new LinkedList<MessageAndOffset>();
    private SortedSet<Long> pendingOffsets = new TreeSet<Long>();
    private SortedSet<Long> failedOffsets = new TreeSet<Long>();
    private long emittingOffset;
    private long lastCommittedOffset;
    private ZkState zkState;
    private Map stormConf;

    public PartitionConsumer(Map conf, MessageConsumer consumer, KafkaSpoutConfig config, Partition partition, ZkState offsetState) {
        this.stormConf = conf;
        this.config = config;
        this.partition = partition;
        this.consumer = consumer;
        this.zkState = offsetState;

        Long jsonOffset = null;
        try {
            Map<Object, Object> json = offsetState.readJSON(zkPath());
            if (json != null) {
                // jsonTopologyId = (String)((Map<Object,Object>)json.get("topology"));
                jsonOffset = (Long) json.get("offset");
            }
        } catch (Throwable e) {
            LOG.warn("Error reading and/or parsing at ZkNode: " + zkPath(), e);
        }

        

        if (config.fromBeginning) {
            emittingOffset = -1;
        } else {
            if(jsonOffset == null) {
                lastCommittedOffset = -1;
             // consumer.getConsumer().getOffsetsBefore(request) getOffsetsBefore(config.topic, partition.getPartition(), config.startOffsetTime,
                // 1)[0];
            }else {
                lastCommittedOffset = jsonOffset;
            }
            emittingOffset = lastCommittedOffset;
        }
    }

    public EmitState emit(SpoutOutputCollector collector) {
        if (emittingMessages.isEmpty()) {
            fillMessages();
        }
        if (emittingMessages.isEmpty()) {
            return EmitState.EMIT_NONE;
        }

        while (true) {
            MessageAndOffset toEmitMsg = emittingMessages.pollFirst();
            if (toEmitMsg == null) {
                return EmitState.EMIT_END;
            }
            Iterable<List<Object>> tups = generateTuples(toEmitMsg.message());
            if (tups != null) {
                for (List<Object> tuple : tups) {
                    collector.emit(tuple, new KafkaMessageId(partition, toEmitMsg.offset()));
                }
                // break;
            } else {
                ack(toEmitMsg.offset());
            }
        }

        // if(!emittingMessages.isEmpty()) {
        // return EmitState.EMIT_MORE;
        // }else {
        // return EmitState.EMIT_END;
        // }
    }

    private void fillMessages() {

        ByteBufferMessageSet msgs;
        try {
            msgs = consumer.fetchMessages(partition.getPartition(), emittingOffset + 1);
            LOG.debug("fetch message from offset " + emittingOffset);
            for (MessageAndOffset msg : msgs) {
                emittingMessages.add(msg);
                emittingOffset = msg.offset();
                pendingOffsets.add(emittingOffset);
                LOG.debug("===== fetched a message: " + msg.message().toString() + " offset:" + msg.offset());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void commitState() {
        long lastOffset = 0;
        if (pendingOffsets.isEmpty()) {
            lastOffset = emittingOffset;
        } else {
            lastOffset = pendingOffsets.first();
        }
        if (lastOffset != lastCommittedOffset) {
            Map<Object, Object> data = new HashMap<Object, Object>();
            data.put("topology", stormConf.get(Config.TOPOLOGY_NAME));
            data.put("offset", lastOffset);
            data.put("partition", partition.getPartition());
            data.put("broker", ImmutableMap.of("host", partition.getBroker().getHost(), "port", partition.getBroker().getPort()));
            data.put("topic", config.topic);
            zkState.writeJSON(zkPath(), data);
            lastCommittedOffset = lastOffset;
        }

    }

    public void ack(long offset) {
        pendingOffsets.remove(offset);
    }

    public void fail(long offset) {
        failedOffsets.remove(offset);
    }

    public void close() {
        coordinator.removeConsumer(partition);
    }

    public Iterable<List<Object>> generateTuples(Message msg) {
        Iterable<List<Object>> tups = null;
        ByteBuffer payload = msg.payload();
        if (payload == null) {
            return null;
        }
        ByteBuffer key = msg.key();
        if (key != null) {
        } else {
            tups = config.scheme.deserialize(Utils.toByteArray(payload));
        }
        return tups;
    }

    private String zkPath() {
        return config.zkRoot + "/" + config.topic + "/id_" + config.clientId + "/" + partition.getPartition()+"/offset";
    }

    public PartitionCoordinator getCoordinator() {
        return coordinator;
    }

    public void setCoordinator(PartitionCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }

}
