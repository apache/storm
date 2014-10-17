package com.alibaba.jstorm.kafka;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.channels.UnresolvedAddressException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.KafkaException;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;

/**
 * 
 * @author feilaoda
 *
 */
public class MessageConsumer {
	
	private static Logger LOG = Logger.getLogger(MessageConsumer.class);

	
	public static final int NO_OFFSET = -1;
	
	private SimpleConsumer consumer;
	

    private KafkaSpoutConfig config;
	private Host broker;
	
	
	private Set<Partition> partitions;
	

	public MessageConsumer(KafkaSpoutConfig config, Host broker) {
		this.config = config;
		this.broker = broker;
		partitions = new HashSet<Partition>();
		consumer = new SimpleConsumer(broker.getHost(), broker.getPort(), config.socketTimeoutMs,config.socketReceiveBufferBytes, config.clientId);
	}
	
	public ByteBufferMessageSet fetchMessages(int partition, long offset) throws IOException {
		String topic = config.topic;
		FetchRequest req = new FetchRequestBuilder()
        .clientId(config.clientId)
        .addFetch(topic, partition, offset, config.fetchMaxBytes)
        .maxWait(config.fetchWaitMaxMs)
        .build();
		FetchResponse fetchResponse = null;
        try {
            fetchResponse = consumer.fetch(req);
            
        }catch (Exception e) {
        	if (e instanceof ConnectException ||
                    e instanceof SocketTimeoutException ||
                    e instanceof IOException ||
                    e instanceof UnresolvedAddressException
                    ) {
                LOG.warn("Network error when fetching messages:", e);
                throw new KafkaException("Network error when fetching messages: "+e.getMessage());
            } else {
                throw new RuntimeException(e);
            }
        }
        if(fetchResponse.hasError()) {
        	short code = fetchResponse.errorCode(topic, partition);
//        	if(code == ErrorMapping.OffsetOutOfRangeCode() && config.resetOffsetIfOutOfRange) {
//        		long startOffset = getOffset(topic, partition, config.startOffsetTime);
//        		offset = startOffset;
//        	}
        	throw new KafkaException("fetch data from kafka topic["+topic+"] partition["+partition+"] error:" + code);
        }else {
        	ByteBufferMessageSet msgs =  fetchResponse.messageSet(topic, partition);
        	return msgs;
        }
	}
	
	
	public long getOffset(String topic, int partition, KafkaSpoutConfig config) {
		long startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		if (config.fromBeginning) {
			startOffsetTime = config.startOffsetTime;
		}
		return getOffset(topic, partition, startOffsetTime);
	}
	
	public long getOffset( String topic,
            int partition, long startOffsetTime) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
                partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
                startOffsetTime, 1));
        OffsetRequest request = new OffsetRequest(requestInfo,
                kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());

        long[] offsets = consumer.getOffsetsBefore(request).offsets(topic,
                partition);
        if (offsets.length > 0) {
            return offsets[0];
        } else {
            return NO_OFFSET;
        }
    }
	
	public void addPartition(Partition partition) {
		partitions.add(partition);
	}
	
	public void removePartition(Partition partition) {
		partitions.remove(partition);
	}

	public Set<Partition> getPartitions() {
		return partitions;
	}

	public void setPartitions(Set<Partition> partitions) {
		this.partitions = partitions;
	}
	
	public void close() {
		consumer.close();
	}
	
	public SimpleConsumer getConsumer() {
        return consumer;
    }

    public void setConsumer(SimpleConsumer consumer) {
        this.consumer = consumer;
    }
    
}
