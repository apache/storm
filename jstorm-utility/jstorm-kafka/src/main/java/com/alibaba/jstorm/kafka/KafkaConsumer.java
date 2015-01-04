package com.alibaba.jstorm.kafka;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.channels.UnresolvedAddressException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.KafkaException;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;

/**
 * 
 * @author feilaoda
 *
 */
public class KafkaConsumer {

    private static Logger LOG = Logger.getLogger(KafkaConsumer.class);

    public static final int NO_OFFSET = -1;

    private int status;
    private SimpleConsumer consumer = null;

    private KafkaSpoutConfig config;
    private LinkedList<Host> brokerList;
    private int brokerIndex;
    private Broker leaderBroker;

    public KafkaConsumer(KafkaSpoutConfig config) {
        this.config = config;
        this.brokerList = new LinkedList<Host>(config.brokers);
        this.brokerIndex = 0;
    }

    public ByteBufferMessageSet fetchMessages(int partition, long offset) throws IOException {

        String topic = config.topic;
        FetchRequest req = new FetchRequestBuilder().clientId(config.clientId).addFetch(topic, partition, offset, config.fetchMaxBytes)
                .maxWait(config.fetchWaitMaxMs).build();
        FetchResponse fetchResponse = null;
        SimpleConsumer simpleConsumer = null;
        try {
            simpleConsumer = findLeaderConsumer(partition);
            if (simpleConsumer == null) {
                // LOG.error(message);
                return null;
            }
            fetchResponse = simpleConsumer.fetch(req);
        } catch (Exception e) {
            if (e instanceof ConnectException || e instanceof SocketTimeoutException || e instanceof IOException
                    || e instanceof UnresolvedAddressException) {
                LOG.warn("Network error when fetching messages:", e);
                if (simpleConsumer != null) {
                    String host = simpleConsumer.host();
                    int port = simpleConsumer.port();
                    simpleConsumer = null;
                    throw new KafkaException("Network error when fetching messages: " + host + ":" + port + " , " + e.getMessage(), e);
                }

            } else {
                throw new RuntimeException(e);
            }
        }
        if (fetchResponse.hasError()) {
            short code = fetchResponse.errorCode(topic, partition);
            if (code == ErrorMapping.OffsetOutOfRangeCode() && config.resetOffsetIfOutOfRange) {
                long startOffset = getOffset(topic, partition, config.startOffsetTime);
                offset = startOffset;
            }
            if(leaderBroker != null) {
                LOG.error("fetch data from kafka topic[" + config.topic + "] host[" + leaderBroker.host() + ":" + leaderBroker.port() + "] partition["
                    + partition + "] error:" + code);
            }else {
                
            }
            return null;
        } else {
            ByteBufferMessageSet msgs = fetchResponse.messageSet(topic, partition);
            return msgs;
        }
    }

    private SimpleConsumer findLeaderConsumer(int partition) {
        try {
            if (consumer != null) {
                return consumer;
            }
            PartitionMetadata metadata = findLeader(partition);
            if (metadata == null) {
                leaderBroker = null;
                consumer = null;
                return null;
            }
            leaderBroker = metadata.leader();
            consumer = new SimpleConsumer(leaderBroker.host(), leaderBroker.port(), config.socketTimeoutMs, config.socketReceiveBufferBytes,
                    config.clientId);

            return consumer;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return null;
    }

    protected PartitionMetadata findLeader(int partition) {
        PartitionMetadata returnMetaData = null;
        int errors = 0;
        int size = brokerList.size();

        Host brokerHost = brokerList.get(brokerIndex);
        try {
            if (consumer == null) {
                consumer = new SimpleConsumer(brokerHost.getHost(), brokerHost.getPort(), config.socketTimeoutMs, config.socketReceiveBufferBytes,
                        config.clientId);
            }
        } catch (Exception e) {
            LOG.warn(e.getMessage(), e);
            consumer = null;
        }
        int i = brokerIndex;
        loop: while (i < size && errors < size + 1) {
            Host host = brokerList.get(i);
            i = (i + 1) % size;
            brokerIndex = i; // next index
            try {

                if (consumer == null) {
                    consumer = new SimpleConsumer(host.getHost(), host.getPort(), config.socketTimeoutMs, config.socketReceiveBufferBytes,
                            config.clientId);
                }
                List<String> topics = Collections.singletonList(config.topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = null;
                try {
                    resp = consumer.send(req);
                } catch (Exception e) {
                    errors += 1;

                    LOG.error("findLeader error, broker:" + host.toString() + ", will change to next broker index:" + (i + 1) % size);
                    if (consumer != null) {
                        consumer.close();
                        consumer = null;
                    }
                    continue;
                }

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == partition) {
                            returnMetaData = part;
                            break loop;
                        }
                    }
                }

            } catch (Exception e) {
                LOG.error("Error communicating with Broker:" + host.toString() + ", find Leader for partition:" + partition);
            } finally {
                if (consumer != null) {
                    consumer.close();
                    consumer = null;
                }
            }
        }

        return returnMetaData;
    }

    public long getOffset(String topic, int partition, long startOffsetTime) {
        SimpleConsumer simpleConsumer = findLeaderConsumer(partition);

        if (simpleConsumer == null) {
            LOG.error("Error consumer is null get offset from partition:" + partition);
            return -1;
        }

        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(startOffsetTime, 1));
        OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), simpleConsumer.clientId());

        long[] offsets = simpleConsumer.getOffsetsBefore(request).offsets(topic, partition);
        if (offsets.length > 0) {
            return offsets[0];
        } else {
            return NO_OFFSET;
        }
    }

    public void close() {
        if (consumer != null) {
            consumer.close();
        }
    }

    public SimpleConsumer getConsumer() {
        return consumer;
    }

    public void setConsumer(SimpleConsumer consumer) {
        this.consumer = consumer;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public Broker getLeaderBroker() {
        return leaderBroker;
    }

    public void setLeaderBroker(Broker leaderBroker) {
        this.leaderBroker = leaderBroker;
    }

}
