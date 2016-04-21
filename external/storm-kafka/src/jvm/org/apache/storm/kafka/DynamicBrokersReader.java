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

import org.apache.storm.utils.Utils;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.kafka.trident.GlobalPartitionInformation;

import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import kafka.cluster.BrokerEndPoint;
import kafka.common.ErrorMapping;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

public class DynamicBrokersReader {

    public static final Logger LOG = LoggerFactory.getLogger(DynamicBrokersReader.class);

    private final String _topic;
    private final Boolean _isWildcardTopic;
    private final String _clientId;
    private final int _socketTimeoutMs;
    private final int _bufferSizeBytes;
    private final ZkMetadataReader _zkMetadataReader;

    public DynamicBrokersReader(Map conf, KafkaConfig kafkaConfig, ZkMetadataReaderFactory zkMetadataReaderFactory) {
        // Check required parameters
        Preconditions.checkNotNull(conf, "conf cannot be null");
        Preconditions.checkNotNull(kafkaConfig, "kafkaConfig cannot be null");
        Preconditions.checkNotNull(kafkaConfig.topic, "topic cannot be null");
        Preconditions.checkNotNull(kafkaConfig.clientId, "clientId cannot be null");
        _topic = kafkaConfig.topic;
        _clientId = kafkaConfig.clientId;
        _socketTimeoutMs = kafkaConfig.socketTimeoutMs;
        _bufferSizeBytes = kafkaConfig.bufferSizeBytes;
        _zkMetadataReader = zkMetadataReaderFactory.createZkMetadataReader(conf, kafkaConfig);

        _isWildcardTopic = Utils.getBoolean(conf.get("kafka.topic.wildcard.match"), false);
    }

    /**
     * Get all partitions with their current leaders from Kafka.
     */
    public List<GlobalPartitionInformation> getBrokerInfo() throws SocketTimeoutException {
        List<Broker> seedBrokers = _zkMetadataReader.getBrokers();
        List<String> topics = getTopics();
        Set<String> missingTopics = new HashSet<>();
        List<GlobalPartitionInformation> partitions = new ArrayList<GlobalPartitionInformation>();

        brokerLoop:
        for (Broker broker : seedBrokers) {
            missingTopics.clear();
            missingTopics.addAll(topics);
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(broker.host, broker.port, _socketTimeoutMs, _bufferSizeBytes, _clientId);
                TopicMetadataResponse metadataResponse = consumer.send(new TopicMetadataRequest(topics));
                List<TopicMetadata> topicsMetadata = metadataResponse.topicsMetadata();
                for (TopicMetadata topicMetadata : topicsMetadata) {
                    if(topicMetadata.errorCode() != ErrorMapping.NoError()){
                        LOG.warn("Got error {} against broker {} for topic {}", ErrorMapping.exceptionNameFor(topicMetadata.errorCode()), broker, topicMetadata.topic());
                        continue brokerLoop;
                    }
                    GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation(topicMetadata.topic(), this._isWildcardTopic);
                    List<PartitionMetadata> partitionsMetadata = topicMetadata.partitionsMetadata();
                    for (PartitionMetadata partitionMetadata : partitionsMetadata) {
                        short partitionErrorCode = partitionMetadata.errorCode();
                        if(partitionErrorCode != ErrorMapping.NoError() && partitionErrorCode != ErrorMapping.ReplicaNotAvailableCode()){
                            LOG.warn("Got error {} against broker {} for topic {} partition {}", ErrorMapping.exceptionNameFor(partitionErrorCode), broker, topicMetadata.topic(), partitionMetadata.partitionId());
                            continue brokerLoop;
                        }
                        BrokerEndPoint leaderBroker = partitionMetadata.leader();
                        globalPartitionInformation.addPartition(partitionMetadata.partitionId(), new Broker(leaderBroker.host(), leaderBroker.port()));
                    }
                    if (globalPartitionInformation.getOrderedPartitions().isEmpty()) {
                        //0 partition topic doesn't make sense, don't know if this can actually happen
                        LOG.info("Failed to retrieve partition information from broker {} for topic {}, trying next broker", broker, topicMetadata.topic());
                        continue brokerLoop;
                    }
                    LOG.info("Read partition info from Kafka: " + globalPartitionInformation);
                    partitions.add(globalPartitionInformation);
                    missingTopics.remove(topicMetadata.topic());
                }
                if(missingTopics.isEmpty()){
                    break;
                }
            } catch (Exception e) {
                LOG.warn("Failed to communicate with broker {}:{}", broker.host, broker.port, e);
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        }
        if (!missingTopics.isEmpty()) {
            throw new RuntimeException("Failed to retrieve partition information from brokers " + seedBrokers + " for topics " + missingTopics);
        }
        return partitions;
    }

    private List<String> getTopics() {
        List<String> topics = new ArrayList<String>();
        if (!_isWildcardTopic) {
            topics.add(_topic);
            return topics;
        } else {
            return _zkMetadataReader.getWildcardTopics(_topic);
        }
    }

    public void close() {
        _zkMetadataReader.close();
    }

}
