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
package storm.kafka;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.trident.GlobalPartitionInformation;
import storm.kafka.trident.IBrokerReader;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class KafkaApiBrokerReader implements IBrokerReader {

    public static final Logger LOG = LoggerFactory.getLogger(KafkaApiBrokerReader.class);

    public static final int DEFAULT_SOCKET_TIMEOUT = 10000;
    public static final int BUFFER_SIZE = 64 * 1024;
    public static final String CLIENT_ID = "leaderLookup";
    private final List<Broker> seedBrokers;
    private final String topic;
    private final int socketTimeout;

    public KafkaApiBrokerReader(List<Broker> seedBrokers, String topic, int socketTimeout) {
        this.seedBrokers = seedBrokers;
        this.topic = topic;
        this.socketTimeout = socketTimeout;
    }

    public KafkaApiBrokerReader(List<Broker> seedBrokers, String topic) {
        this(seedBrokers, topic, DEFAULT_SOCKET_TIMEOUT);
    }


    @Override
    public GlobalPartitionInformation getCurrentBrokers() {
        GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation();
        boolean foundPartitionInfo = false;
        List<String> topics = Collections.singletonList(topic);
        Iterator<Broker> brokerIterator = seedBrokers.iterator();
        while (brokerIterator.hasNext() && !foundPartitionInfo) {
            Broker brokerHost = brokerIterator.next();
            try {
                SimpleConsumer simpleConsumer = new SimpleConsumer(brokerHost.host, brokerHost.port, socketTimeout, BUFFER_SIZE, CLIENT_ID);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                TopicMetadataResponse resp = simpleConsumer.send(req);
                List<TopicMetadata> topicMetadatas = resp.topicsMetadata();
                for (TopicMetadata topicMetadata : topicMetadatas) {
                    List<PartitionMetadata> partitionMetadatas = topicMetadata.partitionsMetadata();
                    for (PartitionMetadata partitionMetadata : partitionMetadatas) {
                        String host = partitionMetadata.leader().host();
                        int port = partitionMetadata.leader().port();
                        int partitionId = partitionMetadata.partitionId();
                        globalPartitionInformation.addPartition(partitionId, new Broker(host, port));
                    }
                }
                simpleConsumer.close();
            } catch (Exception e) {
                LOG.warn("Error getting topic partition info from seed broker {}, will try another seed host.", brokerHost, e);
            }
        }
        LOG.info("Read partition info from kafka: " + globalPartitionInformation);
        return globalPartitionInformation;
    }

    @Override
    public void close() {

    }
}
