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

import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import kafka.admin.AdminUtils;
import kafka.common.ErrorMapping;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.utils.ZkUtils;
import org.apache.curator.test.TestingServer;
import org.apache.storm.kafka.trident.GlobalPartitionInformation;
import static org.hamcrest.Matchers.empty;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import static org.mockito.Mockito.when;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import org.junit.rules.ExpectedException;

/**
 * Date: 16/05/2013 Time: 20:35
 */
public class DynamicBrokersReaderTest {

    private DynamicBrokersReader dynamicBrokersReader;

    private TestingServer kafkaZookeeper;
    private KafkaTestBroker broker;

    private final int maxWaitTimeMs = 30_000;

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();
    
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private ZkMetadataReader zkMetadataReader;

    @Mock
    private ZkMetadataReaderFactory mockZkReaderFactory;

    @Before
    public void setUp() throws Exception {
        kafkaZookeeper = new TestingServer();
        broker = new KafkaTestBroker(kafkaZookeeper.getConnectString(), "0");
        Map conf = new HashMap();
        KafkaConfig kafkaConfig = TestUtils.getKafkaConfig(broker);
        when(mockZkReaderFactory.createZkMetadataReader(conf, kafkaConfig)).thenReturn(zkMetadataReader);
        dynamicBrokersReader = new DynamicBrokersReader(conf, kafkaConfig, mockZkReaderFactory);
    }

    @After
    public void tearDown() throws Exception {
        dynamicBrokersReader.close();
        broker.shutdown();
        kafkaZookeeper.close();
    }

    private void createTopic(String topic, int numPartitions, int numReplicas) {
        ZkUtils zkUtils = ZkUtils.apply(kafkaZookeeper.getConnectString(), 30_000, 30_000, false);
        AdminUtils.createTopic(zkUtils, topic, numPartitions, numReplicas, new Properties());
    }

    private GlobalPartitionInformation getByTopic(List<GlobalPartitionInformation> partitions, String topic) {
        for (GlobalPartitionInformation partitionInformation : partitions) {
            if (partitionInformation.topic.equals(topic)) {
                return partitionInformation;
            }
        }
        return null;
    }

    private void waitUntilTopicsCreatedAndFullyReplicated(int numReplicas, String... topics) throws InterruptedException, SocketTimeoutException {
        SimpleConsumer consumer = null;
        try {
            consumer = TestUtils.getKafkaConsumer(broker);
            long currentTime = System.currentTimeMillis();
            while (System.currentTimeMillis() < currentTime + maxWaitTimeMs) {
                TopicMetadataResponse metadataResponse = consumer.send(new TopicMetadataRequest(Arrays.asList(topics)));
                Set<String> remainingTopics = new HashSet<>(Arrays.asList(topics));
                List<TopicMetadata> topicsMetadata = metadataResponse.topicsMetadata();
                for (TopicMetadata topicMetadata : topicsMetadata) {
                    if (topicMetadata.errorCode() == ErrorMapping.NoError()) {
                        List<PartitionMetadata> partitionsMetadata = topicMetadata.partitionsMetadata();
                        boolean fullyReplicated = true;
                        for (PartitionMetadata partitionMetadata : partitionsMetadata) {
                            short partitionErrorCode = partitionMetadata.errorCode();
                            if(partitionErrorCode != ErrorMapping.NoError() && partitionErrorCode != ErrorMapping.ReplicaNotAvailableCode()){
                                fullyReplicated = false;
                                break;
                            }
                            fullyReplicated = fullyReplicated && (partitionMetadata.replicas().size() == numReplicas);
                        }
                        if (fullyReplicated) {
                            remainingTopics.remove(topicMetadata.topic());
                        }
                    }
                }
                if (remainingTopics.isEmpty()) {
                    break;
                } else {
                    //Topics still not available, retry later
                    Thread.sleep(10);
                }
            }
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }

    }

    @Test
    public void testGetBrokerInfo() throws Exception {
        String host = "localhost";
        int partition = 0;
        createTopic(TestUtils.TOPIC, 1, 1);
        waitUntilTopicsCreatedAndFullyReplicated(1, TestUtils.TOPIC);
        List<Broker> seedBrokers = Collections.singletonList(new Broker(host, broker.getPort()));
        when(zkMetadataReader.getBrokers()).thenReturn(seedBrokers);

        List<GlobalPartitionInformation> partitions = dynamicBrokersReader.getBrokerInfo();
        GlobalPartitionInformation brokerInfo = getByTopic(partitions, TestUtils.TOPIC);
        assertThat(brokerInfo, not(nullValue()));
        assertThat(brokerInfo.getOrderedPartitions().size(), is(1));
        assertThat(brokerInfo.getBrokerFor(partition).port, is(broker.getPort()));
        assertThat(brokerInfo.getBrokerFor(partition).host, is(host));
    }

    @Test
    public void testGetBrokerInfoWildcardMatch() throws Exception {
        String host = "localhost";
        int partition = 0;
        String firstTopic = "testTopic1";
        String secondTopic = "testTopic2";
        List<String> matchingTopics = new ArrayList<>();
        matchingTopics.add(firstTopic);
        matchingTopics.add(secondTopic);
        String thirdTopic = "Topic3";
        createTopic(firstTopic, 1, 1);
        createTopic(secondTopic, 1, 1);
        createTopic(thirdTopic, 1, 1);
        waitUntilTopicsCreatedAndFullyReplicated(1, firstTopic, secondTopic, thirdTopic);
        
        Map conf = new HashMap();
        conf.putAll(conf);
        conf.put("kafka.topic.wildcard.match", true);
        DynamicBrokersReader wildcardBrokerReader = null;
        try {
            ZkHosts zkHosts = new ZkHosts(kafkaZookeeper.getConnectString());
            String topicPattern = "^test.*$";
            KafkaConfig kafkaConfig = new KafkaConfig(zkHosts, topicPattern);
            when(zkMetadataReader.getWildcardTopics(topicPattern)).thenReturn(matchingTopics);
            when(mockZkReaderFactory.createZkMetadataReader(conf, kafkaConfig)).thenReturn(zkMetadataReader);
            wildcardBrokerReader = new DynamicBrokersReader(conf, kafkaConfig, mockZkReaderFactory);
            List<Broker> seedBrokers = Collections.singletonList(new Broker(host, broker.getPort()));
            when(zkMetadataReader.getBrokers()).thenReturn(seedBrokers);

            List<GlobalPartitionInformation> partitions = wildcardBrokerReader.getBrokerInfo();

            for (String topic : matchingTopics) {
                GlobalPartitionInformation brokerInfo = getByTopic(partitions, topic);
                assertThat(brokerInfo, not(nullValue()));
                assertThat(brokerInfo.getOrderedPartitions().size(), is(1));
                assertThat(brokerInfo.getBrokerFor(partition).port, is(broker.getPort()));
                assertThat(brokerInfo.getBrokerFor(partition).host, is(host));
            }

            GlobalPartitionInformation brokerInfo = getByTopic(partitions, thirdTopic);
            assertThat("Did not expect third topic to match wildcard pattern", brokerInfo, is(nullValue()));
        } finally {
            if (wildcardBrokerReader != null) {
                wildcardBrokerReader.close();
            }
        }
    }

    @Test
    public void testMultiplePartitionsOnDifferentHosts() throws Exception {
        KafkaTestBroker broker2 = null;
        try {
            broker2 = new KafkaTestBroker(kafkaZookeeper.getConnectString(), "1");
            String host = "localhost";
            int partitionCount = 2;
            createTopic(TestUtils.TOPIC, 2, 1);
            waitUntilTopicsCreatedAndFullyReplicated(1, TestUtils.TOPIC);
            List<Broker> seedBrokers = Collections.singletonList(new Broker(host, broker.getPort()));
            when(zkMetadataReader.getBrokers()).thenReturn(seedBrokers);

            List<GlobalPartitionInformation> partitions = dynamicBrokersReader.getBrokerInfo();
            GlobalPartitionInformation brokerInfo = getByTopic(partitions, TestUtils.TOPIC);
            assertThat(brokerInfo, not(nullValue()));
            assertThat(brokerInfo.getOrderedPartitions().size(), is(partitionCount));
            Set<Integer> unmatchedBrokerPorts = new HashSet<>();
            unmatchedBrokerPorts.add(broker.getPort());
            unmatchedBrokerPorts.add(broker2.getPort());
            for (int i = 0; i < partitionCount; i++) {
                int brokerPort = brokerInfo.getBrokerFor(i).port;
                unmatchedBrokerPorts.remove(brokerPort);
                assertThat(brokerPort, anyOf(is(broker.getPort()), is(broker2.getPort())));
                assertThat(brokerInfo.getBrokerFor(i).host, is(host));
            }
            assertThat("Expected all brokers to have at least 1 partition", unmatchedBrokerPorts, is(empty()));
        } finally {
            if (broker2 != null) {
                broker2.shutdown();
            }
        }
    }

    @Test
    public void testMultiplePartitionsOnSameHost() throws Exception {
        String host = "localhost";
        int partition = 0;
        int secondPartition = partition + 1;
        createTopic(TestUtils.TOPIC, 2, 1);
        waitUntilTopicsCreatedAndFullyReplicated(1, TestUtils.TOPIC);
        List<Broker> seedBrokers = Collections.singletonList(new Broker(host, broker.getPort()));
        when(zkMetadataReader.getBrokers()).thenReturn(seedBrokers);

        List<GlobalPartitionInformation> partitions = dynamicBrokersReader.getBrokerInfo();
        GlobalPartitionInformation brokerInfo = getByTopic(partitions, TestUtils.TOPIC);
        assertThat(brokerInfo, not(nullValue()));
        assertThat(brokerInfo.getOrderedPartitions().size(), is(2));
        assertThat(brokerInfo.getBrokerFor(partition).port, is(broker.getPort()));
        assertThat(brokerInfo.getBrokerFor(partition).host, is(host));
        assertThat(brokerInfo.getBrokerFor(secondPartition).port, is(broker.getPort()));
        assertThat(brokerInfo.getBrokerFor(secondPartition).host, is(host));
    }

    @Test
    public void testSwitchHostForPartition() throws Exception {
        boolean shutdownBroker2 = false;
        int partitionCount = 32;
        KafkaTestBroker broker2 = null;
        try {
            broker2 = new KafkaTestBroker(kafkaZookeeper.getConnectString(), "1");
            createTopic(TestUtils.TOPIC, partitionCount, 2);
            waitUntilTopicsCreatedAndFullyReplicated(2, TestUtils.TOPIC);
            List<Broker> seedBrokers = Collections.singletonList(new Broker("localhost", broker.getPort()));
            when(zkMetadataReader.getBrokers()).thenReturn(seedBrokers);

            {
                List<GlobalPartitionInformation> partitions = dynamicBrokersReader.getBrokerInfo();
                assertThat(partitions.size(), is(1));
                boolean atLeastOneAssignedToBroker2 = false;
                GlobalPartitionInformation brokerInfo = partitions.get(0);
                for (int i = 0; i < partitionCount; i++) {
                    atLeastOneAssignedToBroker2 = atLeastOneAssignedToBroker2 || (brokerInfo.getBrokerFor(i).port == broker2.getPort());
                }
                assertThat("Expected at least one partition assigned to broker 2 pre-shutdown", atLeastOneAssignedToBroker2, is(true));
            }

            broker2.shutdown();
            shutdownBroker2 = true;
            waitUntilTopicsCreatedAndFullyReplicated(1, TestUtils.TOPIC);
            List<GlobalPartitionInformation> partitions = dynamicBrokersReader.getBrokerInfo();
            assertThat(partitions.size(), is(1));
            GlobalPartitionInformation brokerInfo = partitions.get(0);
            for (int i = 0; i < partitionCount; i++) {
                assertThat("Expected all partitions assigned to surviving broker", brokerInfo.getBrokerFor(i).port, is(broker.getPort()));
            }
        } finally {
            if (broker2 != null && !shutdownBroker2) {
                broker2.shutdown();
            }
        }
    }

    @Test(expected = NullPointerException.class)
    public void testErrorLogsWhenConfigIsMissing() throws Exception {
        Map conf = new HashMap();
        ZkHosts zkHosts = new ZkHosts(kafkaZookeeper.getConnectString());
        KafkaConfig kafkaConfig = new KafkaConfig(zkHosts, null);

        DynamicBrokersReader misconfiguredBrokersReader = new DynamicBrokersReader(conf, kafkaConfig, mockZkReaderFactory);
    }
    
    @Test
    public void testErrorIfTopicIsMissing() throws Exception {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Failed to retrieve partition information from brokers");
        String host = "localhost";
        List<Broker> seedBrokers = Collections.singletonList(new Broker(host, broker.getPort()));
        when(zkMetadataReader.getBrokers()).thenReturn(seedBrokers);

        dynamicBrokersReader.getBrokerInfo();
    }
    
    @Test
    public void testErrorIfBrokersAreMissing() throws Exception {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Failed to retrieve partition information from brokers");
        createTopic(TestUtils.TOPIC, 1, 1);
        waitUntilTopicsCreatedAndFullyReplicated(1, TestUtils.TOPIC);
        List<Broker> seedBrokers = Collections.singletonList(new Broker("FakeHost", broker.getPort()));
        when(zkMetadataReader.getBrokers()).thenReturn(seedBrokers);

        dynamicBrokersReader.getBrokerInfo();
    }
    
    @Test
    public void testErrorIfBrokerIsMissingWildcardTopics() throws Exception {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Failed to retrieve partition information from brokers");
        String host = "localhost";
        String firstTopic = "testTopic1";
        String secondTopic = "testTopic2";
        List<String> matchingTopics = new ArrayList<>();
        matchingTopics.add(firstTopic);
        matchingTopics.add(secondTopic);
        createTopic(firstTopic, 1, 1);
        waitUntilTopicsCreatedAndFullyReplicated(1, firstTopic);

        Map conf = new HashMap();
        conf.putAll(conf);
        conf.put("kafka.topic.wildcard.match", true);
        DynamicBrokersReader wildcardBrokerReader = null;
        try {
            ZkHosts zkHosts = new ZkHosts(kafkaZookeeper.getConnectString());
            String topicPattern = "^test.*$";
            KafkaConfig kafkaConfig = new KafkaConfig(zkHosts, topicPattern);
            when(zkMetadataReader.getWildcardTopics(topicPattern)).thenReturn(matchingTopics);
            when(mockZkReaderFactory.createZkMetadataReader(conf, kafkaConfig)).thenReturn(zkMetadataReader);
            wildcardBrokerReader = new DynamicBrokersReader(conf, kafkaConfig, mockZkReaderFactory);
            List<Broker> seedBrokers = Collections.singletonList(new Broker(host, broker.getPort()));
            when(zkMetadataReader.getBrokers()).thenReturn(seedBrokers);

            wildcardBrokerReader.getBrokerInfo();
        } finally {
            if (wildcardBrokerReader != null) {
                wildcardBrokerReader.close();
            }
        }
    }
}
