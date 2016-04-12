/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.kafka;

import org.apache.storm.Config;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.ZKPaths;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

public class ZkMetadataReaderTest {

    @Rule
    public ExpectedException expect = ExpectedException.none();

    private ZkMetadataReader zkMetadataReader;
    private CuratorFramework zookeeper;
    private TestingServer server;

    @Before
    public void setUp() throws Exception {
        server = new TestingServer();
        String connectionString = server.getConnectString();
        Map conf = new HashMap();
        conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 1000);
        conf.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 1000);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 4);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 5);
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        zookeeper = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        ZkHosts zkHosts = new ZkHosts(connectionString);
        KafkaConfig kafkaConfig = new KafkaConfig(zkHosts, "testTopic");
        zkMetadataReader = new ZkMetadataReader(conf, kafkaConfig);
        zookeeper.start();
        //Not starting Kafka, so have to create this path manually
        ZKPaths.mkdirs(zookeeper.getZookeeperClient().getZooKeeper(), zkMetadataReader.brokerPath());
    }

    @After
    public void tearDown() throws Exception {
        zkMetadataReader.close();
        zookeeper.close();
    }

    private void addTopic(String topic) throws Exception {
        writeDataToPath(zkMetadataReader.topicsPath() + "/" + topic + "/partitions", "0");
    }

    private void writeDataToPath(String path, String data) throws Exception {
        ZKPaths.mkdirs(zookeeper.getZookeeperClient().getZooKeeper(), path);
        zookeeper.setData().forPath(path, data.getBytes());
    }

    private void writeBrokerDetails(int brokerId, String host, int port) throws Exception {
        String path = zkMetadataReader.brokerPath() + "/" + brokerId;
        String value = "{ \"host\":\"" + host + "\", \"jmx_port\":9999, \"port\":" + port + ", \"version\":1 }";
        writeDataToPath(path, value);
    }

    @Test
    public void testGetSingleBroker() throws Exception {
        String host = "localhost";
        int port = 9092;
        int leaderId = 0;
        writeBrokerDetails(leaderId, host, port);
        List<Broker> brokers = zkMetadataReader.getBrokers();
        assertThat("Expected 1 broker", brokers.size(), is(1));
        Broker kafkaBroker = brokers.get(0);
        assertThat("Expected broker host to match host in zookeeper", kafkaBroker.host, is(host));
        assertThat("Expected broker port to match port in zookeeper", kafkaBroker.port, is(port));
    }

    @Test
    public void testGetMultipleBrokers() throws Exception {
        String host = "localhost";
        int basePort = 9092;
        for (int i = 0; i < 2; i++) {
            int port = basePort + i;
            int leaderId = i;
            writeBrokerDetails(leaderId, host, port);
        }
        List<Broker> brokers = zkMetadataReader.getBrokers();
        assertThat("Expected 2 brokers", brokers.size(), is(2));
        Collections.sort(brokers);
        for (int i = 0; i < 2; i++) {
            Broker kafkaBroker = brokers.get(i);
            assertThat("Expected broker host to match host in zookeeper", kafkaBroker.host, is(host));
            assertThat("Expected broker port to match port in zookeeper", kafkaBroker.port, is(basePort + i));
        }
    }

    @Test
    public void testErrorIfNoBrokers() throws Exception {
        expect.expectMessage("No live brokers listed in Zookeeper");
        expect.expect(RuntimeException.class);
        zkMetadataReader.getBrokers();
    }
    
    @Test
    public void testCanFindWildcardTopics() throws Exception {
        String topic1 = "testTopic1";
        String topic2 = "testTopic2";
        String topic3 = "Topic3";
        addTopic(topic1);
        addTopic(topic2);
        addTopic(topic3);
        List<String> topics = zkMetadataReader.getWildcardTopics("^test.*$");
        assertThat("Expected topics to contain both wildcard topics", topics, containsInAnyOrder(topic1, topic2));
    }

    @Test(expected = NullPointerException.class)
    public void testErrorIfConfigIsMissing() throws Exception {
        String connectionString = server.getConnectString();
        Map conf = new HashMap();
        conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 1000);
//        conf.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 1000);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 4);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 5);
        ZkHosts zkHosts = new ZkHosts(connectionString);
        KafkaConfig kafkaConfig = new KafkaConfig(zkHosts, "testTopic");
        ZkMetadataReader newReader = new ZkMetadataReader(conf, kafkaConfig);
    }
}
