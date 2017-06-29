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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.Config;
import org.apache.storm.kafka.KafkaSpout.EmitState;
import org.apache.storm.kafka.PartitionManager.KafkaMessageId;
import org.apache.storm.kafka.trident.ZkBrokerReader;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class PartitionManagerTest {

    private static final String TOPIC_NAME = "testTopic";

    private KafkaTestBroker broker;
    private TestingSpoutOutputCollector outputCollector;
    private ZkState zkState;
    private ZkCoordinator coordinator;
    private KafkaProducer<String, String> producer;

    @Before
    public void setup() {
        outputCollector = new TestingSpoutOutputCollector();

        Properties brokerProps = new Properties();
        brokerProps.setProperty("log.retention.check.interval.ms", "1000");

        broker = new KafkaTestBroker(brokerProps);

        // Configure Kafka to remove messages after 2 seconds
        Properties topicProperties = new Properties();
        topicProperties.put("delete.retention.ms", "2000");
        topicProperties.put("retention.ms", "2000");

        broker.createTopic(TOPIC_NAME, 1, topicProperties);

        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TRANSACTIONAL_ZOOKEEPER_PORT, broker.getZookeeperPort());
        conf.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, Collections.singletonList("127.0.0.1"));
        conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 20000);
        conf.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 20000);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 3);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 30);
        conf.put(Config.TOPOLOGY_NAME, "test");

        zkState = new ZkState(conf);

        ZkHosts zkHosts = new ZkHosts(broker.getZookeeperConnectionString());

        SpoutConfig spoutConfig = new SpoutConfig(zkHosts, TOPIC_NAME, "/test", "id");

        coordinator = new ZkCoordinator(
            new DynamicPartitionConnections(spoutConfig, new ZkBrokerReader(conf, TOPIC_NAME, zkHosts)),
            conf,
            spoutConfig,
            zkState,
            0,
            1,
            1,
            "topo"
        );

        Properties producerProps = new Properties();
        producerProps.put("acks", "1");
        producerProps.put("bootstrap.servers", broker.getBrokerConnectionString());
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("metadata.fetch.timeout.ms", 1000);
        producer = new KafkaProducer<>(producerProps);
    }

    @After
    public void shutdown() {
        producer.close();
        broker.shutdown();
    }

    /**
     * Test for STORM-2608
     *
     * - Send a few messages to topic
     * - Emit those messages from the partition manager
     * - Fail those tuples so that they are added to the failedMsgRetryManager
     * - Commit partition info to Zookeeper
     * - Wait for kafka to roll logs and remove those messages
     * - Send a new message to the topic
     * - On the next fetch request, a TopicOffsetOutOfRangeException is thrown and the new offset is after
     *      the offset that is currently sitting in both the pending tree and the failedMsgRetryManager
     * - Ack latest message to partition manager
     * - Commit partition info to zookeeper
     * - The committed offset should be the next offset _after_ the last one that was committed
     *
     */
    @Test
    public void test2608() throws Exception {
        SpoutOutputCollector spoutOutputCollector = new SpoutOutputCollector(outputCollector);
        List<PartitionManager> partitionManagers = coordinator.getMyManagedPartitions();
        Assert.assertEquals(1, partitionManagers.size());

        PartitionManager partitionManager = partitionManagers.get(0);

        for (int i=0; i < 5; i++) {
            sendMessage("message-" + i);
        }

        waitForEmitState(partitionManager, spoutOutputCollector, EmitState.EMITTED_MORE_LEFT);
        waitForEmitState(partitionManager, spoutOutputCollector, EmitState.EMITTED_MORE_LEFT);
        waitForEmitState(partitionManager, spoutOutputCollector, EmitState.EMITTED_MORE_LEFT);
        waitForEmitState(partitionManager, spoutOutputCollector, EmitState.EMITTED_MORE_LEFT);
        waitForEmitState(partitionManager, spoutOutputCollector, EmitState.EMITTED_END);

        partitionManager.commit();

        Map<KafkaMessageId, List<Object>> emitted = outputCollector.getEmitted();

        Assert.assertEquals(5, emitted.size());

        for (KafkaMessageId messageId : emitted.keySet()) {
            partitionManager.fail(messageId.offset);
        }

        // Kafka log roller task has an initial delay of 30 seconds so we need to wait for it
        Thread.sleep(TimeUnit.SECONDS.toMillis(35));

        outputCollector.clearEmittedMessages();

        sendMessage("new message");

        // First request will fail due to offset out of range
        Assert.assertEquals(EmitState.NO_EMITTED, partitionManager.next(spoutOutputCollector));
        waitForEmitState(partitionManager, spoutOutputCollector, EmitState.EMITTED_END);

        emitted = outputCollector.getEmitted();

        Assert.assertEquals(1, emitted.size());
        KafkaMessageId messageId = emitted.keySet().iterator().next();

        partitionManager.ack(messageId.offset);
        partitionManager.commit();

        Map<Object, Object> json = zkState.readJSON(partitionManager.committedPath());
        Assert.assertNotNull(json);
        long committedOffset = (long) json.get("offset");

        Assert.assertEquals(messageId.offset + 1, committedOffset);
    }

    private void waitForEmitState(PartitionManager partitionManager, SpoutOutputCollector outputCollector, EmitState expectedState) {
        int maxRetries = 5;
        EmitState state = null;

        for (int retryCount = 0; retryCount < maxRetries; retryCount++) {
            state = partitionManager.next(outputCollector);

            if (state == EmitState.NO_EMITTED) {
                retryCount++;
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for message");
                }
            } else {
                break;
            }
        }

        Assert.assertEquals(expectedState, state);
    }

    private void sendMessage(String value) {
        try {
            producer.send(new ProducerRecord<>(TOPIC_NAME, (String) null, value)).get();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    private static class TestingSpoutOutputCollector implements ISpoutOutputCollector {

        private final Map<KafkaMessageId, List<Object>> emitted = new HashMap<>();

        Map<KafkaMessageId, List<Object>> getEmitted() {
            return emitted;
        }

        void clearEmittedMessages() {
            emitted.clear();
        }

        @Override
        public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
            emitted.put((KafkaMessageId) messageId, tuple);
            return Collections.emptyList();
        }

        @Override
        public void reportError(Throwable error) {
            throw new RuntimeException("Spout error", error);
        }


        @Override
        public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getPendingCount() {
            throw new UnsupportedOperationException();
        }
    }

}