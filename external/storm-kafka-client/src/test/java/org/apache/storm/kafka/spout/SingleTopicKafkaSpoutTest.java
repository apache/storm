/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.apache.storm.kafka.spout;


import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.kafka.spout.builders.SingleTopicKafkaSpoutConfiguration;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.Properties;

import static org.apache.storm.kafka.spout.builders.SingleTopicKafkaSpoutConfiguration.getKafkaSpoutConfig;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;


public class SingleTopicKafkaSpoutTest {
    // ref: https://github.com/asmaier/mini-kafka
    private KafkaServer kafkaServer;
    private EmbeddedZookeeper zkServer;
    private ZkUtils zkUtils;
    private static final String ZK_HOST = "127.0.0.1";
    private static final String KAFKA_HOST = "127.0.0.1";
    private static final int KAFKA_PORT = 9092;

    private class SpoutContext {
        public KafkaSpout<String, String> spout;
        public SpoutOutputCollector collector;

        public SpoutContext(KafkaSpout<String, String> spout,
                            SpoutOutputCollector collector) {
            this.spout = spout;
            this.collector = collector;
        }
    }

    @Before
    public void setUp() throws IOException {
        // setup ZK
        zkServer = new EmbeddedZookeeper();
        String zkConnect = ZK_HOST + ":" + zkServer.port();
        ZkClient zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        zkUtils = ZkUtils.apply(zkClient, false);

        // setup Broker
        Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://" + KAFKA_HOST + ":" + KAFKA_PORT);
        KafkaConfig config = new KafkaConfig(brokerProps);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
    }

    @After
    public void tearDown() throws IOException {
        kafkaServer.shutdown();
        zkUtils.close();
        zkServer.shutdown();
    }

    void populateTopicData(String topicName, int msgCount) {
        AdminUtils.createTopic(zkUtils, topicName, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);

        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", KAFKA_HOST + ":" + KAFKA_PORT);
        producerProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        for (int i = 0; i < msgCount; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                    topicName, Integer.toString(i),
                    Integer.toString(i));
            producer.send(producerRecord);
        }
        producer.close();
    }

    SpoutContext initializeSpout(int msgCount) {
        populateTopicData(SingleTopicKafkaSpoutConfiguration.TOPIC, msgCount);

        TopologyContext topology = mock(TopologyContext.class);
        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
        Map conf = mock(Map.class);

        KafkaSpout<String, String> spout = new KafkaSpout<>(getKafkaSpoutConfig(KAFKA_PORT));
        spout.open(conf, topology, collector);
        spout.activate();
        return new SpoutContext(spout, collector);
    }

    /*
     * Asserts that the next possible offset to commit or the committed offset is the provided offset.
     * An offset that is ready to be committed is not guarenteed to be already committed.
     */
    private void assertOffsetCommitted(int offset, KafkaSpout.OffsetEntry entry) {

        boolean currentOffsetMatch = entry.getCommittedOffset() == offset;
        OffsetAndMetadata nextOffset = entry.findNextCommitOffset();
        boolean nextOffsetMatch = nextOffset != null && nextOffset.offset() == offset;
        assertTrue("Next offset: " +
                   entry.findNextCommitOffset() +
                   " OR current offset: " +
                   entry.getCommittedOffset() +
                   " must equal desired offset: " +
                   offset,
                   currentOffsetMatch | nextOffsetMatch);
    }

    @Test
    public void shouldContinueWithSlowDoubleAcks() throws Exception {
        int messageCount = 20;
        SpoutContext context = initializeSpout(messageCount);

        //play 1st tuple
        ArgumentCaptor<Object> messageIdToDoubleAck = ArgumentCaptor.forClass(Object.class);
        context.spout.nextTuple();
        verify(context.collector).emit(anyString(), anyList(), messageIdToDoubleAck.capture());
        context.spout.ack(messageIdToDoubleAck.getValue());

        for (int i = 0; i < messageCount / 2; i++) {
            context.spout.nextTuple();
        }

        context.spout.ack(messageIdToDoubleAck.getValue());

        for (int i = 0; i < messageCount; i++) {
            context.spout.nextTuple();
        }

        ArgumentCaptor<Object> remainingIds = ArgumentCaptor.forClass(Object.class);

        verify(context.collector, times(messageCount)).emit(
                eq(SingleTopicKafkaSpoutConfiguration.STREAM),
                anyList(),
                remainingIds.capture());
        for (Object id : remainingIds.getAllValues()) {
            context.spout.ack(id);
        }

        for (Object item : context.spout.acked.values()) {
            assertOffsetCommitted(messageCount - 1, (KafkaSpout.OffsetEntry) item);
        }
    }

    @Test
    public void shouldEmitAllMessages() throws Exception {
        int messageCount = 10;
        SpoutContext context = initializeSpout(messageCount);

        for (int i = 0; i < messageCount; i++) {
            context.spout.nextTuple();
            ArgumentCaptor<Object> messageId = ArgumentCaptor.forClass(Object.class);
            verify(context.collector).emit(
                    eq(SingleTopicKafkaSpoutConfiguration.STREAM),
                    eq(new Values(SingleTopicKafkaSpoutConfiguration.TOPIC,
                                  Integer.toString(i),
                                  Integer.toString(i))),
                    messageId.capture());
            context.spout.ack(messageId.getValue());
            reset(context.collector);
        }

        for (Object item : context.spout.acked.values()) {
            assertOffsetCommitted(messageCount - 1, (KafkaSpout.OffsetEntry) item);
        }
    }

    @Test
    public void shouldReplayInOrderFailedMessages() throws Exception {
        int messageCount = 10;
        SpoutContext context = initializeSpout(messageCount);

        //play and ack 1 tuple
        ArgumentCaptor<Object> messageIdAcked = ArgumentCaptor.forClass(Object.class);
        context.spout.nextTuple();
        verify(context.collector).emit(anyString(), anyList(), messageIdAcked.capture());
        context.spout.ack(messageIdAcked.getValue());
        reset(context.collector);

        //play and fail 1 tuple
        ArgumentCaptor<Object> messageIdFailed = ArgumentCaptor.forClass(Object.class);
        context.spout.nextTuple();
        verify(context.collector).emit(anyString(), anyList(), messageIdFailed.capture());
        context.spout.fail(messageIdFailed.getValue());
        reset(context.collector);

        //pause so that failed tuples will be retried
        Thread.sleep(200);

        //allow for some calls to nextTuple() to fail to emit a tuple
        for (int i = 0; i < messageCount + 5; i++) {
            context.spout.nextTuple();
        }

        ArgumentCaptor<Object> remainingMessageIds = ArgumentCaptor.forClass(Object.class);

        //1 message replayed, messageCount - 2 messages emitted for the first time
        verify(context.collector, times(messageCount - 1)).emit(
                eq(SingleTopicKafkaSpoutConfiguration.STREAM),
                anyList(),
                remainingMessageIds.capture());
        for (Object id : remainingMessageIds.getAllValues()) {
            context.spout.ack(id);
        }

        for (Object item : context.spout.acked.values()) {
            assertOffsetCommitted(messageCount - 1, (KafkaSpout.OffsetEntry) item);
        }
    }

    @Test
    public void shouldReplayFirstTupleFailedOutOfOrder() throws Exception {
        int messageCount = 10;
        SpoutContext context = initializeSpout(messageCount);

        //play 1st tuple
        ArgumentCaptor<Object> messageIdToFail = ArgumentCaptor.forClass(Object.class);
        context.spout.nextTuple();
        verify(context.collector).emit(anyString(), anyList(), messageIdToFail.capture());
        reset(context.collector);

        //play 2nd tuple
        ArgumentCaptor<Object> messageIdToAck = ArgumentCaptor.forClass(Object.class);
        context.spout.nextTuple();
        verify(context.collector).emit(anyString(), anyList(), messageIdToAck.capture());
        reset(context.collector);

        //ack 2nd tuple
        context.spout.ack(messageIdToAck.getValue());
        //fail 1st tuple
        context.spout.fail(messageIdToFail.getValue());

        //pause so that failed tuples will be retried
        Thread.sleep(200);

        //allow for some calls to nextTuple() to fail to emit a tuple
        for (int i = 0; i < messageCount + 5; i++) {
            context.spout.nextTuple();
        }

        ArgumentCaptor<Object> remainingIds = ArgumentCaptor.forClass(Object.class);
        //1 message replayed, messageCount - 2 messages emitted for the first time
        verify(context.collector, times(messageCount - 1)).emit(
                eq(SingleTopicKafkaSpoutConfiguration.STREAM),
                anyList(),
                remainingIds.capture());
        for (Object id : remainingIds.getAllValues()) {
            context.spout.ack(id);
        }

        for (Object item : context.spout.acked.values()) {
            assertOffsetCommitted(messageCount - 1, (KafkaSpout.OffsetEntry) item);
        }
    }
}