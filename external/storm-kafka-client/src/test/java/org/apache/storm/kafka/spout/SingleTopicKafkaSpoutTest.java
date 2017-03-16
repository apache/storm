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

import static org.apache.storm.kafka.spout.builders.SingleTopicKafkaSpoutConfiguration.getKafkaSpoutConfig;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.kafka.KafkaUnitRule;
import org.apache.storm.kafka.spout.builders.SingleTopicKafkaSpoutConfiguration;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.internal.KafkaConsumerFactory;
import org.apache.storm.kafka.spout.internal.KafkaConsumerFactoryDefault;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Time.SimulatedTime;
import org.junit.Before;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;


public class SingleTopicKafkaSpoutTest {

    @Rule
    public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule();

    @Captor
    private ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> commitCapture;

    private final TopologyContext topologyContext = mock(TopologyContext.class);
    private final Map<String, Object> conf = new HashMap<>();
    private final SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
    private final long commitOffsetPeriodMs = 2_000;
    private KafkaConsumer<String, String> consumerSpy;
    private KafkaConsumerFactory<String, String> consumerFactory;
    private KafkaSpout<String, String> spout;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        KafkaSpoutConfig spoutConfig = getKafkaSpoutConfig(kafkaUnitRule.getKafkaUnit().getKafkaPort(), commitOffsetPeriodMs);
        this.consumerSpy = spy(new KafkaConsumerFactoryDefault().createConsumer(spoutConfig));
        this.consumerFactory = new KafkaConsumerFactory<String, String>() {
            @Override
            public KafkaConsumer<String, String> createConsumer(KafkaSpoutConfig<String, String> kafkaSpoutConfig) {
                return consumerSpy;
            }
        
        };
        this.spout = new KafkaSpout<>(spoutConfig, consumerFactory);
    }

    void populateTopicData(String topicName, int msgCount) throws InterruptedException, ExecutionException, TimeoutException {
        kafkaUnitRule.getKafkaUnit().createTopic(topicName);

        for (int i = 0; i < msgCount; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                    topicName, Integer.toString(i),
                    Integer.toString(i));
            kafkaUnitRule.getKafkaUnit().sendMessage(producerRecord);
        }
    }

    private void initializeSpout(int msgCount) throws InterruptedException, ExecutionException, TimeoutException {
        populateTopicData(SingleTopicKafkaSpoutConfiguration.TOPIC, msgCount);
        spout.open(conf, topologyContext, collector);
        spout.activate();
    }

    /*
     * Asserts that commitSync has been called once, 
     * that there are only commits on one topic,
     * and that the committed offset covers messageCount messages
     */
    private void verifyAllMessagesCommitted(long messageCount) {
        verify(consumerSpy, times(1)).commitSync(commitCapture.capture());
        Map<TopicPartition, OffsetAndMetadata> commits = commitCapture.getValue();
        assertThat("Expected commits for only one topic partition", commits.entrySet().size(), is(1));
        OffsetAndMetadata offset = commits.entrySet().iterator().next().getValue();
        assertThat("Expected committed offset to cover all emitted messages", offset.offset(), is(messageCount - 1));
    }

    @Test
    public void shouldContinueWithSlowDoubleAcks() throws Exception {
        try (SimulatedTime simulatedTime = new SimulatedTime()) {
            int messageCount = 20;
            initializeSpout(messageCount);

            //play 1st tuple
            ArgumentCaptor<Object> messageIdToDoubleAck = ArgumentCaptor.forClass(Object.class);
            spout.nextTuple();
            verify(collector).emit(anyString(), anyList(), messageIdToDoubleAck.capture());
            spout.ack(messageIdToDoubleAck.getValue());

            //Emit some more messages
            for(int i = 0; i < messageCount / 2; i++) {
                spout.nextTuple();
            }

            spout.ack(messageIdToDoubleAck.getValue());

            //Emit any remaining messages
            for(int i = 0; i < messageCount; i++) {
                spout.nextTuple();
            }

            //Verify that all messages are emitted, ack all the messages
            ArgumentCaptor<Object> messageIds = ArgumentCaptor.forClass(Object.class);
            verify(collector, times(messageCount)).emit(eq(SingleTopicKafkaSpoutConfiguration.STREAM),
                anyList(),
                messageIds.capture());
            for(Object id : messageIds.getAllValues()) {
                spout.ack(id);
            }

            Time.advanceTime(commitOffsetPeriodMs + KafkaSpout.TIMER_DELAY_MS);
            //Commit offsets
            spout.nextTuple();

            verifyAllMessagesCommitted(messageCount);
        }
    }

    @Test
    public void shouldEmitAllMessages() throws Exception {
        try (SimulatedTime simulatedTime = new SimulatedTime()) {
            int messageCount = 10;
            initializeSpout(messageCount);

            //Emit all messages and check that they are emitted. Ack the messages too
            for(int i = 0; i < messageCount; i++) {
                spout.nextTuple();
                ArgumentCaptor<Object> messageId = ArgumentCaptor.forClass(Object.class);
                verify(collector).emit(
                    eq(SingleTopicKafkaSpoutConfiguration.STREAM),
                    eq(new Values(SingleTopicKafkaSpoutConfiguration.TOPIC,
                        Integer.toString(i),
                        Integer.toString(i))),
                    messageId.capture());
                spout.ack(messageId.getValue());
                reset(collector);
            }

            Time.advanceTime(commitOffsetPeriodMs + KafkaSpout.TIMER_DELAY_MS);
            //Commit offsets
            spout.nextTuple();

            verifyAllMessagesCommitted(messageCount);
        }
    }

    @Test
    public void shouldReplayInOrderFailedMessages() throws Exception {
        try (SimulatedTime simulatedTime = new SimulatedTime()) {
            int messageCount = 10;
            initializeSpout(messageCount);

            //play and ack 1 tuple
            ArgumentCaptor<Object> messageIdAcked = ArgumentCaptor.forClass(Object.class);
            spout.nextTuple();
            verify(collector).emit(anyString(), anyList(), messageIdAcked.capture());
            spout.ack(messageIdAcked.getValue());
            reset(collector);

            //play and fail 1 tuple
            ArgumentCaptor<Object> messageIdFailed = ArgumentCaptor.forClass(Object.class);
            spout.nextTuple();
            verify(collector).emit(anyString(), anyList(), messageIdFailed.capture());
            spout.fail(messageIdFailed.getValue());
            reset(collector);

            //Emit all remaining messages. Failed tuples retry immediately with current configuration, so no need to wait.
            for(int i = 0; i < messageCount; i++) {
                spout.nextTuple();
            }

            ArgumentCaptor<Object> remainingMessageIds = ArgumentCaptor.forClass(Object.class);
            //All messages except the first acked message should have been emitted
            verify(collector, times(messageCount - 1)).emit(
                eq(SingleTopicKafkaSpoutConfiguration.STREAM),
                anyList(),
                remainingMessageIds.capture());
            for(Object id : remainingMessageIds.getAllValues()) {
                spout.ack(id);
            }

            Time.advanceTime(commitOffsetPeriodMs + KafkaSpout.TIMER_DELAY_MS);
            //Commit offsets
            spout.nextTuple();

            verifyAllMessagesCommitted(messageCount);
        }
    }

    @Test
    public void shouldReplayFirstTupleFailedOutOfOrder() throws Exception {
        try (SimulatedTime simulatedTime = new SimulatedTime()) {
            int messageCount = 10;
            initializeSpout(messageCount);

            //play 1st tuple
            ArgumentCaptor<Object> messageIdToFail = ArgumentCaptor.forClass(Object.class);
            spout.nextTuple();
            verify(collector).emit(anyString(), anyList(), messageIdToFail.capture());
            reset(collector);

            //play 2nd tuple
            ArgumentCaptor<Object> messageIdToAck = ArgumentCaptor.forClass(Object.class);
            spout.nextTuple();
            verify(collector).emit(anyString(), anyList(), messageIdToAck.capture());
            reset(collector);

            //ack 2nd tuple
            spout.ack(messageIdToAck.getValue());
            //fail 1st tuple
            spout.fail(messageIdToFail.getValue());

            //Emit all remaining messages. Failed tuples retry immediately with current configuration, so no need to wait.
            for(int i = 0; i < messageCount; i++) {
                spout.nextTuple();
            }

            ArgumentCaptor<Object> remainingIds = ArgumentCaptor.forClass(Object.class);
            //All messages except the first acked message should have been emitted
            verify(collector, times(messageCount - 1)).emit(
                eq(SingleTopicKafkaSpoutConfiguration.STREAM),
                anyList(),
                remainingIds.capture());
            for(Object id : remainingIds.getAllValues()) {
                spout.ack(id);
            }

            Time.advanceTime(commitOffsetPeriodMs + KafkaSpout.TIMER_DELAY_MS);
            //Commit offsets
            spout.nextTuple();

            verifyAllMessagesCommitted(messageCount);
        }
    }
}
