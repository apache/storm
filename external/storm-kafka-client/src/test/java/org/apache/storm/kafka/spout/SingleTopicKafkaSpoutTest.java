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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.KafkaUnitRule;
import org.apache.storm.kafka.spout.config.builder.SingleTopicKafkaSpoutConfiguration;
import org.apache.storm.kafka.spout.internal.KafkaConsumerFactoryDefault;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Time.SimulatedTime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import static org.mockito.Matchers.anyList;

import java.util.regex.Pattern;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.hamcrest.Matchers;

public class SingleTopicKafkaSpoutTest {

    @Rule
    public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule();

    @Captor
    private ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> commitCapture;

    private final TopologyContext topologyContext = mock(TopologyContext.class);
    private final Map<String, Object> conf = new HashMap<>();
    private final SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
    private final long commitOffsetPeriodMs = 2_000;
    private final int maxRetries = 3;
    private KafkaConsumer<String, String> consumerSpy;
    private KafkaSpout<String, String> spout;
    private final int maxPollRecords = 10;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        KafkaSpoutConfig<String, String> spoutConfig =
            SingleTopicKafkaSpoutConfiguration.setCommonSpoutConfig(
                KafkaSpoutConfig.builder("127.0.0.1:" + kafkaUnitRule.getKafkaUnit().getKafkaPort(),
                    Pattern.compile(SingleTopicKafkaSpoutConfiguration.TOPIC)))
                .setOffsetCommitPeriodMs(commitOffsetPeriodMs)
                .setRetry(new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(0), KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(0),
                    maxRetries, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(0)))
                .setProp(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords)
                .build();
        this.consumerSpy = spy(new KafkaConsumerFactoryDefault<String, String>().createConsumer(spoutConfig));
        this.spout = new KafkaSpout<>(spoutConfig, (ignored) -> consumerSpy);
    }

    private void prepareSpout(int messageCount) throws Exception {
        SingleTopicKafkaUnitSetupHelper.populateTopicData(kafkaUnitRule.getKafkaUnit(), SingleTopicKafkaSpoutConfiguration.TOPIC, messageCount);
        SingleTopicKafkaUnitSetupHelper.initializeSpout(spout, conf, topologyContext, collector);
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
        assertThat("Expected committed offset to cover all emitted messages", offset.offset(), is(messageCount));
    }

    @Test
    public void testSeekToCommittedOffsetIfConsumerPositionIsBehindWhenCommitting() throws Exception {
        try (SimulatedTime simulatedTime = new SimulatedTime()) {
            int messageCount = maxPollRecords * 2;
            prepareSpout(messageCount);

            //Emit all messages and fail the first one while acking the rest
            for (int i = 0; i < messageCount; i++) {
                spout.nextTuple();
            }
            ArgumentCaptor<KafkaSpoutMessageId> messageIdCaptor = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
            verify(collector, times(messageCount)).emit(anyString(), anyList(), messageIdCaptor.capture());
            List<KafkaSpoutMessageId> messageIds = messageIdCaptor.getAllValues();
            for (int i = 1; i < messageIds.size(); i++) {
                spout.ack(messageIds.get(i));
            }
            KafkaSpoutMessageId failedTuple = messageIds.get(0);
            spout.fail(failedTuple);

            //Advance the time and replay the failed tuple. 
            reset(collector);
            spout.nextTuple();
            ArgumentCaptor<KafkaSpoutMessageId> failedIdReplayCaptor = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
            verify(collector).emit(anyString(), anyList(), failedIdReplayCaptor.capture());

            assertThat("Expected replay of failed tuple", failedIdReplayCaptor.getValue(), is(failedTuple));

            /* Ack the tuple, and commit.
             * Since the tuple is more than max poll records behind the most recent emitted tuple, the consumer won't catch up in this poll.
             */
            Time.advanceTime(KafkaSpout.TIMER_DELAY_MS + commitOffsetPeriodMs);
            spout.ack(failedIdReplayCaptor.getValue());
            spout.nextTuple();
            verify(consumerSpy).commitSync(commitCapture.capture());
            
            Map<TopicPartition, OffsetAndMetadata> capturedCommit = commitCapture.getValue();
            TopicPartition expectedTp = new TopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, 0);
            assertThat("Should have committed to the right topic", capturedCommit, Matchers.hasKey(expectedTp));
            assertThat("Should have committed all the acked messages", capturedCommit.get(expectedTp).offset(), is((long)messageCount));

            /* Verify that the following acked (now committed) tuples are not emitted again
             * Since the consumer position was somewhere in the middle of the acked tuples when the commit happened,
             * this verifies that the spout keeps the consumer position ahead of the committed offset when committing
             */
            reset(collector);
            //Just do a few polls to check that nothing more is emitted
            for(int i = 0; i < 3; i++) {
                spout.nextTuple();
            }
            verify(collector, never()).emit(any(), any(), any());
        }
    }

    @Test
    public void testShouldContinueWithSlowDoubleAcks() throws Exception {
        try (SimulatedTime simulatedTime = new SimulatedTime()) {
            int messageCount = 20;
            prepareSpout(messageCount);

            //play 1st tuple
            ArgumentCaptor<Object> messageIdToDoubleAck = ArgumentCaptor.forClass(Object.class);
            spout.nextTuple();
            verify(collector).emit(anyString(), anyList(), messageIdToDoubleAck.capture());
            spout.ack(messageIdToDoubleAck.getValue());

            //Emit some more messages
            IntStream.range(0, messageCount / 2).forEach(value -> {
                spout.nextTuple();
            });

            spout.ack(messageIdToDoubleAck.getValue());

            //Emit any remaining messages
            IntStream.range(0, messageCount).forEach(value -> {
                spout.nextTuple();
            });

            //Verify that all messages are emitted, ack all the messages
            ArgumentCaptor<Object> messageIds = ArgumentCaptor.forClass(Object.class);
            verify(collector, times(messageCount)).emit(eq(SingleTopicKafkaSpoutConfiguration.STREAM),
                anyList(),
                messageIds.capture());
            messageIds.getAllValues().iterator().forEachRemaining(spout::ack);

            Time.advanceTime(commitOffsetPeriodMs + KafkaSpout.TIMER_DELAY_MS);
            //Commit offsets
            spout.nextTuple();

            verifyAllMessagesCommitted(messageCount);
        }
    }

    @Test
    public void testShouldEmitAllMessages() throws Exception {
        try (SimulatedTime simulatedTime = new SimulatedTime()) {
            int messageCount = 10;
            prepareSpout(messageCount);

            //Emit all messages and check that they are emitted. Ack the messages too
            IntStream.range(0, messageCount).forEach(value -> {
                spout.nextTuple();
                ArgumentCaptor<Object> messageId = ArgumentCaptor.forClass(Object.class);
                verify(collector).emit(
                    eq(SingleTopicKafkaSpoutConfiguration.STREAM),
                    eq(new Values(SingleTopicKafkaSpoutConfiguration.TOPIC,
                        Integer.toString(value),
                        Integer.toString(value))),
                    messageId.capture());
                spout.ack(messageId.getValue());
                reset(collector);
            });

            Time.advanceTime(commitOffsetPeriodMs + KafkaSpout.TIMER_DELAY_MS);
            //Commit offsets
            spout.nextTuple();

            verifyAllMessagesCommitted(messageCount);
        }
    }

    @Test
    public void testShouldReplayInOrderFailedMessages() throws Exception {
        try (SimulatedTime simulatedTime = new SimulatedTime()) {
            int messageCount = 10;
            prepareSpout(messageCount);

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
            IntStream.range(0, messageCount).forEach(value -> {
                spout.nextTuple();
            });

            ArgumentCaptor<Object> remainingMessageIds = ArgumentCaptor.forClass(Object.class);
            //All messages except the first acked message should have been emitted
            verify(collector, times(messageCount - 1)).emit(
                eq(SingleTopicKafkaSpoutConfiguration.STREAM),
                anyList(),
                remainingMessageIds.capture());
            remainingMessageIds.getAllValues().iterator().forEachRemaining(spout::ack);

            Time.advanceTime(commitOffsetPeriodMs + KafkaSpout.TIMER_DELAY_MS);
            //Commit offsets
            spout.nextTuple();

            verifyAllMessagesCommitted(messageCount);
        }
    }

    @Test
    public void testShouldReplayFirstTupleFailedOutOfOrder() throws Exception {
        try (SimulatedTime simulatedTime = new SimulatedTime()) {
            int messageCount = 10;
            prepareSpout(messageCount);

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
            IntStream.range(0, messageCount).forEach(value -> {
                spout.nextTuple();
            });

            ArgumentCaptor<Object> remainingIds = ArgumentCaptor.forClass(Object.class);
            //All messages except the first acked message should have been emitted
            verify(collector, times(messageCount - 1)).emit(
                eq(SingleTopicKafkaSpoutConfiguration.STREAM),
                anyList(),
                remainingIds.capture());
            remainingIds.getAllValues().iterator().forEachRemaining(spout::ack);

            Time.advanceTime(commitOffsetPeriodMs + KafkaSpout.TIMER_DELAY_MS);
            //Commit offsets
            spout.nextTuple();

            verifyAllMessagesCommitted(messageCount);
        }
    }

    @Test
    public void testShouldReplayAllFailedTuplesWhenFailedOutOfOrder() throws Exception {
        //The spout must reemit retriable tuples, even if they fail out of order.
        //The spout should be able to skip tuples it has already emitted when retrying messages, even if those tuples are also retries.
        int messageCount = 10;
        prepareSpout(messageCount);

        //play all tuples
        for (int i = 0; i < messageCount; i++) {
            spout.nextTuple();
        }
        ArgumentCaptor<KafkaSpoutMessageId> messageIds = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
        verify(collector, times(messageCount)).emit(anyString(), anyList(), messageIds.capture());
        reset(collector);
        //Fail tuple 5 and 3, call nextTuple, then fail tuple 2
        List<KafkaSpoutMessageId> capturedMessageIds = messageIds.getAllValues();
        spout.fail(capturedMessageIds.get(5));
        spout.fail(capturedMessageIds.get(3));
        spout.nextTuple();
        spout.fail(capturedMessageIds.get(2));

        //Check that the spout will reemit all 3 failed tuples and no other tuples
        ArgumentCaptor<KafkaSpoutMessageId> reemittedMessageIds = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
        for (int i = 0; i < messageCount; i++) {
            spout.nextTuple();
        }
        verify(collector, times(3)).emit(anyString(), anyList(), reemittedMessageIds.capture());
        Set<KafkaSpoutMessageId> expectedReemitIds = new HashSet<>();
        expectedReemitIds.add(capturedMessageIds.get(5));
        expectedReemitIds.add(capturedMessageIds.get(3));
        expectedReemitIds.add(capturedMessageIds.get(2));
        assertThat("Expected reemits to be the 3 failed tuples", new HashSet<>(reemittedMessageIds.getAllValues()), is(expectedReemitIds));
    }

    @Test
    public void testShouldDropMessagesAfterMaxRetriesAreReached() throws Exception {
        //Check that if one message fails repeatedly, the retry cap limits how many times the message can be reemitted
        int messageCount = 1;
        prepareSpout(messageCount);

        //Emit and fail the same tuple until we've reached retry limit
        for (int i = 0; i <= maxRetries; i++) {
            ArgumentCaptor<KafkaSpoutMessageId> messageIdFailed = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
            spout.nextTuple();
            verify(collector).emit(anyString(), anyList(), messageIdFailed.capture());
            KafkaSpoutMessageId msgId = messageIdFailed.getValue();
            spout.fail(msgId);
            assertThat("Expected message id number of failures to match the number of times the message has failed", msgId.numFails(), is(i + 1));
            reset(collector);
        }

        //Verify that the tuple is not emitted again
        spout.nextTuple();
        verify(collector, never()).emit(any(), any(), any());
    }

    @Test
    public void testSpoutMustRefreshPartitionsEvenIfNotPolling() throws Exception {
        try (SimulatedTime time = new SimulatedTime()) {
            SingleTopicKafkaUnitSetupHelper.initializeSpout(spout, conf, topologyContext, collector);

            //Nothing is assigned yet, should emit nothing
            spout.nextTuple();
            verify(collector, never()).emit(anyString(), anyList(), any(KafkaSpoutMessageId.class));

            SingleTopicKafkaUnitSetupHelper.populateTopicData(kafkaUnitRule.getKafkaUnit(), SingleTopicKafkaSpoutConfiguration.TOPIC, 1);
            Time.advanceTime(KafkaSpoutConfig.DEFAULT_PARTITION_REFRESH_PERIOD_MS + KafkaSpout.TIMER_DELAY_MS);

            //The new partition should be discovered and the message should be emitted
            spout.nextTuple();
            verify(collector).emit(anyString(), anyList(), any(KafkaSpoutMessageId.class));
        }
    }
}
