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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyListOf;
import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.storm.kafka.spout.config.builder.SingleTopicKafkaSpoutConfiguration;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Time;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class KafkaSpoutSingleTopicTest extends KafkaSpoutAbstractTest {
    private final int maxPollRecords = 10;
    private final int maxRetries = 3;

    public KafkaSpoutSingleTopicTest() {
        super(2_000);
    }

    @Override
    KafkaSpoutConfig<String, String> createSpoutConfig() {
        return SingleTopicKafkaSpoutConfiguration.setCommonSpoutConfig(
            KafkaSpoutConfig.builder("127.0.0.1:" + kafkaUnitExtension.getKafkaUnit().getKafkaPort(),
                Pattern.compile(SingleTopicKafkaSpoutConfiguration.TOPIC)))
            .setOffsetCommitPeriodMs(commitOffsetPeriodMs)
            .setRetry(new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(0), KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(0),
                maxRetries, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(0)))
            .setProp(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords)
            .build();
    }

    @Test
    public void testSeekToCommittedOffsetIfConsumerPositionIsBehindWhenCommitting() throws Exception {
        final int messageCount = maxPollRecords * 2;
        prepareSpout(messageCount);

        //Emit all messages and fail the first one while acking the rest
        for (int i = 0; i < messageCount; i++) {
            spout.nextTuple();
        }
        ArgumentCaptor<KafkaSpoutMessageId> messageIdCaptor = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
        verify(collectorMock, times(messageCount)).emit(anyString(), anyList(), messageIdCaptor.capture());
        List<KafkaSpoutMessageId> messageIds = messageIdCaptor.getAllValues();
        for (int i = 1; i < messageIds.size(); i++) {
            spout.ack(messageIds.get(i));
        }
        KafkaSpoutMessageId failedTuple = messageIds.get(0);
        spout.fail(failedTuple);

        //Advance the time and replay the failed tuple. 
        reset(collectorMock);
        spout.nextTuple();
        ArgumentCaptor<KafkaSpoutMessageId> failedIdReplayCaptor = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
        verify(collectorMock).emit(anyString(), anyList(), failedIdReplayCaptor.capture());

        assertThat("Expected replay of failed tuple", failedIdReplayCaptor.getValue(), is(failedTuple));

        /* Ack the tuple, and commit.
         * Since the tuple is more than max poll records behind the most recent emitted tuple, the consumer won't catch up in this poll.
         */
        clearInvocations(collectorMock);
        Time.advanceTime(KafkaSpout.TIMER_DELAY_MS + commitOffsetPeriodMs);
        spout.ack(failedIdReplayCaptor.getValue());
        spout.nextTuple();
        verify(getKafkaConsumer()).commitSync(commitCapture.capture());

        Map<TopicPartition, OffsetAndMetadata> capturedCommit = commitCapture.getValue();
        TopicPartition expectedTp = new TopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, 0);
        assertThat("Should have committed to the right topic", capturedCommit, Matchers.hasKey(expectedTp));
        assertThat("Should have committed all the acked messages", capturedCommit.get(expectedTp).offset(), is((long)messageCount));

        /* Verify that the following acked (now committed) tuples are not emitted again
         * Since the consumer position was somewhere in the middle of the acked tuples when the commit happened,
         * this verifies that the spout keeps the consumer position ahead of the committed offset when committing
         */
        //Just do a few polls to check that nothing more is emitted
        for(int i = 0; i < 3; i++) {
            spout.nextTuple();
        }
        verify(collectorMock, never()).emit(anyString(), anyList(), anyObject());
    }
    
    @Test
    public void testClearingWaitingToEmitIfConsumerPositionIsNotBehindWhenCommitting() throws Exception {
        final int messageCountExcludingLast = maxPollRecords;
        int messagesInKafka = messageCountExcludingLast + 1;
        prepareSpout(messagesInKafka);

        //Emit all messages and fail the first one while acking the rest
        for (int i = 0; i < messageCountExcludingLast; i++) {
            spout.nextTuple();
        }
        ArgumentCaptor<KafkaSpoutMessageId> messageIdCaptor = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
        verify(collectorMock, times(messageCountExcludingLast)).emit(anyString(), anyList(), messageIdCaptor.capture());
        List<KafkaSpoutMessageId> messageIds = messageIdCaptor.getAllValues();
        for (int i = 1; i < messageIds.size(); i++) {
            spout.ack(messageIds.get(i));
        }
        KafkaSpoutMessageId failedTuple = messageIds.get(0);
        spout.fail(failedTuple);

        //Advance the time and replay the failed tuple. 
        //Since the last tuple on the partition is more than maxPollRecords ahead of the failed tuple, it shouldn't be emitted here
        reset(collectorMock);
        spout.nextTuple();
        ArgumentCaptor<KafkaSpoutMessageId> failedIdReplayCaptor = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
        verify(collectorMock).emit(anyString(), anyList(), failedIdReplayCaptor.capture());

        assertThat("Expected replay of failed tuple", failedIdReplayCaptor.getValue(), is(failedTuple));

        /* Ack the tuple, and commit.
         * 
         * The waiting to emit list should now be cleared, and the next emitted tuple should be the last tuple on the partition,
         * which hasn't been emitted yet
         */
        reset(collectorMock);
        Time.advanceTime(KafkaSpout.TIMER_DELAY_MS + commitOffsetPeriodMs);
        spout.ack(failedIdReplayCaptor.getValue());
        spout.nextTuple();
        verify(getKafkaConsumer()).commitSync(commitCapture.capture());

        Map<TopicPartition, OffsetAndMetadata> capturedCommit = commitCapture.getValue();
        TopicPartition expectedTp = new TopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, 0);
        assertThat("Should have committed to the right topic", capturedCommit, Matchers.hasKey(expectedTp));
        assertThat("Should have committed all the acked messages", capturedCommit.get(expectedTp).offset(), is((long)messageCountExcludingLast));
        
        ArgumentCaptor<KafkaSpoutMessageId> lastOffsetMessageCaptor = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
        verify(collectorMock).emit(anyString(), anyList(), lastOffsetMessageCaptor.capture());
        assertThat("Expected emit of the final tuple in the partition", lastOffsetMessageCaptor.getValue().offset(), is(messagesInKafka - 1L));
        reset(collectorMock);

        //Nothing else should be emitted, all tuples are acked except for the final tuple, which is pending.
        for(int i = 0; i < 3; i++) {
            spout.nextTuple();
        }
        verify(collectorMock, never()).emit(anyString(), anyList(), anyObject());
    }

    @Test
    public void testShouldContinueWithSlowDoubleAcks() throws Exception {
        final int messageCount = 20;
        prepareSpout(messageCount);

        //play 1st tuple
        ArgumentCaptor<Object> messageIdToDoubleAck = ArgumentCaptor.forClass(Object.class);
        spout.nextTuple();
        verify(collectorMock).emit(anyString(), anyList(), messageIdToDoubleAck.capture());
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
        verify(collectorMock, times(messageCount)).emit(eq(SingleTopicKafkaSpoutConfiguration.STREAM),
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

    @Test
    public void testShouldEmitAllMessages() throws Exception {
        final int messageCount = 10;
        prepareSpout(messageCount);

        //Emit all messages and check that they are emitted. Ack the messages too
        for(int i = 0; i < messageCount; i++) {
            spout.nextTuple();
            ArgumentCaptor<Object> messageId = ArgumentCaptor.forClass(Object.class);
            verify(collectorMock).emit(
                eq(SingleTopicKafkaSpoutConfiguration.STREAM),
                eq(new Values(SingleTopicKafkaSpoutConfiguration.TOPIC,
                    Integer.toString(i),
                    Integer.toString(i))),
                messageId.capture());
            spout.ack(messageId.getValue());
            reset(collectorMock);
        }

        Time.advanceTime(commitOffsetPeriodMs + KafkaSpout.TIMER_DELAY_MS);
        //Commit offsets
        spout.nextTuple();

        verifyAllMessagesCommitted(messageCount);
    }

    @Test
    public void testShouldReplayInOrderFailedMessages() throws Exception {
        final int messageCount = 10;
        prepareSpout(messageCount);

        //play and ack 1 tuple
        ArgumentCaptor<Object> messageIdAcked = ArgumentCaptor.forClass(Object.class);
        spout.nextTuple();
        verify(collectorMock).emit(anyString(), anyList(), messageIdAcked.capture());
        spout.ack(messageIdAcked.getValue());
        reset(collectorMock);

        //play and fail 1 tuple
        ArgumentCaptor<Object> messageIdFailed = ArgumentCaptor.forClass(Object.class);
        spout.nextTuple();
        verify(collectorMock).emit(anyString(), anyList(), messageIdFailed.capture());
        spout.fail(messageIdFailed.getValue());
        reset(collectorMock);

        //Emit all remaining messages. Failed tuples retry immediately with current configuration, so no need to wait.
        for(int i = 0; i < messageCount; i++) {
            spout.nextTuple();
        }

        ArgumentCaptor<Object> remainingMessageIds = ArgumentCaptor.forClass(Object.class);
        //All messages except the first acked message should have been emitted
        verify(collectorMock, times(messageCount - 1)).emit(
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

    @Test
    public void testShouldReplayFirstTupleFailedOutOfOrder() throws Exception {
        final int messageCount = 10;
        prepareSpout(messageCount);

        //play 1st tuple
        ArgumentCaptor<Object> messageIdToFail = ArgumentCaptor.forClass(Object.class);
        spout.nextTuple();
        verify(collectorMock).emit(anyString(), anyList(), messageIdToFail.capture());
        reset(collectorMock);

        //play 2nd tuple
        ArgumentCaptor<Object> messageIdToAck = ArgumentCaptor.forClass(Object.class);
        spout.nextTuple();
        verify(collectorMock).emit(anyString(), anyList(), messageIdToAck.capture());
        reset(collectorMock);

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
        verify(collectorMock, times(messageCount - 1)).emit(
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

    @Test
    public void testShouldReplayAllFailedTuplesWhenFailedOutOfOrder() throws Exception {
        //The spout must reemit retriable tuples, even if they fail out of order.
        //The spout should be able to skip tuples it has already emitted when retrying messages, even if those tuples are also retries.
        final int messageCount = 10;
        prepareSpout(messageCount);

        //play all tuples
        for (int i = 0; i < messageCount; i++) {
            spout.nextTuple();
        }
        ArgumentCaptor<KafkaSpoutMessageId> messageIds = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
        verify(collectorMock, times(messageCount)).emit(anyString(), anyList(), messageIds.capture());
        reset(collectorMock);
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
        verify(collectorMock, times(3)).emit(anyString(), anyList(), reemittedMessageIds.capture());
        Set<KafkaSpoutMessageId> expectedReemitIds = new HashSet<>();
        expectedReemitIds.add(capturedMessageIds.get(5));
        expectedReemitIds.add(capturedMessageIds.get(3));
        expectedReemitIds.add(capturedMessageIds.get(2));
        assertThat("Expected reemits to be the 3 failed tuples", new HashSet<>(reemittedMessageIds.getAllValues()), is(expectedReemitIds));
    }

    @Test
    public void testShouldDropMessagesAfterMaxRetriesAreReached() throws Exception {
        //Check that if one message fails repeatedly, the retry cap limits how many times the message can be reemitted
        final int messageCount = 1;
        prepareSpout(messageCount);

        //Emit and fail the same tuple until we've reached retry limit
        for (int i = 0; i <= maxRetries; i++) {
            ArgumentCaptor<KafkaSpoutMessageId> messageIdFailed = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
            spout.nextTuple();
            verify(collectorMock).emit(anyString(), anyListOf(Object.class), messageIdFailed.capture());
            KafkaSpoutMessageId msgId = messageIdFailed.getValue();
            spout.fail(msgId);
            assertThat("Expected message id number of failures to match the number of times the message has failed", msgId.numFails(), is(i + 1));
            reset(collectorMock);
        }

        //Verify that the tuple is not emitted again
        spout.nextTuple();
        verify(collectorMock, never()).emit(anyString(), anyListOf(Object.class), anyObject());
    }

    @Test
    public void testSpoutMustRefreshPartitionsEvenIfNotPolling() throws Exception {
        SingleTopicKafkaUnitSetupHelper.initializeSpout(spout, conf, topologyContext, collectorMock);

        //Nothing is assigned yet, should emit nothing
        spout.nextTuple();
        verify(collectorMock, never()).emit(anyString(), anyList(), any(KafkaSpoutMessageId.class));

        SingleTopicKafkaUnitSetupHelper.populateTopicData(kafkaUnitExtension.getKafkaUnit(), SingleTopicKafkaSpoutConfiguration.TOPIC, 1);
        Time.advanceTime(KafkaSpoutConfig.DEFAULT_PARTITION_REFRESH_PERIOD_MS + KafkaSpout.TIMER_DELAY_MS);

        //The new partition should be discovered and the message should be emitted
        spout.nextTuple();
        verify(collectorMock).emit(anyString(), anyList(), any(KafkaSpoutMessageId.class));
    }

    @Test
    public void testOffsetMetrics() throws Exception {
        final int messageCount = 10;
        prepareSpout(messageCount);

        Map<String, Long> offsetMetric  = (Map<String, Long>) spout.getKafkaOffsetMetric().getValueAndReset();
        assertEquals(offsetMetric.get(SingleTopicKafkaSpoutConfiguration.TOPIC+"/totalEarliestTimeOffset").longValue(), 0);
        // the offset of the last available message + 1.
        assertEquals(offsetMetric.get(SingleTopicKafkaSpoutConfiguration.TOPIC+"/totalLatestTimeOffset").longValue(), 10);
        assertEquals(offsetMetric.get(SingleTopicKafkaSpoutConfiguration.TOPIC+"/totalRecordsInPartitions").longValue(), 10);
        assertEquals(offsetMetric.get(SingleTopicKafkaSpoutConfiguration.TOPIC+"/totalLatestEmittedOffset").longValue(), 0);
        assertEquals(offsetMetric.get(SingleTopicKafkaSpoutConfiguration.TOPIC+"/totalLatestCompletedOffset").longValue(), 0);
        //totalSpoutLag = totalLatestTimeOffset-totalLatestCompletedOffset
        assertEquals(offsetMetric.get(SingleTopicKafkaSpoutConfiguration.TOPIC+"/totalSpoutLag").longValue(), 10);

        //Emit all messages and check that they are emitted. Ack the messages too
        for (int i = 0; i < messageCount; i++) {
            nextTuple_verifyEmitted_ack_resetCollector(i);
        }

        commitAndVerifyAllMessagesCommitted(messageCount);

        offsetMetric  = (Map<String, Long>) spout.getKafkaOffsetMetric().getValueAndReset();
        assertEquals(offsetMetric.get(SingleTopicKafkaSpoutConfiguration.TOPIC+"/totalEarliestTimeOffset").longValue(), 0);
        assertEquals(offsetMetric.get(SingleTopicKafkaSpoutConfiguration.TOPIC+"/totalLatestTimeOffset").longValue(), 10);
        //latest offset
        assertEquals(offsetMetric.get(SingleTopicKafkaSpoutConfiguration.TOPIC+"/totalLatestEmittedOffset").longValue(), 9);
        // offset where processing will resume upon spout restart
        assertEquals(offsetMetric.get(SingleTopicKafkaSpoutConfiguration.TOPIC+"/totalLatestCompletedOffset").longValue(), 10);
        assertEquals(offsetMetric.get(SingleTopicKafkaSpoutConfiguration.TOPIC+"/totalSpoutLag").longValue(), 0);
    }

    @Test
    public void testOffsetMetricsReturnsNullWhenRetriableExceptionThrown() throws Exception {
        final int messageCount = 10;
        prepareSpout(messageCount);

        // Ensure a timeout exception results in the return value being null
        when(getKafkaConsumer().beginningOffsets(anyCollection())).thenThrow(TimeoutException.class);

        Map<String, Long> offsetMetric  = (Map<String, Long>) spout.getKafkaOffsetMetric().getValueAndReset();
        assertNull(offsetMetric);
    }
}
