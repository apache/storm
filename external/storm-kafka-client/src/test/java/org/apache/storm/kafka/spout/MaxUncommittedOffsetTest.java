/*
 * Copyright 2017 The Apache Software Foundation.
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
package org.apache.storm.kafka.spout;

import static org.apache.storm.kafka.spout.config.builder.SingleTopicKafkaSpoutConfiguration.createKafkaSpoutConfigBuilder;
import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.kafka.KafkaUnitExtension;
import org.apache.storm.kafka.spout.config.builder.SingleTopicKafkaSpoutConfiguration;
import org.apache.storm.kafka.spout.internal.ConsumerFactoryDefault;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;


@ExtendWith(MockitoExtension.class)
public class MaxUncommittedOffsetTest {

    @RegisterExtension
    public KafkaUnitExtension kafkaUnitExtension = new KafkaUnitExtension();

    @Mock
    private TopologyContext topologyContext;
    private final Map<String, Object> conf = new HashMap<>();
    @Mock
    private SpoutOutputCollector collector;
    private final long commitOffsetPeriodMs = 2_000;
    private final int numMessages = 100;
    private final int maxUncommittedOffsets = 10;
    private final int maxPollRecords = 5;
    private final int initialRetryDelaySecs = 60;
    private final KafkaSpoutConfig<String, String> spoutConfig = createKafkaSpoutConfigBuilder(kafkaUnitExtension.getKafkaUnit().getKafkaPort())
        .setOffsetCommitPeriodMs(commitOffsetPeriodMs)
        .setProp(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords)
        .setMaxUncommittedOffsets(maxUncommittedOffsets)
        .setRetry(new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(initialRetryDelaySecs), KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(0),
            1, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(initialRetryDelaySecs))) //Retry once after a minute
        .build();
    private KafkaSpout<String, String> spout;

    @BeforeEach
    public void setUp() {
        //This is because the tests are checking that a hard cap of maxUncommittedOffsets + maxPollRecords - 1 uncommitted offsets exists
        //so Kafka must be able to return more messages than that in order for the tests to be meaningful
        assertThat("Current tests require numMessages >= 2*maxUncommittedOffsets", numMessages, greaterThanOrEqualTo(maxUncommittedOffsets * 2));
        //This is to verify that a low maxPollRecords does not interfere with reemitting failed tuples
        //The spout must be able to reemit all retriable tuples, even if the maxPollRecords is set to a low value compared to maxUncommittedOffsets.
        assertThat("Current tests require maxPollRecords < maxUncommittedOffsets", maxPollRecords, lessThanOrEqualTo(maxUncommittedOffsets));
        spout = new KafkaSpout<>(spoutConfig);
        new ConsumerFactoryDefault<String, String>().createConsumer(spoutConfig.getKafkaProps());
    }

    private void prepareSpout(int msgCount) throws Exception {
        SingleTopicKafkaUnitSetupHelper.populateTopicData(kafkaUnitExtension.getKafkaUnit(), SingleTopicKafkaSpoutConfiguration.TOPIC, msgCount);
        SingleTopicKafkaUnitSetupHelper.initializeSpout(spout, conf, topologyContext, collector);
    }

    private ArgumentCaptor<KafkaSpoutMessageId> emitMaxUncommittedOffsetsMessagesAndCheckNoMoreAreEmitted(int messageCount) throws Exception {
        assertThat("The message count is less than maxUncommittedOffsets. This test is not meaningful with this configuration.", messageCount, greaterThanOrEqualTo(maxUncommittedOffsets));
        //The spout must respect maxUncommittedOffsets when requesting/emitting tuples
        prepareSpout(messageCount);

        //Try to emit all messages. Ensure only maxUncommittedOffsets are emitted
        ArgumentCaptor<KafkaSpoutMessageId> messageIds = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
        for (int i = 0; i < messageCount; i++) {
            spout.nextTuple();
        }
        verify(collector, times(maxUncommittedOffsets)).emit(
            any(),
            any(),
            messageIds.capture());
        return messageIds;
    }

    @Test
    public void testNextTupleCanEmitMoreMessagesWhenDroppingBelowMaxUncommittedOffsetsDueToCommit() throws Exception {
        //The spout must respect maxUncommittedOffsets after committing a set of records
        try (Time.SimulatedTime simulatedTime = new Time.SimulatedTime()) {
            //First check that maxUncommittedOffsets is respected when emitting from scratch
            ArgumentCaptor<KafkaSpoutMessageId> messageIds = emitMaxUncommittedOffsetsMessagesAndCheckNoMoreAreEmitted(numMessages);
            reset(collector);

            //Ack all emitted messages and commit them
            for (KafkaSpoutMessageId messageId : messageIds.getAllValues()) {
                spout.ack(messageId);
            }
            Time.advanceTime(commitOffsetPeriodMs + KafkaSpout.TIMER_DELAY_MS);

            spout.nextTuple();

            //Now check that the spout will emit another maxUncommittedOffsets messages
            for (int i = 0; i < numMessages; i++) {
                spout.nextTuple();
            }
            verify(collector, times(maxUncommittedOffsets)).emit(
                any(),
                any(),
                any());
        }
    }

    @Test
    public void testNextTupleWillRespectMaxUncommittedOffsetsWhenThereAreAckedUncommittedTuples() throws Exception {
        //The spout must respect maxUncommittedOffsets even if some tuples have been acked but not committed
        try (Time.SimulatedTime simulatedTime = new Time.SimulatedTime()) {
            //First check that maxUncommittedOffsets is respected when emitting from scratch
            ArgumentCaptor<KafkaSpoutMessageId> messageIds = emitMaxUncommittedOffsetsMessagesAndCheckNoMoreAreEmitted(numMessages);
            reset(collector);

            //Fail all emitted messages except the last one. Try to commit.
            List<KafkaSpoutMessageId> messageIdList = messageIds.getAllValues();
            for (int i = 0; i < messageIdList.size() - 1; i++) {
                spout.fail(messageIdList.get(i));
            }
            spout.ack(messageIdList.get(messageIdList.size() - 1));
            Time.advanceTime(commitOffsetPeriodMs + KafkaSpout.TIMER_DELAY_MS);
            spout.nextTuple();

            //Now check that the spout will not emit anything else since nothing has been committed
            for (int i = 0; i < numMessages; i++) {
                spout.nextTuple();
            }

            verify(collector, times(0)).emit(
                any(),
                any(),
                any());
        }
    }

    private void failAllExceptTheFirstMessageThenCommit(ArgumentCaptor<KafkaSpoutMessageId> messageIds) {
        //Fail all emitted messages except the first. Commit the first.
        List<KafkaSpoutMessageId> messageIdList = messageIds.getAllValues();
        for (int i = 1; i < messageIdList.size(); i++) {
            spout.fail(messageIdList.get(i));
        }
        spout.ack(messageIdList.get(0));
        Time.advanceTime(commitOffsetPeriodMs + KafkaSpout.TIMER_DELAY_MS);
        spout.nextTuple();
    }

    @Test
    public void testNextTupleWillNotEmitMoreThanMaxUncommittedOffsetsPlusMaxPollRecordsMessages() throws Exception {
        /*
        For each partition the spout is allowed to retry all tuples between the committed offset, and maxUncommittedOffsets ahead.
        It is not allowed to retry tuples past that limit.
        This makes the actual limit per partition maxUncommittedOffsets + maxPollRecords - 1,
        reached if the tuple at the maxUncommittedOffsets limit is the earliest retriable tuple,
        or if the spout is 1 tuple below the limit, and receives a full maxPollRecords tuples in the poll.
         */

        try (Time.SimulatedTime simulatedTime = new Time.SimulatedTime()) {
            //First check that maxUncommittedOffsets is respected when emitting from scratch
            ArgumentCaptor<KafkaSpoutMessageId> messageIds = emitMaxUncommittedOffsetsMessagesAndCheckNoMoreAreEmitted(numMessages);
            reset(collector);

            //Fail only the last tuple
            List<KafkaSpoutMessageId> messageIdList = messageIds.getAllValues();
            KafkaSpoutMessageId failedMessageId = messageIdList.get(messageIdList.size() - 1);
            spout.fail(failedMessageId);

            //Offset 0 to maxUncommittedOffsets - 2 are pending, maxUncommittedOffsets - 1 is failed but not retriable
            //The spout should not emit any more tuples.
            spout.nextTuple();
            verify(collector, never()).emit(
                any(),
                any(),
                any());

            //Allow the failed record to retry
            Time.advanceTimeSecs(initialRetryDelaySecs);
            for (int i = 0; i < maxPollRecords; i++) {
                spout.nextTuple();
            }
            ArgumentCaptor<KafkaSpoutMessageId> secondRunMessageIds = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
            verify(collector, times(maxPollRecords)).emit(
                any(),
                any(),
                secondRunMessageIds.capture());
            reset(collector);
            assertThat(secondRunMessageIds.getAllValues().get(0), is(failedMessageId));
            
            //There should now be maxUncommittedOffsets + maxPollRecords emitted in all.
            //Fail the last emitted tuple and verify that the spout won't retry it because it's above the emit limit.
            spout.fail(secondRunMessageIds.getAllValues().get(secondRunMessageIds.getAllValues().size() - 1));
            Time.advanceTimeSecs(initialRetryDelaySecs);
            spout.nextTuple();
            verify(collector, never()).emit(any(), any(), any());
        }
    }

    @Test
    public void testNextTupleWillAllowRetryForTuplesBelowEmitLimit() throws Exception {
        /*
        For each partition the spout is allowed to retry all tuples between the committed offset, and maxUncommittedOffsets ahead.
        It must retry tuples within that limit, even if more tuples were emitted.
         */
        try (Time.SimulatedTime simulatedTime = new Time.SimulatedTime()) {
            //First check that maxUncommittedOffsets is respected when emitting from scratch
            ArgumentCaptor<KafkaSpoutMessageId> messageIds = emitMaxUncommittedOffsetsMessagesAndCheckNoMoreAreEmitted(numMessages);
            reset(collector);

            failAllExceptTheFirstMessageThenCommit(messageIds);

            //Offset 0 is committed, 1 to maxUncommittedOffsets - 1 are failed but not retriable
            //The spout should now emit another maxPollRecords messages
            //This is allowed because the committed message brings the numUncommittedOffsets below the cap
            for (int i = 0; i < maxUncommittedOffsets; i++) {
                spout.nextTuple();
            }

            ArgumentCaptor<KafkaSpoutMessageId> secondRunMessageIds = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
            verify(collector, times(maxPollRecords)).emit(
                any(),
                any(),
                secondRunMessageIds.capture());
            reset(collector);

            List<Long> firstRunOffsets = messageIds.getAllValues().stream()
                .map(messageId -> messageId.offset())
                .collect(Collectors.toList());
            List<Long> secondRunOffsets = secondRunMessageIds.getAllValues().stream()
                .map(messageId -> messageId.offset())
                .collect(Collectors.toList());
            assertThat("Expected the newly emitted messages to have no overlap with the first batch", secondRunOffsets.removeAll(firstRunOffsets), is(false));

            //Offset 0 is committed, 1 to maxUncommittedOffsets-1 are failed, maxUncommittedOffsets to maxUncommittedOffsets + maxPollRecords-1 are emitted
            //Fail the last tuples so only offset 0 is not failed.
            //Advance time so the failed tuples become ready for retry, and check that the spout will emit retriable tuples
            //for all the failed tuples that are within maxUncommittedOffsets tuples of the committed offset
            //This means 1 to maxUncommitteddOffsets, but not maxUncommittedOffsets+1...maxUncommittedOffsets+maxPollRecords-1
            for (KafkaSpoutMessageId msgId : secondRunMessageIds.getAllValues()) {
                spout.fail(msgId);
            }
            Time.advanceTimeSecs(initialRetryDelaySecs);
            for (int i = 0; i < numMessages; i++) {
                spout.nextTuple();
            }
            ArgumentCaptor<KafkaSpoutMessageId> thirdRunMessageIds = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
            verify(collector, times(maxUncommittedOffsets)).emit(
                anyString(),
                anyList(),
                thirdRunMessageIds.capture());
            reset(collector);

            List<Long> thirdRunOffsets = thirdRunMessageIds.getAllValues().stream()
                .map(msgId -> msgId.offset())
                .collect(Collectors.toList());
            assertThat("Expected the emitted messages to be retries of the failed tuples from the first batch, plus the first failed tuple from the second batch", thirdRunOffsets, everyItem(either(isIn(firstRunOffsets)).or(is(secondRunMessageIds.getAllValues().get(0).offset()))));
        }
    }
}
