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

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.builders.SingleTopicKafkaSpoutConfiguration;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;

import org.apache.storm.utils.Time;
import org.apache.storm.utils.Time.SimulatedTime;
import org.junit.Before;
import org.mockito.InOrder;

import static org.apache.storm.kafka.spout.builders.SingleTopicKafkaSpoutConfiguration.createKafkaSpoutConfigBuilder;
import static org.mockito.Matchers.eq;

public class KafkaSpoutEmitTest {

    private final long offsetCommitPeriodMs = 2_000;
    private final TopologyContext contextMock = mock(TopologyContext.class);
    private final SpoutOutputCollector collectorMock = mock(SpoutOutputCollector.class);
    private final Map<String, Object> conf = new HashMap<>();
    private final TopicPartition partition = new TopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, 1);
    private KafkaConsumer<String, String> consumerMock;
    private KafkaSpoutConfig<String, String> spoutConfig;

    @Before
    public void setUp() {
        spoutConfig = createKafkaSpoutConfigBuilder(mock(Subscription.class), -1)
            .setOffsetCommitPeriodMs(offsetCommitPeriodMs)
            .build();
        consumerMock = mock(KafkaConsumer.class);
    }

    @Test
    public void testNextTupleEmitsAtMostOneTuple() {
        //The spout should emit at most one message per call to nextTuple
        //This is necessary for Storm to be able to throttle the spout according to maxSpoutPending
        KafkaSpout<String, String> spout = SpoutWithMockedConsumerSetupHelper.setupSpout(spoutConfig, conf, contextMock, collectorMock, consumerMock, partition);
        Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
        records.put(partition, SpoutWithMockedConsumerSetupHelper.<String, String>createRecords(partition, 0, 10));

        when(consumerMock.poll(anyLong()))
            .thenReturn(new ConsumerRecords<>(records));

        spout.nextTuple();

        verify(collectorMock, times(1)).emit(anyString(), anyList(), anyObject());
    }

    @Test
    public void testNextTupleEmitsFailedMessagesEvenWhenMaxUncommittedOffsetsIsExceeded() throws IOException {
        //The spout must reemit failed messages waiting for retry even if it is not allowed to poll for new messages due to maxUncommittedOffsets being exceeded

        //Emit maxUncommittedOffsets messages, and fail all of them. Then ensure that the spout will retry them when the retry backoff has passed
        try (SimulatedTime simulatedTime = new SimulatedTime()) {
            KafkaSpout<String, String> spout = SpoutWithMockedConsumerSetupHelper.setupSpout(spoutConfig, conf, contextMock, collectorMock, consumerMock, partition);
            Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
            int numRecords = spoutConfig.getMaxUncommittedOffsets();
            //This is cheating a bit since maxPollRecords would normally spread this across multiple polls
            records.put(partition, SpoutWithMockedConsumerSetupHelper.<String, String>createRecords(partition, 0, numRecords));

            when(consumerMock.poll(anyLong()))
                .thenReturn(new ConsumerRecords<>(records));

            for (int i = 0; i < numRecords; i++) {
                spout.nextTuple();
            }

            ArgumentCaptor<KafkaSpoutMessageId> messageIds = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
            verify(collectorMock, times(numRecords)).emit(anyString(), anyList(), messageIds.capture());

            for (KafkaSpoutMessageId messageId : messageIds.getAllValues()) {
                spout.fail(messageId);
            }

            reset(collectorMock);

            Time.advanceTime(50);
            //No backoff for test retry service, just check that messages will retry immediately
            for (int i = 0; i < numRecords; i++) {
                spout.nextTuple();
            }

            ArgumentCaptor<KafkaSpoutMessageId> retryMessageIds = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
            verify(collectorMock, times(numRecords)).emit(anyString(), anyList(), retryMessageIds.capture());

            //Verify that the poll started at the earliest retriable tuple offset
            List<Long> failedOffsets = new ArrayList<>();
            for (KafkaSpoutMessageId msgId : messageIds.getAllValues()) {
                failedOffsets.add(msgId.offset());
            }
            InOrder inOrder = inOrder(consumerMock);
            inOrder.verify(consumerMock).seek(partition, failedOffsets.get(0));
            inOrder.verify(consumerMock).poll(anyLong());
        }
    }

    @Test
    public void testSpoutWillSkipPartitionsAtTheMaxUncommittedOffsetsLimit() {
        //This verifies that partitions can't prevent each other from retrying tuples due to the maxUncommittedOffsets limit.
        try (SimulatedTime simulatedTime = new SimulatedTime()) {
            TopicPartition partitionTwo = new TopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, 2);
            KafkaSpout<String, String> spout = SpoutWithMockedConsumerSetupHelper.setupSpout(spoutConfig, conf, contextMock, collectorMock, consumerMock, partition, partitionTwo);
            Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
            //This is cheating a bit since maxPollRecords would normally spread this across multiple polls
            records.put(partition, SpoutWithMockedConsumerSetupHelper.<String, String>createRecords(partition, 0, spoutConfig.getMaxUncommittedOffsets()));
            records.put(partitionTwo, SpoutWithMockedConsumerSetupHelper.<String, String>createRecords(partitionTwo, 0, spoutConfig.getMaxUncommittedOffsets() + 1));
            int numMessages = spoutConfig.getMaxUncommittedOffsets()*2 + 1;

            when(consumerMock.poll(anyLong()))
                .thenReturn(new ConsumerRecords<>(records));

            for (int i = 0; i < numMessages; i++) {
                spout.nextTuple();
            }

            ArgumentCaptor<KafkaSpoutMessageId> messageIds = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
            verify(collectorMock, times(numMessages)).emit(anyString(), anyList(), messageIds.capture());
            
            //Now fail a tuple on partition one and verify that it is allowed to retry, because the failed tuple is below the maxUncommittedOffsets limit
            KafkaSpoutMessageId failedMessageIdPartitionOne = null;
            for (KafkaSpoutMessageId msgId : messageIds.getAllValues()) {
                if (msgId.partition() == partition.partition()) {
                    failedMessageIdPartitionOne = msgId;
                    break;
                }
            }
            
            spout.fail(failedMessageIdPartitionOne);

            //Also fail the last tuple from partition two. Since the failed tuple is beyond the maxUncommittedOffsets limit, it should not be retried until earlier messages are acked.
            KafkaSpoutMessageId failedMessageIdPartitionTwo = null;
            for (KafkaSpoutMessageId msgId: messageIds.getAllValues()) {
                if (msgId.partition() == partitionTwo.partition()) {
                    if (failedMessageIdPartitionTwo != null) {
                        if (msgId.offset() >= failedMessageIdPartitionTwo.offset()) {
                            failedMessageIdPartitionTwo = msgId;
                        }
                    } else {
                        failedMessageIdPartitionTwo = msgId;
                    }
                }
            }

            spout.fail(failedMessageIdPartitionTwo);
            
            reset(collectorMock);
            
            Time.advanceTime(50);
            when(consumerMock.poll(anyLong()))
                .thenReturn(new ConsumerRecords<>(Collections.singletonMap(partition, SpoutWithMockedConsumerSetupHelper.<String, String>createRecords(partition, failedMessageIdPartitionOne.offset(), 1))));
            
            spout.nextTuple();
            
            verify(collectorMock, times(1)).emit(anyString(), anyList(), anyObject());
            
            InOrder inOrder = inOrder(consumerMock);
            inOrder.verify(consumerMock).seek(partition, failedMessageIdPartitionOne.offset());
            //Should not seek on the paused partition
            inOrder.verify(consumerMock, never()).seek(eq(partitionTwo), anyLong());
            inOrder.verify(consumerMock).pause(Collections.singleton(partitionTwo));
            inOrder.verify(consumerMock).poll(anyLong());
            inOrder.verify(consumerMock).resume(Collections.singleton(partitionTwo));
            
            reset(collectorMock);
            
            //Now also check that no more tuples are polled for, since both partitions are at their limits
            spout.nextTuple();

            verify(collectorMock, never()).emit(anyString(), anyList(), anyObject());
        }
    }

}
