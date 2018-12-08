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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.config.builder.SingleTopicKafkaSpoutConfiguration;
import org.apache.storm.kafka.spout.subscription.ManualPartitioner;
import org.apache.storm.kafka.spout.subscription.TopicFilter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Time.SimulatedTime;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.MockitoAnnotations;

public class KafkaSpoutLogCompactionSupportTest {

    private final long offsetCommitPeriodMs = 2_000;
    private final TopologyContext contextMock = mock(TopologyContext.class);
    private final SpoutOutputCollector collectorMock = mock(SpoutOutputCollector.class);
    private final Map<String, Object> conf = new HashMap<>();
    private final TopicPartition partition = new TopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, 1);
    private KafkaConsumer<String, String> consumerMock;
    private KafkaSpoutConfig<String, String> spoutConfig;

    @Captor
    private ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> commitCapture;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        spoutConfig = createKafkaSpoutConfigBuilder(mock(TopicFilter.class), mock(ManualPartitioner.class), -1)
            .setOffsetCommitPeriodMs(offsetCommitPeriodMs)
            .build();
        consumerMock = mock(KafkaConsumer.class);
    }

    @Test
    public void testCommitSuccessWithOffsetVoids() {
        //Verify that the commit logic can handle offset voids due to log compaction
        try (SimulatedTime simulatedTime = new SimulatedTime()) {
            KafkaSpout<String, String> spout = SpoutWithMockedConsumerSetupHelper.setupSpout(spoutConfig, conf, contextMock, collectorMock, consumerMock, partition);
            Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
            List<ConsumerRecord<String, String>> recordsForPartition = new ArrayList<>();
            // Offsets emitted are 0,1,2,3,4,<void>,8,9
            recordsForPartition.addAll(SpoutWithMockedConsumerSetupHelper.createRecords(partition, 0, 5));
            recordsForPartition.addAll(SpoutWithMockedConsumerSetupHelper.createRecords(partition, 8, 2));
            records.put(partition, recordsForPartition);

            when(consumerMock.poll(anyLong()))
                    .thenReturn(new ConsumerRecords<>(records));

            for (int i = 0; i < recordsForPartition.size(); i++) {
                spout.nextTuple();
            }

            ArgumentCaptor<KafkaSpoutMessageId> messageIds = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
            verify(collectorMock, times(recordsForPartition.size())).emit(anyString(), anyList(), messageIds.capture());

            for (KafkaSpoutMessageId messageId : messageIds.getAllValues()) {
                spout.ack(messageId);
            }

            // Advance time and then trigger first call to kafka consumer commit; the commit must progress to offset 9
            Time.advanceTime(KafkaSpout.TIMER_DELAY_MS + offsetCommitPeriodMs);
            when(consumerMock.poll(anyLong()))
                    .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));
            spout.nextTuple();

            InOrder inOrder = inOrder(consumerMock);
            inOrder.verify(consumerMock).commitSync(commitCapture.capture());
            inOrder.verify(consumerMock).poll(anyLong());

            //verify that Offset 10 was last committed offset, since this is the offset the spout should resume at
            Map<TopicPartition, OffsetAndMetadata> commits = commitCapture.getValue();
            assertTrue(commits.containsKey(partition));
            assertEquals(10, commits.get(partition).offset());
        }
    }
    
    @Test
    public void testWillSkipRetriableTuplesIfOffsetsAreCompactedAway() {
        /*
          Verify that failed offsets will only retry if the corresponding message exists. 
          When log compaction is enabled in Kafka it is possible that a tuple can fail, 
          and then be impossible to retry because the message in Kafka has been deleted.
          The spout needs to quietly ack such tuples to allow commits to progress past the deleted offset.
         */
        try (SimulatedTime simulatedTime = new SimulatedTime()) {
            TopicPartition partitionTwo = new TopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, 2);
            KafkaSpout<String, String> spout = SpoutWithMockedConsumerSetupHelper.setupSpout(spoutConfig, conf, contextMock, collectorMock, consumerMock, partition, partitionTwo);
            
            List<KafkaSpoutMessageId> firstPartitionMsgIds = SpoutWithMockedConsumerSetupHelper
                .pollAndEmit(spout, consumerMock, 3, collectorMock, partition, 0, 1, 2);
            reset(collectorMock);
            List<KafkaSpoutMessageId> secondPartitionMsgIds = SpoutWithMockedConsumerSetupHelper
                .pollAndEmit(spout, consumerMock, 3, collectorMock, partitionTwo, 0, 1, 2);
            reset(collectorMock);
            
            for(int i = 0; i < 3; i++) {
                spout.fail(firstPartitionMsgIds.get(i));
                spout.fail(secondPartitionMsgIds.get(i));
            }
            
            Time.advanceTime(50);
            
            //The failed tuples are ready for retry. Make it appear like 0 and 1 on the first partition were compacted away.
            //In this case the second partition acts as control to verify that we only skip past offsets that are no longer present.
            Map<TopicPartition, int[]> retryOffsets = new HashMap<>();
            retryOffsets.put(partition, new int[] {2});
            retryOffsets.put(partitionTwo, new int[] {0, 1, 2});
            int expectedEmits = 4; //2 on first partition, 0-2 on second partition
            List<KafkaSpoutMessageId> retryMessageIds = SpoutWithMockedConsumerSetupHelper.pollAndEmit(spout, consumerMock, expectedEmits, collectorMock, retryOffsets);
            
            Time.advanceTime(KafkaSpout.TIMER_DELAY_MS + offsetCommitPeriodMs);
            spout.nextTuple();
            
            verify(consumerMock).commitSync(commitCapture.capture());
            Map<TopicPartition, OffsetAndMetadata> committed = commitCapture.getValue();
            assertThat(committed.keySet(), is(Collections.singleton(partition)));
            assertThat("The first partition should have committed up to the first retriable tuple that is not missing", committed.get(partition).offset(), is(2L));
            
            for(KafkaSpoutMessageId msgId : retryMessageIds) {
                spout.ack(msgId);
            }
            
            //The spout should now commit all the offsets, since all offsets are either acked or were missing when retrying
            Time.advanceTime(KafkaSpout.TIMER_DELAY_MS + offsetCommitPeriodMs);
            spout.nextTuple();

            verify(consumerMock, times(2)).commitSync(commitCapture.capture());
            committed = commitCapture.getValue();
            assertThat(committed, hasKey(partition));
            assertThat(committed, hasKey(partitionTwo));
            assertThat(committed.get(partition).offset(), is(3L));
            assertThat(committed.get(partitionTwo).offset(), is(3L));
        }
    }
    
    @Test
    public void testWillSkipRetriableTuplesIfOffsetsAreCompactedAwayWithoutAckingPendingTuples() {
        //Demonstrate that the spout doesn't ack pending tuples when skipping compacted tuples. The pending tuples should be allowed to finish normally.
        try (SimulatedTime simulatedTime = new SimulatedTime()) {
            KafkaSpout<String, String> spout = SpoutWithMockedConsumerSetupHelper.setupSpout(spoutConfig, conf, contextMock, collectorMock, consumerMock, partition);
            
            List<KafkaSpoutMessageId> firstPartitionMsgIds = SpoutWithMockedConsumerSetupHelper
                .pollAndEmit(spout, consumerMock, 3, collectorMock, partition, 0, 1, 2);
            reset(collectorMock);
            
            spout.fail(firstPartitionMsgIds.get(0));            
            spout.fail(firstPartitionMsgIds.get(2));
            
            Time.advanceTime(50);
            
            //The failed tuples are ready for retry. Make it appear like 0 and 1 were compacted away.
            List<KafkaSpoutMessageId> retryMessageIds = SpoutWithMockedConsumerSetupHelper.pollAndEmit(spout, consumerMock, 1, collectorMock, partition, 2);
            for(KafkaSpoutMessageId msgId : retryMessageIds) {
                spout.ack(msgId);
            }
            
            Time.advanceTime(KafkaSpout.TIMER_DELAY_MS + offsetCommitPeriodMs);
            spout.nextTuple();
            
            verify(consumerMock).commitSync(commitCapture.capture());
            Map<TopicPartition, OffsetAndMetadata> committed = commitCapture.getValue();
            assertThat(committed.keySet(), is(Collections.singleton(partition)));
            assertThat("The first partition should have committed the missing offset, but no further since the next tuple is pending",
                committed.get(partition).offset(), is(1L));
            
            spout.ack(firstPartitionMsgIds.get(1));
            
            Time.advanceTime(KafkaSpout.TIMER_DELAY_MS + offsetCommitPeriodMs);
            spout.nextTuple();
            
            verify(consumerMock, times(2)).commitSync(commitCapture.capture());
            committed = commitCapture.getValue();
            assertThat(committed.keySet(), is(Collections.singleton(partition)));
            assertThat("The first partition should have committed all offsets", committed.get(partition).offset(), is(3L));
        }
    }
    
    @Test
    public void testCommitTupleAfterCompactionGap() {
        //If there is an acked tupled after a compaction gap, the spout should commit it immediately
        try (SimulatedTime simulatedTime = new SimulatedTime()) {
            KafkaSpout<String, String> spout = SpoutWithMockedConsumerSetupHelper.setupSpout(spoutConfig, conf, contextMock, collectorMock, consumerMock, partition);
            
            List<KafkaSpoutMessageId> firstMessage = SpoutWithMockedConsumerSetupHelper
                .pollAndEmit(spout, consumerMock, 1, collectorMock, partition, 0);
            reset(collectorMock);
            
            List<KafkaSpoutMessageId> messageAfterGap = SpoutWithMockedConsumerSetupHelper.pollAndEmit(spout, consumerMock, 1, collectorMock, partition, 2);
            reset(collectorMock);
            
            spout.ack(firstMessage.get(0));
            
            Time.advanceTime(KafkaSpout.TIMER_DELAY_MS + offsetCommitPeriodMs);
            spout.nextTuple();
            verify(consumerMock).commitSync(commitCapture.capture());
            Map<TopicPartition, OffsetAndMetadata> committed = commitCapture.getValue();
            assertThat(committed.keySet(), is(Collections.singleton(partition)));
            assertThat("The consumer should have committed the offset before the gap",
                committed.get(partition).offset(), is(1L));
            reset(consumerMock);
            
            spout.ack(messageAfterGap.get(0));
            
            Time.advanceTime(KafkaSpout.TIMER_DELAY_MS + offsetCommitPeriodMs);
            spout.nextTuple();
            
            verify(consumerMock).commitSync(commitCapture.capture());
            committed = commitCapture.getValue();
            assertThat(committed.keySet(), is(Collections.singleton(partition)));
            assertThat("The consumer should have committed the offset after the gap, since offset 1 wasn't emitted and both 0 and 2 are acked",
                committed.get(partition).offset(), is(3L));
        }
    }

}