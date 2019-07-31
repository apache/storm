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
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.NullRecordTranslator;
import org.apache.storm.kafka.spout.config.builder.SingleTopicKafkaSpoutConfiguration;
import org.apache.storm.kafka.spout.internal.CommitMetadataManager;
import org.apache.storm.kafka.spout.subscription.ManualPartitioner;
import org.apache.storm.kafka.spout.subscription.TopicFilter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Time.SimulatedTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KafkaSpoutMessagingGuaranteeTest {

    @Captor
    private ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> commitCapture;

    private final TopologyContext contextMock = mock(TopologyContext.class);
    private final SpoutOutputCollector collectorMock = mock(SpoutOutputCollector.class);
    private final Map<String, Object> conf = new HashMap<>();
    private final TopicPartition partition = new TopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, 1);
    private KafkaConsumer<String, String> consumerMock;

    @Before
    public void setUp() {
        consumerMock = mock(KafkaConsumer.class);
    }

    @Test
    public void testAtMostOnceModeCommitsBeforeEmit() throws Exception {
        //At-most-once mode must commit tuples before they are emitted to the topology to ensure that a spout crash won't cause replays.
        KafkaSpoutConfig<String, String> spoutConfig = createKafkaSpoutConfigBuilder(mock(TopicFilter.class), mock(ManualPartitioner.class), -1)
            .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_MOST_ONCE)
            .build();
        KafkaSpout<String, String> spout = SpoutWithMockedConsumerSetupHelper.setupSpout(spoutConfig, conf, contextMock, collectorMock, consumerMock, partition);

        when(consumerMock.poll(anyLong())).thenReturn(new ConsumerRecords<>(Collections.singletonMap(partition,
            SpoutWithMockedConsumerSetupHelper.createRecords(partition, 0, 1))));

        spout.nextTuple();

        //The spout should have emitted the tuple, and must have committed it before emit
        InOrder inOrder = inOrder(consumerMock, collectorMock);
        inOrder.verify(consumerMock).poll(anyLong());
        inOrder.verify(consumerMock).commitSync(commitCapture.capture());
        inOrder.verify(collectorMock).emit(eq(SingleTopicKafkaSpoutConfiguration.STREAM), anyList());

        CommitMetadataManager metadataManager = new CommitMetadataManager(contextMock, KafkaSpoutConfig.ProcessingGuarantee.AT_MOST_ONCE);
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = commitCapture.getValue();
        assertThat(committedOffsets.get(partition).offset(), is(0L));
        assertThat(committedOffsets.get(partition).metadata(), is(metadataManager.getCommitMetadata()));
    }

    private void doTestModeDisregardsMaxUncommittedOffsets(KafkaSpoutConfig<String, String> spoutConfig) {
        KafkaSpout<String, String> spout = SpoutWithMockedConsumerSetupHelper.setupSpout(spoutConfig, conf, contextMock, collectorMock, consumerMock, partition);

        when(consumerMock.poll(anyLong()))
            .thenReturn(new ConsumerRecords<>(Collections.singletonMap(partition,
                SpoutWithMockedConsumerSetupHelper.createRecords(partition, 0, spoutConfig.getMaxUncommittedOffsets()))))
            .thenReturn(new ConsumerRecords<>(Collections.singletonMap(partition,
                SpoutWithMockedConsumerSetupHelper.createRecords(partition, spoutConfig.getMaxUncommittedOffsets() - 1, spoutConfig.getMaxUncommittedOffsets()))));

        for (int i = 0; i < spoutConfig.getMaxUncommittedOffsets() * 2; i++) {
            spout.nextTuple();
        }

        verify(consumerMock, times(2)).poll(anyLong());
        verify(collectorMock, times(spoutConfig.getMaxUncommittedOffsets() * 2)).emit(eq(SingleTopicKafkaSpoutConfiguration.STREAM), anyList());
    }

    @Test
    public void testAtMostOnceModeDisregardsMaxUncommittedOffsets() throws Exception {
        //The maxUncommittedOffsets limit should not be enforced, since it is only meaningful in at-least-once mode
        KafkaSpoutConfig<String, String> spoutConfig = createKafkaSpoutConfigBuilder(mock(TopicFilter.class), mock(ManualPartitioner.class), -1)
            .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_MOST_ONCE)
            .build();
        doTestModeDisregardsMaxUncommittedOffsets(spoutConfig);
    }

    @Test
    public void testNoGuaranteeModeDisregardsMaxUncommittedOffsets() throws Exception {
        //The maxUncommittedOffsets limit should not be enforced, since it is only meaningful in at-least-once mode
        KafkaSpoutConfig<String, String> spoutConfig = createKafkaSpoutConfigBuilder(mock(TopicFilter.class), mock(ManualPartitioner.class), -1)
            .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.NO_GUARANTEE)
            .build();
        doTestModeDisregardsMaxUncommittedOffsets(spoutConfig);
    }

    private void doTestModeCannotReplayTuples(KafkaSpoutConfig<String, String> spoutConfig) {
        KafkaSpout<String, String> spout = SpoutWithMockedConsumerSetupHelper.setupSpout(spoutConfig, conf, contextMock, collectorMock, consumerMock, partition);

        when(consumerMock.poll(anyLong())).thenReturn(new ConsumerRecords<>(Collections.singletonMap(partition,
            SpoutWithMockedConsumerSetupHelper.createRecords(partition, 0, 1))));

        spout.nextTuple();

        ArgumentCaptor<KafkaSpoutMessageId> msgIdCaptor = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
        verify(collectorMock).emit(eq(SingleTopicKafkaSpoutConfiguration.STREAM), anyList(), msgIdCaptor.capture());
        assertThat("Should have captured a message id", msgIdCaptor.getValue(), not(nullValue()));

        spout.fail(msgIdCaptor.getValue());

        reset(consumerMock);

        spout.nextTuple();

        //The consumer should not be seeking to retry the failed tuple, it should just be continuing from the current position
        verify(consumerMock, never()).seek(eq(partition), anyLong());
    }

    @Test
    public void testAtMostOnceModeCannotReplayTuples() throws Exception {
        //When tuple tracking is enabled, the spout must not replay tuples in at-most-once mode
        KafkaSpoutConfig<String, String> spoutConfig = createKafkaSpoutConfigBuilder(mock(TopicFilter.class), mock(ManualPartitioner.class), -1)
            .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_MOST_ONCE)
            .setTupleTrackingEnforced(true)
            .build();
        doTestModeCannotReplayTuples(spoutConfig);
    }

    @Test
    public void testNoGuaranteeModeCannotReplayTuples() throws Exception {
        //When tuple tracking is enabled, the spout must not replay tuples in no guarantee mode
        KafkaSpoutConfig<String, String> spoutConfig = createKafkaSpoutConfigBuilder(mock(TopicFilter.class), mock(ManualPartitioner.class), -1)
            .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.NO_GUARANTEE)
            .setTupleTrackingEnforced(true)
            .build();
        doTestModeCannotReplayTuples(spoutConfig);
    }

    @Test
    public void testAtMostOnceModeDoesNotCommitAckedTuples() throws Exception {
        //When tuple tracking is enabled, the spout must not commit acked tuples in at-most-once mode because they were committed before being emitted
        KafkaSpoutConfig<String, String> spoutConfig = createKafkaSpoutConfigBuilder(mock(TopicFilter.class), mock(ManualPartitioner.class), -1)
            .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_MOST_ONCE)
            .setTupleTrackingEnforced(true)
            .build();
        try (SimulatedTime time = new SimulatedTime()) {
            KafkaSpout<String, String> spout = SpoutWithMockedConsumerSetupHelper.setupSpout(spoutConfig, conf, contextMock, collectorMock, consumerMock, partition);

            when(consumerMock.poll(anyLong())).thenReturn(new ConsumerRecords<>(Collections.singletonMap(partition,
                SpoutWithMockedConsumerSetupHelper.createRecords(partition, 0, 1))));

            spout.nextTuple();
            clearInvocations(consumerMock);

            ArgumentCaptor<KafkaSpoutMessageId> msgIdCaptor = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
            verify(collectorMock).emit(eq(SingleTopicKafkaSpoutConfiguration.STREAM), anyList(), msgIdCaptor.capture());
            assertThat("Should have captured a message id", msgIdCaptor.getValue(), not(nullValue()));

            spout.ack(msgIdCaptor.getValue());

            Time.advanceTime(KafkaSpout.TIMER_DELAY_MS + spoutConfig.getOffsetsCommitPeriodMs());

            when(consumerMock.poll(anyLong())).thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

            spout.nextTuple();

            verify(consumerMock, never()).commitSync(argThat((Map<TopicPartition, OffsetAndMetadata> arg) -> {
                return !arg.containsKey(partition);
            }));
        }
    }

    @Test
    public void testNoGuaranteeModeCommitsPolledTuples() throws Exception {
        //When using the no guarantee mode, the spout must commit tuples periodically, regardless of whether they've been acked
        KafkaSpoutConfig<String, String> spoutConfig = createKafkaSpoutConfigBuilder(mock(TopicFilter.class), mock(ManualPartitioner.class), -1)
            .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.NO_GUARANTEE)
            .setTupleTrackingEnforced(true)
            .build();

        try (SimulatedTime time = new SimulatedTime()) {
            KafkaSpout<String, String> spout = SpoutWithMockedConsumerSetupHelper.setupSpout(spoutConfig, conf, contextMock, collectorMock, consumerMock, partition);

            when(consumerMock.poll(anyLong())).thenReturn(new ConsumerRecords<>(Collections.singletonMap(partition,
                SpoutWithMockedConsumerSetupHelper.createRecords(partition, 0, 1))));

            spout.nextTuple();

            when(consumerMock.position(partition)).thenReturn(1L);

            ArgumentCaptor<KafkaSpoutMessageId> msgIdCaptor = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
            verify(collectorMock).emit(eq(SingleTopicKafkaSpoutConfiguration.STREAM), anyList(), msgIdCaptor.capture());
            assertThat("Should have captured a message id", msgIdCaptor.getValue(), not(nullValue()));

            Time.advanceTime(KafkaSpout.TIMER_DELAY_MS + spoutConfig.getOffsetsCommitPeriodMs());

            spout.nextTuple();

            verify(consumerMock).commitAsync(commitCapture.capture(), isNull());

            CommitMetadataManager metadataManager = new CommitMetadataManager(contextMock, KafkaSpoutConfig.ProcessingGuarantee.NO_GUARANTEE);
            Map<TopicPartition, OffsetAndMetadata> committedOffsets = commitCapture.getValue();
            assertThat(committedOffsets.get(partition).offset(), is(1L));
            assertThat(committedOffsets.get(partition).metadata(), is(metadataManager.getCommitMetadata()));
        }
    }

    private void doFilterNullTupleTest(KafkaSpoutConfig.ProcessingGuarantee processingGuaranteee) {
        //STORM-3059
        KafkaSpoutConfig<String, String> spoutConfig = createKafkaSpoutConfigBuilder(mock(TopicFilter.class), mock(ManualPartitioner.class), -1)
            .setProcessingGuarantee(processingGuaranteee)
            .setTupleTrackingEnforced(true)
            .setRecordTranslator(new NullRecordTranslator<>())
            .build();
        
        KafkaSpout<String, String> spout = SpoutWithMockedConsumerSetupHelper.setupSpout(spoutConfig, conf, contextMock, collectorMock, consumerMock, partition);

        when(consumerMock.poll(anyLong())).thenReturn(new ConsumerRecords<>(Collections.singletonMap(partition,
            SpoutWithMockedConsumerSetupHelper.createRecords(partition, 0, 1))));

        spout.nextTuple();
        
        verify(collectorMock, never()).emit(any(), any(), any());
    }
    
    @Test
    public void testAtMostOnceModeCanFilterNullTuples() {
        doFilterNullTupleTest(KafkaSpoutConfig.ProcessingGuarantee.AT_MOST_ONCE);
    }
    
    @Test
    public void testNoGuaranteeModeCanFilterNullTuples() {
        doFilterNullTupleTest(KafkaSpoutConfig.ProcessingGuarantee.NO_GUARANTEE);
    }

}
