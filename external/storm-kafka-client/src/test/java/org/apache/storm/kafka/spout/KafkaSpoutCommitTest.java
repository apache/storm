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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.config.builder.SingleTopicKafkaSpoutConfiguration;
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

import static org.apache.storm.kafka.spout.config.builder.SingleTopicKafkaSpoutConfiguration.createKafkaSpoutConfigBuilder;
import static org.mockito.ArgumentMatchers.anyList;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaSpoutCommitTest {

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
        spoutConfig = createKafkaSpoutConfigBuilder(-1)
            .setOffsetCommitPeriodMs(offsetCommitPeriodMs)
            .build();
        consumerMock = mock(KafkaConsumer.class);
    }

    @Test
    public void testCommitSuccessWithOffsetVoids() {
        //Verify that the commit logic can handle offset voids
        try (SimulatedTime simulatedTime = new SimulatedTime()) {
            Set<TopicPartition> assignedPartitions = Collections.singleton(partition);
            KafkaSpout<String, String> spout = SpoutWithMockedConsumerSetupHelper.setupSpout(spoutConfig, conf, contextMock, collectorMock, consumerMock, assignedPartitions);
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

}