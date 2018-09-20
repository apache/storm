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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class KafkaSpoutRetryLimitTest {
    
    @Rule
    public MockitoRule mockito = MockitoJUnit.rule();

    private final long offsetCommitPeriodMs = 2_000;
    private final TopologyContext contextMock = mock(TopologyContext.class);
    private final SpoutOutputCollector collectorMock = mock(SpoutOutputCollector.class);
    private final Map<String, Object> conf = new HashMap<>();
    private final TopicPartition partition = new TopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, 1);
    private KafkaConsumer<String, String> consumerMock;
    private KafkaSpoutConfig<String, String> spoutConfig;
    
    public static final KafkaSpoutRetryService ZERO_RETRIES_RETRY_SERVICE =
        new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(0), KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(0),
            0, KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(0));
    
    @Captor
    private ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> commitCapture;
    
    @Before
    public void setUp() {
        spoutConfig = createKafkaSpoutConfigBuilder(mock(TopicFilter.class), mock(ManualPartitioner.class), -1)
            .setOffsetCommitPeriodMs(offsetCommitPeriodMs)
            .setRetry(ZERO_RETRIES_RETRY_SERVICE)
            .build();
        consumerMock = mock(KafkaConsumer.class);
    }
    
    @Test
    public void testFailingTupleCompletesAckAfterRetryLimitIsMet() {
        //Spout should ack failed messages after they hit the retry limit
        try (SimulatedTime simulatedTime = new SimulatedTime()) {
            KafkaSpout<String, String> spout = SpoutWithMockedConsumerSetupHelper.setupSpout(spoutConfig, conf, contextMock, collectorMock, consumerMock, partition);
            Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
            int lastOffset = 3;
            int numRecords = lastOffset + 1;
            records.put(partition, SpoutWithMockedConsumerSetupHelper.createRecords(partition, 0, numRecords));
            
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

            // Advance time and then trigger call to kafka consumer commit
            Time.advanceTime(KafkaSpout.TIMER_DELAY_MS + offsetCommitPeriodMs);
            spout.nextTuple();
            
            InOrder inOrder = inOrder(consumerMock);
            inOrder.verify(consumerMock).commitSync(commitCapture.capture());
            inOrder.verify(consumerMock).poll(anyLong());

            //verify that offset 4 was committed for the given TopicPartition, since processing should resume at 4.
            assertTrue(commitCapture.getValue().containsKey(partition));
            assertEquals(lastOffset + 1, ((OffsetAndMetadata) (commitCapture.getValue().get(partition))).offset());
        }
    }
    
}
