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

import static org.apache.storm.kafka.spout.builders.SingleTopicKafkaSpoutConfiguration.getKafkaSpoutConfigBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.builders.SingleTopicKafkaSpoutConfiguration;
import org.apache.storm.kafka.spout.internal.KafkaConsumerFactory;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Time.SimulatedTime;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

public class KafkaSpoutRetryLimitTest {

    private final long offsetCommitPeriodMs = 2_000;
    private final TopologyContext contextMock = mock(TopologyContext.class);
    private final SpoutOutputCollector collectorMock = mock(SpoutOutputCollector.class);
    private final Map<String, Object> conf = new HashMap<>();
    private final TopicPartition partition = new TopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, 1);
    private KafkaConsumer<String, String> consumerMock;
    private KafkaSpout<String, String> spout;
    private KafkaSpoutConfig spoutConfig;

    public static final KafkaSpoutRetryService ZERO_RETRIES_RETRY_SERVICE =
            new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(0), KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(0),
                    0, KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(0));

    private void setupSpoutWithNoRetry(Set<TopicPartition> assignedPartitions) {
        spoutConfig = getKafkaSpoutConfigBuilder(-1)
                .setOffsetCommitPeriodMs(offsetCommitPeriodMs)
                .setRetry(ZERO_RETRIES_RETRY_SERVICE)
                .build();

        consumerMock = mock(KafkaConsumer.class);
        KafkaConsumerFactory<String, String> consumerFactory = new KafkaConsumerFactory<String, String>() {
            @Override
            public KafkaConsumer<String, String> createConsumer(KafkaSpoutConfig<String, String> kafkaSpoutConfig) {
                return consumerMock;
            }
        };

        spout = new KafkaSpout<>(spoutConfig, consumerFactory);

        spout.open(conf, contextMock, collectorMock);
        spout.activate();

        ArgumentCaptor<ConsumerRebalanceListener> rebalanceListenerCapture = ArgumentCaptor.forClass(ConsumerRebalanceListener.class);
        verify(consumerMock).subscribe(anyCollection(), rebalanceListenerCapture.capture());

        //Assign partitions to the spout
        ConsumerRebalanceListener consumerRebalanceListener = rebalanceListenerCapture.getValue();
        consumerRebalanceListener.onPartitionsAssigned(assignedPartitions);
    }

    @Test
    public void testFailingTupleCompletesAckAfterRetryLimitIsMet() {
        //Spout should ack failed messages after they hit the retry limit
        try (SimulatedTime simulatedTime = new SimulatedTime()) {
            setupSpoutWithNoRetry(Collections.singleton(partition));
            Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
            List<ConsumerRecord<String, String>> recordsForPartition = new ArrayList<>();
            int lastOffset = 3;
            for (int i = 0; i <= lastOffset; i++) {
                recordsForPartition.add(new ConsumerRecord(partition.topic(), partition.partition(), i, "key", "value"));
            }
            records.put(partition, recordsForPartition);

            when(consumerMock.poll(anyLong()))
                    .thenReturn(new ConsumerRecords(records));

            for (int i = 0; i < recordsForPartition.size(); i++) {
                spout.nextTuple();
            }

            ArgumentCaptor<KafkaSpoutMessageId> messageIds = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
            verify(collectorMock, times(recordsForPartition.size())).emit(anyString(), anyList(), messageIds.capture());

            for (KafkaSpoutMessageId messageId : messageIds.getAllValues()) {
                spout.fail(messageId);
            }

            // Advance time and then trigger call to kafka consumer commit
            Time.advanceTime(KafkaSpout.TIMER_DELAY_MS + offsetCommitPeriodMs);
            spout.nextTuple();

            ArgumentCaptor<Map> committedOffsets=ArgumentCaptor.forClass(Map.class);
            InOrder inOrder = inOrder(consumerMock);
            inOrder.verify(consumerMock).commitSync(committedOffsets.capture());
            inOrder.verify(consumerMock).poll(anyLong());

            //verify that Offset 3 was committed for the given TopicPartition
            assertTrue(committedOffsets.getValue().containsKey(partition));
            assertEquals(lastOffset, ((OffsetAndMetadata) (committedOffsets.getValue().get(partition))).offset());
        }
    }

}