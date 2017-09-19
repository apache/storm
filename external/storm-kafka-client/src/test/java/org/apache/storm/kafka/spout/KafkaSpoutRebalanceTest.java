/*
 * Copyright 2016 The Apache Software Foundation.
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

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.config.builder.SingleTopicKafkaSpoutConfiguration;
import org.apache.storm.kafka.spout.internal.KafkaConsumerFactory;
import org.apache.storm.kafka.spout.subscription.Subscription;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Time.SimulatedTime;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import static org.apache.storm.kafka.spout.config.builder.SingleTopicKafkaSpoutConfiguration.createKafkaSpoutConfigBuilder;

public class KafkaSpoutRebalanceTest {

    @Captor
    private ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> commitCapture;

    private final long offsetCommitPeriodMs = 2_000;
    private final Map<String, Object> conf = new HashMap<>();
    private TopologyContext contextMock;
    private SpoutOutputCollector collectorMock;
    private KafkaConsumer<String, String> consumerMock;
    private KafkaConsumerFactory<String, String> consumerFactory;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        contextMock = mock(TopologyContext.class);
        collectorMock = mock(SpoutOutputCollector.class);
        consumerMock = mock(KafkaConsumer.class);
        consumerFactory = (kafkaSpoutConfig) -> consumerMock;
    }

    //Returns messageIds in order of emission
    private List<KafkaSpoutMessageId> emitOneMessagePerPartitionThenRevokeOnePartition(KafkaSpout<String, String> spout, TopicPartition partitionThatWillBeRevoked, TopicPartition assignedPartition, ArgumentCaptor<ConsumerRebalanceListener> rebalanceListenerCapture) {
        //Setup spout with mock consumer so we can get at the rebalance listener   
        spout.open(conf, contextMock, collectorMock);
        spout.activate();

        //Assign partitions to the spout
        ConsumerRebalanceListener consumerRebalanceListener = rebalanceListenerCapture.getValue();
        List<TopicPartition> assignedPartitions = new ArrayList<>();
        assignedPartitions.add(partitionThatWillBeRevoked);
        assignedPartitions.add(assignedPartition);
        consumerRebalanceListener.onPartitionsAssigned(assignedPartitions);

        //Make the consumer return a single message for each partition
        Map<TopicPartition, List<ConsumerRecord<String, String>>> firstPartitionRecords = new HashMap<>();
        firstPartitionRecords.put(partitionThatWillBeRevoked, Collections.singletonList(new ConsumerRecord(partitionThatWillBeRevoked.topic(), partitionThatWillBeRevoked.partition(), 0L, "key", "value")));
        Map<TopicPartition, List<ConsumerRecord<String, String>>> secondPartitionRecords = new HashMap<>();
        secondPartitionRecords.put(assignedPartition, Collections.singletonList(new ConsumerRecord(assignedPartition.topic(), assignedPartition.partition(), 0L, "key", "value")));
        when(consumerMock.poll(anyLong()))
            .thenReturn(new ConsumerRecords<>(firstPartitionRecords))
            .thenReturn(new ConsumerRecords<>(secondPartitionRecords))
            .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

        //Emit the messages
        spout.nextTuple();
        ArgumentCaptor<KafkaSpoutMessageId> messageIdForRevokedPartition = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
        verify(collectorMock).emit(anyObject(), anyObject(), messageIdForRevokedPartition.capture());
        reset(collectorMock);
        spout.nextTuple();
        ArgumentCaptor<KafkaSpoutMessageId> messageIdForAssignedPartition = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
        verify(collectorMock).emit(anyObject(), anyObject(), messageIdForAssignedPartition.capture());

        //Now rebalance
        consumerRebalanceListener.onPartitionsRevoked(assignedPartitions);
        consumerRebalanceListener.onPartitionsAssigned(Collections.singleton(assignedPartition));

        List<KafkaSpoutMessageId> emittedMessageIds = new ArrayList<>();
        emittedMessageIds.add(messageIdForRevokedPartition.getValue());
        emittedMessageIds.add(messageIdForAssignedPartition.getValue());
        return emittedMessageIds;
    }

    @Test
    public void spoutMustIgnoreAcksForTuplesItIsNotAssignedAfterRebalance() throws Exception {
        //Acking tuples for partitions that are no longer assigned is useless since the spout will not be allowed to commit them
        try (SimulatedTime simulatedTime = new SimulatedTime()) {
            ArgumentCaptor<ConsumerRebalanceListener> rebalanceListenerCapture = ArgumentCaptor.forClass(ConsumerRebalanceListener.class);
            Subscription subscriptionMock = mock(Subscription.class);
            doNothing()
                .when(subscriptionMock)
                .subscribe(any(), rebalanceListenerCapture.capture(), any());
            KafkaSpout<String, String> spout = new KafkaSpout<>(createKafkaSpoutConfigBuilder(subscriptionMock, -1)
                .setOffsetCommitPeriodMs(offsetCommitPeriodMs)
                .build(), consumerFactory);
            String topic = SingleTopicKafkaSpoutConfiguration.TOPIC;
            TopicPartition partitionThatWillBeRevoked = new TopicPartition(topic, 1);
            TopicPartition assignedPartition = new TopicPartition(topic, 2);

            //Emit a message on each partition and revoke the first partition
            List<KafkaSpoutMessageId> emittedMessageIds = emitOneMessagePerPartitionThenRevokeOnePartition(
                spout, partitionThatWillBeRevoked, assignedPartition, rebalanceListenerCapture);

            //Ack both emitted tuples
            spout.ack(emittedMessageIds.get(0));
            spout.ack(emittedMessageIds.get(1));

            //Ensure the commit timer has expired
            Time.advanceTime(offsetCommitPeriodMs + KafkaSpout.TIMER_DELAY_MS);
            //Make the spout commit any acked tuples
            spout.nextTuple();
            //Verify that it only committed the message on the assigned partition
            verify(consumerMock, times(1)).commitSync(commitCapture.capture());

            Map<TopicPartition, OffsetAndMetadata> commitCaptureMap = commitCapture.getValue();
            assertThat(commitCaptureMap, hasKey(assignedPartition));
            assertThat(commitCaptureMap, not(hasKey(partitionThatWillBeRevoked)));
        }
    }

    @Test
    public void spoutMustIgnoreFailsForTuplesItIsNotAssignedAfterRebalance() throws Exception {
        //Failing tuples for partitions that are no longer assigned is useless since the spout will not be allowed to commit them if they later pass
        ArgumentCaptor<ConsumerRebalanceListener> rebalanceListenerCapture = ArgumentCaptor.forClass(ConsumerRebalanceListener.class);
            Subscription subscriptionMock = mock(Subscription.class);
            doNothing()
                .when(subscriptionMock)
                .subscribe(any(), rebalanceListenerCapture.capture(), any());
        KafkaSpoutRetryService retryServiceMock = mock(KafkaSpoutRetryService.class);
        KafkaSpout<String, String> spout = new KafkaSpout<>(createKafkaSpoutConfigBuilder(subscriptionMock, -1)
            .setOffsetCommitPeriodMs(10)
            .setRetry(retryServiceMock)
            .build(), consumerFactory);
        String topic = SingleTopicKafkaSpoutConfiguration.TOPIC;
        TopicPartition partitionThatWillBeRevoked = new TopicPartition(topic, 1);
        TopicPartition assignedPartition = new TopicPartition(topic, 2);

        when(retryServiceMock.getMessageId(anyObject()))
            .thenReturn(new KafkaSpoutMessageId(partitionThatWillBeRevoked, 0))
            .thenReturn(new KafkaSpoutMessageId(assignedPartition, 0));
        
        //Emit a message on each partition and revoke the first partition
        List<KafkaSpoutMessageId> emittedMessageIds = emitOneMessagePerPartitionThenRevokeOnePartition(
            spout, partitionThatWillBeRevoked, assignedPartition, rebalanceListenerCapture);

        //Check that only two message ids were generated
        verify(retryServiceMock, times(2)).getMessageId(anyObject());
        
        //Fail both emitted tuples
        spout.fail(emittedMessageIds.get(0));
        spout.fail(emittedMessageIds.get(1));

        //Check that only the tuple on the currently assigned partition is retried
        verify(retryServiceMock, never()).schedule(emittedMessageIds.get(0));
        verify(retryServiceMock).schedule(emittedMessageIds.get(1));
    }
}
