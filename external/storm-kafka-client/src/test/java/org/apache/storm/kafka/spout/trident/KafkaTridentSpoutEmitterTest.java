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

package org.apache.storm.kafka.spout.trident;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.SpoutWithMockedConsumerSetupHelper;
import org.apache.storm.kafka.spout.config.builder.SingleTopicKafkaSpoutConfiguration;
import org.apache.storm.kafka.spout.internal.KafkaConsumerFactory;
import org.apache.storm.kafka.spout.subscription.ManualPartitioner;
import org.apache.storm.kafka.spout.subscription.TopicAssigner;
import org.apache.storm.kafka.spout.subscription.TopicFilter;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class KafkaTridentSpoutEmitterTest {
    
    @Rule
    public MockitoRule mockito = MockitoJUnit.rule();
    
    @Captor
    public ArgumentCaptor<Collection<TopicPartition>> assignmentCaptor;
    
    private final TopicPartitionSerializer tpSerializer = new TopicPartitionSerializer();
    
    @Test
    public void testGetOrderedPartitionsIsConsistent() {
        KafkaTridentSpoutEmitter<String, String> emitter = new KafkaTridentSpoutEmitter<>(
            SingleTopicKafkaSpoutConfiguration.createKafkaSpoutConfigBuilder(-1)
                .build(),
            mock(TopologyContext.class),
            mock(KafkaConsumerFactory.class), new TopicAssigner());
        
        Set<TopicPartition> allPartitions = new HashSet<>();
        int numPartitions = 10;
        for (int i = 0; i < numPartitions; i++) {
            allPartitions.add(new TopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, i));
        }
        List<Map<String, Object>> serializedPartitions = allPartitions.stream()
            .map(tp -> tpSerializer.toMap(tp))
            .collect(Collectors.toList());
        
        List<KafkaTridentSpoutTopicPartition> orderedPartitions = emitter.getOrderedPartitions(serializedPartitions);
        assertThat("Should contain all partitions", orderedPartitions.size(), is(allPartitions.size()));
        Collections.shuffle(serializedPartitions);
        List<KafkaTridentSpoutTopicPartition> secondGetOrderedPartitions = emitter.getOrderedPartitions(serializedPartitions);
        assertThat("Ordering must be consistent", secondGetOrderedPartitions, is(orderedPartitions));
        
        serializedPartitions.add(tpSerializer.toMap(new TopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, numPartitions)));
        List<KafkaTridentSpoutTopicPartition> orderedPartitionsWithNewPartition = emitter.getOrderedPartitions(serializedPartitions);
        orderedPartitionsWithNewPartition.remove(orderedPartitionsWithNewPartition.size() - 1);
        assertThat("Adding new partitions should not shuffle the existing ordering", orderedPartitionsWithNewPartition, is(orderedPartitions));
    }
    
    @Test
    public void testGetPartitionsForTask() {
        //Verify correct wrapping/unwrapping of partition and delegation of partition assignment
        ManualPartitioner partitionerMock = mock(ManualPartitioner.class);
        when(partitionerMock.getPartitionsForThisTask(any(), any()))
            .thenAnswer(invocation -> {
                List<TopicPartition> partitions = new ArrayList<>((List<TopicPartition>) invocation.getArguments()[0]);
                partitions.remove(0);
                return new HashSet<>(partitions);
            });
        
        KafkaTridentSpoutEmitter<String, String> emitter = new KafkaTridentSpoutEmitter<>(
            SingleTopicKafkaSpoutConfiguration.createKafkaSpoutConfigBuilder(mock(TopicFilter.class), partitionerMock, -1)
                .build(),
            mock(TopologyContext.class),
            mock(KafkaConsumerFactory.class), new TopicAssigner());
        
        List<KafkaTridentSpoutTopicPartition> allPartitions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            allPartitions.add(new KafkaTridentSpoutTopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, i));
        }
        List<TopicPartition> unwrappedPartitions = allPartitions.stream()
            .map(kttp -> kttp.getTopicPartition())
            .collect(Collectors.toList());
        
        List<KafkaTridentSpoutTopicPartition> partitionsForTask = emitter.getPartitionsForTask(0, 2, allPartitions);
        verify(partitionerMock).getPartitionsForThisTask(eq(unwrappedPartitions), any(TopologyContext.class));
        allPartitions.remove(0);
        assertThat("Should have assigned all except the first partition to this task", new HashSet<>(partitionsForTask), is(new HashSet<>(allPartitions)));
    }
    
    @Test
    public void testAssignPartitions() {
        //Verify correct unwrapping of partitions and delegation of assignment
        KafkaConsumer<String, String> consumerMock = mock(KafkaConsumer.class);
        KafkaConsumerFactory<String, String> consumerFactory = spoutConfig -> consumerMock;
        TopicAssigner assignerMock = mock(TopicAssigner.class);
        
        KafkaTridentSpoutEmitter<String, String> emitter = new KafkaTridentSpoutEmitter<>(
            SingleTopicKafkaSpoutConfiguration.createKafkaSpoutConfigBuilder(-1)
                .build(),
            mock(TopologyContext.class),
            consumerFactory, assignerMock);
        
        List<KafkaTridentSpoutTopicPartition> allPartitions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            allPartitions.add(new KafkaTridentSpoutTopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, i));
        }
        Set<TopicPartition> unwrappedPartitions = allPartitions.stream()
            .map(kttp -> kttp.getTopicPartition())
            .collect(Collectors.toSet());
        
        emitter.refreshPartitions(allPartitions);
        
        verify(assignerMock).assignPartitions(any(KafkaConsumer.class), eq(unwrappedPartitions), any(ConsumerRebalanceListener.class));
    }
    
    private Map<String, Object> doEmitBatchTest(KafkaConsumer<String, String> consumerMock, TridentCollector collectorMock, TopicPartition tp, long firstOffset, int numRecords, Map<String, Object> previousBatchMeta) {
        when(consumerMock.assignment()).thenReturn(Collections.singleton(tp));
        when(consumerMock.poll(anyLong())).thenReturn(new ConsumerRecords<>(Collections.singletonMap(
            tp, SpoutWithMockedConsumerSetupHelper.createRecords(tp, firstOffset, numRecords))));
        KafkaConsumerFactory<String, String> consumerFactory = spoutConfig -> consumerMock;
        
        KafkaTridentSpoutEmitter<String, String> emitter = new KafkaTridentSpoutEmitter<>(
            SingleTopicKafkaSpoutConfiguration.createKafkaSpoutConfigBuilder(-1)
                .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST)
                .build(),
            mock(TopologyContext.class),
            consumerFactory, new TopicAssigner());
        
        TransactionAttempt txid = new TransactionAttempt(10L, 0);
        KafkaTridentSpoutTopicPartition kttp = new KafkaTridentSpoutTopicPartition(tp);
        return emitter.emitPartitionBatch(txid, collectorMock, kttp, previousBatchMeta);
    }
    
    @Test
    public void testEmitBatchWithNullMeta() {
        //Check that null meta makes the spout seek according to FirstPollOffsetStrategy, and that the returned meta is correct
        KafkaConsumer<String, String> consumerMock = mock(KafkaConsumer.class);
        TridentCollector collectorMock = mock(TridentCollector.class);
        TopicPartition tp = new TopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, 0);
        long firstOffset = 0;
        int numRecords = 10;
        Map<String, Object> batchMeta = doEmitBatchTest(consumerMock, collectorMock, tp, firstOffset, numRecords, null);
        
        InOrder inOrder = inOrder(consumerMock, collectorMock);
        inOrder.verify(consumerMock).seekToBeginning(Collections.singleton(tp));
        inOrder.verify(consumerMock).poll(anyLong());
        inOrder.verify(collectorMock, times(numRecords)).emit(anyList());
        KafkaTridentSpoutBatchMetadata deserializedMeta = KafkaTridentSpoutBatchMetadata.fromMap(batchMeta);
        assertThat("The batch should start at the first offset of the polled records", deserializedMeta.getFirstOffset(), is(firstOffset));
        assertThat("The batch should end at the last offset of the polled messages", deserializedMeta.getLastOffset(), is(firstOffset + numRecords - 1));
    }
    
    @Test
    public void testEmitBatchWithPreviousMeta() {
        //Check that non-null meta makes the spout seek according to the provided metadata, and that the returned meta is correct
        KafkaConsumer<String, String> consumerMock = mock(KafkaConsumer.class);
        TridentCollector collectorMock = mock(TridentCollector.class);
        TopicPartition tp = new TopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, 0);
        long firstOffset = 50;
        int numRecords = 10;
        KafkaTridentSpoutBatchMetadata previousBatchMeta = new KafkaTridentSpoutBatchMetadata(0, firstOffset - 1);
        Map<String, Object> batchMeta = doEmitBatchTest(consumerMock, collectorMock, tp, firstOffset, numRecords, previousBatchMeta.toMap());
        
        InOrder inOrder = inOrder(consumerMock, collectorMock);
        inOrder.verify(consumerMock).seek(tp, firstOffset);
        inOrder.verify(consumerMock).poll(anyLong());
        inOrder.verify(collectorMock, times(numRecords)).emit(anyList());
        KafkaTridentSpoutBatchMetadata deserializedMeta = KafkaTridentSpoutBatchMetadata.fromMap(batchMeta);
        assertThat("The batch should start at the first offset of the polled records", deserializedMeta.getFirstOffset(), is(firstOffset));
        assertThat("The batch should end at the last offset of the polled messages", deserializedMeta.getLastOffset(), is(firstOffset + numRecords - 1));
    }
    
}
