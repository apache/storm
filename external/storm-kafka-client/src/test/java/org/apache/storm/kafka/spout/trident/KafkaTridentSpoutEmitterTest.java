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

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.SpoutWithMockedConsumerSetupHelper;
import org.apache.storm.kafka.spout.config.builder.SingleTopicKafkaSpoutConfiguration;
import org.apache.storm.kafka.spout.internal.ConsumerFactory;
import org.apache.storm.kafka.spout.subscription.ManualPartitioner;
import org.apache.storm.kafka.spout.subscription.TopicAssigner;
import org.apache.storm.kafka.spout.subscription.TopicFilter;
import org.apache.storm.kafka.spout.trident.config.builder.SingleTopicKafkaTridentSpoutConfiguration;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class KafkaTridentSpoutEmitterTest {
    
    @Rule
    public MockitoRule mockito = MockitoJUnit.rule();
    
    @Captor
    public ArgumentCaptor<List<Object>> emitCaptor;
    
    @Mock
    public TopologyContext topologyContextMock;

    private final MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.NONE);
    private final TopicPartitionSerializer tpSerializer = new TopicPartitionSerializer();
    private final String topologyId = "topologyId";

    @Before
    public void setUp() {
        when(topologyContextMock.getStormId()).thenReturn(topologyId);
    }
    
    @Test
    public void testGetOrderedPartitionsIsConsistent() {
        KafkaTridentSpoutEmitter<String, String> emitter = new KafkaTridentSpoutEmitter<>(
            SingleTopicKafkaTridentSpoutConfiguration.createKafkaSpoutConfigBuilder(-1)
                .build(),
            topologyContextMock,
            config -> consumer, new TopicAssigner());
        
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
            SingleTopicKafkaTridentSpoutConfiguration.createKafkaSpoutConfigBuilder(mock(TopicFilter.class), partitionerMock, -1)
                .build(),
            topologyContextMock,
            config -> consumer, new TopicAssigner());
        
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
        TopicAssigner assignerMock = mock(TopicAssigner.class);
        
        KafkaTridentSpoutEmitter<String, String> emitter = new KafkaTridentSpoutEmitter<>(
            SingleTopicKafkaTridentSpoutConfiguration.createKafkaSpoutConfigBuilder(-1)
                .build(),
            topologyContextMock,
            config -> consumer, assignerMock);
        
        List<KafkaTridentSpoutTopicPartition> allPartitions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            allPartitions.add(new KafkaTridentSpoutTopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, i));
        }
        Set<TopicPartition> unwrappedPartitions = allPartitions.stream()
            .map(kttp -> kttp.getTopicPartition())
            .collect(Collectors.toSet());
        
        emitter.refreshPartitions(allPartitions);
        
        verify(assignerMock).assignPartitions(eq(consumer), eq(unwrappedPartitions), any(ConsumerRebalanceListener.class));
    }
    
    private KafkaTridentSpoutEmitter<String, String> createEmitterWithMessages(TopicPartition tp, long firstOffset, int numRecords, FirstPollOffsetStrategy firstPollOffsetStrategy) {
        consumer.assign(Collections.singleton(tp));
        //Pretend that the topic offsets start at 0, even if the batch should start with a later offset
        consumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
        List<ConsumerRecord<String, String>> records = SpoutWithMockedConsumerSetupHelper.createRecords(tp, firstOffset, numRecords);
        records.forEach(record -> consumer.addRecord(record));
        return new KafkaTridentSpoutEmitter<>(
            SingleTopicKafkaTridentSpoutConfiguration.createKafkaSpoutConfigBuilder(-1)
                .setRecordTranslator(r -> new Values(r.offset()), new Fields("offset"))
                .setFirstPollOffsetStrategy(firstPollOffsetStrategy)
                .build(),
            topologyContextMock,
            config -> consumer, new TopicAssigner());
    }
    
    private Map<String, Object> doEmitNewBatchTest(MockConsumer<String, String> consumer, TridentCollector collectorMock, TopicPartition tp, long firstOffset, int numRecords, Map<String, Object> previousBatchMeta) {
        KafkaTridentSpoutEmitter<String, String> emitter = createEmitterWithMessages(tp, firstOffset, numRecords, FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST);
        
        TransactionAttempt txid = new TransactionAttempt(10L, 0);
        KafkaTridentSpoutTopicPartition kttp = new KafkaTridentSpoutTopicPartition(tp);
        return emitter.emitPartitionBatchNew(txid, collectorMock, kttp, previousBatchMeta);
    }
    
    @Test
    public void testEmitNewBatchWithNullMeta() {
        //Check that null meta makes the spout seek according to FirstPollOffsetStrategy, and that the returned meta is correct
        TridentCollector collectorMock = mock(TridentCollector.class);
        TopicPartition tp = new TopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, 0);
        long firstOffset = 0;
        int numRecords = 10;
        Map<String, Object> batchMeta = doEmitNewBatchTest(consumer, collectorMock, tp, firstOffset, numRecords, null);
        
        verify(collectorMock, times(numRecords)).emit(emitCaptor.capture());
        List<List<Object>> emits = emitCaptor.getAllValues();
        assertThat(emits.get(0).get(0), is(firstOffset));
        assertThat(emits.get(emits.size() - 1).get(0), is(firstOffset + numRecords - 1));
        KafkaTridentSpoutBatchMetadata deserializedMeta = KafkaTridentSpoutBatchMetadata.fromMap(batchMeta);
        assertThat("The batch should start at the first offset of the polled records", deserializedMeta.getFirstOffset(), is(firstOffset));
        assertThat("The batch should end at the last offset of the polled messages", deserializedMeta.getLastOffset(), is(firstOffset + numRecords - 1));
    }
    
    @Test
    public void testEmitNewBatchWithPreviousMeta() {
        //Check that non-null meta makes the spout seek according to the provided metadata, and that the returned meta is correct
        TridentCollector collectorMock = mock(TridentCollector.class);
        TopicPartition tp = new TopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, 0);
        long firstOffset = 50;
        int numRecords = 10;
        KafkaTridentSpoutBatchMetadata previousBatchMeta = new KafkaTridentSpoutBatchMetadata(0, firstOffset - 1, topologyId);
        Map<String, Object> batchMeta = doEmitNewBatchTest(consumer, collectorMock, tp, firstOffset, numRecords, previousBatchMeta.toMap());
        
        verify(collectorMock, times(numRecords)).emit(emitCaptor.capture());
        List<List<Object>> emits = emitCaptor.getAllValues();
        assertThat(emits.get(0).get(0), is(firstOffset));
        assertThat(emits.get(emits.size() - 1).get(0), is(firstOffset + numRecords - 1));
        KafkaTridentSpoutBatchMetadata deserializedMeta = KafkaTridentSpoutBatchMetadata.fromMap(batchMeta);
        assertThat("The batch should start at the first offset of the polled records", deserializedMeta.getFirstOffset(), is(firstOffset));
        assertThat("The batch should end at the last offset of the polled messages", deserializedMeta.getLastOffset(), is(firstOffset + numRecords - 1));
    }
    
    @Test
    public void testEmitEmptyBatches() throws Exception {
        //Check that the emitter can handle emitting empty batches on a new partition.
        //If the spout is configured to seek to LATEST, or the partition is empty, the initial batches may be empty
        KafkaConsumer<String, String> consumerMock = mock(KafkaConsumer.class);
        TridentCollector collectorMock = mock(TridentCollector.class);
        TopicPartition tp = new TopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, 0);
        when(consumerMock.assignment()).thenReturn(Collections.singleton(tp));
        ConsumerFactory<String, String> consumerFactory = spoutConfig -> consumerMock;
        KafkaTridentSpoutEmitter<String, String> emitter = new KafkaTridentSpoutEmitter<>(
            SingleTopicKafkaTridentSpoutConfiguration.createKafkaSpoutConfigBuilder(-1)
                .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.LATEST)
                .build(),
            mock(TopologyContext.class),
            consumerFactory, new TopicAssigner());
        KafkaTridentSpoutTopicPartition kttp = new KafkaTridentSpoutTopicPartition(tp);
        Map<String, Object> lastBatchMeta = null;
        //Emit 10 empty batches, simulating no new records being present in Kafka
        for(int i = 0; i < 10; i++) {
            clearInvocations(consumerMock);
            when(consumerMock.poll(anyLong())).thenReturn(new ConsumerRecords<>(Collections.emptyMap()));
            TransactionAttempt txid = new TransactionAttempt((long) i, 0);
            lastBatchMeta = emitter.emitPartitionBatchNew(txid, collectorMock, kttp, lastBatchMeta);
            assertThat(lastBatchMeta, nullValue());
            if (i == 0) {
                InOrder inOrder = inOrder(consumerMock, collectorMock);
                inOrder.verify(consumerMock).seekToEnd(Collections.singleton(tp));
                inOrder.verify(consumerMock).poll(anyLong());
            } else {
                verify(consumerMock).poll(anyLong());
            }
        }
        clearInvocations(consumerMock);
        //Simulate that new records were added in Kafka, and check that the next batch contains these records
        long firstOffset = 0;
        int numRecords = 10;
        when(consumerMock.poll(anyLong())).thenReturn(new ConsumerRecords<>(Collections.singletonMap(
            tp, SpoutWithMockedConsumerSetupHelper.createRecords(tp, firstOffset, numRecords))));
        lastBatchMeta = emitter.emitPartitionBatchNew(new TransactionAttempt(11L, 0), collectorMock, kttp, lastBatchMeta);
        
        verify(consumerMock).poll(anyLong());
        verify(collectorMock, times(numRecords)).emit(anyList());
        KafkaTridentSpoutBatchMetadata deserializedMeta = KafkaTridentSpoutBatchMetadata.fromMap(lastBatchMeta);
        assertThat("The batch should start at the first offset of the polled records", deserializedMeta.getFirstOffset(), is(firstOffset));
        assertThat("The batch should end at the last offset of the polled messages", deserializedMeta.getLastOffset(), is(firstOffset + numRecords - 1));
    }

    @Test
    public void testReEmitBatch() {
        TridentCollector collectorMock = mock(TridentCollector.class);
        TopicPartition tp = new TopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, 0);
        long firstOffset = 50;
        int numRecordsEmitted = 10;
        //Make sure the consumer can return extra records, so we test that re-emit only emits the original messages
        int numRecordsPresent = 100;
        KafkaTridentSpoutBatchMetadata batchMeta = new KafkaTridentSpoutBatchMetadata(firstOffset, firstOffset + numRecordsEmitted - 1, topologyId);
        KafkaTridentSpoutEmitter<String, String> emitter = createEmitterWithMessages(tp, firstOffset, numRecordsPresent, FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST);
        TransactionAttempt txid = new TransactionAttempt(10L, 0);
        KafkaTridentSpoutTopicPartition kttp = new KafkaTridentSpoutTopicPartition(tp);
        emitter.reEmitPartitionBatch(txid, collectorMock, kttp, batchMeta.toMap());
        
        verify(collectorMock, times(numRecordsEmitted)).emit(emitCaptor.capture());
        List<List<Object>> emits = emitCaptor.getAllValues();
        assertThat(emits.get(0).get(0), is(firstOffset));
        assertThat(emits.get(emits.size() - 1).get(0), is(firstOffset + numRecordsEmitted - 1));
    }
    
    @Test
    public void testReEmitBatchForOldTopologyWhenIgnoringCommittedOffsets() {
        //In some cases users will want to drop retrying old batches, e.g. if the topology should start over from scratch.
        //If the FirstPollOffsetStrategy ignores committed offsets, we should not retry batches for old topologies
        //The batch retry should be skipped entirely
        TridentCollector collectorMock = mock(TridentCollector.class);
        TopicPartition tp = new TopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, 0);
        long firstOffset = 50;
        int numRecordsEmitted = 10;
        KafkaTridentSpoutBatchMetadata batchMeta = new KafkaTridentSpoutBatchMetadata(firstOffset, firstOffset + numRecordsEmitted - 1, "a new storm id");
        KafkaTridentSpoutEmitter<String, String> emitter = createEmitterWithMessages(tp, firstOffset, numRecordsEmitted, FirstPollOffsetStrategy.EARLIEST);
        TransactionAttempt txid = new TransactionAttempt(10L, 0);
        KafkaTridentSpoutTopicPartition kttp = new KafkaTridentSpoutTopicPartition(tp);
        emitter.reEmitPartitionBatch(txid, collectorMock, kttp, batchMeta.toMap());
        
        verify(collectorMock, never()).emit(anyList());
    }
    
}
