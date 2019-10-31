/*
 * Copyright 2018 The Apache Software Foundation.
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
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.SpoutWithMockedConsumerSetupHelper;
import org.apache.storm.kafka.spout.config.builder.SingleTopicKafkaSpoutConfiguration;
import org.apache.storm.kafka.spout.subscription.TopicAssigner;
import org.apache.storm.kafka.spout.trident.config.builder.SingleTopicKafkaTridentSpoutConfiguration;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class KafkaTridentSpoutEmitterEmitTest {

    @Captor
    public ArgumentCaptor<List<Object>> emitCaptor;

    @Mock
    public TopologyContext topologyContextMock;

    @Mock
    public TridentCollector collectorMock = mock(TridentCollector.class);

    private final MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.NONE);
    private final TopicPartition partition = new TopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, 0);
    private final String topologyId = "topologyId";
    private final long firstOffsetInKafka = 0;
    private final int recordsInKafka = 100;
    private final long lastOffsetInKafka = 99;
    private final long startTimeStamp = 1557214606103L;

    @BeforeEach
    public void setUp() {
        when(topologyContextMock.getStormId()).thenReturn(topologyId);
        consumer.assign(Collections.singleton(partition));
        consumer.updateBeginningOffsets(Collections.singletonMap(partition, firstOffsetInKafka));
        consumer.updateEndOffsets(Collections.singletonMap(partition, firstOffsetInKafka + recordsInKafka));
        List<ConsumerRecord<String, String>> records = SpoutWithMockedConsumerSetupHelper.createRecords(partition, firstOffsetInKafka, recordsInKafka);
        records.forEach(record -> consumer.addRecord(record));
    }

    private KafkaTridentSpoutEmitter<String, String> createEmitter(Consumer<String,String> kafkaConsumer, FirstPollOffsetStrategy firstPollOffsetStrategy) {
        return new KafkaTridentSpoutEmitter<>(
                SingleTopicKafkaTridentSpoutConfiguration.createKafkaSpoutConfigBuilder(-1)
                        .setRecordTranslator(r -> new Values(r.offset()), new Fields("offset"))
                        .setFirstPollOffsetStrategy(firstPollOffsetStrategy)
                        .setPollTimeoutMs(1)
                        .setStartTimeStamp(startTimeStamp)
                        .build(),
                topologyContextMock,
                config -> kafkaConsumer, new TopicAssigner());
    }

    private KafkaTridentSpoutEmitter<String, String> createEmitter(FirstPollOffsetStrategy firstPollOffsetStrategy) {
        return createEmitter(consumer,firstPollOffsetStrategy);
    }

    private Map<String, Object> doEmitNewBatchTest(FirstPollOffsetStrategy firstPollOffsetStrategy, TridentCollector collectorMock, TopicPartition tp, Map<String, Object> previousBatchMeta) {
        KafkaTridentSpoutEmitter<String, String> emitter = createEmitter(firstPollOffsetStrategy);

        TransactionAttempt txid = new TransactionAttempt(10L, 0);
        KafkaTridentSpoutTopicPartition kttp = new KafkaTridentSpoutTopicPartition(tp);
        return emitter.emitPartitionBatchNew(txid, collectorMock, kttp, previousBatchMeta);
    }

    @Test
    public void testEmitNewBatchWithNullMetaUncommittedEarliest() {
        //Check that null meta makes the spout seek to EARLIEST, and that the returned meta is correct
        Map<String, Object> batchMeta = doEmitNewBatchTest(FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST, collectorMock, partition, null);

        verify(collectorMock, times(recordsInKafka)).emit(emitCaptor.capture());
        List<List<Object>> emits = emitCaptor.getAllValues();
        assertThat(emits.get(0).get(0), is(firstOffsetInKafka));
        assertThat(emits.get(emits.size() - 1).get(0), is(lastOffsetInKafka));
        KafkaTridentSpoutBatchMetadata deserializedMeta = KafkaTridentSpoutBatchMetadata.fromMap(batchMeta);
        assertThat("The batch should start at the first offset of the polled records", deserializedMeta.getFirstOffset(), is(firstOffsetInKafka));
        assertThat("The batch should end at the last offset of the polled messages", deserializedMeta.getLastOffset(), is(lastOffsetInKafka));
    }

    @Test
    public void testEmitNewBatchWithNullMetaUncommittedLatest() {
        //Check that null meta makes the spout seek to LATEST, and that the returned meta is correct
        Map<String, Object> batchMeta = doEmitNewBatchTest(FirstPollOffsetStrategy.UNCOMMITTED_LATEST, collectorMock, partition, null);

        verify(collectorMock, never()).emit(anyList());
        KafkaTridentSpoutBatchMetadata deserializedMeta = KafkaTridentSpoutBatchMetadata.fromMap(batchMeta);
        assertThat("The batch should start at the first offset of the polled records", deserializedMeta.getFirstOffset(), is(lastOffsetInKafka));
        assertThat("The batch should end at the last offset of the polled messages", deserializedMeta.getLastOffset(), is(lastOffsetInKafka));
    }

    @ParameterizedTest
    @EnumSource(value = FirstPollOffsetStrategy.class, names = {"EARLIEST", "LATEST", "TIMESTAMP"})
    public void testEmitNewBatchWithPreviousMeta(FirstPollOffsetStrategy firstPollOffsetStrategy) {
        //Check that non-null meta makes the spout seek according to the provided metadata, and that the returned meta is correct
        long firstExpectedEmittedOffset = 50;
        int expectedEmittedRecords = 50;
        KafkaTridentSpoutBatchMetadata previousBatchMeta = new KafkaTridentSpoutBatchMetadata(firstOffsetInKafka, firstExpectedEmittedOffset - 1, topologyId);
        Map<String, Object> batchMeta = doEmitNewBatchTest(firstPollOffsetStrategy, collectorMock, partition, previousBatchMeta.toMap());

        verify(collectorMock, times(expectedEmittedRecords)).emit(emitCaptor.capture());
        List<List<Object>> emits = emitCaptor.getAllValues();
        assertThat(emits.get(0).get(0), is(firstExpectedEmittedOffset));
        assertThat(emits.get(emits.size() - 1).get(0), is(lastOffsetInKafka));
        KafkaTridentSpoutBatchMetadata deserializedMeta = KafkaTridentSpoutBatchMetadata.fromMap(batchMeta);
        assertThat("The batch should start at the first offset of the polled records", deserializedMeta.getFirstOffset(), is(firstExpectedEmittedOffset));
        assertThat("The batch should end at the last offset of the polled messages", deserializedMeta.getLastOffset(), is(lastOffsetInKafka));
    }

    @Test
    public void testEmitEmptyBatches() throws Exception {
        //Check that the emitter can handle emitting empty batches on a new partition.
        //If the spout is configured to seek to LATEST, or the partition is empty, the initial batches may be empty
        KafkaTridentSpoutEmitter<String, String> emitter = createEmitter(FirstPollOffsetStrategy.LATEST);
        KafkaTridentSpoutTopicPartition kttp = new KafkaTridentSpoutTopicPartition(partition);
        Map<String, Object> lastBatchMeta = null;
        //Emit 10 empty batches, simulating no new records being present in Kafka
        for (int i = 0; i < 10; i++) {
            TransactionAttempt txid = new TransactionAttempt((long) i, 0);
            lastBatchMeta = emitter.emitPartitionBatchNew(txid, collectorMock, kttp, lastBatchMeta);
            KafkaTridentSpoutBatchMetadata deserializedMeta = KafkaTridentSpoutBatchMetadata.fromMap(lastBatchMeta);
            assertThat("Since the first poll strategy is LATEST, the meta should indicate that the last message has already been emitted", deserializedMeta.getFirstOffset(), is(lastOffsetInKafka));
            assertThat("Since the first poll strategy is LATEST, the meta should indicate that the last message has already been emitted", deserializedMeta.getLastOffset(), is(lastOffsetInKafka));
        }
        //Add new records to Kafka, and check that the next batch contains these records
        long firstNewRecordOffset = lastOffsetInKafka + 1;
        int numNewRecords = 10;
        List<ConsumerRecord<String, String>> newRecords = SpoutWithMockedConsumerSetupHelper.createRecords(partition, firstNewRecordOffset, numNewRecords);
        newRecords.forEach(consumer::addRecord);
        lastBatchMeta = emitter.emitPartitionBatchNew(new TransactionAttempt(11L, 0), collectorMock, kttp, lastBatchMeta);

        verify(collectorMock, times(numNewRecords)).emit(emitCaptor.capture());
        List<List<Object>> emits = emitCaptor.getAllValues();
        assertThat(emits.get(0).get(0), is(firstNewRecordOffset));
        assertThat(emits.get(emits.size() - 1).get(0), is(firstNewRecordOffset + numNewRecords - 1));
        KafkaTridentSpoutBatchMetadata deserializedMeta = KafkaTridentSpoutBatchMetadata.fromMap(lastBatchMeta);
        assertThat("The batch should start at the first offset of the polled records", deserializedMeta.getFirstOffset(), is(firstNewRecordOffset));
        assertThat("The batch should end at the last offset of the polled messages", deserializedMeta.getLastOffset(), is(firstNewRecordOffset + numNewRecords - 1));
    }

    @Test
    public void testReEmitBatch() {
        //Check that a reemit emits exactly the same tuples as the last batch, even if Kafka returns more messages
        long firstEmittedOffset = 50;
        int numEmittedRecords = 10;
        KafkaTridentSpoutBatchMetadata batchMeta = new KafkaTridentSpoutBatchMetadata(firstEmittedOffset, firstEmittedOffset + numEmittedRecords - 1, topologyId);
        KafkaTridentSpoutEmitter<String, String> emitter = createEmitter(FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST);
        TransactionAttempt txid = new TransactionAttempt(10L, 0);
        KafkaTridentSpoutTopicPartition kttp = new KafkaTridentSpoutTopicPartition(partition);
        emitter.reEmitPartitionBatch(txid, collectorMock, kttp, batchMeta.toMap());

        verify(collectorMock, times(numEmittedRecords)).emit(emitCaptor.capture());
        List<List<Object>> emits = emitCaptor.getAllValues();
        assertThat(emits.get(0).get(0), is(firstEmittedOffset));
        assertThat(emits.get(emits.size() - 1).get(0), is(firstEmittedOffset + numEmittedRecords - 1));
    }

    @Test
    public void testReEmitBatchForOldTopologyWhenIgnoringCommittedOffsets() {
        //In some cases users will want to drop retrying old batches, e.g. if the topology should start over from scratch.
        //If the FirstPollOffsetStrategy ignores committed offsets, we should not retry batches for old topologies
        //The batch retry should be skipped entirely
        KafkaTridentSpoutBatchMetadata batchMeta = new KafkaTridentSpoutBatchMetadata(firstOffsetInKafka, lastOffsetInKafka, "a new storm id");
        KafkaTridentSpoutEmitter<String, String> emitter = createEmitter(FirstPollOffsetStrategy.EARLIEST);
        TransactionAttempt txid = new TransactionAttempt(10L, 0);
        KafkaTridentSpoutTopicPartition kttp = new KafkaTridentSpoutTopicPartition(partition);
        emitter.reEmitPartitionBatch(txid, collectorMock, kttp, batchMeta.toMap());

        verify(collectorMock, never()).emit(anyList());
    }

    @Test
    public void testEmitEmptyFirstBatch() {
        /**
         * Check that when the first batch after a redeploy is empty, the emitter does not restart at the pre-redeploy offset. STORM-3279.
         */
        long firstEmittedOffset = 50;
        int emittedRecords = 10;
        KafkaTridentSpoutBatchMetadata preRedeployLastMeta = new KafkaTridentSpoutBatchMetadata(firstEmittedOffset, firstEmittedOffset + emittedRecords - 1, "an old topology");
        KafkaTridentSpoutEmitter<String, String> emitter = createEmitter(FirstPollOffsetStrategy.LATEST);
        TransactionAttempt txid = new TransactionAttempt(0L, 0);
        KafkaTridentSpoutTopicPartition kttp = new KafkaTridentSpoutTopicPartition(partition);
        Map<String, Object> meta = emitter.emitPartitionBatchNew(txid, collectorMock, kttp, preRedeployLastMeta.toMap());

        verify(collectorMock, never()).emit(anyList());
        KafkaTridentSpoutBatchMetadata deserializedMeta = KafkaTridentSpoutBatchMetadata.fromMap(meta);
        assertThat(deserializedMeta.getFirstOffset(), is(lastOffsetInKafka));
        assertThat(deserializedMeta.getLastOffset(), is(lastOffsetInKafka));

        long firstNewRecordOffset = lastOffsetInKafka + 1;
        int numNewRecords = 10;
        List<ConsumerRecord<String, String>> newRecords = SpoutWithMockedConsumerSetupHelper.createRecords(partition, firstNewRecordOffset, numNewRecords);
        newRecords.forEach(consumer::addRecord);
        meta = emitter.emitPartitionBatchNew(txid, collectorMock, kttp, meta);

        verify(collectorMock, times(numNewRecords)).emit(emitCaptor.capture());
        List<List<Object>> emits = emitCaptor.getAllValues();
        assertThat(emits.get(0).get(0), is(firstNewRecordOffset));
        assertThat(emits.get(emits.size() - 1).get(0), is(firstNewRecordOffset + numNewRecords - 1));
        deserializedMeta = KafkaTridentSpoutBatchMetadata.fromMap(meta);
        assertThat("The batch should start at the first offset of the polled records", deserializedMeta.getFirstOffset(), is(firstNewRecordOffset));
        assertThat("The batch should end at the last offset of the polled messages", deserializedMeta.getLastOffset(), is(firstNewRecordOffset + numNewRecords - 1));
    }

    @ParameterizedTest
    @EnumSource(value = FirstPollOffsetStrategy.class, names = {"EARLIEST", "LATEST", "TIMESTAMP"})
    public void testUnconditionalStrategyWhenSpoutWorkerIsRestarted(FirstPollOffsetStrategy firstPollOffsetStrategy) {
        /**
         * EARLIEST/LATEST/TIMESTAMP should act like UNCOMMITTED_EARLIEST/LATEST/TIMESTAMP if the emitter is new but the
         * topology has not restarted (storm id has not changed)
         */
        long preRestartEmittedOffset = 20;
        int lastBatchEmittedRecords = 10;
        int preRestartEmittedRecords = 30;
        KafkaTridentSpoutBatchMetadata preExecutorRestartLastMeta = new KafkaTridentSpoutBatchMetadata(preRestartEmittedOffset, preRestartEmittedOffset + lastBatchEmittedRecords - 1, topologyId);
        KafkaTridentSpoutEmitter<String, String> emitter = createEmitter(firstPollOffsetStrategy);
        TransactionAttempt txid = new TransactionAttempt(0L, 0);
        KafkaTridentSpoutTopicPartition kttp = new KafkaTridentSpoutTopicPartition(partition);
        Map<String, Object> meta = emitter.emitPartitionBatchNew(txid, collectorMock, kttp, preExecutorRestartLastMeta.toMap());

        long firstEmittedOffset = preRestartEmittedOffset + lastBatchEmittedRecords;
        int emittedRecords = recordsInKafka - preRestartEmittedRecords;
        verify(collectorMock, times(emittedRecords)).emit(emitCaptor.capture());
        List<List<Object>> emits = emitCaptor.getAllValues();
        assertThat(emits.get(0).get(0), is(firstEmittedOffset));
        assertThat(emits.get(emits.size() - 1).get(0), is(lastOffsetInKafka));
        KafkaTridentSpoutBatchMetadata deserializedMeta = KafkaTridentSpoutBatchMetadata.fromMap(meta);
        assertThat("The batch should start at the first offset of the polled records", deserializedMeta.getFirstOffset(), is(firstEmittedOffset));
        assertThat("The batch should end at the last offset of the polled messages", deserializedMeta.getLastOffset(), is(lastOffsetInKafka));
    }

    @Test
    public void testEarliestStrategyWhenTopologyIsRedeployed() {
        /**
         * EARLIEST should be applied if the emitter is new and the topology has been redeployed (storm id has changed)
         */
        long preRestartEmittedOffset = 20;
        int preRestartEmittedRecords = 10;
        KafkaTridentSpoutBatchMetadata preExecutorRestartLastMeta = new KafkaTridentSpoutBatchMetadata(preRestartEmittedOffset, preRestartEmittedOffset + preRestartEmittedRecords - 1, "Some older topology");
        KafkaTridentSpoutEmitter<String, String> emitter = createEmitter(FirstPollOffsetStrategy.EARLIEST);
        TransactionAttempt txid = new TransactionAttempt(0L, 0);
        KafkaTridentSpoutTopicPartition kttp = new KafkaTridentSpoutTopicPartition(partition);
        Map<String, Object> meta = emitter.emitPartitionBatchNew(txid, collectorMock, kttp, preExecutorRestartLastMeta.toMap());

        verify(collectorMock, times(recordsInKafka)).emit(emitCaptor.capture());
        List<List<Object>> emits = emitCaptor.getAllValues();
        assertThat(emits.get(0).get(0), is(firstOffsetInKafka));
        assertThat(emits.get(emits.size() - 1).get(0), is(lastOffsetInKafka));
        KafkaTridentSpoutBatchMetadata deserializedMeta = KafkaTridentSpoutBatchMetadata.fromMap(meta);
        assertThat("The batch should start at the first offset of the polled records", deserializedMeta.getFirstOffset(), is(firstOffsetInKafka));
        assertThat("The batch should end at the last offset of the polled messages", deserializedMeta.getLastOffset(), is(lastOffsetInKafka));
    }

    @Test
    public void testLatestStrategyWhenTopologyIsRedeployed() {
        /**
         * EARLIEST should be applied if the emitter is new and the topology has been redeployed (storm id has changed)
         */
        long preRestartEmittedOffset = 20;
        int preRestartEmittedRecords = 10;
        KafkaTridentSpoutBatchMetadata preExecutorRestartLastMeta = new KafkaTridentSpoutBatchMetadata(preRestartEmittedOffset, preRestartEmittedOffset + preRestartEmittedRecords - 1, "Some older topology");
        KafkaTridentSpoutEmitter<String, String> emitter = createEmitter(FirstPollOffsetStrategy.LATEST);
        TransactionAttempt txid = new TransactionAttempt(0L, 0);
        KafkaTridentSpoutTopicPartition kttp = new KafkaTridentSpoutTopicPartition(partition);
        Map<String, Object> meta = emitter.emitPartitionBatchNew(txid, collectorMock, kttp, preExecutorRestartLastMeta.toMap());

        verify(collectorMock, never()).emit(anyList());
    }

    @Test
    public void testTimeStampStrategyWhenTopologyIsRedeployed() {
        /**
         * TIMESTAMP strategy should be applied if the emitter is new and the topology has been redeployed (storm id has changed)
         * Offset should be reset according to the offset corresponding to startTimeStamp
         */
        long preRestartEmittedOffset = 20;
        int preRestartEmittedRecords = 10;
        long timeStampStartOffset = 2L;
        long pollTimeout = 1L;
        KafkaTridentSpoutBatchMetadata preExecutorRestartLastMeta = new KafkaTridentSpoutBatchMetadata(preRestartEmittedOffset, preRestartEmittedOffset + preRestartEmittedRecords - 1, "Some older topology");
        
        KafkaConsumer<String, String> kafkaConsumer = Mockito.mock(KafkaConsumer.class);
        when(kafkaConsumer.assignment()).thenReturn(Collections.singleton(partition));
        OffsetAndTimestamp offsetAndTimestamp = new OffsetAndTimestamp(timeStampStartOffset, startTimeStamp);
        HashMap<TopicPartition, OffsetAndTimestamp> map = new HashMap<>();
        map.put(partition, offsetAndTimestamp);
        when(kafkaConsumer.offsetsForTimes(Collections.singletonMap(partition, startTimeStamp))).thenReturn(map);
        HashMap<TopicPartition, List<ConsumerRecord<String, String>>> topicPartitionMap = new HashMap<>();
        List<ConsumerRecord<String, String>> newRecords = SpoutWithMockedConsumerSetupHelper.createRecords(partition, timeStampStartOffset, recordsInKafka);
        topicPartitionMap.put(partition, newRecords);
        when(kafkaConsumer.poll(pollTimeout)).thenReturn(new ConsumerRecords<>(topicPartitionMap));

        KafkaTridentSpoutEmitter<String, String> emitter = createEmitter(kafkaConsumer, FirstPollOffsetStrategy.TIMESTAMP);
        TransactionAttempt txid = new TransactionAttempt(0L, 0);
        KafkaTridentSpoutTopicPartition kttp = new KafkaTridentSpoutTopicPartition(partition);
        Map<String, Object> meta = emitter.emitPartitionBatchNew(txid, collectorMock, kttp, preExecutorRestartLastMeta.toMap());

        verify(collectorMock, times(recordsInKafka)).emit(emitCaptor.capture());
        verify(kafkaConsumer, times(1)).seek(partition, timeStampStartOffset);
        List<List<Object>> emits = emitCaptor.getAllValues();
        assertThat(emits.get(0).get(0), is(timeStampStartOffset));
        KafkaTridentSpoutBatchMetadata deserializedMeta = KafkaTridentSpoutBatchMetadata.fromMap(meta);
        assertThat("The batch should start at the first offset for startTimestamp", deserializedMeta.getFirstOffset(), is(timeStampStartOffset));
    }

}
