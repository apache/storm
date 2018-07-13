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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.internal.ConsumerFactory;
import org.apache.storm.kafka.spout.subscription.ManualPartitioner;
import org.apache.storm.kafka.spout.subscription.TopicAssigner;
import org.apache.storm.kafka.spout.subscription.TopicFilter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.mockito.ArgumentCaptor;

public class SpoutWithMockedConsumerSetupHelper {

    /**
     * Creates, opens and activates a KafkaSpout using a mocked consumer. The TopicFilter and ManualPartitioner should be mock objects,
     * since this method shortcircuits the TopicPartition assignment process and just calls onPartitionsAssigned on the rebalance listener.
     *
     * @param <K> The Kafka key type
     * @param <V> The Kafka value type
     * @param spoutConfig The spout config to use
     * @param topoConf The topo conf to pass to the spout
     * @param contextMock The topo context to pass to the spout
     * @param collectorMock The mocked collector to pass to the spout
     * @param consumerMock The mocked consumer
     * @param assignedPartitions The partitions to assign to this spout. The consumer will act like these partitions are assigned to it.
     * @return The spout
     */
    public static <K, V> KafkaSpout<K, V> setupSpout(KafkaSpoutConfig<K, V> spoutConfig, Map<String, Object> topoConf,
        TopologyContext contextMock, SpoutOutputCollector collectorMock, KafkaConsumer<K, V> consumerMock, TopicPartition... assignedPartitions) {
        TopicFilter topicFilter = spoutConfig.getTopicFilter();
        ManualPartitioner topicPartitioner = spoutConfig.getTopicPartitioner();
        if (!mockingDetails(topicFilter).isMock() || !mockingDetails(topicPartitioner).isMock()) {
            throw new IllegalStateException("Use a mocked TopicFilter and a mocked ManualPartitioner when using this method, it helps avoid complex stubbing");
        }
        
        Set<TopicPartition> assignedPartitionsSet = new HashSet<>(Arrays.asList(assignedPartitions));
        
        TopicAssigner assigner = mock(TopicAssigner.class);
        doAnswer(invocation -> {
            ConsumerRebalanceListener listener = invocation.getArgument(2);
            listener.onPartitionsAssigned(assignedPartitionsSet);
            return null;
        }).when(assigner).assignPartitions(any(), any(), any());
        when(consumerMock.assignment()).thenReturn(assignedPartitionsSet);
        
        ConsumerFactory<K, V> consumerFactory = (kafkaSpoutConfig) -> consumerMock;
        KafkaSpout<K, V> spout = new KafkaSpout<>(spoutConfig, consumerFactory, assigner);
        
        spout.open(topoConf, contextMock, collectorMock);
        spout.activate();

        return spout;
    }

    /**
     * Creates sequential dummy records
     *
     * @param <K> The Kafka key type
     * @param <V> The Kafka value type
     * @param topic The topic partition to create records for
     * @param startingOffset The starting offset of the records
     * @param numRecords The number of records to create
     * @return The dummy records
     */
    public static <K, V> List<ConsumerRecord<K, V>> createRecords(TopicPartition topic, long startingOffset, int numRecords) {
        List<ConsumerRecord<K, V>> recordsForPartition = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            recordsForPartition.add(new ConsumerRecord<>(topic.topic(), topic.partition(), startingOffset + i, null, null));
        }
        return recordsForPartition;
    }

    /**
     * Creates messages for the input offsets, emits the messages by calling nextTuple once per offset and returns the captured message ids
     * @param <K> The Kafka key type
     * @param <V> The Kafka value type
     * @param spout The spout
     * @param consumerMock The consumer used by the spout
     * @param expectedEmits The number of expected emits
     * @param collectorMock The collector used by the spout
     * @param partition The partition to emit messages on
     * @param offsetsToEmit The offsets to emit
     * @return The message ids emitted by the spout during the nextTuple calls
     */
    public static <K, V> List<KafkaSpoutMessageId> pollAndEmit(KafkaSpout<K, V> spout, KafkaConsumer<K, V> consumerMock, int expectedEmits, SpoutOutputCollector collectorMock, TopicPartition partition, int... offsetsToEmit) {
        return pollAndEmit(spout, consumerMock, expectedEmits, collectorMock, Collections.singletonMap(partition, offsetsToEmit));
    }

    /**
     * Creates messages for the input offsets, emits the messages by calling nextTuple once per offset and returns the captured message ids
     * @param <K> The Kafka key type
     * @param <V> The Kafka value type
     * @param spout The spout
     * @param consumerMock The consumer used by the spout
     * @param collectorMock The collector used by the spout
     * @param offsetsToEmit The offsets to emit per partition
     * @return The message ids emitted by the spout during the nextTuple calls
     */
    public static <K, V> List<KafkaSpoutMessageId> pollAndEmit(KafkaSpout<K, V> spout, KafkaConsumer<K, V> consumerMock, int expectedEmits, SpoutOutputCollector collectorMock, Map<TopicPartition, int[]> offsetsToEmit) {
        int totalOffsets = 0;
        Map<TopicPartition, List<ConsumerRecord<K, V>>> records = new HashMap<>();
        for (Entry<TopicPartition, int[]> entry : offsetsToEmit.entrySet()) {
            TopicPartition tp = entry.getKey();
            List<ConsumerRecord<K, V>> tpRecords = new ArrayList<>();
            for (Integer offset : entry.getValue()) {
                tpRecords.add(new ConsumerRecord<>(tp.topic(), tp.partition(), offset, null, null));
                totalOffsets++;
            }
            records.put(tp, tpRecords);
        }

        when(consumerMock.poll(anyLong()))
            .thenReturn(new ConsumerRecords<>(records));

        for (int i = 0; i < totalOffsets; i++) {
            spout.nextTuple();
        }

        ArgumentCaptor<KafkaSpoutMessageId> messageIds = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
        verify(collectorMock, times(expectedEmits)).emit(anyString(), anyList(), messageIds.capture());
        return messageIds.getAllValues();
    }

}
