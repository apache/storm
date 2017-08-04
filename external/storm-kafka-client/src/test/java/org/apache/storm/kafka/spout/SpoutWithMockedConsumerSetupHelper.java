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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.internal.KafkaConsumerFactory;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;

public class SpoutWithMockedConsumerSetupHelper {

    /**
     * Creates, opens and activates a KafkaSpout using a mocked consumer.
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
        TopologyContext contextMock, SpoutOutputCollector collectorMock, KafkaConsumer<K, V> consumerMock, Set<TopicPartition> assignedPartitions) {

        Map<String, List<PartitionInfo>> partitionInfos = assignedPartitions.stream()
            .map(tp -> new PartitionInfo(tp.topic(), tp.partition(), null, null, null))
            .collect(Collectors.groupingBy(info -> info.topic()));
        partitionInfos.keySet()
            .forEach(key -> when(consumerMock.partitionsFor(key))
            .thenReturn(partitionInfos.get(key)));
        KafkaConsumerFactory<K, V> consumerFactory = (kafkaSpoutConfig) -> consumerMock;

        KafkaSpout<K, V> spout = new KafkaSpout<>(spoutConfig, consumerFactory);

        when(contextMock.getComponentTasks(any())).thenReturn(Collections.singletonList(0));
        when(contextMock.getThisTaskIndex()).thenReturn(0);

        spout.open(topoConf, contextMock, collectorMock);
        spout.activate();

        verify(consumerMock).assign(assignedPartitions);

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

}
