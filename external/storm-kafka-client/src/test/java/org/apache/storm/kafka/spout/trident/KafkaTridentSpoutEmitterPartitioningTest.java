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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
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
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.config.builder.SingleTopicKafkaSpoutConfiguration;
import org.apache.storm.kafka.spout.subscription.ManualPartitioner;
import org.apache.storm.kafka.spout.subscription.TopicAssigner;
import org.apache.storm.kafka.spout.subscription.TopicFilter;
import org.apache.storm.kafka.spout.trident.config.builder.SingleTopicKafkaTridentSpoutConfiguration;
import org.apache.storm.task.TopologyContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class KafkaTridentSpoutEmitterPartitioningTest {
    
    @Mock
    public TopologyContext topologyContextMock;

    private final MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.NONE);
    private final TopicPartitionSerializer tpSerializer = new TopicPartitionSerializer();
    
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
                List<TopicPartition> partitions = new ArrayList<>(invocation.getArgument(0));
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
    
}
