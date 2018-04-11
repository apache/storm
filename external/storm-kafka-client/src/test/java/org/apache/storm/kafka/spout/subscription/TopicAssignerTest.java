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

package org.apache.storm.kafka.spout.subscription;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.config.builder.SingleTopicKafkaSpoutConfiguration;
import org.junit.Test;
import org.mockito.InOrder;

public class TopicAssignerTest {

    @Test
    public void testCanReassignPartitions() {    
        Set<TopicPartition> onePartition = Collections.singleton(new TopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, 0));
        Set<TopicPartition> twoPartitions = new HashSet<>();
        twoPartitions.add(new TopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, 0));
        twoPartitions.add(new TopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, 1));
        KafkaConsumer<String, String> consumerMock = mock(KafkaConsumer.class);
        ConsumerRebalanceListener listenerMock = mock(ConsumerRebalanceListener.class);
        TopicAssigner assigner = new TopicAssigner();
        
        //Set the first assignment
        assigner.assignPartitions(consumerMock, onePartition, listenerMock);
        
        InOrder inOrder = inOrder(consumerMock, listenerMock);
        inOrder.verify(listenerMock).onPartitionsRevoked(Collections.emptySet());
        inOrder.verify(consumerMock).assign(new HashSet<>(onePartition));
        inOrder.verify(listenerMock).onPartitionsAssigned(new HashSet<>(onePartition));
        
        clearInvocations(consumerMock, listenerMock);
        
        when(consumerMock.assignment()).thenReturn(new HashSet<>(onePartition));
        
        //Update to set the second assignment
        assigner.assignPartitions(consumerMock, twoPartitions, listenerMock);
        
        //The partition revocation hook must be called before the new partitions are assigned to the consumer,
        //to allow the revocation hook to commit offsets for the revoked partitions.
        inOrder.verify(listenerMock).onPartitionsRevoked(new HashSet<>(onePartition));
        inOrder.verify(consumerMock).assign(new HashSet<>(twoPartitions));
        inOrder.verify(listenerMock).onPartitionsAssigned(new HashSet<>(twoPartitions));
    }
    
}
