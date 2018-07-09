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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

public class NamedTopicFilterTest {

    private KafkaConsumer<?, ?> consumerMock;
    
    @Before
    public void setUp() {
        consumerMock = mock(KafkaConsumer.class);
    }
    
    @Test
    public void testFilter() {
        String matchingTopicOne = "test-1";
        String matchingTopicTwo = "test-11";
        String unmatchedTopic = "unmatched";
        
        NamedTopicFilter filter = new NamedTopicFilter(matchingTopicOne, matchingTopicTwo);
        
        when(consumerMock.partitionsFor(matchingTopicOne)).thenReturn(Collections.singletonList(createPartitionInfo(matchingTopicOne, 0)));
        List<PartitionInfo> partitionTwoPartitions = new ArrayList<>();
        partitionTwoPartitions.add(createPartitionInfo(matchingTopicTwo, 0));
        partitionTwoPartitions.add(createPartitionInfo(matchingTopicTwo, 1));
        when(consumerMock.partitionsFor(matchingTopicTwo)).thenReturn(partitionTwoPartitions);
        when(consumerMock.partitionsFor(unmatchedTopic)).thenReturn(Collections.singletonList(createPartitionInfo(unmatchedTopic, 0)));
        
        Set<TopicPartition> matchedPartitions = filter.getAllSubscribedPartitions(consumerMock);
        
        assertThat("Expected filter to pass only topics with exact name matches", matchedPartitions, 
            containsInAnyOrder(new TopicPartition(matchingTopicOne, 0), new TopicPartition(matchingTopicTwo, 0), new TopicPartition(matchingTopicTwo, 1)));
            
    }
    
    @Test
    public void testFilterOnAbsentTopic() {
        String presentTopic = "present";
        String absentTopic = "absent";
        
        NamedTopicFilter filter = new NamedTopicFilter(presentTopic, absentTopic);
        when(consumerMock.partitionsFor(presentTopic)).thenReturn(Collections.singletonList(createPartitionInfo(presentTopic, 2)));
        when(consumerMock.partitionsFor(absentTopic)).thenReturn(null);
        
        Set<TopicPartition> presentPartitions = filter.getAllSubscribedPartitions(consumerMock);
        assertThat("Expected filter to pass only topics which are present", presentPartitions,
            contains(new TopicPartition(presentTopic, 2)));
    }
    
    private PartitionInfo createPartitionInfo(String topic, int partition) {
        return new PartitionInfo(topic, partition, null, null, null);
    }
    
}
