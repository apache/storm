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

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PatternTopicFilterTest {

    private KafkaConsumer<?, ?> consumerMock;
    
    @Before
    public void setUp(){
        consumerMock = mock(KafkaConsumer.class);
    }
    
    @Test
    public void testFilter() {
        Pattern pattern = Pattern.compile("test-\\d+");
        PatternTopicFilter filter = new PatternTopicFilter(pattern);
        
        String matchingTopicOne = "test-1";
        String matchingTopicTwo = "test-11";
        String unmatchedTopic = "unmatched";
        
        Map<String, List<PartitionInfo>> allTopics = new HashMap<>();
        allTopics.put(matchingTopicOne, Collections.singletonList(createPartitionInfo(matchingTopicOne, 0)));
        List<PartitionInfo> testTwoPartitions = new ArrayList<>();
        testTwoPartitions.add(createPartitionInfo(matchingTopicTwo, 0));
        testTwoPartitions.add(createPartitionInfo(matchingTopicTwo, 1));
        allTopics.put(matchingTopicTwo, testTwoPartitions);
        allTopics.put(unmatchedTopic, Collections.singletonList(createPartitionInfo(unmatchedTopic, 0)));
        
        when(consumerMock.listTopics()).thenReturn(allTopics);
        
        List<TopicPartition> matchedPartitions = filter.getFilteredTopicPartitions(consumerMock);
        
        assertThat("Expected topic partitions matching the pattern to be passed by the filter", matchedPartitions,
            containsInAnyOrder(new TopicPartition(matchingTopicOne, 0), new TopicPartition(matchingTopicTwo, 0), new TopicPartition(matchingTopicTwo, 1)));
    }
    
    private PartitionInfo createPartitionInfo(String topic, int partition) {
        return new PartitionInfo(topic, partition, null, null, null);
    }
}
