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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.config.builder.SingleTopicKafkaSpoutConfiguration;
import org.apache.storm.task.TopologyContext;
import org.junit.Test;

public class RoundRobinManualPartitionerTest {

    private TopicPartition createTp(int partition) {
        return new TopicPartition(SingleTopicKafkaSpoutConfiguration.TOPIC, partition);
    }
    
    private Set<TopicPartition> partitionsToTps(int[] expectedPartitions) {
        Set<TopicPartition> expectedTopicPartitions = new HashSet<>();
        for(int i = 0; i < expectedPartitions.length; i++) {
            expectedTopicPartitions.add(createTp(expectedPartitions[i]));
        }
        return expectedTopicPartitions;
    }
    
    @Test
    public void testRoundRobinPartitioning() {
        List<TopicPartition> allPartitions = new ArrayList<>();
        for(int i = 0; i < 11; i++) {
            allPartitions.add(createTp(i));
        }
        List<TopologyContext> contextMocks = new ArrayList<>();
        String thisComponentId = "A spout";
        List<Integer> allTasks = Arrays.asList(new Integer[]{0, 1, 2});
        for(int i = 0; i < 3; i++) {
            TopologyContext contextMock = mock(TopologyContext.class);
            when(contextMock.getThisTaskIndex()).thenReturn(i);
            when(contextMock.getThisComponentId()).thenReturn(thisComponentId);
            when(contextMock.getComponentTasks(thisComponentId)).thenReturn(allTasks);
            contextMocks.add(contextMock);
        }
        RoundRobinManualPartitioner partitioner = new RoundRobinManualPartitioner();
        
        Set<TopicPartition> partitionsForFirstTask = partitioner.getPartitionsForThisTask(allPartitions, contextMocks.get(0));
        assertThat(partitionsForFirstTask, is(partitionsToTps(new int[]{0, 3, 6, 9})));
        
        Set<TopicPartition> partitionsForSecondTask = partitioner.getPartitionsForThisTask(allPartitions, contextMocks.get(1));
        assertThat(partitionsForSecondTask, is(partitionsToTps(new int[]{1, 4, 7, 10})));
        
        Set<TopicPartition> partitionsForThirdTask = partitioner.getPartitionsForThisTask(allPartitions, contextMocks.get(2));
        assertThat(partitionsForThirdTask, is(partitionsToTps(new int[]{2, 5, 8})));
    }
    
}
