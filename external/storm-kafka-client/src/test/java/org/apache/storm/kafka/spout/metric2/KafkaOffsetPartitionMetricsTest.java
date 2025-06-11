/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.kafka.spout.metric2;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.internal.OffsetManager;
import org.apache.storm.kafka.spout.metrics2.KafkaOffsetPartitionMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class KafkaOffsetPartitionMetricsTest {

    private Set<TopicPartition> assignment;
    private Admin admin = mock(Admin.class);
    private HashMap<TopicPartition, OffsetManager> offsetManagers;
    private KafkaFuture kafkaFuture = mock(KafkaFuture.class);

    @BeforeEach
    public void initializeTests() {
        reset(admin, kafkaFuture);

    }

    @Test
    public void registerMetricsGetSpoutLagAndPartitionRecords() throws ExecutionException, InterruptedException {

        TopicPartition topicAPartition1 = new TopicPartition("topicA", 1);

        ListOffsetsResult.ListOffsetsResultInfo topicAPartition1LatestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(100, System.currentTimeMillis(), Optional.empty());

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> topicPartitionLatestListOffsetsResultInfoMap = new HashMap<>();
        topicPartitionLatestListOffsetsResultInfoMap.put(topicAPartition1, topicAPartition1LatestListOffsetsResultInfo);

        when(kafkaFuture.get()).thenReturn(topicPartitionLatestListOffsetsResultInfoMap);

        ListOffsetsResult listOffsetsResult = mock(ListOffsetsResult.class);
        when(listOffsetsResult.all()).thenReturn(kafkaFuture);

        admin = mock(Admin.class);
        when(admin.listOffsets(anyMap())).thenReturn(listOffsetsResult);

        OffsetManager offsetManagertopicAPartition1 = mock(OffsetManager.class);
        when(offsetManagertopicAPartition1.getCommittedOffset()).thenReturn(90L);


        offsetManagers = new HashMap<>();
        offsetManagers.put(topicAPartition1, offsetManagertopicAPartition1);

        KafkaOffsetPartitionMetrics kafkaOffsetPartitionAndTopicMetrics = new KafkaOffsetPartitionMetrics(() -> Collections.unmodifiableMap(offsetManagers), () -> admin, topicAPartition1);
        Map<String, Metric> result = kafkaOffsetPartitionAndTopicMetrics.getMetrics();
        Gauge g1 = (Gauge) result.get("topicA/partition_1/spoutLag");
        assertEquals(10L, g1.getValue());


        //get partition records

        ListOffsetsResult.ListOffsetsResultInfo topicAPartition1EarliestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(1, System.currentTimeMillis(), Optional.empty());

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> topicPartitionEarliestListOffsetsResultInfoMap = new HashMap<>();
        topicPartitionEarliestListOffsetsResultInfoMap.put(topicAPartition1, topicAPartition1EarliestListOffsetsResultInfo);

        //mock consecutive calls. Each call to the recordsInPartition gauge will call kafkaFuture.get() twice
        when(kafkaFuture.get()).thenReturn(topicPartitionLatestListOffsetsResultInfoMap, topicPartitionEarliestListOffsetsResultInfoMap);

        result = kafkaOffsetPartitionAndTopicMetrics.getMetrics();
        g1 = (Gauge) result.get("topicA/partition_1/recordsInPartition");
        assertEquals(99L, g1.getValue());
    }

    @Test
    public void registerMetricsGetEarliestAndLatest() throws ExecutionException, InterruptedException {

        TopicPartition topicAPartition1 = new TopicPartition("topicA", 1);

        ListOffsetsResult.ListOffsetsResultInfo topicAPartition1EarliestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(1, System.currentTimeMillis(), Optional.empty());

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> topicPartitionEarliestListOffsetsResultInfoMap = new HashMap<>();
        topicPartitionEarliestListOffsetsResultInfoMap.put(topicAPartition1, topicAPartition1EarliestListOffsetsResultInfo);

        when(kafkaFuture.get()).thenReturn(topicPartitionEarliestListOffsetsResultInfoMap);

        ListOffsetsResult listOffsetsResult = mock(ListOffsetsResult.class);
        when(listOffsetsResult.all()).thenReturn(kafkaFuture);

        admin = mock(Admin.class);
        when(admin.listOffsets(anyMap())).thenReturn(listOffsetsResult);

        OffsetManager offsetManagertopicAPartition1 = mock(OffsetManager.class);
        when(offsetManagertopicAPartition1.getLatestEmittedOffset()).thenReturn(50L);
        when(offsetManagertopicAPartition1.getCommittedOffset()).thenReturn(40L);


        offsetManagers = new HashMap<>();
        offsetManagers.put(topicAPartition1, offsetManagertopicAPartition1);

        assignment = new HashSet<>();
        assignment.add(topicAPartition1);

        KafkaOffsetPartitionMetrics kafkaOffsetPartitionAndTopicMetrics = new KafkaOffsetPartitionMetrics(() -> Collections.unmodifiableMap(offsetManagers), () -> admin, topicAPartition1);
        Map<String, Metric> result = kafkaOffsetPartitionAndTopicMetrics.getMetrics();
        Gauge g1 = (Gauge) result.get("topicA/partition_1/earliestTimeOffset");

        assertEquals(g1.getValue(), 1L);

        //get the latest offsets

        ListOffsetsResult.ListOffsetsResultInfo topicAPartition1LatestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(100, System.currentTimeMillis(), Optional.empty());


        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> topicPartitionLatestListOffsetsResultInfoMap = new HashMap<>();
        topicPartitionLatestListOffsetsResultInfoMap.put(topicAPartition1, topicAPartition1LatestListOffsetsResultInfo);

        when(kafkaFuture.get()).thenReturn(topicPartitionLatestListOffsetsResultInfoMap);

        result = kafkaOffsetPartitionAndTopicMetrics.getMetrics();

        g1 = (Gauge) result.get("topicA/partition_1/latestTimeOffset");
        assertEquals(100L, g1.getValue());

        g1 = (Gauge) result.get("topicA/partition_1/latestEmittedOffset");
        assertEquals(50L, g1.getValue());


        g1 = (Gauge) result.get("topicA/partition_1/latestCompletedOffset");
        assertEquals(40L, g1.getValue());

    }
}
