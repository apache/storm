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


import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.internal.OffsetManager;
import org.apache.storm.kafka.spout.metrics2.KafkaOffsetPartitionAndTopicMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class KafkaOffsetPartitionAndTopicMetricsTest {

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
        TopicPartition topicAPartition2 = new TopicPartition("topicA", 2);
        TopicPartition topicBPartition1 = new TopicPartition("topicB", 1);
        TopicPartition topicBPartition2 = new TopicPartition("topicB", 2);

        ListOffsetsResult.ListOffsetsResultInfo topicAPartition1LatestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(100, System.currentTimeMillis(), Optional.empty());
        ListOffsetsResult.ListOffsetsResultInfo topicAPartition2LatestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(200, System.currentTimeMillis(), Optional.empty());
        ListOffsetsResult.ListOffsetsResultInfo topicBPartition1LatestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(300, System.currentTimeMillis(), Optional.empty());
        ListOffsetsResult.ListOffsetsResultInfo topicBPartition2LatestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(400, System.currentTimeMillis(), Optional.empty());

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> topicPartitionLatestListOffsetsResultInfoMap = new HashMap<>();
        topicPartitionLatestListOffsetsResultInfoMap.put(topicAPartition1, topicAPartition1LatestListOffsetsResultInfo);
        topicPartitionLatestListOffsetsResultInfoMap.put(topicAPartition2, topicAPartition2LatestListOffsetsResultInfo);
        topicPartitionLatestListOffsetsResultInfoMap.put(topicBPartition1, topicBPartition1LatestListOffsetsResultInfo);
        topicPartitionLatestListOffsetsResultInfoMap.put(topicBPartition2, topicBPartition2LatestListOffsetsResultInfo);

        when(kafkaFuture.get()).thenReturn(topicPartitionLatestListOffsetsResultInfoMap);

        ListOffsetsResult listOffsetsResult = mock(ListOffsetsResult.class);
        when(listOffsetsResult.all()).thenReturn(kafkaFuture);

        admin = mock(Admin.class);
        when(admin.listOffsets(anyMap())).thenReturn(listOffsetsResult);

        OffsetManager offsetManagertopicAPartition1 = mock(OffsetManager.class);
        when(offsetManagertopicAPartition1.getCommittedOffset()).thenReturn(90L);

        OffsetManager offsetManagertopicAPartition2 = mock(OffsetManager.class);
        when(offsetManagertopicAPartition2.getCommittedOffset()).thenReturn(170L);

        OffsetManager offsetManagertopicBPartition1 = mock(OffsetManager.class);
        when(offsetManagertopicBPartition1.getCommittedOffset()).thenReturn(200L);

        OffsetManager offsetManagertopicBPartition2 = mock(OffsetManager.class);
        when(offsetManagertopicBPartition2.getCommittedOffset()).thenReturn(350L);

        offsetManagers = new HashMap<>();
        offsetManagers.put(topicAPartition1, offsetManagertopicAPartition1);
        offsetManagers.put(topicAPartition2, offsetManagertopicAPartition2);
        offsetManagers.put(topicBPartition1, offsetManagertopicBPartition1);
        offsetManagers.put(topicBPartition2, offsetManagertopicBPartition2);

        assignment = new HashSet<>();
        assignment.add(topicAPartition1);
        assignment.add(topicAPartition2);
        assignment.add(topicBPartition1);
        assignment.add(topicBPartition2);


        KafkaOffsetPartitionAndTopicMetrics kafkaOffsetPartitionAndTopicMetrics = new KafkaOffsetPartitionAndTopicMetrics(() -> Collections.unmodifiableMap(offsetManagers), () -> admin, assignment);
        Map<String, Metric> result = kafkaOffsetPartitionAndTopicMetrics.getMetrics();
        Gauge g1 = (Gauge) result.get("topicA/partition_1/spoutLag");
        Gauge g2 = (Gauge) result.get("topicA/partition_2/spoutLag");
        Gauge g3 = (Gauge) result.get("topicB/partition_1/spoutLag");
        Gauge g4 = (Gauge) result.get("topicB/partition_2/spoutLag");
        assertEquals(10L, g1.getValue());
        assertEquals(30L, g2.getValue());
        assertEquals(100L, g3.getValue());
        assertEquals(50L, g4.getValue());

        Gauge gATotal = (Gauge) result.get("topicA/totalSpoutLag");
        assertEquals(40L, gATotal.getValue());
        Gauge gBTotal = (Gauge) result.get("topicB/totalSpoutLag");
        assertEquals(150L, gBTotal.getValue());

        //get the metrics a second time. Values should be the same. In particular, the total values for the topic should not accumulate. Each call to getMetrics should reset the total values.

        result = kafkaOffsetPartitionAndTopicMetrics.getMetrics();
        g1 = (Gauge) result.get("topicA/partition_1/spoutLag");
        g2 = (Gauge) result.get("topicA/partition_2/spoutLag");
        g3 = (Gauge) result.get("topicB/partition_1/spoutLag");
        g4 = (Gauge) result.get("topicB/partition_2/spoutLag");
        assertEquals(g1.getValue(), 10L);
        assertEquals(g2.getValue(), 30L);
        assertEquals(g3.getValue(), 100L);
        assertEquals(g4.getValue(), 50L);

        gATotal = (Gauge) result.get("topicA/totalSpoutLag");
        assertEquals(gATotal.getValue(), 40L);
        gBTotal = (Gauge) result.get("topicB/totalSpoutLag");
        assertEquals(gBTotal.getValue(), 150L);

        //get partition records

        ListOffsetsResult.ListOffsetsResultInfo topicAPartition1EarliestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(1, System.currentTimeMillis(), Optional.empty());
        ListOffsetsResult.ListOffsetsResultInfo topicAPartition2EarliestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(2, System.currentTimeMillis(), Optional.empty());
        ListOffsetsResult.ListOffsetsResultInfo topicBPartition1EarliestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(3, System.currentTimeMillis(), Optional.empty());
        ListOffsetsResult.ListOffsetsResultInfo topicBPartition2EarliestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(4, System.currentTimeMillis(), Optional.empty());

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> topicPartitionEarliestListOffsetsResultInfoMap = new HashMap<>();
        topicPartitionEarliestListOffsetsResultInfoMap.put(topicAPartition1, topicAPartition1EarliestListOffsetsResultInfo);
        topicPartitionEarliestListOffsetsResultInfoMap.put(topicAPartition2, topicAPartition2EarliestListOffsetsResultInfo);
        topicPartitionEarliestListOffsetsResultInfoMap.put(topicBPartition1, topicBPartition1EarliestListOffsetsResultInfo);
        topicPartitionEarliestListOffsetsResultInfoMap.put(topicBPartition2, topicBPartition2EarliestListOffsetsResultInfo);

        //mock consecutive calls. Each call to the recordsInPartition gauge will call kafkaFuture.get() twice
        when(kafkaFuture.get()).thenReturn(topicPartitionLatestListOffsetsResultInfoMap, topicPartitionEarliestListOffsetsResultInfoMap,
                topicPartitionLatestListOffsetsResultInfoMap, topicPartitionEarliestListOffsetsResultInfoMap,
                topicPartitionLatestListOffsetsResultInfoMap, topicPartitionEarliestListOffsetsResultInfoMap,
                topicPartitionLatestListOffsetsResultInfoMap, topicPartitionEarliestListOffsetsResultInfoMap);

        result = kafkaOffsetPartitionAndTopicMetrics.getMetrics();
        g1 = (Gauge) result.get("topicA/partition_1/recordsInPartition");
        g2 = (Gauge) result.get("topicA/partition_2/recordsInPartition");
        g3 = (Gauge) result.get("topicB/partition_1/recordsInPartition");
        g4 = (Gauge) result.get("topicB/partition_2/recordsInPartition");
        assertEquals(99L, g1.getValue());
        assertEquals(198L, g2.getValue());
        assertEquals(297L, g3.getValue());
        assertEquals(396L, g4.getValue());

        gATotal = (Gauge) result.get("topicA/totalRecordsInPartitions");
        assertEquals(297L, gATotal.getValue());
        gBTotal = (Gauge) result.get("topicB/totalRecordsInPartitions");
        assertEquals(693L, gBTotal.getValue());

    }

    @Test
    public void registerMetricsGetEarliestAndLatest() throws ExecutionException, InterruptedException {

        TopicPartition topicAPartition1 = new TopicPartition("topicA", 1);
        TopicPartition topicAPartition2 = new TopicPartition("topicA", 2);
        TopicPartition topicBPartition1 = new TopicPartition("topicB", 1);
        TopicPartition topicBPartition2 = new TopicPartition("topicB", 2);

        ListOffsetsResult.ListOffsetsResultInfo topicAPartition1EarliestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(1, System.currentTimeMillis(), Optional.empty());
        ListOffsetsResult.ListOffsetsResultInfo topicAPartition2EarliestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(1, System.currentTimeMillis(), Optional.empty());
        ListOffsetsResult.ListOffsetsResultInfo topicBPartition1EarliestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(1, System.currentTimeMillis(), Optional.empty());
        ListOffsetsResult.ListOffsetsResultInfo topicBPartition2EarliestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(1, System.currentTimeMillis(), Optional.empty());

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> topicPartitionEarliestListOffsetsResultInfoMap = new HashMap<>();
        topicPartitionEarliestListOffsetsResultInfoMap.put(topicAPartition1, topicAPartition1EarliestListOffsetsResultInfo);
        topicPartitionEarliestListOffsetsResultInfoMap.put(topicAPartition2, topicAPartition2EarliestListOffsetsResultInfo);
        topicPartitionEarliestListOffsetsResultInfoMap.put(topicBPartition1, topicBPartition1EarliestListOffsetsResultInfo);
        topicPartitionEarliestListOffsetsResultInfoMap.put(topicBPartition2, topicBPartition2EarliestListOffsetsResultInfo);

        when(kafkaFuture.get()).thenReturn(topicPartitionEarliestListOffsetsResultInfoMap);

        ListOffsetsResult listOffsetsResult = mock(ListOffsetsResult.class);
        when(listOffsetsResult.all()).thenReturn(kafkaFuture);

        admin = mock(Admin.class);
        when(admin.listOffsets(anyMap())).thenReturn(listOffsetsResult);

        OffsetManager offsetManagertopicAPartition1 = mock(OffsetManager.class);
        when(offsetManagertopicAPartition1.getLatestEmittedOffset()).thenReturn(50L);
        when(offsetManagertopicAPartition1.getCommittedOffset()).thenReturn(40L);

        OffsetManager offsetManagertopicAPartition2 = mock(OffsetManager.class);
        when(offsetManagertopicAPartition2.getLatestEmittedOffset()).thenReturn(100L);
        when(offsetManagertopicAPartition2.getCommittedOffset()).thenReturn(90L);

        OffsetManager offsetManagertopicBPartition1 = mock(OffsetManager.class);
        when(offsetManagertopicBPartition1.getLatestEmittedOffset()).thenReturn(150L);
        when(offsetManagertopicBPartition1.getCommittedOffset()).thenReturn(149L);

        OffsetManager offsetManagertopicBPartition2 = mock(OffsetManager.class);
        when(offsetManagertopicBPartition2.getLatestEmittedOffset()).thenReturn(200L);
        when(offsetManagertopicBPartition2.getCommittedOffset()).thenReturn(200L);

        offsetManagers = new HashMap<>();
        offsetManagers.put(topicAPartition1, offsetManagertopicAPartition1);
        offsetManagers.put(topicAPartition2, offsetManagertopicAPartition2);
        offsetManagers.put(topicBPartition1, offsetManagertopicBPartition1);
        offsetManagers.put(topicBPartition2, offsetManagertopicBPartition2);

        assignment = new HashSet<>();
        assignment.add(topicAPartition1);
        assignment.add(topicAPartition2);
        assignment.add(topicBPartition1);
        assignment.add(topicBPartition2);

        KafkaOffsetPartitionAndTopicMetrics kafkaOffsetPartitionAndTopicMetrics = new KafkaOffsetPartitionAndTopicMetrics(() -> Collections.unmodifiableMap(offsetManagers), () -> admin, assignment);
        Map<String, Metric> result = kafkaOffsetPartitionAndTopicMetrics.getMetrics();
        Gauge g1 = (Gauge) result.get("topicA/partition_1/earliestTimeOffset");
        Gauge g2 = (Gauge) result.get("topicA/partition_2/earliestTimeOffset");
        Gauge g3 = (Gauge) result.get("topicB/partition_1/earliestTimeOffset");
        Gauge g4 = (Gauge) result.get("topicB/partition_2/earliestTimeOffset");
        assertEquals(g1.getValue(), 1L);
        assertEquals(g2.getValue(), 1L);
        assertEquals(g3.getValue(), 1L);
        assertEquals(g4.getValue(), 1L);

        Gauge gATotal = (Gauge) result.get("topicA/totalEarliestTimeOffset");
        assertEquals(2L, gATotal.getValue());
        Gauge gBTotal = (Gauge) result.get("topicB/totalEarliestTimeOffset");
        assertEquals(2L, gBTotal.getValue());

        //get the metrics a second time. Values should be the same. In particular, the total values for the topic should not accumulate. Each call to getMetrics should reset the total values.

        result = kafkaOffsetPartitionAndTopicMetrics.getMetrics();

        g1 = (Gauge) result.get("topicA/partition_1/earliestTimeOffset");
        g2 = (Gauge) result.get("topicA/partition_2/earliestTimeOffset");
        g3 = (Gauge) result.get("topicB/partition_1/earliestTimeOffset");
        g4 = (Gauge) result.get("topicB/partition_2/earliestTimeOffset");
        assertEquals(g1.getValue(), 1L);
        assertEquals(g2.getValue(), 1L);
        assertEquals(g3.getValue(), 1L);
        assertEquals(g4.getValue(), 1L);

        gATotal = (Gauge) result.get("topicA/totalEarliestTimeOffset");
        assertEquals(2L, gATotal.getValue());
        gBTotal = (Gauge) result.get("topicB/totalEarliestTimeOffset");
        assertEquals(2L, gBTotal.getValue());

        //get the latest offsets

        ListOffsetsResult.ListOffsetsResultInfo topicAPartition1LatestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(100, System.currentTimeMillis(), Optional.empty());
        ListOffsetsResult.ListOffsetsResultInfo topicAPartition2LatestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(200, System.currentTimeMillis(), Optional.empty());
        ListOffsetsResult.ListOffsetsResultInfo topicBPartition1LatestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(300, System.currentTimeMillis(), Optional.empty());
        ListOffsetsResult.ListOffsetsResultInfo topicBPartition2LatestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(400, System.currentTimeMillis(), Optional.empty());

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> topicPartitionLatestListOffsetsResultInfoMap = new HashMap<>();
        topicPartitionLatestListOffsetsResultInfoMap.put(topicAPartition1, topicAPartition1LatestListOffsetsResultInfo);
        topicPartitionLatestListOffsetsResultInfoMap.put(topicAPartition2, topicAPartition2LatestListOffsetsResultInfo);
        topicPartitionLatestListOffsetsResultInfoMap.put(topicBPartition1, topicBPartition1LatestListOffsetsResultInfo);
        topicPartitionLatestListOffsetsResultInfoMap.put(topicBPartition2, topicBPartition2LatestListOffsetsResultInfo);

        when(kafkaFuture.get()).thenReturn(topicPartitionLatestListOffsetsResultInfoMap);

        result = kafkaOffsetPartitionAndTopicMetrics.getMetrics();
        g1 = (Gauge) result.get("topicA/partition_1/latestTimeOffset");
        g2 = (Gauge) result.get("topicA/partition_2/latestTimeOffset");
        g3 = (Gauge) result.get("topicB/partition_1/latestTimeOffset");
        g4 = (Gauge) result.get("topicB/partition_2/latestTimeOffset");
        assertEquals(100L, g1.getValue());
        assertEquals(200L, g2.getValue());
        assertEquals(300L, g3.getValue());
        assertEquals(400L, g4.getValue());

        gATotal = (Gauge) result.get("topicA/totalLatestTimeOffset");
        assertEquals(300L, gATotal.getValue());
        gBTotal = (Gauge) result.get("topicB/totalLatestTimeOffset");
        assertEquals(700L, gBTotal.getValue());

        g1 = (Gauge) result.get("topicA/partition_1/latestEmittedOffset");
        g2 = (Gauge) result.get("topicA/partition_2/latestEmittedOffset");
        g3 = (Gauge) result.get("topicB/partition_1/latestEmittedOffset");
        g4 = (Gauge) result.get("topicB/partition_2/latestEmittedOffset");
        assertEquals(50L, g1.getValue());
        assertEquals(100L, g2.getValue());
        assertEquals(150L, g3.getValue());
        assertEquals(200L, g4.getValue());

        gATotal = (Gauge) result.get("topicA/totalLatestEmittedOffset");
        assertEquals(150L, gATotal.getValue());
        gBTotal = (Gauge) result.get("topicB/totalLatestEmittedOffset");
        assertEquals(350L, gBTotal.getValue());

        g1 = (Gauge) result.get("topicA/partition_1/latestCompletedOffset");
        g2 = (Gauge) result.get("topicA/partition_2/latestCompletedOffset");
        g3 = (Gauge) result.get("topicB/partition_1/latestCompletedOffset");
        g4 = (Gauge) result.get("topicB/partition_2/latestCompletedOffset");
        assertEquals(40L, g1.getValue());
        assertEquals(90L, g2.getValue());
        assertEquals(149L, g3.getValue());
        assertEquals(200L, g4.getValue());

        gATotal = (Gauge) result.get("topicA/totalLatestCompletedOffset");
        assertEquals(130L, gATotal.getValue());
        gBTotal = (Gauge) result.get("topicB/totalLatestCompletedOffset");
        assertEquals(349L, gBTotal.getValue());
    }
}
