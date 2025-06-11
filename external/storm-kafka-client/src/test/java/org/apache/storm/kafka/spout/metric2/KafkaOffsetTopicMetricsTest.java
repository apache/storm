/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.kafka.spout.metric2;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.internal.OffsetManager;
import org.apache.storm.kafka.spout.metrics2.KafkaOffsetTopicMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class KafkaOffsetTopicMetricsTest {

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

        offsetManagers = new HashMap<>();
        offsetManagers.put(topicAPartition1, offsetManagertopicAPartition1);
        offsetManagers.put(topicAPartition2, offsetManagertopicAPartition2);

        assignment = new HashSet<>();
        assignment.add(topicAPartition1);
        assignment.add(topicAPartition2);
        assignment.add(topicBPartition1);
        assignment.add(topicBPartition2);


        KafkaOffsetTopicMetrics kafkaOffsetTopicMetricsA = new KafkaOffsetTopicMetrics("topicA", () -> Collections.unmodifiableMap(offsetManagers), () -> admin, assignment);
        Map<String, Metric> result = kafkaOffsetTopicMetricsA.getMetrics();
        Gauge g1 = (Gauge) result.get("topicA/totalSpoutLag");
        assertEquals(40L, g1.getValue());

        //get again the values from the Gauge. Values cannot change
        g1 = (Gauge) result.get("topicA/totalSpoutLag");
        assertEquals(40L, g1.getValue());

        assertNull(result.get("topicB/totalSpoutLag"));

        //get topic records

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
        when(kafkaFuture.get()).thenReturn(
                topicPartitionLatestListOffsetsResultInfoMap, topicPartitionEarliestListOffsetsResultInfoMap,
                topicPartitionLatestListOffsetsResultInfoMap, topicPartitionEarliestListOffsetsResultInfoMap,
                topicPartitionLatestListOffsetsResultInfoMap, topicPartitionEarliestListOffsetsResultInfoMap,
                topicPartitionLatestListOffsetsResultInfoMap, topicPartitionEarliestListOffsetsResultInfoMap);

        Gauge gATotal = (Gauge) result.get("topicA/totalRecordsInPartitions");
        assertEquals(297L, gATotal.getValue());

        //get again the values from the Gauge. Values cannot change
         gATotal = (Gauge) result.get("topicA/totalRecordsInPartitions");
        assertEquals(297L, gATotal.getValue());

        assertNull(result.get("topicB/totalRecordsInPartitions"));
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


        offsetManagers = new HashMap<>();
        offsetManagers.put(topicAPartition1, offsetManagertopicAPartition1);
        offsetManagers.put(topicAPartition2, offsetManagertopicAPartition2);

        assignment = new HashSet<>();
        assignment.add(topicAPartition1);
        assignment.add(topicAPartition2);
        assignment.add(topicBPartition1);
        assignment.add(topicBPartition2);

        KafkaOffsetTopicMetrics kafkaOffsetPartitionAndTopicMetrics = new KafkaOffsetTopicMetrics("topicA",() -> Collections.unmodifiableMap(offsetManagers), () -> admin, assignment);
        Map<String, Metric> result = kafkaOffsetPartitionAndTopicMetrics.getMetrics();

        Gauge gATotal = (Gauge) result.get("topicA/totalEarliestTimeOffset");
        assertEquals(2L, gATotal.getValue());
        assertNull(result.get("topicB/totalEarliestTimeOffset"));

        //get the metrics a second time. Values should be the same

        gATotal = (Gauge) result.get("topicA/totalEarliestTimeOffset");
        assertEquals(2L, gATotal.getValue());
        assertNull(result.get("topicB/totalEarliestTimeOffset"));

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

        gATotal = (Gauge) result.get("topicA/totalLatestTimeOffset");
        assertEquals(300L, gATotal.getValue());
        assertNull((result.get("topicB/totalLatestTimeOffset")));

        gATotal = (Gauge) result.get("topicA/totalLatestEmittedOffset");
        assertEquals(150L, gATotal.getValue());
        assertNull( result.get("topicB/totalLatestEmittedOffset"));


        gATotal = (Gauge) result.get("topicA/totalLatestCompletedOffset");
        assertEquals(130L, gATotal.getValue());
        assertNull(result.get("topiBA/totalLatestCompletedOffset"));

        //get the metrics a second time. Values should be the same

        gATotal = (Gauge) result.get("topicA/totalLatestTimeOffset");
        assertEquals(300L, gATotal.getValue());
        assertNull((result.get("topicB/totalLatestTimeOffset")));

        gATotal = (Gauge) result.get("topicA/totalLatestEmittedOffset");
        assertEquals(150L, gATotal.getValue());
        assertNull( result.get("topicB/totalLatestEmittedOffset"));


        gATotal = (Gauge) result.get("topicA/totalLatestCompletedOffset");
        assertEquals(130L, gATotal.getValue());
        assertNull(result.get("topiBA/totalLatestCompletedOffset"));

    }
}
