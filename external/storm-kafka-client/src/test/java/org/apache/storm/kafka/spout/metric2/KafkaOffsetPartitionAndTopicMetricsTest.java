package org.apache.storm.kafka.spout.metric2;

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

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class KafkaOffsetPartitionAndTopicMetricsTest {

    private Set<TopicPartition> assignment;
    private Admin admin = mock(Admin.class);
    private HashMap<TopicPartition, OffsetManager> offsetManagers;
    private ListOffsetsResult listOffsetsResultEarliest;
    private KafkaFuture kafkaFuture = mock(KafkaFuture.class);

    @BeforeEach
    public void initializeTests() {
        reset(admin, kafkaFuture);

    }

    @Test
    public void registerMetricsGetLatest() throws ExecutionException, InterruptedException {

        TopicPartition tAp1 = new TopicPartition("topicA", 1);
        TopicPartition tAp2 = new TopicPartition("topicA", 2);
        TopicPartition tBp1 = new TopicPartition("topicB", 1);
        TopicPartition tBp2 = new TopicPartition("topicB", 2);

        ListOffsetsResult.ListOffsetsResultInfo tAp1LatestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(100, System.currentTimeMillis(), Optional.empty());
        ListOffsetsResult.ListOffsetsResultInfo tAp2LatestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(200, System.currentTimeMillis(), Optional.empty());
        ListOffsetsResult.ListOffsetsResultInfo tBp1LatestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(300, System.currentTimeMillis(), Optional.empty());
        ListOffsetsResult.ListOffsetsResultInfo tBp2LatestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(400, System.currentTimeMillis(), Optional.empty());

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> topicPartitionLatestListOffsetsResultInfoMap;

        topicPartitionLatestListOffsetsResultInfoMap = new HashMap<>();
        topicPartitionLatestListOffsetsResultInfoMap.put(tAp1, tAp1LatestListOffsetsResultInfo);
        topicPartitionLatestListOffsetsResultInfoMap.put(tAp2, tAp2LatestListOffsetsResultInfo);
        topicPartitionLatestListOffsetsResultInfoMap.put(tBp1, tBp1LatestListOffsetsResultInfo);
        topicPartitionLatestListOffsetsResultInfoMap.put(tBp2, tBp2LatestListOffsetsResultInfo);

        when(kafkaFuture.get()).thenReturn(topicPartitionLatestListOffsetsResultInfoMap);

        listOffsetsResultEarliest = mock(ListOffsetsResult.class);
        when(listOffsetsResultEarliest.all()).thenReturn(kafkaFuture);

        admin = mock(Admin.class);
        when(admin.listOffsets(anyMap())).thenReturn(listOffsetsResultEarliest);

        OffsetManager offsetManagerTaP1 = mock(OffsetManager.class);
        when(offsetManagerTaP1.getCommittedOffset()).thenReturn(90L);

        OffsetManager offsetManagerTaP2 = mock(OffsetManager.class);
        when(offsetManagerTaP2.getCommittedOffset()).thenReturn(170L);

        OffsetManager offsetManagerTbP1 = mock(OffsetManager.class);
        when(offsetManagerTbP1.getCommittedOffset()).thenReturn(200L);

        OffsetManager offsetManagerTbP2 = mock(OffsetManager.class);
        when(offsetManagerTbP2.getCommittedOffset()).thenReturn(350L);

        offsetManagers = new HashMap<>();
        offsetManagers.put(tAp1, offsetManagerTaP1);
        offsetManagers.put(tAp2, offsetManagerTaP2);
        offsetManagers.put(tBp1, offsetManagerTbP1);
        offsetManagers.put(tBp2, offsetManagerTbP2);

        assignment = new HashSet<>();
        assignment.add(tAp1);
        assignment.add(tAp2);
        assignment.add(tBp1);
        assignment.add(tBp2);


        KafkaOffsetPartitionAndTopicMetrics kafkaOffsetPartitionAndTopicMetrics = new KafkaOffsetPartitionAndTopicMetrics(() -> Collections.unmodifiableMap(offsetManagers), () -> admin, assignment);
        Map<String, Metric> result = kafkaOffsetPartitionAndTopicMetrics.getMetrics();
        Gauge g1 = (Gauge) result.get("topicA/partition_1/spoutLag");
        Gauge g2 = (Gauge) result.get("topicA/partition_2/spoutLag");
        Gauge g3 = (Gauge) result.get("topicB/partition_1/spoutLag");
        Gauge g4 = (Gauge) result.get("topicB/partition_2/spoutLag");
        assertEquals(g1.getValue(), 10L);
        assertEquals(g2.getValue(), 30L);
        assertEquals(g3.getValue(), 100L);
        assertEquals(g4.getValue(), 50L);

        Gauge gATotal = (Gauge) result.get("topicA/totalSpoutLag");
        assertEquals(gATotal.getValue(), 40L);
        Gauge gBTotal = (Gauge) result.get("topicB/totalSpoutLag");
        assertEquals(gBTotal.getValue(), 150L);

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


    }

    @Test
    public void registerMetricsGetEarliest() throws ExecutionException, InterruptedException {

        TopicPartition tAp1 = new TopicPartition("topicA", 1);
        TopicPartition tAp2 = new TopicPartition("topicA", 2);
        TopicPartition tBp1 = new TopicPartition("topicB", 1);
        TopicPartition tBp2 = new TopicPartition("topicB", 2);

        ListOffsetsResult.ListOffsetsResultInfo tAp1EarliestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(1, System.currentTimeMillis(), Optional.empty());
        ListOffsetsResult.ListOffsetsResultInfo tAp2EarliestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(1, System.currentTimeMillis(), Optional.empty());
        ListOffsetsResult.ListOffsetsResultInfo tBp1EarliestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(1, System.currentTimeMillis(), Optional.empty());
        ListOffsetsResult.ListOffsetsResultInfo tBp2EarliestListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(1, System.currentTimeMillis(), Optional.empty());

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> topicPartitionEarliestListOffsetsResultInfoMap;

        topicPartitionEarliestListOffsetsResultInfoMap = new HashMap<>();
        topicPartitionEarliestListOffsetsResultInfoMap.put(tAp1, tAp1EarliestListOffsetsResultInfo);
        topicPartitionEarliestListOffsetsResultInfoMap.put(tAp2, tAp2EarliestListOffsetsResultInfo);
        topicPartitionEarliestListOffsetsResultInfoMap.put(tBp1, tBp1EarliestListOffsetsResultInfo);
        topicPartitionEarliestListOffsetsResultInfoMap.put(tBp2, tBp2EarliestListOffsetsResultInfo);

        when(kafkaFuture.get()).thenReturn(topicPartitionEarliestListOffsetsResultInfoMap);

        listOffsetsResultEarliest = mock(ListOffsetsResult.class);
        when(listOffsetsResultEarliest.all()).thenReturn(kafkaFuture);

        admin = mock(Admin.class);
        when(admin.listOffsets(anyMap())).thenReturn(listOffsetsResultEarliest);

        offsetManagers = new HashMap<>();

        assignment = new HashSet<>();
        assignment.add(tAp1);
        assignment.add(tAp2);
        assignment.add(tBp1);
        assignment.add(tBp2);

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
    }
}
