package org.apache.storm.kafka.spout.metric2;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.internal.OffsetManager;
import org.apache.storm.kafka.spout.metrics2.KafkaOffsetPartitionAndTopicMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class KafkaOffsetPartitionAndTopicMetricsTest {

    private Set<TopicPartition> assignment;
    private Admin admin;
    private HashMap<TopicPartition, OffsetManager> offsetManagers;
    private ListOffsetsResult listOffsetsResult;
    private KafkaFuture<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> kafkaFuture;
    private Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> topicPartitionListOffsetsResultInfoMap;

    @Test
    public void registerPartitionAndTopicMetrics() throws ExecutionException, InterruptedException {

        TopicPartition tAp1 = new TopicPartition("topicA",1);
        TopicPartition tAp2 = new TopicPartition("topicA",2);
        TopicPartition tBp1 = new TopicPartition("topicB",1);
        TopicPartition tBp2 = new TopicPartition("topicB",2);

        ListOffsetsResult.ListOffsetsResultInfo tAp1ListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(100,System.currentTimeMillis(),Optional.empty());
        ListOffsetsResult.ListOffsetsResultInfo tAp2ListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(200,System.currentTimeMillis(),Optional.empty());
        ListOffsetsResult.ListOffsetsResultInfo tBp1ListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(300,System.currentTimeMillis(),Optional.empty());
        ListOffsetsResult.ListOffsetsResultInfo tBp2ListOffsetsResultInfo = new ListOffsetsResult.ListOffsetsResultInfo(400,System.currentTimeMillis(),Optional.empty());

        topicPartitionListOffsetsResultInfoMap = new HashMap<>();

        topicPartitionListOffsetsResultInfoMap.put(tAp1,tAp1ListOffsetsResultInfo);
        topicPartitionListOffsetsResultInfoMap.put(tAp2,tAp2ListOffsetsResultInfo);
        topicPartitionListOffsetsResultInfoMap.put(tBp1,tBp1ListOffsetsResultInfo);
        topicPartitionListOffsetsResultInfoMap.put(tBp2,tBp2ListOffsetsResultInfo);

        kafkaFuture = mock(KafkaFuture.class);
        when(kafkaFuture.get()).thenReturn(topicPartitionListOffsetsResultInfoMap);

        listOffsetsResult = mock(ListOffsetsResult.class);
        when(listOffsetsResult.all()).thenReturn(kafkaFuture);

        admin=mock(Admin.class);
        when(admin.listOffsets(anyMap())).thenReturn(listOffsetsResult);

        offsetManagers= new HashMap<>();

        OffsetManager offsetManagerTaP1 = mock(OffsetManager.class);
        when(offsetManagerTaP1.getCommittedOffset()).thenReturn(90L);

        OffsetManager offsetManagerTaP2 = mock(OffsetManager.class);
        when(offsetManagerTaP2.getCommittedOffset()).thenReturn(170L);

        OffsetManager offsetManagerTbP1 = mock(OffsetManager.class);
        when(offsetManagerTbP1.getCommittedOffset()).thenReturn(200L);

        OffsetManager offsetManagerTbP2 = mock(OffsetManager.class);
        when(offsetManagerTbP2.getCommittedOffset()).thenReturn(350L);

        offsetManagers.put(tAp1,offsetManagerTaP1);
        offsetManagers.put(tAp2,offsetManagerTaP2);
        offsetManagers.put(tBp1,offsetManagerTbP1);
        offsetManagers.put(tBp2,offsetManagerTbP2);

        assignment=new HashSet<>();
        assignment.add(tAp1);
        assignment.add(tAp2);
        assignment.add(tBp1);
        assignment.add(tBp2);


        KafkaOffsetPartitionAndTopicMetrics kafkaOffsetPartitionAndTopicMetrics = new KafkaOffsetPartitionAndTopicMetrics(() -> Collections.unmodifiableMap(offsetManagers),() -> admin,assignment);
        Map<String, Metric>  result = kafkaOffsetPartitionAndTopicMetrics.getMetrics();
        Gauge g1= (Gauge) result.get("topicA/partition_1/spoutLag");
        Gauge g2= (Gauge) result.get("topicA/partition_2/spoutLag");
        Gauge g3= (Gauge) result.get("topicB/partition_1/spoutLag");
        Gauge g4= (Gauge) result.get("topicB/partition_2/spoutLag");
        assertEquals(g1.getValue(),10L);
        assertEquals(g2.getValue(),30L);
        assertEquals(g3.getValue(),100L);
        assertEquals(g4.getValue(),50L);

        Gauge gATotal= (Gauge) result.get("topicA/totalSpoutLag");
        assertEquals(gATotal.getValue(),40L);
        Gauge gBTotal= (Gauge) result.get("topicB/totalSpoutLag");
        assertEquals(gBTotal.getValue(),150L);

        //get the metrics a second time. Values should be the same. In particular, the total values for the topic should not accumulate. Each call to getMetrics should reset the total values.

        result = kafkaOffsetPartitionAndTopicMetrics.getMetrics();
         g1= (Gauge) result.get("topicA/partition_1/spoutLag");
         g2= (Gauge) result.get("topicA/partition_2/spoutLag");
         g3= (Gauge) result.get("topicB/partition_1/spoutLag");
         g4= (Gauge) result.get("topicB/partition_2/spoutLag");
        assertEquals(g1.getValue(),10L);
        assertEquals(g2.getValue(),30L);
        assertEquals(g3.getValue(),100L);
        assertEquals(g4.getValue(),50L);

        gATotal= (Gauge) result.get("topicA/totalSpoutLag");
        assertEquals(gATotal.getValue(),40L);
        gBTotal= (Gauge) result.get("topicB/totalSpoutLag");
        assertEquals(gBTotal.getValue(),150L);


    }
}
