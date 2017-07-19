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

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.internal.KafkaConsumerFactory;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;

public class SpoutWithMockedConsumerSetupHelper {
    
    /**
     * Creates, opens and activates a KafkaSpout using a mocked consumer.
     * @param <K> The Kafka key type
     * @param <V> The Kafka value type
     * @param spoutConfig The spout config to use
     * @param topoConf The topo conf to pass to the spout
     * @param contextMock The topo context to pass to the spout
     * @param collectorMock The mocked collector to pass to the spout
     * @param consumerMock The mocked consumer
     * @param assignedPartitions The partitions to assign to this spout. The consumer will act like these partitions are assigned to it.
     * @return The spout
     */
    public static <K, V> KafkaSpout<K, V> setupSpout(KafkaSpoutConfig<K, V> spoutConfig, Map<String, Object> topoConf,
        TopologyContext contextMock, SpoutOutputCollector collectorMock, final KafkaConsumer<K, V> consumerMock, Set<TopicPartition> assignedPartitions) {     

        Map<String, List<PartitionInfo>> partitionInfos = new HashMap<>();
        for(TopicPartition tp : assignedPartitions) {
            PartitionInfo info = new PartitionInfo(tp.topic(), tp.partition(), null, null, null);
            List<PartitionInfo> infos = partitionInfos.get(tp.topic());
            if(infos == null) {
                infos = new ArrayList<>();
                partitionInfos.put(tp.topic(), infos);
            }
            infos.add(info);
        }
        for(String topic : partitionInfos.keySet()) {
            when(consumerMock.partitionsFor(topic))
                .thenReturn(partitionInfos.get(topic));
        }
        KafkaConsumerFactory<K, V> consumerFactory = new KafkaConsumerFactory<K, V>() {
            @Override
            public KafkaConsumer<K, V> createConsumer(KafkaSpoutConfig<K, V> kafkaSpoutConfig) {
                return consumerMock;
            }
        };

        KafkaSpout<K, V> spout = new KafkaSpout<>(spoutConfig, consumerFactory);

        when(contextMock.getComponentTasks(anyString())).thenReturn(Collections.singletonList(0));
        when(contextMock.getThisTaskIndex()).thenReturn(0);
        
        spout.open(topoConf, contextMock, collectorMock);
        spout.activate();

        verify(consumerMock).assign(assignedPartitions);
        
        return spout;
    }
    
}
