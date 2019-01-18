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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.KafkaUnit;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.mockito.ArgumentCaptor;

public class SingleTopicKafkaUnitSetupHelper {

    /**
     * Using the given KafkaUnit instance, put some messages in the specified topic.
     *
     * @param kafkaUnit The KafkaUnit instance to use
     * @param topicName The topic to produce messages for
     * @param msgCount The number of messages to produce
     */
    public static void populateTopicData(KafkaUnit kafkaUnit, String topicName, int msgCount) throws Exception {
        kafkaUnit.createTopic(topicName);
        
        for (int i = 0; i < msgCount; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                topicName, Integer.toString(i),
                Integer.toString(i));
            kafkaUnit.sendMessage(producerRecord);
        }
    }

    /*
     * Asserts that commitSync has been called once, 
     * that there are only commits on one topic,
     * and that the committed offset covers messageCount messages
     */
    public static <K, V> void verifyAllMessagesCommitted(Consumer<K, V> consumerSpy,
        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> commitCapture, long messageCount) {
        verify(consumerSpy, times(1)).commitSync(commitCapture.capture());
        Map<TopicPartition, OffsetAndMetadata> commits = commitCapture.getValue();
        assertThat("Expected commits for only one topic partition", commits.entrySet().size(), is(1));
        OffsetAndMetadata offset = commits.entrySet().iterator().next().getValue();
        assertThat("Expected committed offset to cover all emitted messages", offset.offset(), is(messageCount));
    }

    /**
     * Open and activate a KafkaSpout that acts as a single-task/executor spout.
     *
     * @param <K> Kafka key type
     * @param <V> Kafka value type
     * @param spout The spout to prepare
     * @param topoConf The topoConf
     * @param topoContextMock The TopologyContext mock
     * @param collectorMock The output collector mock
     */
    public static <K, V> void initializeSpout(KafkaSpout<K, V> spout, Map<String, Object> topoConf, TopologyContext topoContextMock,
        SpoutOutputCollector collectorMock) throws Exception {
        when(topoContextMock.getThisTaskIndex()).thenReturn(0);
        when(topoContextMock.getComponentTasks(any())).thenReturn(Collections.singletonList(0));
        spout.open(topoConf, topoContextMock, collectorMock);
        spout.activate();
    }

}
