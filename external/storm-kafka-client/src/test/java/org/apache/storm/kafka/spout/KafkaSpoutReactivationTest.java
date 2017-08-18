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

import static org.apache.storm.kafka.spout.KafkaSpout.TIMER_DELAY_MS;
import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.KafkaUnitRule;
import org.apache.storm.kafka.spout.config.builder.SingleTopicKafkaSpoutConfiguration;
import org.apache.storm.kafka.spout.internal.KafkaConsumerFactory;
import org.apache.storm.kafka.spout.internal.KafkaConsumerFactoryDefault;
import org.apache.storm.kafka.spout.subscription.TopicAssigner;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.Time;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KafkaSpoutReactivationTest {

    @Rule
    public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule();

    @Captor
    private ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> commitCapture;

    private final TopologyContext topologyContext = mock(TopologyContext.class);
    private final Map<String, Object> conf = new HashMap<>();
    private final SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
    private final long commitOffsetPeriodMs = 2_000;
    private KafkaConsumer<String, String> consumerSpy;
    private KafkaConsumer<String, String> postReactivationConsumerSpy;
    private KafkaSpout<String, String> spout;
    private final int maxPollRecords = 10;

    @Before
    public void setUp() {
        KafkaSpoutConfig<String, String> spoutConfig =
            SingleTopicKafkaSpoutConfiguration.setCommonSpoutConfig(
                KafkaSpoutConfig.builder("127.0.0.1:" + kafkaUnitRule.getKafkaUnit().getKafkaPort(),
                    SingleTopicKafkaSpoutConfiguration.TOPIC))
                .setFirstPollOffsetStrategy(UNCOMMITTED_EARLIEST)
                .setOffsetCommitPeriodMs(commitOffsetPeriodMs)
                .setProp(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords)
                .build();
        KafkaConsumerFactory<String, String> consumerFactory = new KafkaConsumerFactoryDefault<>();
        this.consumerSpy = spy(consumerFactory.createConsumer(spoutConfig));
        this.postReactivationConsumerSpy = spy(consumerFactory.createConsumer(spoutConfig));
        KafkaConsumerFactory<String, String> consumerFactoryMock = mock(KafkaConsumerFactory.class);
        when(consumerFactoryMock.createConsumer(any()))
            .thenReturn(consumerSpy)
            .thenReturn(postReactivationConsumerSpy);
        this.spout = new KafkaSpout<>(spoutConfig, consumerFactoryMock, new TopicAssigner());
    }

    private void prepareSpout(int messageCount) throws Exception {
        SingleTopicKafkaUnitSetupHelper.populateTopicData(kafkaUnitRule.getKafkaUnit(), SingleTopicKafkaSpoutConfiguration.TOPIC, messageCount);
        SingleTopicKafkaUnitSetupHelper.initializeSpout(spout, conf, topologyContext, collector);
    }

    private KafkaSpoutMessageId emitOne() {
        ArgumentCaptor<KafkaSpoutMessageId> messageId = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
        spout.nextTuple();
        verify(collector).emit(anyString(), anyList(), messageId.capture());
        clearInvocations(collector);
        return messageId.getValue();
    }

    @Test
    public void testSpoutMustHandleReactivationGracefully() throws Exception {
        try (Time.SimulatedTime time = new Time.SimulatedTime()) {
            int messageCount = maxPollRecords * 2;
            prepareSpout(messageCount);

            //Emit and ack some tuples, ensure that some polled tuples remain cached in the spout by emitting less than maxPollRecords
            int beforeReactivationEmits = maxPollRecords - 3;
            for (int i = 0; i < beforeReactivationEmits - 1; i++) {
                KafkaSpoutMessageId msgId = emitOne();
                spout.ack(msgId);
            }

            KafkaSpoutMessageId ackAfterDeactivateMessageId = emitOne();
            
            //Cycle spout activation
            spout.deactivate();
            SingleTopicKafkaUnitSetupHelper.verifyAllMessagesCommitted(consumerSpy, commitCapture, beforeReactivationEmits - 1);
            //Tuples may be acked/failed after the spout deactivates, so we have to be able to handle this too
            spout.ack(ackAfterDeactivateMessageId);
            spout.activate();

            //Emit and ack the rest
            for (int i = beforeReactivationEmits; i < messageCount; i++) {
                KafkaSpoutMessageId msgId = emitOne();
                spout.ack(msgId);
            }

            //Commit
            Time.advanceTime(TIMER_DELAY_MS + commitOffsetPeriodMs);
            spout.nextTuple();

            //Verify that no more tuples are emitted and all tuples are committed
            SingleTopicKafkaUnitSetupHelper.verifyAllMessagesCommitted(postReactivationConsumerSpy, commitCapture, messageCount);

            clearInvocations(collector);
            spout.nextTuple();
            verify(collector, never()).emit(any(), any(), any());
        }

    }

}
