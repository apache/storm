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
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.KafkaUnitExtension;
import org.apache.storm.kafka.spout.config.builder.SingleTopicKafkaSpoutConfiguration;
import org.apache.storm.kafka.spout.internal.ConsumerFactory;
import org.apache.storm.kafka.spout.internal.ConsumerFactoryDefault;
import org.apache.storm.kafka.spout.subscription.TopicAssigner;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.Time;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class KafkaSpoutReactivationTest {

    @RegisterExtension
    public KafkaUnitExtension kafkaUnitExtension = new KafkaUnitExtension();

    @Captor
    private ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> commitCapture;

    private final TopologyContext topologyContext = mock(TopologyContext.class);
    private final Map<String, Object> conf = new HashMap<>();
    private final SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
    private final long commitOffsetPeriodMs = 2_000;
    private Consumer<String, String> consumerSpy;
    private KafkaSpout<String, String> spout;
    private final int maxPollRecords = 10;

    public void prepareSpout(int messageCount, FirstPollOffsetStrategy firstPollOffsetStrategy) throws Exception {
        KafkaSpoutConfig<String, String> spoutConfig =
            SingleTopicKafkaSpoutConfiguration.setCommonSpoutConfig(KafkaSpoutConfig.builder("127.0.0.1:" + kafkaUnitExtension.getKafkaUnit().getKafkaPort(),
                    SingleTopicKafkaSpoutConfiguration.TOPIC))
                .setFirstPollOffsetStrategy(firstPollOffsetStrategy)
                .setOffsetCommitPeriodMs(commitOffsetPeriodMs)
                .setProp(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords)
                .build();
        ConsumerFactory<String, String> consumerFactory = new ConsumerFactoryDefault<>();
        this.consumerSpy = spy(consumerFactory.createConsumer(spoutConfig.getKafkaProps()));
        ConsumerFactory<String, String> consumerFactoryMock = mock(ConsumerFactory.class);
        when(consumerFactoryMock.createConsumer(any()))
            .thenReturn(consumerSpy);
        this.spout = new KafkaSpout<>(spoutConfig, consumerFactoryMock, new TopicAssigner());
        SingleTopicKafkaUnitSetupHelper.populateTopicData(kafkaUnitExtension.getKafkaUnit(), SingleTopicKafkaSpoutConfiguration.TOPIC, messageCount);
        SingleTopicKafkaUnitSetupHelper.initializeSpout(spout, conf, topologyContext, collector);
    }

    private KafkaSpoutMessageId emitOne() {
        ArgumentCaptor<KafkaSpoutMessageId> messageId = ArgumentCaptor.forClass(KafkaSpoutMessageId.class);
        spout.nextTuple();
        verify(collector).emit(anyString(), anyList(), messageId.capture());
        clearInvocations(collector);
        return messageId.getValue();
    }

    private void doReactivationTest(FirstPollOffsetStrategy firstPollOffsetStrategy) throws Exception {
        try (Time.SimulatedTime time = new Time.SimulatedTime()) {
            int messageCount = maxPollRecords * 2;
            prepareSpout(messageCount, firstPollOffsetStrategy);

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
            clearInvocations(consumerSpy);
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
            SingleTopicKafkaUnitSetupHelper.verifyAllMessagesCommitted(consumerSpy, commitCapture, messageCount);

            clearInvocations(collector);
            spout.nextTuple();
            verify(collector, never()).emit(any(), any(), any());
        }

    }

    @Test
    public void testSpoutShouldResumeWhereItLeftOffWithUncommittedEarliestStrategy() throws Exception {
        //With uncommitted earliest the spout should pick up where it left off when reactivating.
        doReactivationTest(FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST);
    }

    @Test
    public void testSpoutShouldResumeWhereItLeftOffWithEarliestStrategy() throws Exception {
        //With earliest, the spout should also resume where it left off, rather than restart at the earliest offset.
        doReactivationTest(FirstPollOffsetStrategy.EARLIEST);
    }

    @Test
    public void testSpoutMustHandleGettingMetricsWhileDeactivated() throws Exception {
        //Storm will try to get metrics from the spout even while deactivated, the spout must be able to handle this
        prepareSpout(10, FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST);

        for (int i = 0; i < 5; i++) {
            KafkaSpoutMessageId msgId = emitOne();
            spout.ack(msgId);
        }

        spout.deactivate();

        Map<String, Long> offsetMetric = (Map<String, Long>) spout.getKafkaOffsetMetric().getValueAndReset();
        assertThat(offsetMetric.get(SingleTopicKafkaSpoutConfiguration.TOPIC + "/totalSpoutLag"), is(5L));
    }

}
