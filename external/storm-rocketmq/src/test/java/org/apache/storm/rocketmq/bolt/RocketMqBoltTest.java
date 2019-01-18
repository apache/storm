/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.rocketmq.bolt;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.common.message.Message;
import org.apache.storm.rocketmq.TestUtils;
import org.apache.storm.rocketmq.common.mapper.FieldNameBasedTupleToMessageMapper;
import org.apache.storm.rocketmq.common.selector.DefaultTopicSelector;
import org.apache.storm.tuple.Tuple;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RocketMqBoltTest {

    private RocketMqBolt rocketMqBolt;
    private DefaultMQProducer producer;

    @Before
    public void setUp() throws Exception {
        rocketMqBolt = new RocketMqBolt();
        rocketMqBolt.withSelector(new DefaultTopicSelector("tpc"));
        rocketMqBolt.withMapper(new FieldNameBasedTupleToMessageMapper("f1", "f2"));
        rocketMqBolt.withBatch(false);
        rocketMqBolt.withBatchSize(5);
        rocketMqBolt.withAsync(true);
        rocketMqBolt.withFlushIntervalSecs(5);

        producer = mock(DefaultMQProducer.class);
        TestUtils.setFieldValue(rocketMqBolt, "producer", producer);
    }

    @Test
    public void execute() throws Exception {
        Tuple tuple = TestUtils.generateTestTuple("f1", "f2", "v1", "v2");
        rocketMqBolt.execute(tuple);

        verify(producer).send(any(Message.class), any(SendCallback.class));
    }

    @Test
    public void cleanup() throws Exception {
        rocketMqBolt.cleanup();
        verify(producer).shutdown();
    }
}