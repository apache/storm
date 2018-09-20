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

package org.apache.storm.rocketmq.spout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.common.message.Message;
import org.apache.storm.rocketmq.ConsumerBatchMessage;
import org.apache.storm.rocketmq.RocketMqUtils;
import org.apache.storm.rocketmq.SpoutConfig;
import org.apache.storm.rocketmq.TestUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RocketMqSpoutTest {
    private RocketMqSpout spout;
    private DefaultMQPushConsumer consumer;
    private SpoutOutputCollector collector;
    private BlockingQueue<ConsumerBatchMessage> queue;
    private Properties properties;

    @Before
    public void setUp() throws Exception {
        properties = new Properties();
        properties.setProperty(SpoutConfig.NAME_SERVER_ADDR, "address");
        properties.setProperty(SpoutConfig.CONSUMER_GROUP, "group");
        properties.setProperty(SpoutConfig.CONSUMER_TOPIC, "topic");

        spout = new RocketMqSpout(properties);
        consumer = mock(DefaultMQPushConsumer.class);
        TestUtils.setFieldValue(spout, "consumer", consumer);
        collector = mock(SpoutOutputCollector.class);
        TestUtils.setFieldValue(spout, "collector", collector);

        queue = new LinkedBlockingQueue<>();
        TestUtils.setFieldValue(spout, "queue", queue);

        Map<String,ConsumerBatchMessage> cache = mock(Map.class);
        TestUtils.setFieldValue(spout, "cache", cache);
    }

    @Test
    public void nextTuple() throws Exception {
        List<List<Object>> list = new ArrayList<>();
        list.add(RocketMqUtils.generateTuples(new Message("tpc", "body".getBytes()),
            RocketMqUtils.createScheme(properties)));
        ConsumerBatchMessage<List<Object>> batchMessage = new ConsumerBatchMessage<>(list);
        queue.put(batchMessage);

        spout.nextTuple();
        verify(collector).emit(anyList(), anyString());
    }

    @Test
    public void close() throws Exception {
        spout.close();
        verify(consumer).shutdown();
    }
}