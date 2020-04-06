/*
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

import static org.apache.storm.rocketmq.RocketMqUtils.getBoolean;
import static org.apache.storm.rocketmq.RocketMqUtils.getLong;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang.Validate;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.storm.Config;
import org.apache.storm.rocketmq.ConsumerBatchMessage;
import org.apache.storm.rocketmq.RocketMqConfig;
import org.apache.storm.rocketmq.RocketMqUtils;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocketMqSpout uses MQPushConsumer as the default implementation.
 * PushConsumer is a high level consumer API, wrapping the pulling details
 * Looks like broker push messages to consumer
 */
public class RocketMqSpout implements IRichSpout {
    // TODO add metrics
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RocketMqSpout.class);

    private DefaultMQPushConsumer consumer;
    private SpoutOutputCollector collector;
    private BlockingQueue<ConsumerBatchMessage<List<Object>>> queue;
    private Map<String, ConsumerBatchMessage<List<Object>>> cache;

    private Properties properties;
    private Scheme scheme;
    private long batchProcessTimeout;

    /**
     * RocketMqSpout Constructor.
     * @param properties Properties Config
     */
    public RocketMqSpout(Properties properties) {
        Validate.notEmpty(properties, "Consumer properties can not be empty");
        this.properties = properties;
        scheme = RocketMqUtils.createScheme(properties);
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        consumer = new DefaultMQPushConsumer();
        consumer.setInstanceName(String.valueOf(context.getThisTaskId()));
        RocketMqConfig.buildConsumerConfigs(properties, consumer);

        boolean ordered = getBoolean(properties, RocketMqConfig.CONSUMER_MESSAGES_ORDERLY, false);
        if (ordered) {
            consumer.registerMessageListener(new MessageListenerOrderly() {
                @Override
                public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs,
                                                           ConsumeOrderlyContext context) {
                    if (process(msgs)) {
                        return ConsumeOrderlyStatus.SUCCESS;
                    } else {
                        return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                    }
                }
            });
        } else {
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                                ConsumeConcurrentlyContext context) {
                    if (process(msgs)) {
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    } else {
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                }
            });
        }

        try {
            consumer.start();
        } catch (MQClientException e) {
            LOG.error("Failed to start RocketMQ consumer.", e);
            throw new RuntimeException(e);
        }

        long defaultBatchProcessTimeout = (long) conf.getOrDefault(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 30) * 1000 + 10000;
        batchProcessTimeout = getLong(properties, RocketMqConfig.CONSUMER_BATCH_PROCESS_TIMEOUT, defaultBatchProcessTimeout);

        queue = new LinkedBlockingQueue<>();
        cache = new ConcurrentHashMap<>();
        this.collector = collector;
    }

    /**
     * Process pushed messages.
     * @param msgs messages
     * @return the boolean flag processed result
     */
    protected boolean process(List<MessageExt> msgs) {
        if (msgs.isEmpty()) {
            return true;
        }

        List<List<Object>> list = new ArrayList<>(msgs.size());
        for (MessageExt msg : msgs) {
            List<Object> data = RocketMqUtils.generateTuples(msg, scheme);
            if (data != null) {
                list.add(data);
            }
        }
        ConsumerBatchMessage<List<Object>> batchMessage = new ConsumerBatchMessage<>(list);
        try {
            queue.put(batchMessage);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        boolean isCompleted;
        try {
            isCompleted = batchMessage.waitFinish(batchProcessTimeout);
        } catch (InterruptedException e) {
            LOG.error("Interrupted when waiting messages to be finished.", e);
            throw new RuntimeException(e);
        }

        boolean isSuccess = batchMessage.isSuccess();

        return isCompleted && isSuccess;
    }

    @Override
    public void nextTuple() {
        ConsumerBatchMessage<List<Object>> batchMessage = queue.poll();
        if (batchMessage != null) {
            List<List<Object>> list = batchMessage.getData();
            for (List<Object> data : list) {
                String messageId = UUID.randomUUID().toString();
                cache.put(messageId, batchMessage);
                collector.emit(data, messageId);
            }
        }
    }

    @Override
    public void ack(Object msgId) {
        ConsumerBatchMessage batchMessage = cache.get(msgId);
        batchMessage.ack();
        cache.remove(msgId);
        LOG.debug("Message acked {}", batchMessage);
    }

    @Override
    public void fail(Object msgId) {
        ConsumerBatchMessage batchMessage = cache.get(msgId);
        batchMessage.fail();
        cache.remove(msgId);
        LOG.debug("Message failed {}", batchMessage);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(scheme.getOutputFields());
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void close() {
        if (consumer != null) {
            consumer.shutdown();
        }
    }

    @Override
    public void activate() {
        if (consumer != null) {
            consumer.resume();
        }
    }

    @Override
    public void deactivate() {
        if (consumer != null) {
            consumer.suspend();
        }
    }
}
