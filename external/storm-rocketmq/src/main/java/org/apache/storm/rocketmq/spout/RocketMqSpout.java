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

import static org.apache.storm.rocketmq.RocketMqUtils.getBoolean;
import static org.apache.storm.rocketmq.RocketMqUtils.getInteger;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang.Validate;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.storm.Config;
import org.apache.storm.rocketmq.ConsumerMessage;
import org.apache.storm.rocketmq.DefaultMessageRetryManager;
import org.apache.storm.rocketmq.MessageRetryManager;
import org.apache.storm.rocketmq.RocketMqConfig;
import org.apache.storm.rocketmq.RocketMqUtils;
import org.apache.storm.rocketmq.SpoutConfig;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.utils.ObjectReader;

/**
 * RocketMqSpout uses MQPushConsumer as the default implementation.
 * PushConsumer is a high level consumer API, wrapping the pulling details
 * Looks like broker push messages to consumer
 */
public class RocketMqSpout implements IRichSpout {
    // TODO add metrics

    private static MQPushConsumer consumer;
    private SpoutOutputCollector collector;
    private BlockingQueue<ConsumerMessage> queue;
    private BlockingQueue<ConsumerMessage> pending;

    private Properties properties;
    private MessageRetryManager messageRetryManager;
    private Scheme scheme;

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
        // Since RocketMQ Consumer is thread-safe, RocketMQSpout uses a single
        // consumer instance across threads to improve the performance.
        synchronized (RocketMqSpout.class) {
            if (consumer == null) {
                buildAndStartConsumer();
            }
        }

        int queueSize = getInteger(properties, SpoutConfig.QUEUE_SIZE, ObjectReader.getInt(conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING)));
        queue = new LinkedBlockingQueue<>(queueSize);
        pending = new LinkedBlockingQueue<>(queueSize);
        int maxRetry = getInteger(properties, SpoutConfig.MESSAGES_MAX_RETRY, SpoutConfig.DEFAULT_MESSAGES_MAX_RETRY);
        int ttl = getInteger(properties, SpoutConfig.MESSAGES_TTL, SpoutConfig.DEFAULT_MESSAGES_TTL);

        this.messageRetryManager = new DefaultMessageRetryManager(queue, maxRetry, ttl);
        this.collector = collector;
    }

    protected void buildAndStartConsumer() {
        consumer = new DefaultMQPushConsumer();
        RocketMqConfig.buildConsumerConfigs(properties, (DefaultMQPushConsumer)consumer);

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
            throw new RuntimeException(e);
        }
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

        boolean notFull = true;
        for (MessageExt msg : msgs) {
            ConsumerMessage message = new ConsumerMessage(msg);
            // returning true upon success and false if this queue is full.
            if (!queue.offer(message)) {
                notFull = false;
                pending.offer(message);
            }
        }
        return notFull;
    }

    @Override
    public void nextTuple() {
        ConsumerMessage message;
        if (!pending.isEmpty()) {
            message = pending.poll();
        } else {
            message = queue.poll();
        }

        if (message == null) {
            return;
        }

        messageRetryManager.mark(message);
        List<Object> tup = RocketMqUtils.generateTuples(message.getData(), scheme);
        if (tup != null) {
            collector.emit(tup, message.getId());
        }
    }

    @Override
    public void ack(Object msgId) {
        String id = msgId.toString();
        messageRetryManager.ack(id);
    }

    @Override
    public void fail(Object msgId) {
        String id = msgId.toString();
        messageRetryManager.fail(id);
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
        synchronized (RocketMqSpout.class) {
            if (consumer != null) {
                consumer.shutdown();
                consumer = null;
            }
        }
    }

    @Override
    public void activate() {
        consumer.resume();
    }

    @Override
    public void deactivate() {
        consumer.suspend();
    }
}
