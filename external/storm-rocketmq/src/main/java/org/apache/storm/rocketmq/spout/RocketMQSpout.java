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
import org.apache.storm.rocketmq.DefaultMessageRetryManager;
import org.apache.storm.rocketmq.MessageRetryManager;
import org.apache.storm.rocketmq.MessageSet;
import org.apache.storm.rocketmq.RocketMQConfig;
import org.apache.storm.rocketmq.SpoutConfig;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.ObjectReader;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.storm.rocketmq.RocketMQUtils.getBoolean;
import static org.apache.storm.rocketmq.RocketMQUtils.getInteger;

/**
 * RocketMQSpout uses MQPushConsumer as the default implementation.
 * PushConsumer is a high level consumer API, wrapping the pulling details
 * Looks like broker push messages to consumer
 */
public class RocketMQSpout implements IRichSpout {
    // TODO add metrics

    private static MQPushConsumer consumer;
    private SpoutOutputCollector collector;
    private BlockingQueue<MessageSet> queue;

    private Properties properties;
    private MessageRetryManager messageRetryManager;

    public RocketMQSpout(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        Validate.notEmpty(properties, "Consumer properties can not be empty");
        boolean ordered = getBoolean(properties, RocketMQConfig.CONSUMER_MESSAGES_ORDERLY, false);

        int queueSize = getInteger(properties, SpoutConfig.QUEUE_SIZE, ObjectReader.getInt(conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING)));
        queue = new LinkedBlockingQueue<>(queueSize);

        // Since RocketMQ Consumer is thread-safe, RocketMQSpout uses a single
        // consumer instance across threads to improve the performance.
        synchronized (RocketMQSpout.class) {
            if (consumer == null) {
                consumer = new DefaultMQPushConsumer();
                RocketMQConfig.buildConsumerConfigs(properties, (DefaultMQPushConsumer)consumer);

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
        }

        int maxRetry = getInteger(properties, SpoutConfig.MESSAGES_MAX_RETRY, SpoutConfig.DEFAULT_MESSAGES_MAX_RETRY);
        int ttl = getInteger(properties, SpoutConfig.MESSAGES_TTL, SpoutConfig.DEFAULT_MESSAGES_TTL);
        this.messageRetryManager = new DefaultMessageRetryManager(queue, maxRetry, ttl);
        this.collector = collector;
    }

    public boolean process(List<MessageExt> msgs) {
        if (msgs.isEmpty()) {
            return true;
        }
        MessageSet messageSet = new MessageSet(msgs);
        // returning true upon success and false if this queue is full.
        return queue.offer(messageSet);
    }

    @Override
    public void nextTuple() {
        MessageSet messageSet = queue.poll();
        if (messageSet == null) {
            return;
        }

        messageRetryManager.mark(messageSet);
        collector.emit(new Values(messageSet.getData()), messageSet.getId());
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
        declarer.declare(new Fields(properties.getProperty(SpoutConfig.DECLARE_FIELDS, SpoutConfig.DEFAULT_DECLARE_FIELDS)));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void close() {
        synchronized (RocketMQSpout.class) {
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
