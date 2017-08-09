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

import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang.Validate;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.storm.rocketmq.RocketMqConfig;
import org.apache.storm.rocketmq.common.mapper.TupleToMessageMapper;
import org.apache.storm.rocketmq.common.selector.TopicSelector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocketMqBolt implements IRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(RocketMqBolt.class);

    private static MQProducer producer;
    private OutputCollector collector;
    private boolean async = true;
    private TopicSelector selector;
    private TupleToMessageMapper mapper;
    private Properties properties;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        Validate.notEmpty(properties, "Producer properties can not be empty");

        // Since RocketMQ Producer is thread-safe, RocketMQBolt uses a single
        // producer instance across threads to improve the performance.
        synchronized (RocketMqBolt.class) {
            if (producer == null) {
                producer = new DefaultMQProducer();
                RocketMqConfig.buildProducerConfigs(properties, (DefaultMQProducer)producer);

                try {
                    producer.start();
                } catch (MQClientException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        this.collector = collector;

        Validate.notNull(selector, "TopicSelector can not be null");
        Validate.notNull(mapper, "TupleToMessageMapper can not be null");
    }

    public RocketMqBolt withSelector(TopicSelector selector) {
        this.selector = selector;
        return this;
    }

    public RocketMqBolt withMapper(TupleToMessageMapper mapper) {
        this.mapper = mapper;
        return this;
    }

    public RocketMqBolt withAsync(boolean async) {
        this.async = async;
        return this;
    }

    public RocketMqBolt withProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    @Override
    public void execute(Tuple input) {
        // Mapping: from storm tuple -> rocketmq Message
        String topic = selector.getTopic(input);
        String tag = selector.getTag(input);
        String key = mapper.getKeyFromTuple(input);
        byte[] value = mapper.getValueFromTuple(input);

        if (topic == null) {
            LOG.warn("skipping Message with Key = " + key + ", topic selector returned null.");
            collector.ack(input);
            return;
        }

        Message msg = new Message(topic,tag, key, value);

        try {
            if (async) {
                // async sending
                producer.send(msg, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        collector.ack(input);
                    }

                    @Override
                    public void onException(Throwable throwable) {
                        if (throwable != null) {
                            collector.reportError(throwable);
                            collector.fail(input);
                        }
                    }
                });
            } else {
                // sync sending, will return a SendResult
                producer.send(msg);
                collector.ack(input);
            }
        } catch (Exception e) {
            collector.reportError(e);
            collector.fail(input);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void cleanup() {
        synchronized (RocketMqBolt.class) {
            if (producer != null) {
                producer.shutdown();
                producer = null;
            }
        }
    }
}
