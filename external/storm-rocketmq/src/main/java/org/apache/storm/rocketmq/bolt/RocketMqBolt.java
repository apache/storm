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

package org.apache.storm.rocketmq.bolt;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang.Validate;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.storm.Config;
import org.apache.storm.rocketmq.RocketMqConfig;
import org.apache.storm.rocketmq.common.mapper.TupleToMessageMapper;
import org.apache.storm.rocketmq.common.selector.TopicSelector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.BatchHelper;
import org.apache.storm.utils.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocketMqBolt implements IRichBolt {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RocketMqBolt.class);

    private static final int DEFAULT_FLUSH_INTERVAL_SECS = 5;
    private static final int DEFAULT_BATCH_SIZE = 20;

    private DefaultMQProducer producer;
    private OutputCollector collector;
    private TopicSelector selector;
    private TupleToMessageMapper mapper;
    private Properties properties;

    private boolean async = true;
    private boolean batch = false;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private int flushIntervalSecs = DEFAULT_FLUSH_INTERVAL_SECS;
    private BatchHelper batchHelper;
    private List<Message> messages;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        Validate.notEmpty(properties, "Producer properties can not be empty");
        Validate.notNull(selector, "TopicSelector can not be null");
        Validate.notNull(mapper, "TupleToMessageMapper can not be null");

        producer = new DefaultMQProducer();
        producer.setInstanceName(String.valueOf(context.getThisTaskId()));
        RocketMqConfig.buildProducerConfigs(properties, producer);

        try {
            producer.start();
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }

        this.collector = collector;
        this.batchHelper = new BatchHelper(batchSize, collector);
        this.messages = new LinkedList<>();
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

    public RocketMqBolt withBatch(boolean batch) {
        this.batch = batch;
        return this;
    }

    public RocketMqBolt withBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public RocketMqBolt withFlushIntervalSecs(int flushIntervalSecs) {
        this.flushIntervalSecs = flushIntervalSecs;
        return this;
    }

    public RocketMqBolt withProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    @Override
    public void execute(Tuple input) {
        if (!batch && TupleUtils.isTick(input)) {
            return;
        }

        String topic = selector.getTopic(input);
        if (topic == null) {
            LOG.warn("skipping Message due to topic selector returned null.");
            collector.ack(input);
            return;
        }

        if (batch) {
            // batch sync sending
            try {
                if (batchHelper.shouldHandle(input)) {
                    batchHelper.addBatch(input);
                    messages.add(prepareMessage(input));
                }

                if (batchHelper.shouldFlush()) {
                    producer.send(messages);
                    batchHelper.ack();
                    messages.clear();
                }
            } catch (Exception e) {
                LOG.error("Batch send messages failure!", e);
                batchHelper.fail(e);
                messages.clear();
            }
        } else {
            if (async) {
                // async sending
                try {
                    producer.send(prepareMessage(input), new SendCallback() {
                        @Override
                        public void onSuccess(SendResult sendResult) {
                            collector.ack(input);
                        }

                        @Override
                        public void onException(Throwable throwable) {
                            if (throwable != null) {
                                LOG.error("Async send messages failure!", throwable);
                                collector.reportError(throwable);
                                collector.fail(input);
                            }
                        }
                    });
                } catch (Exception e) {
                    LOG.error("Async send messages failure!", e);
                    collector.reportError(e);
                    collector.fail(input);
                }
            } else {
                // sync sending, will return a SendResult
                try {
                    producer.send(prepareMessage(input));
                    collector.ack(input);
                } catch (Exception e) {
                    LOG.error("Sync send messages failure!", e);
                    collector.reportError(e);
                    collector.fail(input);
                }
            }
        }
    }

    // Mapping: from storm tuple -> rocketmq Message
    private Message prepareMessage(Tuple input) {
        String topic = selector.getTopic(input);
        String tag = selector.getTag(input);
        String key = mapper.getKeyFromTuple(input);
        byte[] value = mapper.getValueFromTuple(input);

        return new Message(topic, tag, key, value);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return TupleUtils.putTickFrequencyIntoComponentConfig(new Config(), flushIntervalSecs);
    }

    @Override
    public void cleanup() {
        if (producer != null) {
            producer.shutdown();
        }
    }
}
