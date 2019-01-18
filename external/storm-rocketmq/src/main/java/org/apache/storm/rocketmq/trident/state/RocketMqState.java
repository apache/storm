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

package org.apache.storm.rocketmq.trident.state;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.lang.Validate;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.storm.rocketmq.RocketMqConfig;
import org.apache.storm.rocketmq.common.mapper.TupleToMessageMapper;
import org.apache.storm.rocketmq.common.selector.TopicSelector;
import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocketMqState implements State {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMqState.class);

    private Options options;
    private DefaultMQProducer producer;

    protected RocketMqState(Map<String, Object> map, Options options) {
        this.options = options;
    }

    public static class Options implements Serializable {
        private TopicSelector selector;
        private TupleToMessageMapper mapper;
        private Properties properties;

        public Options withSelector(TopicSelector selector) {
            this.selector = selector;
            return this;
        }

        public Options withMapper(TupleToMessageMapper mapper) {
            this.mapper = mapper;
            return this;
        }

        public Options withProperties(Properties properties) {
            this.properties = properties;
            return this;
        }
    }

    protected void prepare() {
        Validate.notEmpty(options.properties, "Producer properties can not be empty");
        Validate.notNull(options.selector, "TopicSelector can not be null");
        Validate.notNull(options.mapper, "TupleToMessageMapper can not be null");

        producer = new DefaultMQProducer();
        producer.setInstanceName(UUID.randomUUID().toString());
        RocketMqConfig.buildProducerConfigs(options.properties, producer);

        try {
            producer.start();
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void beginCommit(Long txid) {
        LOG.debug("beginCommit is noop.");
    }

    @Override
    public void commit(Long txid) {
        LOG.debug("commit is noop.");
    }

    /**
     * Update the RocketMQ state.
     * @param tuples trident tuples
     * @param collector trident collector
     */
    public void updateState(List<TridentTuple> tuples, TridentCollector collector) {
        List<Message> messages = new LinkedList<>();

        for (TridentTuple tuple : tuples) {
            String topic = options.selector.getTopic(tuple);
            String tag = options.selector.getTag(tuple);
            String key = options.mapper.getKeyFromTuple(tuple);
            byte[] value = options.mapper.getValueFromTuple(tuple);

            if (topic == null) {
                LOG.warn("skipping Message with Key = " + key + ", topic selector returned null.");
                continue;
            }

            Message msg = new Message(topic, tag, key, value);
            messages.add(msg);
        }

        try {
            this.producer.send(messages);
        } catch (Exception e) {
            LOG.warn("Batch write failed. Triggering replay.", e);
            collector.reportError(e);
            throw new FailedException(e);
        }
    }

}
