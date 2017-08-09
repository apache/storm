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

package org.apache.storm.rocketmq;

import static org.apache.storm.rocketmq.RocketMqUtils.getInteger;

import java.util.Properties;
import java.util.UUID;

import org.apache.commons.lang.Validate;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.remoting.common.RemotingUtil;

/**
 * RocketMqConfig for Consumer/Producer.
 */
public class RocketMqConfig {
    // common
    public static final String NAME_SERVER_ADDR = "nameserver.addr"; // Required

    public static final String CLIENT_NAME = "client.name";

    public static final String CLIENT_IP = "client.ip";
    public static final String DEFAULT_CLIENT_IP = RemotingUtil.getLocalAddress();

    public static final String CLIENT_CALLBACK_EXECUTOR_THREADS = "client.callback.executor.threads";
    public static final int DEFAULT_CLIENT_CALLBACK_EXECUTOR_THREADS = Runtime.getRuntime().availableProcessors();

    public static final String NAME_SERVER_POLL_INTERVAL = "nameserver.poll.interval";
    public static final int DEFAULT_NAME_SERVER_POLL_INTERVAL = 30000; // 30 seconds

    public static final String BROKER_HEART_BEAT_INTERVAL = "brokerserver.heartbeat.interval";
    public static final int DEFAULT_BROKER_HEART_BEAT_INTERVAL = 30000; // 30 seconds


    // producer
    public static final String PRODUCER_GROUP = "producer.group";

    public static final String PRODUCER_RETRY_TIMES = "producer.retry.times";
    public static final int DEFAULT_PRODUCER_RETRY_TIMES = 2;

    public static final String PRODUCER_TIMEOUT = "producer.timeout";
    public static final int DEFAULT_PRODUCER_TIMEOUT = 3000; // 3 seconds


    // consumer
    public static final String CONSUMER_GROUP = "consumer.group"; // Required

    public static final String CONSUMER_TOPIC = "consumer.topic"; // Required

    public static final String CONSUMER_TAG = "consumer.tag";
    public static final String DEFAULT_TAG = "*";

    public static final String CONSUMER_OFFSET_RESET_TO = "consumer.offset.reset.to";
    public static final String CONSUMER_OFFSET_LATEST = "latest";
    public static final String CONSUMER_OFFSET_EARLIEST = "earliest";
    public static final String CONSUMER_OFFSET_TIMESTAMP = "timestamp";

    public static final String CONSUMER_MESSAGES_ORDERLY = "consumer.messages.orderly";

    public static final String CONSUMER_OFFSET_PERSIST_INTERVAL = "consumer.offset.persist.interval";
    public static final int DEFAULT_CONSUMER_OFFSET_PERSIST_INTERVAL = 5000; // 5 seconds

    public static final String CONSUMER_MIN_THREADS = "consumer.min.threads";
    public static final int DEFAULT_CONSUMER_MIN_THREADS = 20;

    public static final String CONSUMER_MAX_THREADS = "consumer.max.threads";
    public static final int DEFAULT_CONSUMER_MAX_THREADS = 64;


    /**
     * Build Producer Configs.
     * @param props Properties
     * @param producer DefaultMQProducer
     */
    public static void buildProducerConfigs(Properties props, DefaultMQProducer producer) {
        buildCommonConfigs(props, producer);

        // According to the RocketMQ official docs, "only one instance is allowed per producer group"
        // So, we use UUID as the producer group by default, to allow many producer instances for one topic
        String defaultGroup = UUID.randomUUID().toString();
        producer.setProducerGroup(props.getProperty(PRODUCER_GROUP, defaultGroup));

        producer.setRetryTimesWhenSendFailed(getInteger(props,
                PRODUCER_RETRY_TIMES, DEFAULT_PRODUCER_RETRY_TIMES));
        producer.setRetryTimesWhenSendAsyncFailed(getInteger(props,
                PRODUCER_RETRY_TIMES, DEFAULT_PRODUCER_RETRY_TIMES));
        producer.setSendMsgTimeout(getInteger(props,
                PRODUCER_TIMEOUT, DEFAULT_PRODUCER_TIMEOUT));
    }

    /**
     * Build Consumer Configs.
     * @param props Properties
     * @param consumer DefaultMQPushConsumer
     */
    public static void buildConsumerConfigs(Properties props, DefaultMQPushConsumer consumer) {
        buildCommonConfigs(props, consumer);

        String group = props.getProperty(CONSUMER_GROUP);
        Validate.notEmpty(group);
        consumer.setConsumerGroup(group);

        consumer.setPersistConsumerOffsetInterval(getInteger(props,
                CONSUMER_OFFSET_PERSIST_INTERVAL, DEFAULT_CONSUMER_OFFSET_PERSIST_INTERVAL));
        consumer.setConsumeThreadMin(getInteger(props,
                CONSUMER_MIN_THREADS, DEFAULT_CONSUMER_MIN_THREADS));
        consumer.setConsumeThreadMax(getInteger(props,
                CONSUMER_MAX_THREADS, DEFAULT_CONSUMER_MAX_THREADS));

        String initOffset = props.getProperty(CONSUMER_OFFSET_RESET_TO, CONSUMER_OFFSET_LATEST);
        switch (initOffset) {
            case CONSUMER_OFFSET_EARLIEST:
                consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
                break;
            case CONSUMER_OFFSET_LATEST:
                consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
                break;
            case CONSUMER_OFFSET_TIMESTAMP:
                consumer.setConsumeTimestamp(initOffset);
                break;
            default:
                consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        }

        String topic = props.getProperty(CONSUMER_TOPIC);
        Validate.notEmpty(topic);
        try {
            consumer.subscribe(topic, props.getProperty(CONSUMER_TAG, DEFAULT_TAG));
        } catch (MQClientException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Build Common Configs.
     * @param props Properties
     * @param client ClientConfig
     */
    public static void buildCommonConfigs(Properties props, ClientConfig client) {
        String namesvr = props.getProperty(NAME_SERVER_ADDR);
        Validate.notEmpty(namesvr);
        client.setNamesrvAddr(namesvr);

        client.setClientIP(props.getProperty(CLIENT_IP, DEFAULT_CLIENT_IP));
        // According to the RocketMQ official docs, "only one instance is allowed per machine"
        // So, we use UUID as the client name by default, to allow RocketMQ spout/bolt instances in one machine.
        String defaultClientName = UUID.randomUUID().toString();
        client.setInstanceName(props.getProperty(CLIENT_NAME, defaultClientName));

        client.setClientCallbackExecutorThreads(getInteger(props,
                CLIENT_CALLBACK_EXECUTOR_THREADS, DEFAULT_CLIENT_CALLBACK_EXECUTOR_THREADS));
        client.setPollNameServerInteval(getInteger(props,
                NAME_SERVER_POLL_INTERVAL, DEFAULT_NAME_SERVER_POLL_INTERVAL));
        client.setHeartbeatBrokerInterval(getInteger(props,
                BROKER_HEART_BEAT_INTERVAL, DEFAULT_BROKER_HEART_BEAT_INTERVAL));
    }
}
