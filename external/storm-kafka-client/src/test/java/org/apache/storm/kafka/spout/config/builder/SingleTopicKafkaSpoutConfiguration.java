/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.apache.storm.kafka.spout.config.builder;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.DEFAULT_MAX_RETRIES;
import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.kafka.spout.subscription.Subscription;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class SingleTopicKafkaSpoutConfiguration {

    public static final String STREAM = "test_stream";
    public static final String TOPIC = "test";

    /**
     * Retry in a tight loop (keep unit tests fasts).
     */
    public static final KafkaSpoutRetryService UNIT_TEST_RETRY_SERVICE =
        new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(0), KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(0),
            DEFAULT_MAX_RETRIES, KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(0));

    public static KafkaSpoutConfig.Builder<String, String> createKafkaSpoutConfigBuilder(int port) {
        return setCommonSpoutConfig(KafkaSpoutConfig.builder("127.0.0.1:" + port, TOPIC));
    }

    public static KafkaSpoutConfig.Builder<String, String> createKafkaSpoutConfigBuilder(Subscription subscription, int port) {
        return setCommonSpoutConfig(new KafkaSpoutConfig.Builder<>("127.0.0.1:" + port, subscription));
    }

    private static KafkaSpoutConfig.Builder<String, String> setCommonSpoutConfig(KafkaSpoutConfig.Builder<String, String> config) {
        return config.setRecordTranslator((r) -> new Values(r.topic(), r.key(), r.value()),
            new Fields("topic", "key", "value"), STREAM)
            .setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutTestGroup")
            .setProp(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5)
            .setRetry(getRetryService())
            .setOffsetCommitPeriodMs(10_000)
            .setFirstPollOffsetStrategy(EARLIEST)
            .setMaxUncommittedOffsets(250)
            .setPollTimeoutMs(1000);
    }

    protected static KafkaSpoutRetryService getRetryService() {
        return UNIT_TEST_RETRY_SERVICE;
    }
}
