/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.kafka.spout.trident.config.builder;

import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.EARLIEST;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.kafka.spout.subscription.ManualPartitioner;
import org.apache.storm.kafka.spout.subscription.TopicFilter;
import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutConfig;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class SingleTopicKafkaTridentSpoutConfiguration {

    public static final String TOPIC = "test";

    public static KafkaTridentSpoutConfig.Builder<String, String> createKafkaSpoutConfigBuilder(int port) {
        return setCommonSpoutConfig(KafkaTridentSpoutConfig.builder("127.0.0.1:" + port, TOPIC));
    }
    
    public static KafkaTridentSpoutConfig.Builder<String, String> createKafkaSpoutConfigBuilder(TopicFilter topicFilter, ManualPartitioner topicPartitioner, int port) {
        return setCommonSpoutConfig(new KafkaTridentSpoutConfig.Builder<>("127.0.0.1:" + port, topicFilter, topicPartitioner));
    }

    public static KafkaTridentSpoutConfig.Builder<String, String> setCommonSpoutConfig(KafkaTridentSpoutConfig.Builder<String, String> config) {
        return config.setRecordTranslator((r) -> new Values(r.topic(), r.key(), r.value()),
            new Fields("topic", "key", "value"))
            .setProp(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5)
            .setFirstPollOffsetStrategy(EARLIEST)
            .setPollTimeoutMs(1000);
    }
}
