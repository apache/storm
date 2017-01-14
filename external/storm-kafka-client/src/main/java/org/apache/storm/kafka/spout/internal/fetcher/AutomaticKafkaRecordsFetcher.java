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

package org.apache.storm.kafka.spout.internal.fetcher;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.kafka.spout.KafkaSpoutStreams;
import org.apache.storm.kafka.spout.KafkaSpoutStreamsNamedTopics;
import org.apache.storm.kafka.spout.KafkaSpoutStreamsWildcardTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.regex.Pattern;

public class AutomaticKafkaRecordsFetcher<K, V> implements KafkaRecordsFetcher<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(AutomaticKafkaRecordsFetcher.class);

    private final KafkaConsumer<K, V> kafkaConsumer;
    private final ConsumerRebalanceListener consumerRebalanceListener;

    public AutomaticKafkaRecordsFetcher(KafkaConsumer<K, V> kafkaConsumer,
                                        ConsumerRebalanceListener consumerRebalanceListener,
                                        KafkaSpoutStreams kafkaSpoutStreams) {
        this.kafkaConsumer = kafkaConsumer;
        this.consumerRebalanceListener = consumerRebalanceListener;

        subscribe(kafkaSpoutStreams);
    }

    private void subscribe(KafkaSpoutStreams kafkaSpoutStreams) {
        if (kafkaSpoutStreams instanceof KafkaSpoutStreamsNamedTopics) {
            final List<String> topics = ((KafkaSpoutStreamsNamedTopics) kafkaSpoutStreams).getTopics();
            kafkaConsumer.subscribe(topics, consumerRebalanceListener);
            LOG.info("Kafka consumer subscribed topics {}", topics);
        } else if (kafkaSpoutStreams instanceof KafkaSpoutStreamsWildcardTopics) {
            final Pattern pattern = ((KafkaSpoutStreamsWildcardTopics) kafkaSpoutStreams).getTopicWildcardPattern();
            kafkaConsumer.subscribe(pattern, consumerRebalanceListener);
            LOG.info("Kafka consumer subscribed topics matching wildcard pattern [{}]", pattern);
        }
        // Initial poll to get the consumer registration process going.
        // KafkaSpoutConsumerRebalanceListener will be called following this poll, upon partition registration
        kafkaConsumer.poll(0);
    }

    @Override
    public ConsumerRecords<K, V> fetchRecords(long fetchTimeoutMs) {
        return kafkaConsumer.poll(fetchTimeoutMs);
    }
}
