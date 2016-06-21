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

package org.apache.storm.kafka.spout.trident;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutStreams;
import org.apache.storm.kafka.spout.KafkaSpoutTuplesBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class KafkaManagerTridentSpout<K, V> implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaManagerTridentSpout.class);

    // Kafka
    private final KafkaSpoutConfig<K, V> kafkaSpoutConfig;
    private transient KafkaConsumer<K, V> kafkaConsumer;

    // Bookkeeping
    private KafkaSpoutStreams kafkaSpoutStreams;                        // Object that wraps all the logic to declare output fields and emit tuples
    private Set<TopicPartition> topicPartitions;
    private KafkaSpoutTuplesBuilder<K, V> tuplesBuilder;      // Object that contains the logic to build tuples for each ConsumerRecord

    public KafkaManagerTridentSpout(KafkaSpoutConfig<K, V> kafkaSpoutConfig) {
        this.kafkaSpoutConfig = kafkaSpoutConfig;

        this.kafkaSpoutStreams = kafkaSpoutConfig.getKafkaSpoutStreams();

        topicPartitions = new HashSet<>();

        // Tuples builder delegate
        tuplesBuilder = kafkaSpoutConfig.getTuplesBuilder();

        //        subscribeKafkaConsumer();
    }

    void subscribeKafkaConsumer() {
        kafkaConsumer = new KafkaConsumer<>(kafkaSpoutConfig.getKafkaProps(),
                kafkaSpoutConfig.getKeyDeserializer(), kafkaSpoutConfig.getValueDeserializer());
        kafkaConsumer.subscribe(kafkaSpoutConfig.getSubscribedTopics(), new KafkaSpoutConsumerRebalanceListener());
        // Initial poll to get the consumer registration process going.
        // KafkaSpoutConsumerRebalanceListener will be called following this poll, upon partition registration
        kafkaConsumer.poll(0);
    }

    private class KafkaSpoutConsumerRebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            LOG.debug("Partitions revoked. [consumer-group={}, consumer={}, topic-partitions={}]",
                    kafkaSpoutConfig.getConsumerGroupId(), kafkaConsumer, partitions);
            getTopicPartitions().removeAll(partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            getTopicPartitions().addAll(partitions);
            LOG.debug("Partitions reassignment. [consumer-group={}, consumer={}, topic-partitions={}]",
                    kafkaSpoutConfig.getConsumerGroupId(), kafkaConsumer, partitions);
        }
    }

    public KafkaConsumer<K, V> getKafkaConsumer() {
        return kafkaConsumer;
    }

    public KafkaSpoutTuplesBuilder<K, V> getTuplesBuilder() {
        return tuplesBuilder;
    }

    public Set<TopicPartition> getTopicPartitions() {
        return TopicPartitionRegistry.INSTANCE.getTopicPartitions();
//        return topicPartitions;
    }

    public KafkaSpoutStreams getKafkaSpoutStreams() {
        return kafkaSpoutStreams;
    }

    public KafkaSpoutConfig<K, V> getKafkaSpoutConfig() {
        return kafkaSpoutConfig;
    }

    @Override
    public String toString() {
        return "KafkaManager{" +
                "kafkaSpoutConfig=" + kafkaSpoutConfig +
                ", kafkaConsumer=" + kafkaConsumer +
                ", kafkaSpoutStreams=" + kafkaSpoutStreams +
                ", topicPartitions=" + topicPartitions +
                ", tuplesBuilder=" + tuplesBuilder +
                '}';
    }
}
