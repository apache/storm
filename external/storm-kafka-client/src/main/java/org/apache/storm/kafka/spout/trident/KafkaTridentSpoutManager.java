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

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.kafka.spout.internal.Timer;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTridentSpoutManager<K, V> implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTridentSpoutManager.class);

    // Kafka
    private transient KafkaConsumer<K, V> kafkaConsumer;

    // Bookkeeping
    private final KafkaSpoutConfig<K, V> kafkaSpoutConfig;
    // Declare some KafkaSpoutConfig references for convenience
    private final Fields fields;

    public KafkaTridentSpoutManager(KafkaSpoutConfig<K, V> kafkaSpoutConfig) {
        this.kafkaSpoutConfig = kafkaSpoutConfig;
        RecordTranslator<K, V> translator = kafkaSpoutConfig.getTranslator();
        Fields fields = null;
        for (String stream: translator.streams()) {
            if (fields == null) {
                fields = translator.getFieldsFor(stream);
            } else {
                if (!fields.equals(translator.getFieldsFor(stream))) {
                    throw new IllegalArgumentException("Trident Spouts do not support multiple output Fields");
                }
            }
        }
        this.fields = fields;
        LOG.debug("Created {}", this);
    }

    void subscribeKafkaConsumer(TopologyContext context) {
        kafkaConsumer = new KafkaConsumer<>(kafkaSpoutConfig.getKafkaProps(),
                kafkaSpoutConfig.getKeyDeserializer(), kafkaSpoutConfig.getValueDeserializer());

        kafkaSpoutConfig.getSubscription().subscribe(kafkaConsumer, new KafkaSpoutConsumerRebalanceListener(), context);

        // Initial poll to get the consumer registration process going.
        // KafkaSpoutConsumerRebalanceListener will be called following this poll, upon partition registration
        kafkaConsumer.poll(0);
    }

    private class KafkaSpoutConsumerRebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            LOG.info("Partitions revoked. [consumer-group={}, consumer={}, topic-partitions={}]",
                    kafkaSpoutConfig.getConsumerGroupId(), kafkaConsumer, partitions);
            KafkaTridentSpoutTopicPartitionRegistry.INSTANCE.removeAll(partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            KafkaTridentSpoutTopicPartitionRegistry.INSTANCE.addAll(partitions);
            LOG.info("Partitions reassignment. [consumer-group={}, consumer={}, topic-partitions={}]",
                    kafkaSpoutConfig.getConsumerGroupId(), kafkaConsumer, partitions);
        }
    }

    KafkaConsumer<K, V> getKafkaConsumer() {
        return kafkaConsumer;
    }

    Set<TopicPartition> getTopicPartitions() {
        return KafkaTridentSpoutTopicPartitionRegistry.INSTANCE.getTopicPartitions();
    }
    
    Fields getFields() {
        return fields;
    }

    KafkaSpoutConfig<K, V> getKafkaSpoutConfig() {
        return kafkaSpoutConfig;
    }

    @Override
    public String toString() {
        return "KafkaTridentSpoutManager{" +
                "kafkaConsumer=" + kafkaConsumer +
                ", kafkaSpoutConfig=" + kafkaSpoutConfig +
                '}';
    }
}
