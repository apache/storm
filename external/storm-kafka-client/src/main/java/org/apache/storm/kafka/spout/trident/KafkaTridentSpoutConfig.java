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

package org.apache.storm.kafka.spout.trident;

import java.util.Set;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.kafka.spout.internal.CommonKafkaSpoutConfig;
import org.apache.storm.kafka.spout.subscription.ManualPartitioner;
import org.apache.storm.kafka.spout.subscription.TopicFilter;

/**
 * Defines the required Kafka-related configuration for the Trident spouts.
 */
public class KafkaTridentSpoutConfig<K, V> extends CommonKafkaSpoutConfig<K, V> {

    private static final long serialVersionUID = 1L;

    public KafkaTridentSpoutConfig(Builder<K, V> builder) {
        super(builder);
    }

    /**
     * Factory method that creates a Builder with String key/value deserializers.
     *
     * @param bootstrapServers The bootstrap servers for the consumer
     * @param topics The topics to subscribe to
     * @return The new builder
     */
    public static Builder<String, String> builder(String bootstrapServers, String... topics) {
        return new Builder<String, String>(bootstrapServers, topics).withStringDeserializers();
    }

    /**
     * Factory method that creates a Builder with String key/value deserializers.
     *
     * @param bootstrapServers The bootstrap servers for the consumer
     * @param topics The topics to subscribe to
     * @return The new builder
     */
    public static Builder<String, String> builder(String bootstrapServers, Set<String> topics) {
        return new Builder<String, String>(bootstrapServers, topics).withStringDeserializers();
    }

    /**
     * Factory method that creates a Builder with String key/value deserializers.
     *
     * @param bootstrapServers The bootstrap servers for the consumer
     * @param topics The topic pattern to subscribe to
     * @return The new builder
     */
    public static Builder<String, String> builder(String bootstrapServers, Pattern topics) {
        return new Builder<String, String>(bootstrapServers, topics).withStringDeserializers();
    }

    public static class Builder<K, V> extends CommonKafkaSpoutConfig.Builder<K, V, Builder<K, V>> {

        public Builder(String bootstrapServers, String... topics) {
            super(bootstrapServers, topics);
        }

        public Builder(String bootstrapServers, Set<String> topics) {
            super(bootstrapServers, topics);
        }

        public Builder(String bootstrapServers, Pattern topics) {
            super(bootstrapServers, topics);
        }

        public Builder(String bootstrapServers, TopicFilter topicFilter, ManualPartitioner topicPartitioner) {
            super(bootstrapServers, topicFilter, topicPartitioner);
        }
        
        private Builder<K, V> withStringDeserializers() {
            setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            return this;
        }

        @Override
        public KafkaTridentSpoutConfig<K, V> build() {
            return new KafkaTridentSpoutConfig<>(this);
        }
    }
}
