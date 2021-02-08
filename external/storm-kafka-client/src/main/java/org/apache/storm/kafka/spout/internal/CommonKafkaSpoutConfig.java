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

package org.apache.storm.kafka.spout.internal;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.kafka.spout.DefaultRecordTranslator;
import org.apache.storm.kafka.spout.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.Func;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.kafka.spout.SimpleRecordTranslator;
import org.apache.storm.kafka.spout.subscription.ManualPartitioner;
import org.apache.storm.kafka.spout.subscription.NamedTopicFilter;
import org.apache.storm.kafka.spout.subscription.PatternTopicFilter;
import org.apache.storm.kafka.spout.subscription.RoundRobinManualPartitioner;
import org.apache.storm.kafka.spout.subscription.TopicFilter;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CommonKafkaSpoutConfig<K, V> implements Serializable {
    public static final long DEFAULT_POLL_TIMEOUT_MS = 200;
    public static final long DEFAULT_PARTITION_REFRESH_PERIOD_MS = 2_000;
    // Earliest start
    public static final long DEFAULT_START_TS = 0L;
    public static final FirstPollOffsetStrategy DEFAULT_FIRST_POLL_OFFSET_STRATEGY = FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST;

    public static final Logger LOG = LoggerFactory.getLogger(CommonKafkaSpoutConfig.class);

    // Kafka consumer configuration
    private final Map<String, Object> kafkaProps;
    private final TopicFilter topicFilter;
    private final ManualPartitioner topicPartitioner;
    private final long pollTimeoutMs;

    // Kafka spout configuration
    private final RecordTranslator<K, V> translator;
    private final FirstPollOffsetStrategy firstPollOffsetStrategy;
    private final long partitionRefreshPeriodMs;
    private final long startTimeStamp;

    /**
     * Creates a new CommonKafkaSpoutConfig using a Builder.
     *
     * @param builder The Builder to construct the CommonKafkaSpoutConfig from
     */
    public CommonKafkaSpoutConfig(Builder<K, V, ?> builder) {
        this.kafkaProps = builder.kafkaProps;
        this.topicFilter = builder.topicFilter;
        this.topicPartitioner = builder.topicPartitioner;
        this.translator = builder.translator;
        this.firstPollOffsetStrategy = builder.firstPollOffsetStrategy;
        this.pollTimeoutMs = builder.pollTimeoutMs;
        this.partitionRefreshPeriodMs = builder.partitionRefreshPeriodMs;
        this.startTimeStamp = builder.startTimeStamp;
    }

    public abstract static class Builder<K, V, T extends Builder<K, V, T>> {

        private final Map<String, Object> kafkaProps;
        private final TopicFilter topicFilter;
        private final ManualPartitioner topicPartitioner;
        private RecordTranslator<K, V> translator;
        private long pollTimeoutMs = DEFAULT_POLL_TIMEOUT_MS;
        private FirstPollOffsetStrategy firstPollOffsetStrategy = DEFAULT_FIRST_POLL_OFFSET_STRATEGY;
        private long partitionRefreshPeriodMs = DEFAULT_PARTITION_REFRESH_PERIOD_MS;
        private long startTimeStamp = DEFAULT_START_TS;

        public Builder(String bootstrapServers, String... topics) {
            this(bootstrapServers, new NamedTopicFilter(topics), new RoundRobinManualPartitioner());
        }

        public Builder(String bootstrapServers, Set<String> topics) {
            this(bootstrapServers, new NamedTopicFilter(topics), new RoundRobinManualPartitioner());
        }

        public Builder(String bootstrapServers, Pattern topics) {
            this(bootstrapServers, new PatternTopicFilter(topics), new RoundRobinManualPartitioner());
        }

        /**
         * Create a KafkaSpoutConfig builder with default property values and no key/value deserializers.
         *
         * @param bootstrapServers The bootstrap servers the consumer will use
         * @param topicFilter The topic filter defining which topics and partitions the spout will read
         * @param topicPartitioner The topic partitioner defining which topics and partitions are assinged to each spout task
         */
        public Builder(String bootstrapServers, TopicFilter topicFilter, ManualPartitioner topicPartitioner) {
            kafkaProps = new HashMap<>();
            if (bootstrapServers == null || bootstrapServers.isEmpty()) {
                throw new IllegalArgumentException("bootstrap servers cannot be null");
            }
            kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            this.topicFilter = topicFilter;
            this.topicPartitioner = topicPartitioner;
            this.translator = new DefaultRecordTranslator<>();
        }

        /**
         * Set a {@link KafkaConsumer} property. 
         */
        public T setProp(String key, Object value) {
            kafkaProps.put(key, value);
            return (T) this;
        }

        /**
         * Set multiple {@link KafkaConsumer} properties. 
         */
        public T setProp(Map<String, Object> props) {
            kafkaProps.putAll(props);
            return (T) this;
        }

        /**
         * Set multiple {@link KafkaConsumer} properties. 
         */
        public T setProp(Properties props) {
            props.forEach((key, value) -> {
                if (key instanceof String) {
                    kafkaProps.put((String) key, value);
                } else {
                    throw new IllegalArgumentException("Kafka Consumer property keys must be Strings");
                }
            });
            return (T) this;
        }

        //Spout Settings
        /**
         * Specifies the time, in milliseconds, spent waiting in poll if data is not available. Default is 200ms.
         *
         * @param pollTimeoutMs time in ms
         */
        public T setPollTimeoutMs(long pollTimeoutMs) {
            this.pollTimeoutMs = pollTimeoutMs;
            return (T) this;
        }

        /**
         * Sets the offset used by the Kafka spout in the first poll to Kafka broker upon process start. Please refer to to the
         * documentation in {@link FirstPollOffsetStrategy}
         *
         * @param firstPollOffsetStrategy Offset used by Kafka spout first poll
         */
        public T setFirstPollOffsetStrategy(FirstPollOffsetStrategy firstPollOffsetStrategy) {
            this.firstPollOffsetStrategy = firstPollOffsetStrategy;
            return (T) this;
        }

        public T setRecordTranslator(RecordTranslator<K, V> translator) {
            this.translator = translator;
            return (T) this;
        }

        /**
         * Configure a translator with tuples to be emitted on the default stream.
         *
         * @param func extracts and turns a Kafka ConsumerRecord into a list of objects to be emitted
         * @param fields the names of the fields extracted
         * @return this to be able to chain configuration
         */
        public T setRecordTranslator(Func<ConsumerRecord<K, V>, List<Object>> func, Fields fields) {
            return setRecordTranslator(new SimpleRecordTranslator<>(func, fields));
        }

        /**
         * Configure a translator with tuples to be emitted to a given stream.
         *
         * @param func extracts and turns a Kafka ConsumerRecord into a list of objects to be emitted
         * @param fields the names of the fields extracted
         * @param stream the stream to emit the tuples on
         * @return this to be able to chain configuration
         */
        public T setRecordTranslator(Func<ConsumerRecord<K, V>, List<Object>> func, Fields fields, String stream) {
            return setRecordTranslator(new SimpleRecordTranslator<>(func, fields, stream));
        }

        /**
         * Sets partition refresh period in milliseconds. This is how often Kafka will be polled to check for new topics and/or new
         * partitions.
         *
         * @param partitionRefreshPeriodMs time in milliseconds
         * @return the builder (this)
         */
        public T setPartitionRefreshPeriodMs(long partitionRefreshPeriodMs) {
            this.partitionRefreshPeriodMs = partitionRefreshPeriodMs;
            return (T) this;
        }

        /**
         * Specifies the startTimeStamp if the first poll strategy is TIMESTAMP or UNCOMMITTED_TIMESTAMP.
         * @param startTimeStamp time in ms
         */
        public T setStartTimeStamp(long startTimeStamp) {
            this.startTimeStamp = startTimeStamp;
            return (T) this;
        }
        
        protected Map<String, Object> getKafkaProps() {
            return kafkaProps;
        }

        public abstract CommonKafkaSpoutConfig<K, V> build();
    }

    /**
     * Gets the properties that will be passed to the KafkaConsumer.
     *
     * @return The Kafka properties map
     */
    public Map<String, Object> getKafkaProps() {
        return kafkaProps;
    }

    public TopicFilter getTopicFilter() {
        return topicFilter;
    }

    public ManualPartitioner getTopicPartitioner() {
        return topicPartitioner;
    }

    public RecordTranslator<K, V> getTranslator() {
        return translator;
    }

    public FirstPollOffsetStrategy getFirstPollOffsetStrategy() {
        return firstPollOffsetStrategy;
    }

    public long getPollTimeoutMs() {
        return pollTimeoutMs;
    }
    
    public long getPartitionRefreshPeriodMs() {
        return partitionRefreshPeriodMs;
    }

    public long getStartTimeStamp() {
        return startTimeStamp;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
            .append("kafkaProps", kafkaProps)
            .append("partitionRefreshPeriodMs", partitionRefreshPeriodMs)
            .append("pollTimeoutMs", pollTimeoutMs)
            .append("topicFilter", topicFilter)
            .append("topicPartitioner", topicPartitioner)
            .append("translator", translator)
            .append("startTimeStamp", startTimeStamp)
            .toString();
    }
}
