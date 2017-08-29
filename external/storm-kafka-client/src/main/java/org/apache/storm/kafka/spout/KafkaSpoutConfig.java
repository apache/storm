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

package org.apache.storm.kafka.spout;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.kafka.spout.subscription.ManualPartitionSubscription;
import org.apache.storm.kafka.spout.subscription.NamedTopicFilter;
import org.apache.storm.kafka.spout.subscription.PatternTopicFilter;
import org.apache.storm.kafka.spout.subscription.RoundRobinManualPartitioner;
import org.apache.storm.kafka.spout.subscription.Subscription;
import org.apache.storm.tuple.Fields;

/**
 * KafkaSpoutConfig defines the required configuration to connect a consumer to a consumer group, as well as the subscribing topics.
 */
public class KafkaSpoutConfig<K, V> implements Serializable {

    private static final long serialVersionUID = 141902646130682494L;
    // 200ms
    public static final long DEFAULT_POLL_TIMEOUT_MS = 200;
    // 30s
    public static final long DEFAULT_OFFSET_COMMIT_PERIOD_MS = 30_000;
    // Retry forever
    public static final int DEFAULT_MAX_RETRIES = Integer.MAX_VALUE;
    // 10,000,000 records => 80MBs of memory footprint in the worst case
    public static final int DEFAULT_MAX_UNCOMMITTED_OFFSETS = 10_000_000;
    // 2s
    public static final long DEFAULT_PARTITION_REFRESH_PERIOD_MS = 2_000;
    public static final FirstPollOffsetStrategy DEFAULT_FIRST_POLL_OFFSET_STRATEGY = FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST;
    public static final KafkaSpoutRetryService DEFAULT_RETRY_SERVICE =
        new KafkaSpoutRetryExponentialBackoff(TimeInterval.seconds(0), TimeInterval.milliSeconds(2),
            DEFAULT_MAX_RETRIES, TimeInterval.seconds(10));

    public static final KafkaTupleListener DEFAULT_TUPLE_LISTENER = new EmptyKafkaTupleListener();

    // Kafka consumer configuration
    private final Map<String, Object> kafkaProps;
    private final Subscription subscription;
    private final long pollTimeoutMs;

    // Kafka spout configuration
    private final RecordTranslator<K, V> translator;
    private final long offsetCommitPeriodMs;
    private final int maxUncommittedOffsets;
    private final FirstPollOffsetStrategy firstPollOffsetStrategy;
    private final KafkaSpoutRetryService retryService;
    private final KafkaTupleListener tupleListener;
    private final long partitionRefreshPeriodMs;
    private final boolean emitNullTuples;

    /**
     * Creates a new KafkaSpoutConfig using a Builder.
     *
     * @param builder The Builder to construct the KafkaSpoutConfig from
     */
    public KafkaSpoutConfig(Builder<K, V> builder) {
        this.kafkaProps = setDefaultsAndGetKafkaProps(builder.kafkaProps);
        this.subscription = builder.subscription;
        this.translator = builder.translator;
        this.pollTimeoutMs = builder.pollTimeoutMs;
        this.offsetCommitPeriodMs = builder.offsetCommitPeriodMs;
        this.firstPollOffsetStrategy = builder.firstPollOffsetStrategy;
        this.maxUncommittedOffsets = builder.maxUncommittedOffsets;
        this.retryService = builder.retryService;
        this.tupleListener = builder.tupleListener;
        this.partitionRefreshPeriodMs = builder.partitionRefreshPeriodMs;
        this.emitNullTuples = builder.emitNullTuples;
    }

    /**
     * The offset used by the Kafka spout in the first poll to Kafka broker. The choice of this parameter will affect the number of consumer
     * records returned in the first poll. By default this parameter is set to UNCOMMITTED_EARLIEST. <br/><br/>
     * The allowed values are EARLIEST, LATEST, UNCOMMITTED_EARLIEST, UNCOMMITTED_LATEST. <br/>
     * <ul>
     * <li>EARLIEST means that the kafka spout polls records starting in the first offset of the partition, regardless of previous
     * commits</li>
     * <li>LATEST means that the kafka spout polls records with offsets greater than the last offset in the partition, regardless of
     * previous commits</li>
     * <li>UNCOMMITTED_EARLIEST means that the kafka spout polls records from the last committed offset, if any. If no offset has been
     * committed, it behaves as EARLIEST.</li>
     * <li>UNCOMMITTED_LATEST means that the kafka spout polls records from the last committed offset, if any. If no offset has been
     * committed, it behaves as LATEST.</li>
     * </ul>
     *
     */
    public static enum FirstPollOffsetStrategy {
        EARLIEST,
        LATEST,
        UNCOMMITTED_EARLIEST,
        UNCOMMITTED_LATEST
    }

    public static class Builder<K, V> {

        private final Map<String, Object> kafkaProps;
        private final Subscription subscription;
        private RecordTranslator<K, V> translator;
        private long pollTimeoutMs = DEFAULT_POLL_TIMEOUT_MS;
        private long offsetCommitPeriodMs = DEFAULT_OFFSET_COMMIT_PERIOD_MS;
        private FirstPollOffsetStrategy firstPollOffsetStrategy = DEFAULT_FIRST_POLL_OFFSET_STRATEGY;
        private int maxUncommittedOffsets = DEFAULT_MAX_UNCOMMITTED_OFFSETS;
        private KafkaSpoutRetryService retryService = DEFAULT_RETRY_SERVICE;
        private KafkaTupleListener tupleListener = DEFAULT_TUPLE_LISTENER;
        private long partitionRefreshPeriodMs = DEFAULT_PARTITION_REFRESH_PERIOD_MS;
        private boolean emitNullTuples = false;

        public Builder(String bootstrapServers, String... topics) {
            this(bootstrapServers, new ManualPartitionSubscription(new RoundRobinManualPartitioner(), new NamedTopicFilter(topics)));
        }

        public Builder(String bootstrapServers, Set<String> topics) {
            this(bootstrapServers, new ManualPartitionSubscription(new RoundRobinManualPartitioner(),
                new NamedTopicFilter(topics)));
        }

        public Builder(String bootstrapServers, Pattern topics) {
            this(bootstrapServers, new ManualPartitionSubscription(new RoundRobinManualPartitioner(), new PatternTopicFilter(topics)));
        }

        /**
         * Create a KafkaSpoutConfig builder with default property values and no key/value deserializers.
         *
         * @param bootstrapServers The bootstrap servers the consumer will use
         * @param subscription The subscription defining which topics and partitions each spout instance will read.
         */
        public Builder(String bootstrapServers, Subscription subscription) {
            kafkaProps = new HashMap<>();
            if (bootstrapServers == null || bootstrapServers.isEmpty()) {
                throw new IllegalArgumentException("bootstrap servers cannot be null");
            }
            kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            this.subscription = subscription;
            this.translator = new DefaultRecordTranslator<>();
        }

        /**
         * Set a {@link KafkaConsumer} property.
         */
        public Builder<K, V> setProp(String key, Object value) {
            kafkaProps.put(key, value);
            return this;
        }

        /**
         * Set multiple {@link KafkaConsumer} properties.
         */
        public Builder<K, V> setProp(Map<String, Object> props) {
            kafkaProps.putAll(props);
            return this;
        }

        /**
         * Set multiple {@link KafkaConsumer} properties.
         */
        public Builder<K, V> setProp(Properties props) {
            props.forEach((key, value) -> {
                if (key instanceof String) {
                    kafkaProps.put((String) key, value);
                } else {
                    throw new IllegalArgumentException("Kafka Consumer property keys must be Strings");
                }
            });
            return this;
        }

        //Spout Settings
        /**
         * Specifies the time, in milliseconds, spent waiting in poll if data is not available. Default is 2s.
         *
         * @param pollTimeoutMs time in ms
         */
        public Builder<K, V> setPollTimeoutMs(long pollTimeoutMs) {
            this.pollTimeoutMs = pollTimeoutMs;
            return this;
        }

        /**
         * Specifies the period, in milliseconds, the offset commit task is periodically called. Default is 15s.
         *
         * @param offsetCommitPeriodMs time in ms
         */
        public Builder<K, V> setOffsetCommitPeriodMs(long offsetCommitPeriodMs) {
            this.offsetCommitPeriodMs = offsetCommitPeriodMs;
            return this;
        }

        /**
         * Defines the max number of polled offsets (records) that can be pending commit, before another poll can take place. Once this
         * limit is reached, no more offsets (records) can be polled until the next successful commit(s) sets the number of pending offsets
         * below the threshold. The default is {@link #DEFAULT_MAX_UNCOMMITTED_OFFSETS}. Note that this limit can in some cases be exceeded,
         * but no partition will exceed this limit by more than maxPollRecords - 1.
         *
         * @param maxUncommittedOffsets max number of records that can be be pending commit
         */
        public Builder<K, V> setMaxUncommittedOffsets(int maxUncommittedOffsets) {
            this.maxUncommittedOffsets = maxUncommittedOffsets;
            return this;
        }

        /**
         * Sets the offset used by the Kafka spout in the first poll to Kafka broker upon process start. Please refer to to the
         * documentation in {@link FirstPollOffsetStrategy}
         *
         * @param firstPollOffsetStrategy Offset used by Kafka spout first poll
         */
        public Builder<K, V> setFirstPollOffsetStrategy(FirstPollOffsetStrategy firstPollOffsetStrategy) {
            this.firstPollOffsetStrategy = firstPollOffsetStrategy;
            return this;
        }

        /**
         * Sets the retry service for the spout to use.
         *
         * @param retryService the new retry service
         * @return the builder (this).
         */
        public Builder<K, V> setRetry(KafkaSpoutRetryService retryService) {
            if (retryService == null) {
                throw new NullPointerException("retryService cannot be null");
            }
            this.retryService = retryService;
            return this;
        }

        /**
         * Sets the tuple listener for the spout to use.
         *
         * @param tupleListener the tuple listener
         * @return the builder (this).
         */
        public Builder<K, V> setTupleListener(KafkaTupleListener tupleListener) {
            if (tupleListener == null) {
                throw new NullPointerException("KafkaTupleListener cannot be null");
            }
            this.tupleListener = tupleListener;
            return this;
        }

        public Builder<K, V> setRecordTranslator(RecordTranslator<K, V> translator) {
            this.translator = translator;
            return this;
        }

        /**
         * Configure a translator with tuples to be emitted on the default stream.
         *
         * @param func extracts and turns a Kafka ConsumerRecord into a list of objects to be emitted
         * @param fields the names of the fields extracted
         * @return this to be able to chain configuration
         */
        public Builder<K, V> setRecordTranslator(Func<ConsumerRecord<K, V>, List<Object>> func, Fields fields) {
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
        public Builder<K, V> setRecordTranslator(Func<ConsumerRecord<K, V>, List<Object>> func, Fields fields, String stream) {
            return setRecordTranslator(new SimpleRecordTranslator<>(func, fields, stream));
        }

        /**
         * Sets partition refresh period in milliseconds. This is how often kafka will be polled to check for new topics and/or new
         * partitions. This is mostly for Subscription implementations that manually assign partitions. NamedSubscription and
         * PatternSubscription rely on kafka to handle this instead.
         *
         * @param partitionRefreshPeriodMs time in milliseconds
         * @return the builder (this)
         */
        public Builder<K, V> setPartitionRefreshPeriodMs(long partitionRefreshPeriodMs) {
            this.partitionRefreshPeriodMs = partitionRefreshPeriodMs;
            return this;
        }

        /**
         * Specifies if the spout should emit null tuples to the component downstream, or rather not emit and directly ack them. By default
         * this parameter is set to false, which means that null tuples are not emitted.
         *
         * @param emitNullTuples sets if null tuples should or not be emitted downstream
         */
        public Builder<K, V> setEmitNullTuples(boolean emitNullTuples) {
            this.emitNullTuples = emitNullTuples;
            return this;
        }

        public KafkaSpoutConfig<K, V> build() {
            return new KafkaSpoutConfig<>(this);
        }
    }

    /**
     * Factory method that creates a Builder with String key/value deserializers.
     *
     * @param bootstrapServers The bootstrap servers for the consumer
     * @param topics The topics to subscribe to
     * @return The new builder
     */
    public static Builder<String, String> builder(String bootstrapServers, String... topics) {
        return setStringDeserializers(new Builder<>(bootstrapServers, topics));
    }

    /**
     * Factory method that creates a Builder with String key/value deserializers.
     *
     * @param bootstrapServers The bootstrap servers for the consumer
     * @param topics The topics to subscribe to
     * @return The new builder
     */
    public static Builder<String, String> builder(String bootstrapServers, Set<String> topics) {
        return setStringDeserializers(new Builder<>(bootstrapServers, topics));
    }

    /**
     * Factory method that creates a Builder with String key/value deserializers.
     *
     * @param bootstrapServers The bootstrap servers for the consumer
     * @param topics The topic pattern to subscribe to
     * @return The new builder
     */
    public static Builder<String, String> builder(String bootstrapServers, Pattern topics) {
        return setStringDeserializers(new Builder<>(bootstrapServers, topics));
    }

    private static Builder<String, String> setStringDeserializers(Builder<String, String> builder) {
        builder.setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        builder.setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return builder;
    }

    private static Map<String, Object> setDefaultsAndGetKafkaProps(Map<String, Object> kafkaProps) {
        // set defaults for properties not specified
        if (!kafkaProps.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        }
        return kafkaProps;
    }

    /**
     * Gets the properties that will be passed to the KafkaConsumer.
     *
     * @return The Kafka properties map
     */
    public Map<String, Object> getKafkaProps() {
        return kafkaProps;
    }

    public Subscription getSubscription() {
        return subscription;
    }

    public RecordTranslator<K, V> getTranslator() {
        return translator;
    }

    public long getPollTimeoutMs() {
        return pollTimeoutMs;
    }

    public long getOffsetsCommitPeriodMs() {
        return offsetCommitPeriodMs;
    }

    public boolean isConsumerAutoCommitMode() {
        return kafkaProps.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) == null // default is false
            || Boolean.valueOf((String) kafkaProps.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    }

    public String getConsumerGroupId() {
        return (String) kafkaProps.get(ConsumerConfig.GROUP_ID_CONFIG);
    }

    public FirstPollOffsetStrategy getFirstPollOffsetStrategy() {
        return firstPollOffsetStrategy;
    }

    public int getMaxUncommittedOffsets() {
        return maxUncommittedOffsets;
    }

    public KafkaSpoutRetryService getRetryService() {
        return retryService;
    }

    public KafkaTupleListener getTupleListener() {
        return tupleListener;
    }

    public long getPartitionRefreshPeriodMs() {
        return partitionRefreshPeriodMs;
    }

    public boolean isEmitNullTuples() {
        return emitNullTuples;
    }

    @Override
    public String toString() {
        return "KafkaSpoutConfig{"
            + "kafkaProps=" + kafkaProps
            + ", pollTimeoutMs=" + pollTimeoutMs
            + ", offsetCommitPeriodMs=" + offsetCommitPeriodMs
            + ", maxUncommittedOffsets=" + maxUncommittedOffsets
            + ", firstPollOffsetStrategy=" + firstPollOffsetStrategy
            + ", subscription=" + subscription
            + ", translator=" + translator
            + ", retryService=" + retryService
            + ", tupleListener=" + tupleListener
            + '}';
    }
}
