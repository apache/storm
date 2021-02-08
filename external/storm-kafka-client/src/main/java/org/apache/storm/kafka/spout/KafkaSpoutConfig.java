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

import java.util.Set;
import java.util.regex.Pattern;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.Config;
import org.apache.storm.annotation.InterfaceStability;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.kafka.spout.internal.CommonKafkaSpoutConfig;
import org.apache.storm.kafka.spout.subscription.ManualPartitioner;
import org.apache.storm.kafka.spout.subscription.TopicFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KafkaSpoutConfig defines the required configuration to connect a consumer to a consumer group, as well as the subscribing topics.
 */
public class KafkaSpoutConfig<K, V> extends CommonKafkaSpoutConfig<K, V> {

    private static final long serialVersionUID = 141902646130682494L;
    public static final long DEFAULT_OFFSET_COMMIT_PERIOD_MS = 30_000;
    // Retry forever
    public static final int DEFAULT_MAX_RETRIES = Integer.MAX_VALUE;
    // 10,000,000 records => 80MBs of memory footprint in the worst case
    public static final int DEFAULT_MAX_UNCOMMITTED_OFFSETS = 10_000_000;

    public static final KafkaSpoutRetryService DEFAULT_RETRY_SERVICE =
        new KafkaSpoutRetryExponentialBackoff(TimeInterval.seconds(0), TimeInterval.milliSeconds(2),
            DEFAULT_MAX_RETRIES, TimeInterval.seconds(10));

    public static final ProcessingGuarantee DEFAULT_PROCESSING_GUARANTEE = ProcessingGuarantee.AT_LEAST_ONCE;

    public static final KafkaTupleListener DEFAULT_TUPLE_LISTENER = new EmptyKafkaTupleListener();
    public static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutConfig.class);

    public static final int DEFAULT_METRICS_TIME_BUCKET_SIZE_SECONDS = 60;

    // Kafka spout configuration
    private final long offsetCommitPeriodMs;
    private final int maxUncommittedOffsets;
    private final KafkaSpoutRetryService retryService;
    private final KafkaTupleListener tupleListener;
    private final boolean emitNullTuples;
    private final ProcessingGuarantee processingGuarantee;
    private final boolean tupleTrackingEnforced;
    private final int metricsTimeBucketSizeInSecs;

    /**
     * Creates a new KafkaSpoutConfig using a Builder.
     *
     * @param builder The Builder to construct the KafkaSpoutConfig from
     */
    public KafkaSpoutConfig(Builder<K, V> builder) {
        super(builder.setKafkaPropsForProcessingGuarantee());
        this.offsetCommitPeriodMs = builder.offsetCommitPeriodMs;
        this.maxUncommittedOffsets = builder.maxUncommittedOffsets;
        this.retryService = builder.retryService;
        this.tupleListener = builder.tupleListener;
        this.emitNullTuples = builder.emitNullTuples;
        this.processingGuarantee = builder.processingGuarantee;
        this.tupleTrackingEnforced = builder.tupleTrackingEnforced;
        this.metricsTimeBucketSizeInSecs = builder.metricsTimeBucketSizeInSecs;
    }

    /**
     * This enum controls when the tuple with the {@link ConsumerRecord} for an offset is marked as processed,
     * i.e. when the offset can be committed to Kafka. The default value is AT_LEAST_ONCE.
     * The commit interval is controlled by {@link KafkaSpoutConfig#getOffsetsCommitPeriodMs() }, if the mode commits on an interval.
     * NO_GUARANTEE may be removed in a later release without warning, we're still evaluating whether it makes sense to keep.
     */
    @InterfaceStability.Unstable
    public enum ProcessingGuarantee {
        /**
         * An offset is ready to commit only after the corresponding tuple has been processed and acked (at least once). If a tuple fails or
         * times out it will be re-emitted, as controlled by the {@link KafkaSpoutRetryService}. Commits synchronously on the defined
         * interval.
         */
        AT_LEAST_ONCE,
        /**
         * Every offset will be synchronously committed to Kafka right after being polled but before being emitted to the downstream
         * components of the topology. The commit interval is ignored. This mode guarantees that the offset is processed at most once by
         * ensuring the spout won't retry tuples that fail or time out after the commit to Kafka has been done
         */
        AT_MOST_ONCE,
        /**
         * The polled offsets are ready to commit immediately after being polled. The offsets are committed periodically, i.e. a message may
         * be processed 0, 1 or more times. This behavior is similar to setting enable.auto.commit=true in the consumer, but allows the
         * spout to control when commits occur. Commits asynchronously on the defined interval.
         */
        NO_GUARANTEE,
    }

    public static class Builder<K, V> extends CommonKafkaSpoutConfig.Builder<K, V, Builder<K, V>> {

        private long offsetCommitPeriodMs = DEFAULT_OFFSET_COMMIT_PERIOD_MS;
        private int maxUncommittedOffsets = DEFAULT_MAX_UNCOMMITTED_OFFSETS;
        private KafkaSpoutRetryService retryService = DEFAULT_RETRY_SERVICE;
        private KafkaTupleListener tupleListener = DEFAULT_TUPLE_LISTENER;
        private boolean emitNullTuples = false;
        private ProcessingGuarantee processingGuarantee = DEFAULT_PROCESSING_GUARANTEE;
        private boolean tupleTrackingEnforced = false;
        private int metricsTimeBucketSizeInSecs = DEFAULT_METRICS_TIME_BUCKET_SIZE_SECONDS;

        public Builder(String bootstrapServers, String... topics) {
            super(bootstrapServers, topics);
        }

        public Builder(String bootstrapServers, Set<String> topics) {
            super(bootstrapServers, topics);
        }

        public Builder(String bootstrapServers, Pattern topics) {
            super(bootstrapServers, topics);
        }

        /**
         * Create a KafkaSpoutConfig builder with default property values and no key/value deserializers.
         *
         * @param bootstrapServers The bootstrap servers the consumer will use
         * @param topicFilter The topic filter defining which topics and partitions the spout will read
         * @param topicPartitioner The topic partitioner defining which topics and partitions are assinged to each spout task
         */
        public Builder(String bootstrapServers, TopicFilter topicFilter, ManualPartitioner topicPartitioner) {
            super(bootstrapServers, topicFilter, topicPartitioner);
        }

        //Spout Settings
        /**
         * Specifies the period, in milliseconds, the offset commit task is periodically called. Default is 15s.
         *
         * <p>This setting only has an effect if the configured {@link ProcessingGuarantee} is {@link ProcessingGuarantee#AT_LEAST_ONCE} or
         * {@link ProcessingGuarantee#NO_GUARANTEE}.
         *
         * @param offsetCommitPeriodMs time in ms
         */
        public Builder<K, V> setOffsetCommitPeriodMs(long offsetCommitPeriodMs) {
            this.offsetCommitPeriodMs = offsetCommitPeriodMs;
            return this;
        }

        /**
         * Defines the max number of polled offsets (records) that can be pending commit, before another poll can take place.
         * Once this limit is reached, no more offsets (records) can be polled until the next successful commit(s) sets the number
         * of pending offsets below the threshold. The default is {@link #DEFAULT_MAX_UNCOMMITTED_OFFSETS}.
         * This limit is per partition and may in some cases be exceeded,
         * but each partition cannot exceed this limit by more than maxPollRecords - 1.
         * 
         * <p>This setting only has an effect if the configured {@link ProcessingGuarantee} is {@link ProcessingGuarantee#AT_LEAST_ONCE}.
         *
         * @param maxUncommittedOffsets max number of records that can be be pending commit
         */
        public Builder<K, V> setMaxUncommittedOffsets(int maxUncommittedOffsets) {
            this.maxUncommittedOffsets = maxUncommittedOffsets;
            return this;
        }

        /**
         * Sets the retry service for the spout to use.
         *
         * <p>This setting only has an effect if the configured {@link ProcessingGuarantee} is {@link ProcessingGuarantee#AT_LEAST_ONCE}.
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

        /**
         * Specifies which processing guarantee the spout should offer. Refer to the documentation for {@link ProcessingGuarantee}.
         *
         * @param processingGuarantee The processing guarantee the spout should offer.
         */
        public Builder<K, V> setProcessingGuarantee(ProcessingGuarantee processingGuarantee) {
            this.processingGuarantee = processingGuarantee;
            return this;
        }

        /**
         * Specifies whether the spout should require Storm to track emitted tuples when using a {@link ProcessingGuarantee} other than
         * {@link ProcessingGuarantee#AT_LEAST_ONCE}. The spout will always track emitted tuples when offering at-least-once guarantees
         * regardless of this setting. This setting is false by default.
         *
         * <p>Enabling tracking can be useful even in cases where reliability is not a concern, because it allows
         * {@link Config#TOPOLOGY_MAX_SPOUT_PENDING} to have an effect, and enables some spout metrics (e.g. complete-latency) that would
         * otherwise be disabled.
         *
         * @param tupleTrackingEnforced true if Storm should track emitted tuples, false otherwise
         */
        public Builder<K, V> setTupleTrackingEnforced(boolean tupleTrackingEnforced) {
            this.tupleTrackingEnforced = tupleTrackingEnforced;
            return this;
        }

        /**
         * The time period that metrics data in bucketed into.
         * @param metricsTimeBucketSizeInSecs time in seconds
         */
        public Builder<K, V> setMetricsTimeBucketSizeInSecs(int metricsTimeBucketSizeInSecs) {
            this.metricsTimeBucketSizeInSecs = metricsTimeBucketSizeInSecs;
            return this;
        }
        
        private Builder<K, V> withStringDeserializers() {
            setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            return this;
        }
        
        private Builder<K, V> setKafkaPropsForProcessingGuarantee() {
            if (getKafkaProps().containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
                throw new IllegalStateException("The KafkaConsumer " + ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG
                    + " setting is not supported."
                    + " You can configure similar behavior through KafkaSpoutConfig.Builder.setProcessingGuarantee");
            }
            String autoOffsetResetPolicy = (String) getKafkaProps().get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
            if (processingGuarantee == ProcessingGuarantee.AT_LEAST_ONCE) {
                if (autoOffsetResetPolicy == null) {
                    /*
                     * If the user wants to explicitly set an auto offset reset policy, we should respect it, but when the spout is
                     * configured for at-least-once processing we should default to seeking to the earliest offset in case there's an offset
                     * out of range error, rather than seeking to the latest (Kafka's default). This type of error will typically happen
                     * when the consumer requests an offset that was deleted.
                     */
                    LOG.info("Setting Kafka consumer property '{}' to 'earliest' to ensure at-least-once processing",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
                    setProp(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                } else if (!autoOffsetResetPolicy.equals("earliest") && !autoOffsetResetPolicy.equals("none")) {
                    LOG.warn("Cannot guarantee at-least-once processing with auto.offset.reset.policy other than 'earliest' or 'none'."
                        + " Some messages may be skipped.");
                }
            } else if (processingGuarantee == ProcessingGuarantee.AT_MOST_ONCE) {
                if (autoOffsetResetPolicy != null
                    && (!autoOffsetResetPolicy.equals("latest") && !autoOffsetResetPolicy.equals("none"))) {
                    LOG.warn("Cannot guarantee at-most-once processing with auto.offset.reset.policy other than 'latest' or 'none'."
                        + " Some messages may be processed more than once.");
                }
            }
            LOG.info("Setting Kafka consumer property '{}' to 'false', because the spout does not support auto-commit",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
            setProp(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            return this;
        }

        @Override
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

    public long getOffsetsCommitPeriodMs() {
        return offsetCommitPeriodMs;
    }

    public ProcessingGuarantee getProcessingGuarantee() {
        return processingGuarantee;
    }

    public boolean isTupleTrackingEnforced() {
        return tupleTrackingEnforced;
    }

    public String getConsumerGroupId() {
        return (String) getKafkaProps().get(ConsumerConfig.GROUP_ID_CONFIG);
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

    public boolean isEmitNullTuples() {
        return emitNullTuples;
    }

    public int getMetricsTimeBucketSizeInSecs() {
        return metricsTimeBucketSizeInSecs;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
            .append("offsetCommitPeriodMs", offsetCommitPeriodMs)
            .append("maxUncommittedOffsets", maxUncommittedOffsets)
            .append("retryService", retryService)
            .append("tupleListener", tupleListener)
            .append("processingGuarantee", processingGuarantee)
            .append("emitNullTuples", emitNullTuples)
            .append("tupleTrackingEnforced", tupleTrackingEnforced)
            .append("metricsTimeBucketSizeInSecs", metricsTimeBucketSizeInSecs)
            .toString();
    }
}
