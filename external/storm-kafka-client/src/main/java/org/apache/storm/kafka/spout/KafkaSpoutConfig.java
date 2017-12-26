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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.Config;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KafkaSpoutConfig defines the required configuration to connect a consumer to a consumer group, as well as the subscribing topics
 */
public class KafkaSpoutConfig<K, V> implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutConfig.class);
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

    public static final ProcessingGuarantee DEFAULT_PROCESSING_GUARANTEE = ProcessingGuarantee.AT_LEAST_ONCE;

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
    private final SerializableDeserializer<K> keyDes;
    private final Class<? extends Deserializer<K>> keyDesClazz;
    private final SerializableDeserializer<V> valueDes;
    private final Class<? extends Deserializer<V>> valueDesClazz;
    private final ProcessingGuarantee processingGuarantee;
    private final boolean tupleTrackingEnforced;

    /**
     * Creates a new KafkaSpoutConfig using a Builder.
     *
     * @param builder The Builder to construct the KafkaSpoutConfig from
     */
    public KafkaSpoutConfig(Builder<K, V> builder) {
        setAutoCommitMode(builder);
        this.kafkaProps = builder.kafkaProps;
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
        this.keyDes = builder.keyDes;
        this.keyDesClazz = builder.keyDesClazz;
        this.valueDes = builder.valueDes;
        this.valueDesClazz = builder.valueDesClazz;
        this.processingGuarantee = builder.processingGuarantee;
        this.tupleTrackingEnforced = builder.tupleTrackingEnforced;
    }

    /**
     * Defines how the {@link KafkaSpout} seeks the offset to be used in the first poll to Kafka upon topology deployment.
     * By default this parameter is set to UNCOMMITTED_EARLIEST. If the strategy is set to:
     * <br/>
     * <ul>
     * <li>EARLIEST - the kafka spout polls records starting in the first offset of the partition, regardless
     * of previous commits. This setting only takes effect on topology deployment.</li>
     * <li>LATEST - the kafka spout polls records with offsets greater than the last offset in the partition,
     * regardless of previous commits. This setting only takes effect on topology deployment.</li>
     * <li>UNCOMMITTED_EARLIEST - the kafka spout polls records from the last committed offset, if any. If no offset has been
     * committed it behaves as EARLIEST.</li>
     * <li>UNCOMMITTED_LATEST - the kafka spout polls records from the last committed offset, if any. If no offset has been
     * committed it behaves as LATEST.</li>
     * </ul>
     */
    public enum FirstPollOffsetStrategy {
        EARLIEST,
        LATEST,
        UNCOMMITTED_EARLIEST,
        UNCOMMITTED_LATEST;

        @Override
        public String toString() {
            return "FirstPollOffsetStrategy{" + super.toString() + "}";
        }
    }

    /**
     * This enum controls when the tuple with the {@link ConsumerRecord} for an offset is marked as processed,
     * i.e. when the offset is committed to Kafka. For AT_LEAST_ONCE and AT_MOST_ONCE the spout controls when
     * the commit happens. When the guarantee is NONE Kafka controls when the commit happens.
     *
     * <ul>
     * <li>AT_LEAST_ONCE - an offset is ready to commit only after the corresponding tuple has been processed (at-least-once)
     * and acked. If a tuple fails or times-out it will be re-emitted. A tuple can be processed more than once if for instance
     * the ack gets lost.</li>
     * <br/>
     * <li>AT_MOST_ONCE - every offset will be committed to Kafka right after being polled but before being emitted
     * to the downstream components of the topology. It guarantees that the offset is processed at-most-once because it
     * won't retry tuples that fail or timeout after the commit to Kafka has been done.</li>
     * <br/>
     * <li>NONE - the polled offsets are committed to Kafka periodically as controlled by the Kafka properties
     * "enable.auto.commit" and "auto.commit.interval.ms". Because the spout does not control when the commit happens
     * it cannot give any message processing guarantees, i.e. a message may be processed 0, 1 or more times.
     * This option requires "enable.auto.commit=true". If "enable.auto.commit=false" an exception will be thrown.</li>
     * </ul>
     */
    public enum ProcessingGuarantee {
        AT_LEAST_ONCE,
        AT_MOST_ONCE,
        NONE,
    }

    public static class Builder<K, V> {

        private final Map<String, Object> kafkaProps;
        private final Subscription subscription;
        private final SerializableDeserializer<K> keyDes;
        private final Class<? extends Deserializer<K>> keyDesClazz;
        private final SerializableDeserializer<V> valueDes;
        private final Class<? extends Deserializer<V>> valueDesClazz;
        private RecordTranslator<K, V> translator;
        private long pollTimeoutMs = DEFAULT_POLL_TIMEOUT_MS;
        private long offsetCommitPeriodMs = DEFAULT_OFFSET_COMMIT_PERIOD_MS;
        private FirstPollOffsetStrategy firstPollOffsetStrategy = DEFAULT_FIRST_POLL_OFFSET_STRATEGY;
        private int maxUncommittedOffsets = DEFAULT_MAX_UNCOMMITTED_OFFSETS;
        private KafkaSpoutRetryService retryService = DEFAULT_RETRY_SERVICE;
        private KafkaTupleListener tupleListener = DEFAULT_TUPLE_LISTENER;
        private long partitionRefreshPeriodMs = DEFAULT_PARTITION_REFRESH_PERIOD_MS;
        private boolean emitNullTuples = false;
        private ProcessingGuarantee processingGuarantee = DEFAULT_PROCESSING_GUARANTEE;
        private boolean tupleTrackingEnforced = false;

        public Builder(String bootstrapServers, String... topics) {
            this(bootstrapServers, (SerializableDeserializer) null, (SerializableDeserializer) null, new ManualPartitionSubscription(new RoundRobinManualPartitioner(), new NamedTopicFilter(topics)));
        }

        public Builder(String bootstrapServers, Collection<String> topics) {
            this(bootstrapServers, (SerializableDeserializer) null, (SerializableDeserializer) null, new ManualPartitionSubscription(new RoundRobinManualPartitioner(),
                new NamedTopicFilter(new HashSet<String>(topics))));
        }

        public Builder(String bootstrapServers, Pattern topics) {
            this(bootstrapServers, (SerializableDeserializer) null, (SerializableDeserializer) null, new ManualPartitionSubscription(new RoundRobinManualPartitioner(), new PatternTopicFilter(topics)));
        }

        /**
         * @deprecated Please use {@link #Builder(java.lang.String, java.lang.String...)} instead, and set the deserializer with
         * {@link #setProp(java.lang.String, java.lang.Object)}, {@link ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG} and
         * {@link ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG}
         */
        @Deprecated
        public Builder(String bootstrapServers, SerializableDeserializer<K> keyDes, SerializableDeserializer<V> valDes, String... topics) {
            this(bootstrapServers, keyDes, valDes, new ManualPartitionSubscription(new RoundRobinManualPartitioner(), new NamedTopicFilter(topics)));
        }

        /**
         * @deprecated Please use {@link #Builder(java.lang.String, java.util.Collection) } instead, and set the deserializer with
         * {@link #setProp(java.lang.String, java.lang.Object)}, {@link ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG} and
         * {@link ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG}
         */
        @Deprecated
        public Builder(String bootstrapServers, SerializableDeserializer<K> keyDes, SerializableDeserializer<V> valDes, Collection<String> topics) {
            this(bootstrapServers, keyDes, valDes, new ManualPartitionSubscription(new RoundRobinManualPartitioner(), new NamedTopicFilter(new HashSet<String>(topics))));
        }

        /**
         * @deprecated Please use {@link #Builder(java.lang.String, java.util.regex.Pattern) } instead, and set the deserializer with
         * {@link #setProp(java.lang.String, java.lang.Object)}, {@link ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG} and
         * {@link ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG}
         */
        @Deprecated
        public Builder(String bootstrapServers, SerializableDeserializer<K> keyDes, SerializableDeserializer<V> valDes, Pattern topics) {
            this(bootstrapServers, keyDes, valDes, new ManualPartitionSubscription(new RoundRobinManualPartitioner(), new PatternTopicFilter(topics)));
        }

        /**
         * @deprecated Please use {@link #Builder(java.lang.String, org.apache.storm.kafka.spout.Subscription) } instead, and set the
         * deserializer with {@link #setProp(java.lang.String, java.lang.Object)}, {@link ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG} and
         * {@link ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG}
         */
        @Deprecated
        public Builder(String bootstrapServers, SerializableDeserializer<K> keyDes, SerializableDeserializer<V> valDes, Subscription subscription) {
            this(bootstrapServers, keyDes, null, valDes, null, subscription);
        }

        /**
         * @deprecated Please use {@link #Builder(java.lang.String, java.lang.String...)} instead, and set the deserializer with {@link #setProp(java.lang.String, java.lang.Object)}, {@link ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG} and {@link ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG}
         */
        @Deprecated
        public Builder(String bootstrapServers, Class<? extends Deserializer<K>> keyDes, Class<? extends Deserializer<V>> valDes, String... topics) {
            this(bootstrapServers, keyDes, valDes, new ManualPartitionSubscription(new RoundRobinManualPartitioner(), new NamedTopicFilter(topics)));
        }

        /**
         * @deprecated Please use {@link #Builder(java.lang.String, java.util.Collection) } instead, and set the deserializer with {@link #setProp(java.lang.String, java.lang.Object)}, {@link ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG} and {@link ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG}
         */
        @Deprecated
        public Builder(String bootstrapServers, Class<? extends Deserializer<K>> keyDes, Class<? extends Deserializer<V>> valDes, Collection<String> topics) {
            this(bootstrapServers, keyDes, valDes, new ManualPartitionSubscription(new RoundRobinManualPartitioner(), new NamedTopicFilter(new HashSet<String>(topics))));
        }

        /**
         * @deprecated Please use {@link #Builder(java.lang.String, java.util.regex.Pattern) } instead, and set the deserializer with {@link #setProp(java.lang.String, java.lang.Object)}, {@link ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG} and {@link ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG}
         */
        @Deprecated
        public Builder(String bootstrapServers, Class<? extends Deserializer<K>> keyDes, Class<? extends Deserializer<V>> valDes, Pattern topics) {
            this(bootstrapServers, keyDes, valDes, new ManualPartitionSubscription(new RoundRobinManualPartitioner(), new PatternTopicFilter(topics)));
        }

        /**
         * @deprecated Please use {@link #Builder(java.lang.String, org.apache.storm.kafka.spout.Subscription) } instead, and set the deserializer with {@link #setProp(java.lang.String, java.lang.Object)}, {@link ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG} and {@link ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG}
         */
        @Deprecated
        public Builder(String bootstrapServers, Class<? extends Deserializer<K>> keyDes, Class<? extends Deserializer<V>> valDes, Subscription subscription) {
            this(bootstrapServers, null, keyDes, null, valDes, subscription);
        }

        /**
         * Create a KafkaSpoutConfig builder with default property values and no key/value deserializers.
         *
         * @param bootstrapServers The bootstrap servers the consumer will use
         * @param subscription The subscription defining which topics and partitions each spout instance will read.
         */
        public Builder(String bootstrapServers, Subscription subscription) {
            this(bootstrapServers, null, null, null, null, subscription);
        }

        private Builder(String bootstrapServers, SerializableDeserializer<K> keyDes, Class<? extends Deserializer<K>> keyDesClazz,
            SerializableDeserializer<V> valDes, Class<? extends Deserializer<V>> valDesClazz, Subscription subscription) {

            this(keyDes, keyDesClazz, valDes, valDesClazz, subscription,
                    new DefaultRecordTranslator<K, V>(), new HashMap<String, Object>());

            if (bootstrapServers == null || bootstrapServers.isEmpty()) {
                throw new IllegalArgumentException("bootstrap servers cannot be null");
            }
            kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

            setNonNullSerDeKafkaProp(keyDes, keyDesClazz, valueDes, valueDesClazz);
        }

        /**
         * This constructor will always be called by one of the methods {@code setKey} or {@code setVal}, which implies
         * that only one of its SerDe parameters will be non null, for which the corresponding Kafka property will be set
         */
        @SuppressWarnings("unchecked")
        private Builder(final Builder<?, ?> builder, SerializableDeserializer<K> keyDes, Class<? extends Deserializer<K>> keyDesClazz,
                        SerializableDeserializer<V> valueDes, Class<? extends Deserializer<V>> valueDesClazz) {

            this(keyDes, keyDesClazz, valueDes, valueDesClazz, builder.subscription,
                    (RecordTranslator<K, V>) builder.translator, new HashMap<>(builder.kafkaProps));

            this.pollTimeoutMs = builder.pollTimeoutMs;
            this.offsetCommitPeriodMs = builder.offsetCommitPeriodMs;
            this.firstPollOffsetStrategy = builder.firstPollOffsetStrategy;
            this.maxUncommittedOffsets = builder.maxUncommittedOffsets;
            this.retryService = builder.retryService;

            setNonNullSerDeKafkaProp(keyDes, keyDesClazz, valueDes, valueDesClazz);
        }

        private Builder(SerializableDeserializer<K> keyDes, Class<? extends Deserializer<K>> keyDesClazz,
               SerializableDeserializer<V> valueDes, Class<? extends Deserializer<V>> valueDesClazz,
               Subscription subscription, RecordTranslator<K, V> translator, Map<String, Object> kafkaProps) {
            this.keyDes = keyDes;
            this.keyDesClazz = keyDesClazz;
            this.valueDes = valueDes;
            this.valueDesClazz = valueDesClazz;
            this.subscription = subscription;
            this.translator = translator;
            this.kafkaProps = kafkaProps;
        }

        private void setNonNullSerDeKafkaProp(SerializableDeserializer<K> keyDes, Class<? extends Deserializer<K>> keyDesClazz,
                SerializableDeserializer<V> valueDes, Class<? extends Deserializer<V>> valueDesClazz) {
            if (keyDesClazz != null) {
                kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDesClazz);
            }
            if (keyDes != null) {
                kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDes.getClass());
            }
            if (valueDesClazz != null) {
                kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDesClazz);
            }
            if (valueDes != null) {
                kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDes.getClass());
            }
        }

        /**
         * Specifying this key deserializer overrides the property key.deserializer. If you have set a custom RecordTranslator before
         * calling this it may result in class cast exceptions at runtime.
         *
         * @deprecated Please use {@link #setProp(java.lang.String, java.lang.Object) } with {@link ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG} instead
         */
        @Deprecated
        public <NK> Builder<NK, V> setKey(SerializableDeserializer<NK> keyDeserializer) {
            return new Builder<>(this, keyDeserializer, null, null, null);
        }

        /**
         * Specify a class that can be instantiated to create a key.deserializer This is the same as setting key.deserializer, but overrides
         * it. If you have set a custom RecordTranslator before calling this it may result in class cast exceptions at runtime.
         *
         * @deprecated Please use {@link #setProp(java.lang.String, java.lang.Object) } with {@link ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG} instead
         */
        @Deprecated
        public <NK> Builder<NK, V> setKey(Class<? extends Deserializer<NK>> clazz) {
            return new Builder<>(this, null, clazz, null, null);
        }

        /**
         * Specifying this value deserializer overrides the property value.deserializer. If you have set a custom RecordTranslator before
         * calling this it may result in class cast exceptions at runtime.
         *
         * @deprecated Please use {@link #setProp(java.lang.String, java.lang.Object) } with {@link ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG} instead
         */
        @Deprecated
        public <NV> Builder<K, NV> setValue(SerializableDeserializer<NV> valueDeserializer) {
            return new Builder<>(this, null, null, valueDeserializer, null);
        }

        /**
         * Specify a class that can be instantiated to create a value.deserializer This is the same as setting value.deserializer, but
         * overrides it. If you have set a custom RecordTranslator before calling this it may result in class cast exceptions at runtime.
         *
         * @deprecated Please use {@link #setProp(java.lang.String, java.lang.Object) } with {@link ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG} instead
         */
        @Deprecated
        public <NV> Builder<K, NV> setValue(Class<? extends Deserializer<NV>> clazz) {
            return new Builder<>(this, null, null, null, clazz);
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
            for(Entry<Object, Object> entry : props.entrySet()) {
            if (entry.getKey() instanceof String) {
                    kafkaProps.put((String) entry.getKey(), entry.getValue());
                } else {
                    throw new IllegalArgumentException("Kafka Consumer property keys must be Strings");
                }
            }
            return this;
        }

        /**
         * Set the group.id for the consumers
         *
         * @deprecated Please use {@link #setProp(java.lang.String, java.lang.Object) } with {@link ConsumerConfig#GROUP_ID_CONFIG} instead
         */
        @Deprecated
        public Builder<K, V> setGroupId(String id) {
            return setProp("group.id", id);
        }

        /**
         * reset the bootstrap servers for the Consumer
         *
         * @deprecated Please use {@link #setProp(java.lang.String, java.lang.Object) } with {@link ConsumerConfig#BOOTSTRAP_SERVERS_CONFIG} instead
         */
        @Deprecated
        public Builder<K, V> setBootstrapServers(String servers) {
            return setProp(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        }

        /**
         * The minimum amount of data the broker should return for a fetch request.
         *
         * @deprecated Please use {@link #setProp(java.lang.String, java.lang.Object) } with {@link ConsumerConfig#FETCH_MIN_BYTES_CONFIG} instead
         */
        @Deprecated
        public Builder<K, V> setFetchMinBytes(int bytes) {
            return setProp(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, bytes);
        }

        /**
         * The maximum amount of data per-partition the broker will return.
         *
         * @deprecated Please use {@link #setProp(java.lang.String, java.lang.Object) } with {@link ConsumerConfig#MAX_PARTITION_FETCH_BYTES_CONFIG} instead
         */
        @Deprecated
        public Builder<K, V> setMaxPartitionFectchBytes(int bytes) {
            return setProp(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, bytes);
        }

        /**
         * The maximum number of records a poll will return.
         *
         * @deprecated Please use {@link #setProp(java.lang.String, java.lang.Object) } with {@link ConsumerConfig#MAX_POLL_RECORDS_CONFIG} instead
         */
        @Deprecated
        public Builder<K, V> setMaxPollRecords(int records) {
            return setProp(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, records);
        }

        //Security Related Configs
        /**
         * Configure the SSL Keystore for mutual authentication
         *
         * @deprecated Please use {@link #setProp(java.lang.String, java.lang.Object) } with "ssl.keystore.location" and "ssl.keystore.password" instead
         */
        @Deprecated
        public Builder<K, V> setSSLKeystore(String location, String password) {
            return setProp("ssl.keystore.location", location)
                .setProp("ssl.keystore.password", password);
        }

        /**
         * Configure the SSL Keystore for mutual authentication
         *
         * @deprecated Please use {@link #setProp(java.lang.String, java.lang.Object) } with "ssl.keystore.location", "ssl.keystore.password" and "ssl.key.password" instead
         */
        @Deprecated
        public Builder<K, V> setSSLKeystore(String location, String password, String keyPassword) {
            return setProp("ssl.key.password", keyPassword)
                .setSSLKeystore(location, password);
        }

        /**
         * Configure the SSL Truststore to authenticate with the brokers
         *
         * @deprecated Please use {@link #setProp(java.lang.String, java.lang.Object) } with "security.protocol", "ssl.truststore.location" and "ssl.truststore.password" instead
         */
        @Deprecated
        public Builder<K, V> setSSLTruststore(String location, String password) {
            return setSecurityProtocol("SSL")
                .setProp("ssl.truststore.location", location)
                .setProp("ssl.truststore.password", password);
        }

        /**
         * Protocol used to communicate with brokers. Valid values are: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.
         *
         * @deprecated Please use {@link #setProp(java.lang.String, java.lang.Object) } with "security.protocol" instead
         */
        @Deprecated
        public Builder<K, V> setSecurityProtocol(String protocol) {
            return setProp("security.protocol", protocol);
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
         * <p>This setting only has an effect if the configured {@link ProcessingGuarantee} is {@link ProcessingGuarantee#AT_LEAST_ONCE}.
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
        return setStringDeserializers(new Builder<String, String>(bootstrapServers, StringDeserializer.class, StringDeserializer.class, topics));
    }

    /**
     * Factory method that creates a Builder with String key/value deserializers.
     *
     * @param bootstrapServers The bootstrap servers for the consumer
     * @param topics The topics to subscribe to
     * @return The new builder
     */
    public static Builder<String, String> builder(String bootstrapServers, Collection<String> topics) {
        return setStringDeserializers(new Builder<String, String>(bootstrapServers, StringDeserializer.class, StringDeserializer.class, topics));
    }

    /**
     * Factory method that creates a Builder with String key/value deserializers.
     *
     * @param bootstrapServers The bootstrap servers for the consumer
     * @param topics The topic pattern to subscribe to
     * @return The new builder
     */
    public static Builder<String, String> builder(String bootstrapServers, Pattern topics) {
        return setStringDeserializers(new Builder<String, String>(bootstrapServers, StringDeserializer.class, StringDeserializer.class, topics));
    }

    private static Builder<String, String> setStringDeserializers(Builder<String, String> builder) {
        builder.setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        builder.setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return builder;
    }

    private static void setAutoCommitMode(Builder<?, ?> builder) {
        if (builder.kafkaProps.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            LOG.warn("Do not set " + ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG + " manually."
                + " Instead use KafkaSpoutConfig.Builder.setProcessingGuarantee."
                + " This will be treated as an error in the next major release."
                + " For now the spout will be configured to behave like it would have in pre-1.2.0 releases.");

            final boolean enableAutoCommit = Boolean.parseBoolean(builder.kafkaProps.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG).toString());
            if(enableAutoCommit) {
                builder.processingGuarantee = ProcessingGuarantee.NONE;
            } else {
                builder.processingGuarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            }
        }
        if (builder.processingGuarantee == ProcessingGuarantee.NONE) {
            builder.kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        } else {
            String autoOffsetResetPolicy = (String)builder.kafkaProps.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
            if (builder.processingGuarantee == ProcessingGuarantee.AT_LEAST_ONCE) {
                if (autoOffsetResetPolicy == null) {
                    /*
                    If the user wants to explicitly set an auto offset reset policy, we should respect it, but when the spout is configured
                    for at-least-once processing we should default to seeking to the earliest offset in case there's an offset out of range
                    error, rather than seeking to the latest (Kafka's default). This type of error will typically happen when the consumer 
                    requests an offset that was deleted.
                     */
                    builder.kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                } else if (!autoOffsetResetPolicy.equals("earliest") && !autoOffsetResetPolicy.equals("none")) {
                    LOG.warn("Cannot guarantee at-least-once processing with auto.offset.reset.policy other than 'earliest' or 'none'."
                        + " Some messages may be skipped.");
                }
            } else if (builder.processingGuarantee == ProcessingGuarantee.AT_MOST_ONCE) {
                if (autoOffsetResetPolicy != null
                    && (!autoOffsetResetPolicy.equals("latest") && !autoOffsetResetPolicy.equals("none"))) {
                    LOG.warn("Cannot guarantee at-most-once processing with auto.offset.reset.policy other than 'latest' or 'none'."
                        + " Some messages may be processed more than once.");
                }
            }
            builder.kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        }
    }

    /**
     * Gets the properties that will be passed to the KafkaConsumer.
     *
     * @return The Kafka properties map
     */
    public Map<String, Object> getKafkaProps() {
        return kafkaProps;
    }

    /**
     * @deprecated Please use {@link #getKafkaProps() } and look up the entry for {@link ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG} instead
     */
    @Deprecated
    public Deserializer<K> getKeyDeserializer() {
        if (keyDesClazz != null) {
            try {
                return keyDesClazz.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException("Could not instantiate key deserializer " + keyDesClazz);
            }
        }
        return keyDes;
    }

    /**
     * @deprecated Please use {@link #getKafkaProps() } and look up the entry for {@link ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG} instead
     */
    @Deprecated
    public Deserializer<V> getValueDeserializer() {
        if (valueDesClazz != null) {
            try {
                return valueDesClazz.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException("Could not instantiate value deserializer " + valueDesClazz);
            }
        }
        return valueDes;
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

    /**
     * @deprecated Use {@link #getProcessingGuarantee()} instead.
     */
    @Deprecated
    public boolean isConsumerAutoCommitMode() {
        return kafkaProps.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) == null // default is false
            || Boolean.valueOf((String) kafkaProps.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    }
    
    public ProcessingGuarantee getProcessingGuarantee() {
        return processingGuarantee;
    }

    public boolean isTupleTrackingEnforced() {
        return tupleTrackingEnforced;
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
            + ", key=" + getKeyDeserializer()
            + ", value=" + getValueDeserializer()
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
