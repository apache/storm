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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.tuple.Fields;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * KafkaSpoutConfig defines the required configuration to connect a consumer to a consumer group, as well as the subscribing topics
 */
public class KafkaSpoutConfig<K, V> implements Serializable {
    private static final long serialVersionUID = 141902646130682494L;
    public static final long DEFAULT_POLL_TIMEOUT_MS = 200;            // 200ms
    public static final long DEFAULT_OFFSET_COMMIT_PERIOD_MS = 30_000;   // 30s
    public static final int DEFAULT_MAX_RETRIES = Integer.MAX_VALUE;     // Retry forever
    public static final int DEFAULT_MAX_UNCOMMITTED_OFFSETS = 10_000_000;    // 10,000,000 records => 80MBs of memory footprint in the worst case
    public static final long DEFAULT_PARTITION_REFRESH_PERIOD_MS = 2_000; // 2s
    public static final KafkaSpoutRetryService DEFAULT_RETRY_SERVICE =  
            new KafkaSpoutRetryExponentialBackoff(TimeInterval.seconds(0), TimeInterval.milliSeconds(2),
                    DEFAULT_MAX_RETRIES, TimeInterval.seconds(10));
    /**
     * Retry in a tight loop (keep unit tests fasts) do not use in production.
     */
    public static final KafkaSpoutRetryService UNIT_TEST_RETRY_SERVICE = 
    new KafkaSpoutRetryExponentialBackoff(TimeInterval.seconds(0), TimeInterval.milliSeconds(0),
            DEFAULT_MAX_RETRIES, TimeInterval.milliSeconds(0));

    /**
     * The offset used by the Kafka spout in the first poll to Kafka broker. The choice of this parameter will
     * affect the number of consumer records returned in the first poll. By default this parameter is set to UNCOMMITTED_EARLIEST. <br/><br/>
     * The allowed values are EARLIEST, LATEST, UNCOMMITTED_EARLIEST, UNCOMMITTED_LATEST. <br/>
     * <ul>
     * <li>EARLIEST means that the kafka spout polls records starting in the first offset of the partition, regardless of previous commits</li>
     * <li>LATEST means that the kafka spout polls records with offsets greater than the last offset in the partition, regardless of previous commits</li>
     * <li>UNCOMMITTED_EARLIEST means that the kafka spout polls records from the last committed offset, if any.
     * If no offset has been committed, it behaves as EARLIEST.</li>
     * <li>UNCOMMITTED_LATEST means that the kafka spout polls records from the last committed offset, if any.
     * If no offset has been committed, it behaves as LATEST.</li>
     * </ul>
     * */
    public static enum FirstPollOffsetStrategy {
        EARLIEST,
        LATEST,
        UNCOMMITTED_EARLIEST,
        UNCOMMITTED_LATEST }

    public static Builder<String, String> builder(String bootstrapServers, String ... topics) {
        return new Builder<>(bootstrapServers, StringDeserializer.class, StringDeserializer.class, topics);
    }
    
    public static Builder<String, String> builder(String bootstrapServers, Collection<String> topics) {
        return new Builder<>(bootstrapServers, StringDeserializer.class, StringDeserializer.class, topics);
    }
    
    public static Builder<String, String> builder(String bootstrapServers, Pattern topics) {
        return new Builder<>(bootstrapServers, StringDeserializer.class, StringDeserializer.class, topics);
    }
    
    private static Map<String, Object> setDefaultsAndGetKafkaProps(Map<String, Object> kafkaProps) {
        // set defaults for properties not specified
        if (!kafkaProps.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        }
        return kafkaProps;
    }
    
    public static class Builder<K,V> {
        private final Map<String, Object> kafkaProps;
        private Subscription subscription;
        private final SerializableDeserializer<K> keyDes;
        private final Class<? extends Deserializer<K>> keyDesClazz;
        private final SerializableDeserializer<V> valueDes;
        private final Class<? extends Deserializer<V>> valueDesClazz;
        private RecordTranslator<K, V> translator;
        private long pollTimeoutMs = DEFAULT_POLL_TIMEOUT_MS;
        private long offsetCommitPeriodMs = DEFAULT_OFFSET_COMMIT_PERIOD_MS;
        private FirstPollOffsetStrategy firstPollOffsetStrategy = FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST;
        private int maxUncommittedOffsets = DEFAULT_MAX_UNCOMMITTED_OFFSETS;
        private KafkaSpoutRetryService retryService = DEFAULT_RETRY_SERVICE;
        private long partitionRefreshPeriodMs = DEFAULT_PARTITION_REFRESH_PERIOD_MS;
        private boolean emitNullTuples = false;
        
        public Builder(String bootstrapServers, SerializableDeserializer<K> keyDes, SerializableDeserializer<V> valDes, String ... topics) {
            this(bootstrapServers, keyDes, valDes, new NamedSubscription(topics));
        }
        
        public Builder(String bootstrapServers, SerializableDeserializer<K> keyDes, SerializableDeserializer<V> valDes, Collection<String> topics) {
            this(bootstrapServers, keyDes, valDes, new NamedSubscription(topics));
        }
        
        public Builder(String bootstrapServers, SerializableDeserializer<K> keyDes, SerializableDeserializer<V> valDes, Pattern topics) {
            this(bootstrapServers, keyDes, valDes, new PatternSubscription(topics));
        }
        
        public Builder(String bootstrapServers, SerializableDeserializer<K> keyDes, SerializableDeserializer<V> valDes, Subscription subscription) {
            this(bootstrapServers, keyDes, null, valDes, null, subscription);
        }
        
        public Builder(String bootstrapServers, Class<? extends Deserializer<K>> keyDes, Class<? extends Deserializer<V>> valDes, String ... topics) {
            this(bootstrapServers, keyDes, valDes, new NamedSubscription(topics));
        }
        
        public Builder(String bootstrapServers, Class<? extends Deserializer<K>> keyDes, Class<? extends Deserializer<V>> valDes, Collection<String> topics) {
            this(bootstrapServers, keyDes, valDes, new NamedSubscription(topics));
        }
        
        public Builder(String bootstrapServers, Class<? extends Deserializer<K>> keyDes, Class<? extends Deserializer<V>> valDes, Pattern topics) {
            this(bootstrapServers, keyDes, valDes, new PatternSubscription(topics));
        }
        
        public Builder(String bootstrapServers, Class<? extends Deserializer<K>> keyDes, Class<? extends Deserializer<V>> valDes, Subscription subscription) {
            this(bootstrapServers, null, keyDes, null, valDes, subscription);
        }
        
        private Builder(String bootstrapServers, SerializableDeserializer<K> keyDes, Class<? extends Deserializer<K>> keyDesClazz,
                SerializableDeserializer<V> valDes, Class<? extends Deserializer<V>> valDesClazz, Subscription subscription) {
            kafkaProps = new HashMap<>();
            if (bootstrapServers == null || bootstrapServers.isEmpty()) {
                throw new IllegalArgumentException("bootstrap servers cannot be null");
            }
            kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            this.keyDes = keyDes;
            this.keyDesClazz = keyDesClazz;
            this.valueDes = valDes;
            this.valueDesClazz = valDesClazz;
            this.subscription = subscription;
            this.translator = new DefaultRecordTranslator<K,V>();
        }

        private Builder(Builder<?, ?> builder, SerializableDeserializer<K> keyDes, Class<? extends Deserializer<K>> keyDesClazz,
                SerializableDeserializer<V> valueDes, Class<? extends Deserializer<V>> valueDesClazz) {
            this.kafkaProps = new HashMap<>(builder.kafkaProps);
            this.subscription = builder.subscription;
            this.pollTimeoutMs = builder.pollTimeoutMs;
            this.offsetCommitPeriodMs = builder.offsetCommitPeriodMs;
            this.firstPollOffsetStrategy = builder.firstPollOffsetStrategy;
            this.maxUncommittedOffsets = builder.maxUncommittedOffsets;
            //this could result in a lot of class case exceptions at runtime,
            // but because some translators will work no matter what the generics
            // are I thought it best not to force someone to reset the translator
            // when they change the key/value types.
            this.translator = (RecordTranslator<K, V>) builder.translator;
            this.retryService = builder.retryService;
            this.keyDes = keyDes;
            this.keyDesClazz = keyDesClazz;
            this.valueDes = valueDes;
            this.valueDesClazz = valueDesClazz;
        }

        /**
         * Specifying this key deserializer overrides the property key.deserializer. If you have
         * set a custom RecordTranslator before calling this it may result in class cast
         * exceptions at runtime.
         */
        public <NK> Builder<NK,V> setKey(SerializableDeserializer<NK> keyDeserializer) {
            return new Builder<>(this, keyDeserializer, null, valueDes, valueDesClazz);
        }
        
        /**
         * Specify a class that can be instantiated to create a key.deserializer
         * This is the same as setting key.deserializer, but overrides it. If you have
         * set a custom RecordTranslator before calling this it may result in class cast
         * exceptions at runtime.
         */
        public <NK> Builder<NK, V> setKey(Class<? extends Deserializer<NK>> clazz) {
            return new Builder<>(this, null, clazz, valueDes, valueDesClazz);
        }

        /**
         * Specifying this value deserializer overrides the property value.deserializer.  If you have
         * set a custom RecordTranslator before calling this it may result in class cast
         * exceptions at runtime.
         */
        public <NV> Builder<K,NV> setValue(SerializableDeserializer<NV> valueDeserializer) {
            return new Builder<>(this, keyDes, keyDesClazz, valueDeserializer, null);
        }
        
        /**
         * Specify a class that can be instantiated to create a value.deserializer
         * This is the same as setting value.deserializer, but overrides it.  If you have
         * set a custom RecordTranslator before calling this it may result in class cast
         * exceptions at runtime.
         */
        public <NV> Builder<K,NV> setValue(Class<? extends Deserializer<NV>> clazz) {
            return new Builder<>(this, keyDes, keyDesClazz, null, clazz);
        }
        
        /**
         * Set a Kafka property config
         */
        public Builder<K,V> setProp(String key, Object value) {
            kafkaProps.put(key, value);
            return this;
        }
        
        /**
         * Set multiple Kafka property configs
         */
        public Builder<K,V> setProp(Map<String, Object> props) {
            kafkaProps.putAll(props);
            return this;
        }
        
        /**
         * Set multiple Kafka property configs
         */
        public Builder<K,V> setProp(Properties props) {
            for (String name: props.stringPropertyNames()) {
                kafkaProps.put(name, props.get(name));
            }
            return this;
        }
        
        /**
         * Set the group.id for the consumers
         */
        public Builder<K,V> setGroupId(String id) {
            return setProp("group.id", id);
        }
        
        /**
         * reset the bootstrap servers for the Consumer
         */
        public Builder<K,V> setBootstrapServers(String servers) {
            return setProp(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        }
        
        /**
         * The minimum amount of data the broker should return for a fetch request.
         */
        public Builder<K,V> setFetchMinBytes(int bytes) {
            return setProp(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, bytes);
        }
        
        /**
         * The maximum amount of data per-partition the broker will return.
         */
        public Builder<K,V> setMaxPartitionFectchBytes(int bytes) {
            return setProp(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, bytes);
        }
        
        /**
         * The maximum number of records a poll will return.
         * Will only work with Kafka 0.10.0 and above.
         */
        public Builder<K,V> setMaxPollRecords(int records) {
            //to avoid issues with 0.9 versions that technically still work
            // with this we do not use ConsumerConfig.MAX_POLL_RECORDS_CONFIG
            return setProp("max.poll.records", records);
        }
        
        //Security Related Configs
        
        /**
         * Configure the SSL Keystore for mutual authentication
         */
        public Builder<K,V> setSSLKeystore(String location, String password) {
            return setProp("ssl.keystore.location", location)
                    .setProp("ssl.keystore.password", password);
        }
       
        /**
         * Configure the SSL Keystore for mutual authentication
         */
        public Builder<K,V> setSSLKeystore(String location, String password, String keyPassword) {
            return setProp("ssl.key.password", keyPassword)
                    .setSSLKeystore(location, password);
        }
        
        /**
         * Configure the SSL Truststore to authenticate with the brokers
         */
        public Builder<K,V> setSSLTruststore(String location, String password) {
            return setSecurityProtocol("SSL")
                    .setProp("ssl.truststore.location", location)
                    .setProp("ssl.truststore.password", password);
        }
        
        /**
         * Protocol used to communicate with brokers. 
         * Valid values are: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.
         */
        public Builder<K, V> setSecurityProtocol(String protocol) {
            return setProp("security.protocol", protocol);
        }

        //Spout Settings
        /**
         * Specifies the time, in milliseconds, spent waiting in poll if data is not available. Default is 2s
         * @param pollTimeoutMs time in ms
         */
        public Builder<K,V> setPollTimeoutMs(long pollTimeoutMs) {
            this.pollTimeoutMs = pollTimeoutMs;
            return this;
        }

        /**
         * Specifies the period, in milliseconds, the offset commit task is periodically called. Default is 15s.
         * @param offsetCommitPeriodMs time in ms
         */
        public Builder<K,V> setOffsetCommitPeriodMs(long offsetCommitPeriodMs) {
            this.offsetCommitPeriodMs = offsetCommitPeriodMs;
            return this;
        }
        
        /**
         * Defines the max number of polled offsets (records) that can be pending commit, before another poll can take place.
         * Once this limit is reached, no more offsets (records) can be polled until the next successful commit(s) sets the number
         * of pending offsets bellow the threshold. The default is {@link #DEFAULT_MAX_UNCOMMITTED_OFFSETS}.
         * @param maxUncommittedOffsets max number of records that can be be pending commit
         */
        public Builder<K,V> setMaxUncommittedOffsets(int maxUncommittedOffsets) {
            this.maxUncommittedOffsets = maxUncommittedOffsets;
            return this;
        }

        /**
         * Sets the offset used by the Kafka spout in the first poll to Kafka broker upon process start.
         * Please refer to to the documentation in {@link FirstPollOffsetStrategy}
         * @param firstPollOffsetStrategy Offset used by Kafka spout first poll
         * */
        public Builder<K, V> setFirstPollOffsetStrategy(FirstPollOffsetStrategy firstPollOffsetStrategy) {
            this.firstPollOffsetStrategy = firstPollOffsetStrategy;
            return this;
        }
        
        /**
         * Sets the retry service for the spout to use.
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

        public Builder<K, V> setRecordTranslator(RecordTranslator<K, V> translator) {
            this.translator = translator;
            return this;
        }
        
        /**
         * Configure a translator with tuples to be emitted on the default stream.
         * @param func extracts and turns a Kafka ConsumerRecord into a list of objects to be emitted
         * @param fields the names of the fields extracted
         * @return this to be able to chain configuration
         */
        public Builder<K, V> setRecordTranslator(Func<ConsumerRecord<K, V>, List<Object>> func, Fields fields) {
            return setRecordTranslator(new SimpleRecordTranslator<>(func, fields));
        }
        
        /**
         * Configure a translator with tuples to be emitted to a given stream.
         * @param func extracts and turns a Kafka ConsumerRecord into a list of objects to be emitted
         * @param fields the names of the fields extracted
         * @param stream the stream to emit the tuples on
         * @return this to be able to chain configuration
         */
        public Builder<K, V> setRecordTranslator(Func<ConsumerRecord<K, V>, List<Object>> func, Fields fields, String stream) {
            return setRecordTranslator(new SimpleRecordTranslator<>(func, fields, stream));
        }
        
        /**
         * Sets partition refresh period in milliseconds. This is how often kafka will be polled
         * to check for new topics and/or new partitions.
         * This is mostly for Subscription implementations that manually assign partitions. NamedSubscription and
         * PatternSubscription rely on kafka to handle this instead.
         * @param partitionRefreshPeriodMs time in milliseconds
         * @return the builder (this)
         */
        public Builder<K, V> setPartitionRefreshPeriodMs(long partitionRefreshPeriodMs) {
            this.partitionRefreshPeriodMs = partitionRefreshPeriodMs;
            return this;
        }

        /**
         * Specifies if the spout should emit null tuples to the component downstream, or rather not emit and directly
         * ack them. By default this parameter is set to false, which means that null tuples are not emitted.
         * @param emitNullTuples sets if null tuples should or not be emitted downstream
         */
        public Builder<K, V> setEmitNullTuples(boolean emitNullTuples) {
            this.emitNullTuples = emitNullTuples;
            return this;
        }

        public KafkaSpoutConfig<K,V> build() {
            return new KafkaSpoutConfig<>(this);
        }
    }

    // Kafka consumer configuration
    private final Map<String, Object> kafkaProps;
    private final Subscription subscription;
    private final SerializableDeserializer<K> keyDes;
    private final Class<? extends Deserializer<K>> keyDesClazz;
    private final SerializableDeserializer<V> valueDes;
    private final Class<? extends Deserializer<V>> valueDesClazz;
    private final long pollTimeoutMs;

    // Kafka spout configuration
    private final RecordTranslator<K, V> translator;
    private final long offsetCommitPeriodMs;
    private final int maxUncommittedOffsets;
    private final FirstPollOffsetStrategy firstPollOffsetStrategy;
    private final KafkaSpoutRetryService retryService;
    private final long partitionRefreshPeriodMs;
    private final boolean emitNullTuples;

    private KafkaSpoutConfig(Builder<K,V> builder) {
        this.kafkaProps = setDefaultsAndGetKafkaProps(builder.kafkaProps);
        this.subscription = builder.subscription;
        this.translator = builder.translator;
        this.pollTimeoutMs = builder.pollTimeoutMs;
        this.offsetCommitPeriodMs = builder.offsetCommitPeriodMs;
        this.firstPollOffsetStrategy = builder.firstPollOffsetStrategy;
        this.maxUncommittedOffsets = builder.maxUncommittedOffsets;
        this.retryService = builder.retryService;
        this.keyDes = builder.keyDes;
        this.keyDesClazz = builder.keyDesClazz;
        this.valueDes = builder.valueDes;
        this.valueDesClazz = builder.valueDesClazz;
        this.partitionRefreshPeriodMs = builder.partitionRefreshPeriodMs;
        this.emitNullTuples = builder.emitNullTuples;
    }

    public Map<String, Object> getKafkaProps() {
        return kafkaProps;
    }

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
    
    public RecordTranslator<K,V> getTranslator() {
        return translator;
    }

    public long getPollTimeoutMs() {
        return pollTimeoutMs;
    }

    public long getOffsetsCommitPeriodMs() {
        return offsetCommitPeriodMs;
    }

    public boolean isConsumerAutoCommitMode() {
        return kafkaProps.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) == null     // default is true
                || Boolean.valueOf((String)kafkaProps.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
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
    
    public long getPartitionRefreshPeriodMs() {
        return partitionRefreshPeriodMs;
    }

    public boolean isEmitNullTuples() {
        return emitNullTuples;
    }

    @Override
    public String toString() {
        return "KafkaSpoutConfig{" +
                "kafkaProps=" + kafkaProps +
                ", key=" + getKeyDeserializer() +
                ", value=" + getValueDeserializer() +
                ", pollTimeoutMs=" + pollTimeoutMs +
                ", offsetCommitPeriodMs=" + offsetCommitPeriodMs +
                ", maxUncommittedOffsets=" + maxUncommittedOffsets +
                ", firstPollOffsetStrategy=" + firstPollOffsetStrategy +
                ", subscription=" + subscription +
                ", translator=" + translator +
                ", retryService=" + retryService +
                '}';
    }
}
