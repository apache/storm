/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.kafka;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class KafkaJavaApiSpout extends BaseRichSpout {


    public static final Logger LOG = LoggerFactory.getLogger(KafkaJavaApiSpout.class);

    private final List<Values> messagesList = new ArrayList<Values>();
    private KafkaConsumer<String, byte[]> consumer;
    private Iterator<ConsumerRecord<String, byte[]>> it;
    private Map<TopicPartition, OffsetAndMetadata> toBeCommitted;
    private AtomicBoolean rebalanceFlag;


    private int batchUpperLimit;
    private int maxBatchDurationMillis;

    private List<String> topicList;

    /**
     * Lock critical section of code to be executed by one thread
     * at the same time in order not to get a corrupted state when
     * stopping kafka source.
     */
    private Lock lock;

    SpoutConfig _spoutConfig;
    SpoutOutputCollector _collector;
    ConcurrentMap<Long, Values> messages;
    long pollTimeout;
    long maxFailCount;

    public KafkaJavaApiSpout(SpoutConfig spoutConfig) {
        lock = new ReentrantLock();
        messages = new ConcurrentHashMap<>();
        this._spoutConfig = spoutConfig;
    }

    @Override
    public void open(Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
        _collector = collector;

        LOG.debug("Opening KafkaJavaApiSpout...");

        if(conf.containsKey(Config.KAFKA_POLL_TIMEOUT)) {
            pollTimeout = (Long) conf.get(Config.KAFKA_POLL_TIMEOUT);
        }else{
            pollTimeout = 1000;
        }
        if(conf.containsKey(Config.KAFKA_MAX_FAIL_ATTEMPTS_COUNT)){
            maxFailCount = (Long) conf.get(Config.KAFKA_MAX_FAIL_ATTEMPTS_COUNT);
        }else{
            maxFailCount = 5;
        }

        toBeCommitted = new HashMap<TopicPartition, OffsetAndMetadata>();
        rebalanceFlag = new AtomicBoolean(false);

        // Subscribe to multiple topics. Check is multi-topic
        topicList = _spoutConfig.topic == null ? _spoutConfig.topics : Collections.singletonList(_spoutConfig.topic);

        if(topicList == null || topicList.isEmpty()) {
            throw new KafkaException("At least one Kafka topic must be specified.");
        }

        batchUpperLimit = conf.containsKey(KafkaSpoutConstants.BATCH_SIZE) ? (int)conf.get(KafkaSpoutConstants.BATCH_SIZE)
                : KafkaSpoutConstants.DEFAULT_BATCH_SIZE;
        maxBatchDurationMillis = conf.containsKey(KafkaSpoutConstants.BATCH_DURATION_MS) ? (int)conf.get(KafkaSpoutConstants.BATCH_DURATION_MS)
                : KafkaSpoutConstants.DEFAULT_BATCH_DURATION;

        try {
            //initialize a consumer.
            consumer = new KafkaConsumer<String, byte[]>(conf);
        } catch (Exception e) {
            throw new KafkaException("Unable to create consumer. " +
                    "Check whether the Bootstrap server is up and that the " +
                    "Storm can connect to it.", e);
        }

        // We can use topic subscription or partition assignment strategy.
        consumer.subscribe(topicList, new RebalanceListener(rebalanceFlag));

        it = consumer.poll(pollTimeout).iterator();

        LOG.info("Kafka spout started.");
    }

    @Override
    public void nextTuple() {
        LOG.debug("Polling next tuple...");
        final String batchUUID = UUID.randomUUID().toString();
        byte[] kafkaMessage;
        String kafkaKey;

        try {
            // prepare time variables for new batch
            final long batchStartTime = System.currentTimeMillis();
            final long batchEndTime = System.currentTimeMillis() + maxBatchDurationMillis;

            while (messagesList.size() < batchUpperLimit &&
                    System.currentTimeMillis() < batchEndTime) {

                if (it == null || !it.hasNext()) {
                    // Obtaining new records
                    // Poll time is remainder time for current batch.
                    ConsumerRecords<String, byte[]> records = consumer.poll(
                            Math.max(0, batchEndTime - System.currentTimeMillis()));
                    it = records.iterator();

                    // this flag is set to true in a callback when some partitions are revoked.
                    // If there are any records we commit them.
                    if (rebalanceFlag.get()) {
                        rebalanceFlag.set(false);
                        break;
                    }
                    // check records after poll
                    if (!it.hasNext()) {
                        LOG.debug("Returning with backoff. No more data to read");
                        // batch time exceeded
                        break;
                    }
                }

                // get next message
                ConsumerRecord<String, byte[]> message = it.next();
                kafkaMessage = message.value();
                kafkaKey = message.key();

                LOG.debug("Message: {}", new String(kafkaMessage));
                LOG.debug("Topic: {} Partition: {}", message.topic(), message.partition());

                Values value = new Values(kafkaKey, kafkaMessage, message.topic(), maxFailCount);
                messagesList.add(value);
                messages.putIfAbsent(message.offset(), value);

                LOG.debug("Waited: {} ", System.currentTimeMillis() - batchStartTime);
                LOG.debug("Messages #: {}", messagesList.size());


                long offset =  message.offset() + 1;
                toBeCommitted.put(new TopicPartition(message.topic(), message.partition()),
                        new OffsetAndMetadata(offset, batchUUID));
            }

            if (messagesList.size() > 0) {

                for (Values v : messagesList){
                    _collector.emit(v);
                }

                LOG.debug("Emitted {}", messagesList.size());

                messagesList.clear();
                // commit must not be interrupted when stops.
                try {
                    lock.lock();
                    consumer.commitSync(toBeCommitted);
                    toBeCommitted.clear();
                } finally {
                    lock.unlock();
                }
            }
        } catch (Exception e) {
            LOG.error("KafkaJavaApiSpout EXCEPTION, {}", e);
        }
    }

    @Override
    public void fail(Object msgId) {
        LOG.debug("Message with offset {} failed", msgId);
        Values message = messages.get(msgId);
        Long currentAttempt = (Long) message.get(3);
        if(currentAttempt < 1){
            LOG.debug("Message with offset {} reached maximum fail attempts. Skipping...", msgId);
        }else{
            message.set(3, currentAttempt-1);
            _collector.emit(message, msgId);
        }
    }

    @Override
    public void ack(Object msgId) {
        LOG.debug("Message with offset {} proceeded successfully", msgId);
        messages.remove(msgId);
    }

    @Override
    public void close() {
        if (consumer != null) {
            try {
                lock.lock();
                consumer.wakeup();
                consumer.close();
            } finally {
                lock.unlock();
            }
        }
        LOG.info("Kafka Spout stopped.");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "message", "topic", "attempt"));
    }

    public class KafkaSpoutConstants {

        public static final String KAFKA_PREFIX = "kafka.";
        public static final String KAFKA_CONSUMER_PREFIX = KAFKA_PREFIX + "consumer.";
        public static final String DEFAULT_KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
        public static final String DEFAULT_VALUE_DESERIAIZER = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
        public static final String BOOTSTRAP_SERVERS = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
        public static final String TOPICS = KAFKA_PREFIX + "topics";
        public static final String DEFAULT_AUTO_COMMIT =  "false";
        public static final String BATCH_SIZE = "batchSize";
        public static final String BATCH_DURATION_MS = "batchDurationMillis";
        public static final int DEFAULT_BATCH_SIZE = 1000;
        public static final int DEFAULT_BATCH_DURATION = 1000;

    }
}

class RebalanceListener implements ConsumerRebalanceListener {
    private static final Logger log = LoggerFactory.getLogger(RebalanceListener.class);
    private AtomicBoolean rebalanceFlag;

    public RebalanceListener(AtomicBoolean rebalanceFlag) {
        this.rebalanceFlag = rebalanceFlag;
    }

    // Set a flag that a rebalance has occurred. Then commit already read to kafka.
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            log.info("topic {} - partition {} revoked.", partition.topic(), partition.partition());
        }
        rebalanceFlag.set(true);
    }

    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            log.info("topic {} - partition {} assigned.", partition.topic(), partition.partition());
        }
    }
}
