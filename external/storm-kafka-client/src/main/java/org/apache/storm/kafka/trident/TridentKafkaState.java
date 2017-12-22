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

package org.apache.storm.kafka.trident;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.storm.kafka.trident.mapper.TridentTupleToKafkaMapper;
import org.apache.storm.kafka.trident.selector.KafkaTopicSelector;
import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TridentKafkaState<K, V> implements State {
    private static final Logger LOG = LoggerFactory.getLogger(TridentKafkaState.class);

    private KafkaProducer<K, V> producer;

    private TridentTupleToKafkaMapper<K, V> mapper;
    private KafkaTopicSelector topicSelector;

    public TridentKafkaState<K, V> withTridentTupleToKafkaMapper(TridentTupleToKafkaMapper<K, V> mapper) {
        this.mapper = mapper;
        return this;
    }

    public TridentKafkaState<K, V> withKafkaTopicSelector(KafkaTopicSelector selector) {
        this.topicSelector = selector;
        return this;
    }

    @Override
    public void beginCommit(Long txid) {
        LOG.debug("beginCommit is Noop.");
    }

    @Override
    public void commit(Long txid) {
        LOG.debug("commit is Noop.");
    }

    /**
     * Prepare this State.
     * @param options The KafkaProducer config.
     */
    public void prepare(Properties options) {
        Objects.requireNonNull(mapper, "mapper can not be null");
        Objects.requireNonNull(topicSelector, "topicSelector can not be null");
        producer = new KafkaProducer<>(options);
    }

    /**
     * Write the given tuples to Kafka.
     * @param tuples The tuples to write.
     * @param collector The Trident collector.
     */
    public void updateState(List<TridentTuple> tuples, TridentCollector collector) {
        String topic = null;
        try {
            final long startTime = System.currentTimeMillis();
            int numberOfRecords = tuples.size();
            List<Future<RecordMetadata>> futures = new ArrayList<>(numberOfRecords);
            for (TridentTuple tuple : tuples) {
                topic = topicSelector.getTopic(tuple);
                V messageFromTuple = mapper.getMessageFromTuple(tuple);
                K keyFromTuple = mapper.getKeyFromTuple(tuple);

                if (topic != null) {
                    if (messageFromTuple != null) {
                        Future<RecordMetadata> result = producer.send(new ProducerRecord<>(topic, keyFromTuple, messageFromTuple));
                        futures.add(result);
                    } else {
                        LOG.warn("skipping Message with Key {} as message was null", keyFromTuple);
                    }

                } else {
                    LOG.warn("skipping key = {}, topic selector returned null.", keyFromTuple);
                }
            }

            int emittedRecords = futures.size();
            List<ExecutionException> exceptions = new ArrayList<>(emittedRecords);
            for (Future<RecordMetadata> future : futures) {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    exceptions.add(e);
                }
            }

            if (exceptions.size() > 0) {
                StringBuilder errorMsg = new StringBuilder("Could not retrieve result for messages ");
                errorMsg.append(tuples).append(" from topic = ").append(topic)
                        .append(" because of the following exceptions:").append(System.lineSeparator());

                for (ExecutionException exception : exceptions) {
                    errorMsg = errorMsg.append(exception.getMessage()).append(System.lineSeparator());
                }
                String message = errorMsg.toString();
                LOG.error(message);
                throw new FailedException(message);
            }
            long latestTime = System.currentTimeMillis();
            LOG.info("Emitted record {} sucessfully in {} ms to topic {} ", emittedRecords, latestTime - startTime, topic);

        } catch (Exception ex) {
            String errorMsg = "Could not send messages " + tuples + " to topic = " + topic;
            LOG.warn(errorMsg, ex);
            throw new FailedException(errorMsg, ex);
        }
    }
}
