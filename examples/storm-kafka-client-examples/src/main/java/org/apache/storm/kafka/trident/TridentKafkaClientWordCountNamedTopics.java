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

package org.apache.storm.kafka.trident;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.kafka.spout.KafkaSpoutStreams;
import org.apache.storm.kafka.spout.KafkaSpoutStreamsNamedTopics;
import org.apache.storm.kafka.spout.KafkaSpoutTupleBuilder;
import org.apache.storm.kafka.spout.KafkaSpoutTuplesBuilder;
import org.apache.storm.kafka.spout.KafkaSpoutTuplesBuilderNamedTopics;
import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutManager;
import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutOpaque;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;

public class TridentKafkaClientWordCountNamedTopics {
    private static final String TOPIC_1 = "test-trident";
    private static final String TOPIC_2 = "test-trident-1";
    private static final String KAFKA_LOCAL_BROKER = "localhost:9092";

    private KafkaTridentSpoutOpaque<String, String> newKafkaTridentSpoutOpaque() {
        return new KafkaTridentSpoutOpaque<>(new KafkaTridentSpoutManager<>(
                        newKafkaSpoutConfig(
                        newKafkaSpoutStreams())));
    }

    private KafkaSpoutConfig<String,String> newKafkaSpoutConfig(KafkaSpoutStreams kafkaSpoutStreams) {
        return new KafkaSpoutConfig.Builder<>(newKafkaConsumerProps(),
                    kafkaSpoutStreams, newTuplesBuilder(), newRetryService())
                .setOffsetCommitPeriodMs(10_000)
                .setFirstPollOffsetStrategy(EARLIEST)
                .setMaxUncommittedOffsets(250)
                .build();
    }

    protected Map<String,Object> newKafkaConsumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(KafkaSpoutConfig.Consumer.BOOTSTRAP_SERVERS, "127.0.0.1:9092");
        props.put(KafkaSpoutConfig.Consumer.GROUP_ID, "kafkaSpoutTestGroup");
        props.put(KafkaSpoutConfig.Consumer.KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(KafkaSpoutConfig.Consumer.VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("max.partition.fetch.bytes", 200);
        return props;
    }

    protected KafkaSpoutTuplesBuilder<String, String> newTuplesBuilder() {
        return new KafkaSpoutTuplesBuilderNamedTopics.Builder<>(
                new TopicsTupleBuilder<String, String>(TOPIC_1, TOPIC_2))
                .build();
    }

    protected KafkaSpoutRetryService newRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(new KafkaSpoutRetryExponentialBackoff.TimeInterval(500L, TimeUnit.MICROSECONDS),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2),
                Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
    }

    protected KafkaSpoutStreams newKafkaSpoutStreams() {
        return new KafkaSpoutStreamsNamedTopics.Builder(new Fields("str"), new String[]{"test-trident","test-trident-1"}).build();
    }

    protected static class TopicsTupleBuilder<K, V> extends KafkaSpoutTupleBuilder<K,V> {
        public TopicsTupleBuilder(String... topics) {
            super(topics);
        }
        @Override
        public List<Object> buildTuple(ConsumerRecord<K, V> consumerRecord) {
            return new Values(consumerRecord.value());
        }
    }

    public static void main(String[] args) throws Exception {
        new TridentKafkaClientWordCountNamedTopics().run(args);
    }

    protected void run(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        if (args.length > 0 && Arrays.binarySearch(args, "-h") >= 0) {
            System.out.printf("Usage: java %s [%s] [%s] [%s] [%s]\n", getClass().getName(),
                    "broker_host:broker_port", "topic1", "topic2", "topology_name");
        } else {
            final String brokerUrl = args.length > 0 ? args[0] : KAFKA_LOCAL_BROKER;
            final String topic1 = args.length > 1 ? args[1] : TOPIC_1;
            final String topic2 = args.length > 2 ? args[2] : TOPIC_2;

            System.out.printf("Running with broker_url: [%s], topics: [%s, %s]\n", brokerUrl, topic1, topic2);

            Config tpConf = LocalSubmitter.defaultConfig();

            if (args.length == 4) { //Submit Remote
                // Producers
                StormSubmitter.submitTopology(topic1 + "-producer", tpConf, KafkaProducerTopology.newTopology(brokerUrl, topic1));
                StormSubmitter.submitTopology(topic2 + "-producer", tpConf, KafkaProducerTopology.newTopology(brokerUrl, topic2));
                // Consumer
                StormSubmitter.submitTopology("topics-consumer", tpConf, TridentKafkaConsumerTopology.newTopology(newKafkaTridentSpoutOpaque()));
            } else { //Submit Local

                final LocalSubmitter localSubmitter = LocalSubmitter.newInstance();
                final String topic1Tp = "topic1-producer";
                final String topic2Tp = "topic2-producer";
                final String consTpName = "topics-consumer";

                try {
                    // Producers
                    localSubmitter.submit(topic1Tp, tpConf, KafkaProducerTopology.newTopology(brokerUrl, topic1));
                    localSubmitter.submit(topic2Tp, tpConf, KafkaProducerTopology.newTopology(brokerUrl, topic2));
                    // Consumer
                    localSubmitter.submit(consTpName, tpConf, TridentKafkaConsumerTopology.newTopology(
                            localSubmitter.getDrpc(), newKafkaTridentSpoutOpaque()));

                    // print
                    localSubmitter.printResults(15, 1, TimeUnit.SECONDS);
                } finally {
                    // kill
                    localSubmitter.kill(topic1Tp);
                    localSubmitter.kill(topic2Tp);
                    localSubmitter.kill(consTpName);
                    // shutdown
                    localSubmitter.shutdown();
                }
            }
        }
        System.exit(0);     // Kill all the non daemon threads
    }
}
