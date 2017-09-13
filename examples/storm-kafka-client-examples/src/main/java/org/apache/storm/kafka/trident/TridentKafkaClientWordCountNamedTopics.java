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
import org.apache.storm.kafka.spout.Func;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutOpaque;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;

public class TridentKafkaClientWordCountNamedTopics {
    private static final String TOPIC_1 = "test-trident";
    private static final String TOPIC_2 = "test-trident-1";
    private static final String KAFKA_LOCAL_BROKER = "localhost:9092";

    private KafkaTridentSpoutOpaque<String, String> newKafkaTridentSpoutOpaque() {
        return new KafkaTridentSpoutOpaque<>(newKafkaSpoutConfig());
    }

    private static Func<ConsumerRecord<String, String>, List<Object>> JUST_VALUE_FUNC = new JustValueFunc();

    /**
     * Needs to be serializable
     */
    private static class JustValueFunc implements Func<ConsumerRecord<String, String>, List<Object>>, Serializable {
        @Override
        public List<Object> apply(ConsumerRecord<String, String> record) {
            return new Values(record.value());
        }
    };

    protected KafkaSpoutConfig<String,String> newKafkaSpoutConfig() {
        return KafkaSpoutConfig.builder(KAFKA_LOCAL_BROKER, TOPIC_1, TOPIC_2)
                .setGroupId("kafkaSpoutTestGroup_" + System.nanoTime())
                .setMaxPartitionFectchBytes(200)
                .setRecordTranslator(JUST_VALUE_FUNC, new Fields("str"))
                .setRetry(newRetryService())
                .setOffsetCommitPeriodMs(10_000)
                .setFirstPollOffsetStrategy(EARLIEST)
                .setMaxUncommittedOffsets(250)
                .build();
    }

    protected KafkaSpoutRetryService newRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(new TimeInterval(500L, TimeUnit.MICROSECONDS),
                TimeInterval.milliSeconds(2), Integer.MAX_VALUE, TimeInterval.seconds(10));
    }

    public static void main(String[] args) throws Exception {
        new TridentKafkaClientWordCountNamedTopics().run(args);
    }

    protected void run(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, InterruptedException {
        if (args.length > 0 && Arrays.binarySearch(args, "-h") >= 0) {
            System.out.printf("Usage: java %s [%s] [%s] [%s] [%s]\n", getClass().getName(),
                    "broker_host:broker_port", "topic1", "topic2", "topology_name");
        } else {
            final String brokerUrl = args.length > 0 ? args[0] : KAFKA_LOCAL_BROKER;
            final String topic1 = args.length > 1 ? args[1] : TOPIC_1;
            final String topic2 = args.length > 2 ? args[2] : TOPIC_2;

            System.out.printf("Running with broker_url: [%s], topics: [%s, %s]\n", brokerUrl, topic1, topic2);

            Config tpConf = LocalSubmitter.defaultConfig(true);

            if (args.length == 4) { //Submit Remote
                // Producers
                StormSubmitter.submitTopology(topic1 + "-producer", tpConf, KafkaProducerTopology.newTopology(brokerUrl, topic1));
                StormSubmitter.submitTopology(topic2 + "-producer", tpConf, KafkaProducerTopology.newTopology(brokerUrl, topic2));
                // Consumer
                StormSubmitter.submitTopology("topics-consumer", tpConf, TridentKafkaConsumerTopology.newTopology(newKafkaTridentSpoutOpaque()));

                // Print results to console, which also causes the print filter in the consumer topology to print the results in the worker log
                Thread.sleep(2000);
                DrpcResultsPrinter.remoteClient().printResults(60, 1, TimeUnit.SECONDS);

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
                    try {
                        localSubmitter.submit(consTpName, tpConf, TridentKafkaConsumerTopology.newTopology(
                                localSubmitter.getDrpc(), newKafkaTridentSpoutOpaque()));
                        // print
                        localSubmitter.printResults(15, 1, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

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
