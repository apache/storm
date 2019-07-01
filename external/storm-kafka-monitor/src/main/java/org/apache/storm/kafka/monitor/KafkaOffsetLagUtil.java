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

package org.apache.storm.kafka.monitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.json.simple.JSONValue;

/**
 * Utility class for querying offset lag for kafka spout.
 */
public class KafkaOffsetLagUtil {
    private static final String OPTION_TOPIC_SHORT = "t";
    private static final String OPTION_TOPIC_LONG = "topics";
    private static final String OPTION_BOOTSTRAP_BROKERS_SHORT = "b";
    private static final String OPTION_BOOTSTRAP_BROKERS_LONG = "bootstrap-brokers";
    private static final String OPTION_GROUP_ID_SHORT = "g";
    private static final String OPTION_GROUP_ID_LONG = "groupid";
    private static final String OPTION_SECURITY_PROTOCOL_SHORT = "s";
    private static final String OPTION_SECURITY_PROTOCOL_LONG = "security-protocol";
    private static final String OPTION_CONSUMER_CONFIG_SHORT = "c";
    private static final String OPTION_CONSUMER_CONFIG_LONG = "consumer-config";

    public static void main(String[] args) {
        try {
            Options options = buildOptions();
            CommandLineParser parser = new DefaultParser();
            CommandLine commandLine = parser.parse(options, args);
            if (!commandLine.hasOption(OPTION_TOPIC_LONG)) {
                printUsageAndExit(options, OPTION_TOPIC_LONG + " is required");
            }
            String securityProtocol = commandLine.getOptionValue(OPTION_SECURITY_PROTOCOL_LONG);
            if (!commandLine.hasOption(OPTION_GROUP_ID_LONG) || !commandLine.hasOption(OPTION_BOOTSTRAP_BROKERS_LONG)) {
                printUsageAndExit(options, OPTION_GROUP_ID_LONG + " and " + OPTION_BOOTSTRAP_BROKERS_LONG + " are required");
            }
            NewKafkaSpoutOffsetQuery newKafkaSpoutOffsetQuery =
                new NewKafkaSpoutOffsetQuery(commandLine.getOptionValue(OPTION_TOPIC_LONG),
                    commandLine.getOptionValue(OPTION_BOOTSTRAP_BROKERS_LONG),
                    commandLine.getOptionValue(OPTION_GROUP_ID_LONG), securityProtocol,
                    commandLine.getOptionValue(OPTION_CONSUMER_CONFIG_LONG));
            List<KafkaOffsetLagResult> results = getOffsetLags(newKafkaSpoutOffsetQuery);

            Map<String, Map<Integer, KafkaPartitionOffsetLag>> keyedResult = keyByTopicAndPartition(results);
            System.out.print(JSONValue.toJSONString(keyedResult));
        } catch (Exception ex) {
            System.out.print("Unable to get offset lags for kafka. Reason: ");
            ex.printStackTrace(System.out);
        }
    }

    private static Map<String, Map<Integer, KafkaPartitionOffsetLag>> keyByTopicAndPartition(
        List<KafkaOffsetLagResult> results) {
        Map<String, Map<Integer, KafkaPartitionOffsetLag>> resultKeyedByTopic = new HashMap<>();

        for (KafkaOffsetLagResult result : results) {
            String topic = result.getTopic();
            Map<Integer, KafkaPartitionOffsetLag> topicResultKeyedByPartition = resultKeyedByTopic.get(topic);
            if (topicResultKeyedByPartition == null) {
                topicResultKeyedByPartition = new HashMap<>();
                resultKeyedByTopic.put(topic, topicResultKeyedByPartition);
            }

            topicResultKeyedByPartition.put(result.getPartition(),
                                            new KafkaPartitionOffsetLag(result.getConsumerCommittedOffset(), result.getLogHeadOffset()));
        }

        return resultKeyedByTopic;
    }

    private static void printUsageAndExit(Options options, String message) {
        System.out.println(message);
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("storm-kafka-monitor ", options);
        System.exit(1);
    }

    private static Options buildOptions() {
        Options options = new Options();
        options.addOption(OPTION_TOPIC_SHORT, OPTION_TOPIC_LONG,
                true,
                "REQUIRED Topics (comma separated list) for fetching log head and spout committed "
                      + "offset");
        options.addOption(OPTION_BOOTSTRAP_BROKERS_SHORT, OPTION_BOOTSTRAP_BROKERS_LONG,
                true,
                "Comma separated list of bootstrap broker hosts for new "
                        + "consumer/spout e.g. hostname1:9092,hostname2:9092");
        options.addOption(OPTION_GROUP_ID_SHORT, OPTION_GROUP_ID_LONG, true, "Group id of consumer");
        options.addOption(OPTION_SECURITY_PROTOCOL_SHORT,
                OPTION_SECURITY_PROTOCOL_LONG,
                true,
                "Security protocol to connect to kafka");
        options.addOption(OPTION_CONSUMER_CONFIG_SHORT,
                OPTION_CONSUMER_CONFIG_LONG,
                true,
                "Properties file with additional Kafka consumer properties");
        return options;
    }

    /**
     * Get offset lags.
     * @param newKafkaSpoutOffsetQuery represents the information needed to query kafka for log head and spout offsets
     * @return log head offset, spout offset and lag for each partition
     */
    public static List<KafkaOffsetLagResult> getOffsetLags(NewKafkaSpoutOffsetQuery newKafkaSpoutOffsetQuery) throws Exception {
        KafkaConsumer<String, String> consumer = null;
        List<KafkaOffsetLagResult> result = new ArrayList<>();
        try {
            Properties props = new Properties();
            props.put("bootstrap.servers", newKafkaSpoutOffsetQuery.getBootStrapBrokers());
            props.put("group.id", newKafkaSpoutOffsetQuery.getConsumerGroupId());
            props.put("enable.auto.commit", "false");
            props.put("session.timeout.ms", "30000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            if (newKafkaSpoutOffsetQuery.getSecurityProtocol() != null) {
                props.put("security.protocol", newKafkaSpoutOffsetQuery.getSecurityProtocol());
            }
            // Read property file for extra consumer properties
            if (newKafkaSpoutOffsetQuery.getConsumerPropertiesFileName() != null) {
                props.putAll(Utils.loadProps(newKafkaSpoutOffsetQuery.getConsumerPropertiesFileName()));
            }
            List<TopicPartition> topicPartitionList = new ArrayList<>();
            consumer = new KafkaConsumer<>(props);
            for (String topic : newKafkaSpoutOffsetQuery.getTopics().split(",")) {
                List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
                if (partitionInfoList != null) {
                    for (PartitionInfo partitionInfo : partitionInfoList) {
                        topicPartitionList.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
                    }
                }
            }
            consumer.assign(topicPartitionList);
            for (TopicPartition topicPartition : topicPartitionList) {
                OffsetAndMetadata offsetAndMetadata = consumer.committed(topicPartition);
                long committedOffset = offsetAndMetadata != null ? offsetAndMetadata.offset() : -1;
                consumer.seekToEnd(toArrayList(topicPartition));
                result.add(new KafkaOffsetLagResult(topicPartition.topic(), topicPartition.partition(), committedOffset,
                                                    consumer.position(topicPartition)));
            }
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
        return result;
    }

    private static Collection<TopicPartition> toArrayList(final TopicPartition tp) {
        return new ArrayList<TopicPartition>(1) {
            {
                add(tp);
            }
        };
    }

}
