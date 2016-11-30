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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.json.simple.JSONValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;

/**
 * Utility class for querying offset lag for kafka spout
 */
public class KafkaOffsetLagUtil {
    private static final String OPTION_TOPIC_SHORT = "t";
    private static final String OPTION_TOPIC_LONG = "topics";
    private static final String OPTION_OLD_CONSUMER_SHORT = "o";
    private static final String OPTION_OLD_CONSUMER_LONG = "old-spout";
    private static final String OPTION_BOOTSTRAP_BROKERS_SHORT = "b";
    private static final String OPTION_BOOTSTRAP_BROKERS_LONG = "bootstrap-brokers";
    private static final String OPTION_GROUP_ID_SHORT = "g";
    private static final String OPTION_GROUP_ID_LONG = "groupid";
    private static final String OPTION_TOPIC_WILDCARD_SHORT = "w";
    private static final String OPTION_TOPIC_WILDCARD_LONG = "wildcard-topic";
    private static final String OPTION_PARTITIONS_SHORT = "p";
    private static final String OPTION_PARTITIONS_LONG = "partitions";
    private static final String OPTION_LEADERS_SHORT = "l";
    private static final String OPTION_LEADERS_LONG = "leaders";
    private static final String OPTION_ZK_SERVERS_SHORT = "z";
    private static final String OPTION_ZK_SERVERS_LONG = "zk-servers";
    private static final String OPTION_ZK_COMMITTED_NODE_SHORT = "n";
    private static final String OPTION_ZK_COMMITTED_NODE_LONG = "zk-node";
    private static final String OPTION_ZK_BROKERS_ROOT_SHORT = "r";
    private static final String OPTION_ZK_BROKERS_ROOT_LONG = "zk-brokers-root-node";
    private static final String OPTION_SECURITY_PROTOCOL_SHORT = "s";
    private static final String OPTION_SECURITY_PROTOCOL_LONG = "security-protocol";

    public static void main (String args[]) {
        try {
            List<KafkaOffsetLagResult> results;
            Options options = buildOptions();
            CommandLineParser parser = new DefaultParser();
            CommandLine commandLine = parser.parse(options, args);
            if (!commandLine.hasOption(OPTION_TOPIC_LONG)) {
                printUsageAndExit(options, OPTION_TOPIC_LONG + " is required");
            }
            if (commandLine.hasOption(OPTION_OLD_CONSUMER_LONG)) {
                OldKafkaSpoutOffsetQuery oldKafkaSpoutOffsetQuery;
                if (commandLine.hasOption(OPTION_GROUP_ID_LONG) || commandLine.hasOption(OPTION_BOOTSTRAP_BROKERS_LONG) || commandLine.hasOption(OPTION_SECURITY_PROTOCOL_LONG)) {
                    printUsageAndExit(options, OPTION_GROUP_ID_LONG + " or " + OPTION_BOOTSTRAP_BROKERS_LONG + " or " + OPTION_SECURITY_PROTOCOL_LONG + " is " +
                            "not accepted with option " + OPTION_OLD_CONSUMER_LONG);
                }
                if (!commandLine.hasOption(OPTION_ZK_SERVERS_LONG) || !commandLine.hasOption(OPTION_ZK_COMMITTED_NODE_LONG)) {
                    printUsageAndExit(options, OPTION_ZK_SERVERS_LONG + " and " + OPTION_ZK_COMMITTED_NODE_LONG + " are required  with " +
                            OPTION_OLD_CONSUMER_LONG);
                }
                String[] topics = commandLine.getOptionValue(OPTION_TOPIC_LONG).split(",");
                if (topics != null && topics.length > 1) {
                    printUsageAndExit(options, "Multiple topics not supported with option " + OPTION_OLD_CONSUMER_LONG + ". Either a single topic or a " +
                            "wildcard string for matching topics is supported");
                }
                if (commandLine.hasOption(OPTION_ZK_BROKERS_ROOT_LONG)) {
                    if (commandLine.hasOption(OPTION_PARTITIONS_LONG) || commandLine.hasOption(OPTION_LEADERS_LONG)) {
                        printUsageAndExit(options, OPTION_PARTITIONS_LONG + " or " + OPTION_LEADERS_LONG + " is not accepted with " +
                                OPTION_ZK_BROKERS_ROOT_LONG);
                    }
                    oldKafkaSpoutOffsetQuery = new OldKafkaSpoutOffsetQuery(commandLine.getOptionValue(OPTION_TOPIC_LONG), commandLine.getOptionValue
                            (OPTION_ZK_SERVERS_LONG), commandLine.getOptionValue(OPTION_ZK_COMMITTED_NODE_LONG), commandLine.hasOption
                            (OPTION_TOPIC_WILDCARD_LONG), commandLine.getOptionValue(OPTION_ZK_BROKERS_ROOT_LONG));
                } else {
                    if (commandLine.hasOption(OPTION_TOPIC_WILDCARD_LONG)) {
                        printUsageAndExit(options, OPTION_TOPIC_WILDCARD_LONG + " is not supported without " + OPTION_ZK_BROKERS_ROOT_LONG);
                    }
                    if (!commandLine.hasOption(OPTION_PARTITIONS_LONG) || !commandLine.hasOption(OPTION_LEADERS_LONG)) {
                        printUsageAndExit(options, OPTION_PARTITIONS_LONG + " and " + OPTION_LEADERS_LONG + " are required if " + OPTION_ZK_BROKERS_ROOT_LONG +
                                " is not provided");
                    }
                    String[] partitions = commandLine.getOptionValue(OPTION_PARTITIONS_LONG).split(",");
                    String[] leaders = commandLine.getOptionValue(OPTION_LEADERS_LONG).split(",");
                    if (partitions.length != leaders.length) {
                        printUsageAndExit(options, OPTION_PARTITIONS_LONG + " and " + OPTION_LEADERS_LONG + " need to be of same size");
                    }
                    oldKafkaSpoutOffsetQuery = new OldKafkaSpoutOffsetQuery(commandLine.getOptionValue(OPTION_TOPIC_LONG), commandLine.getOptionValue
                            (OPTION_ZK_SERVERS_LONG), commandLine.getOptionValue(OPTION_ZK_COMMITTED_NODE_LONG), commandLine.getOptionValue
                            (OPTION_PARTITIONS_LONG), commandLine.getOptionValue(OPTION_LEADERS_LONG));
                }
                results = getOffsetLags(oldKafkaSpoutOffsetQuery);
            } else {
                String securityProtocol = commandLine.getOptionValue(OPTION_SECURITY_PROTOCOL_LONG);
                String[] oldSpoutOptions = {OPTION_TOPIC_WILDCARD_LONG, OPTION_PARTITIONS_LONG, OPTION_LEADERS_LONG, OPTION_ZK_SERVERS_LONG,
                        OPTION_ZK_COMMITTED_NODE_LONG, OPTION_ZK_BROKERS_ROOT_LONG};
                for (String oldOption: oldSpoutOptions) {
                    if (commandLine.hasOption(oldOption)) {
                        printUsageAndExit(options, oldOption + " is not accepted without " + OPTION_OLD_CONSUMER_LONG);
                    }
                }
                if (!commandLine.hasOption(OPTION_GROUP_ID_LONG) || !commandLine.hasOption(OPTION_BOOTSTRAP_BROKERS_LONG)) {
                    printUsageAndExit(options, OPTION_GROUP_ID_LONG + " and " + OPTION_BOOTSTRAP_BROKERS_LONG + " are required if " + OPTION_OLD_CONSUMER_LONG +
                            " is not specified");
                }
                NewKafkaSpoutOffsetQuery newKafkaSpoutOffsetQuery = new NewKafkaSpoutOffsetQuery(commandLine.getOptionValue(OPTION_TOPIC_LONG),
                        commandLine.getOptionValue(OPTION_BOOTSTRAP_BROKERS_LONG), commandLine.getOptionValue(OPTION_GROUP_ID_LONG), securityProtocol);
                results = getOffsetLags(newKafkaSpoutOffsetQuery);
            }

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

    private static void printUsageAndExit (Options options, String message) {
        System.out.println(message);
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("storm-kafka-monitor ", options);
        System.exit(1);
    }

    private static Options buildOptions () {
        Options options = new Options();
        options.addOption(OPTION_TOPIC_SHORT, OPTION_TOPIC_LONG, true, "REQUIRED Topics (comma separated list) for fetching log head and spout committed " +
                "offset");
        options.addOption(OPTION_OLD_CONSUMER_SHORT, OPTION_OLD_CONSUMER_LONG, false, "Whether request is for old spout");
        options.addOption(OPTION_BOOTSTRAP_BROKERS_SHORT, OPTION_BOOTSTRAP_BROKERS_LONG, true, "Comma separated list of bootstrap broker hosts for new " +
                "consumer/spout e.g. hostname1:9092,hostname2:9092");
        options.addOption(OPTION_GROUP_ID_SHORT, OPTION_GROUP_ID_LONG, true, "Group id of consumer (applicable only for new kafka spout) ");
        options.addOption(OPTION_TOPIC_WILDCARD_SHORT, OPTION_TOPIC_WILDCARD_LONG, false, "Whether topic provided is a wildcard as supported by ZkHosts in " +
                "old spout");
        options.addOption(OPTION_PARTITIONS_SHORT, OPTION_PARTITIONS_LONG, true, "Comma separated list of partitions corresponding to " +
                OPTION_LEADERS_LONG + " for old spout with StaticHosts");
        options.addOption(OPTION_LEADERS_SHORT, OPTION_LEADERS_LONG, true, "Comma separated list of broker leaders corresponding to " +
                OPTION_PARTITIONS_LONG + " for old spout with StaticHosts e.g. hostname1:9092,hostname2:9092");
        options.addOption(OPTION_ZK_SERVERS_SHORT, OPTION_ZK_SERVERS_LONG, true, "Comma separated list of zk servers for fetching spout committed offsets  " +
                "and/or topic metadata for ZkHosts e.g hostname1:2181,hostname2:2181");
        options.addOption(OPTION_ZK_COMMITTED_NODE_SHORT, OPTION_ZK_COMMITTED_NODE_LONG, true, "Zk node prefix where old kafka spout stores the committed" +
                " offsets without the topic and partition nodes");
        options.addOption(OPTION_ZK_BROKERS_ROOT_SHORT, OPTION_ZK_BROKERS_ROOT_LONG, true, "Zk node prefix where kafka stores broker information e.g. " +
                "/brokers (applicable only for old kafka spout) ");
        options.addOption(OPTION_SECURITY_PROTOCOL_SHORT, OPTION_SECURITY_PROTOCOL_LONG, true, "Security protocol to connect to kafka");
        return options;
    }

    /**
     *
     * @param newKafkaSpoutOffsetQuery represents the information needed to query kafka for log head and spout offsets
     * @return log head offset, spout offset and lag for each partition
     */
    public static List<KafkaOffsetLagResult> getOffsetLags (NewKafkaSpoutOffsetQuery newKafkaSpoutOffsetQuery) {
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
            List<TopicPartition> topicPartitionList = new ArrayList<>();
            consumer = new KafkaConsumer<>(props);
            for (String topic: newKafkaSpoutOffsetQuery.getTopics().split(",")) {
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
                result.add(new KafkaOffsetLagResult(topicPartition.topic(), topicPartition.partition(), committedOffset, consumer.position(topicPartition)));
            }
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
        return result;
    }

    private static Collection<TopicPartition> toArrayList(final TopicPartition tp) {
        return new ArrayList<TopicPartition>(1){{add(tp);}};
    }

    /**
     *
     * @param oldKafkaSpoutOffsetQuery represents the information needed to query kafka for log head and spout offsets
     * @return log head offset, spout offset and lag for each partition
     */
    public static List<KafkaOffsetLagResult> getOffsetLags (OldKafkaSpoutOffsetQuery oldKafkaSpoutOffsetQuery) throws Exception {
        List<KafkaOffsetLagResult> result = new ArrayList<>();
        Map<String, List<TopicPartition>> leaders = getLeadersAndTopicPartitions(oldKafkaSpoutOffsetQuery);
        if (leaders != null) {
            Map<String, Map<Integer, Long>> logHeadOffsets = getLogHeadOffsets(leaders);
            Map<String, List<Integer>> topicPartitions = new HashMap<>();
            for (Map.Entry<String, List<TopicPartition>> entry: leaders.entrySet()) {
                for (TopicPartition topicPartition: entry.getValue()) {
                    if (!topicPartitions.containsKey(topicPartition.topic())) {
                        topicPartitions.put(topicPartition.topic(), new ArrayList<Integer>());
                    }
                    topicPartitions.get(topicPartition.topic()).add(topicPartition.partition());
                }
            }
            Map<String, Map<Integer, Long>> oldConsumerOffsets = getOldConsumerOffsetsFromZk(topicPartitions, oldKafkaSpoutOffsetQuery);
            for (Map.Entry<String, Map<Integer, Long>> topicOffsets: logHeadOffsets.entrySet()) {
                for (Map.Entry<Integer, Long> partitionOffsets: topicOffsets.getValue().entrySet()) {
                    Long consumerCommittedOffset = oldConsumerOffsets.get(topicOffsets.getKey()) != null ? (Long) oldConsumerOffsets.get(topicOffsets.getKey()).get
                        (partitionOffsets.getKey()) : -1;
                    consumerCommittedOffset = (consumerCommittedOffset == null ? -1 : consumerCommittedOffset);
                    KafkaOffsetLagResult kafkaOffsetLagResult = new KafkaOffsetLagResult(topicOffsets.getKey(), partitionOffsets.getKey(),
                            consumerCommittedOffset, partitionOffsets.getValue());
                    result.add(kafkaOffsetLagResult);
                }
            }
        }
        return result;
    }

    private static Map<String, List<TopicPartition>> getLeadersAndTopicPartitions (OldKafkaSpoutOffsetQuery oldKafkaSpoutOffsetQuery) throws Exception {
        Map<String, List<TopicPartition>> result = new HashMap<>();
        // this means that kafka spout was configured with StaticHosts hosts (leader for partition)
        if (oldKafkaSpoutOffsetQuery.getPartitions() != null) {
            String[] partitions = oldKafkaSpoutOffsetQuery.getPartitions().split(",");
            String[] leaders = oldKafkaSpoutOffsetQuery.getLeaders().split(",");
            for (int i = 0; i < leaders.length; ++i) {
                if (!result.containsKey(leaders[i])) {
                    result.put(leaders[i], new ArrayList<TopicPartition>());
                }
                result.get(leaders[i]).add(new TopicPartition(oldKafkaSpoutOffsetQuery.getTopic(), Integer.parseInt(partitions[i])));
            }
        } else {
            // else use zk nodes to figure out partitions and leaders for topics i.e. ZkHosts
            CuratorFramework curatorFramework = null;
            try {
                String brokersZkNode = oldKafkaSpoutOffsetQuery.getBrokersZkPath();
                if (!brokersZkNode.endsWith("/")) {
                    brokersZkNode += "/";
                }
                String topicsZkPath = brokersZkNode + "topics";
                curatorFramework = CuratorFrameworkFactory.newClient(oldKafkaSpoutOffsetQuery.getZkServers(), 20000, 15000, new RetryOneTime(1000));
                curatorFramework.start();
                List<String> topics = new ArrayList<>();
                if (oldKafkaSpoutOffsetQuery.isWildCardTopic()) {
                    List<String> children = curatorFramework.getChildren().forPath(topicsZkPath);
                    for (String child : children) {
                        if (child.matches(oldKafkaSpoutOffsetQuery.getTopic())) {
                            topics.add(child);
                        }
                    }
                } else {
                    topics.add(oldKafkaSpoutOffsetQuery.getTopic());
                }
                for (String topic: topics) {
                    String partitionsPath = topicsZkPath + "/" + topic + "/partitions";
                    List<String> children = curatorFramework.getChildren().forPath(partitionsPath);
                    for (int i = 0; i < children.size(); ++i) {
                        byte[] leaderData = curatorFramework.getData().forPath(partitionsPath + "/" + i + "/state");
                        Map<Object, Object> value = (Map<Object, Object>) JSONValue.parseWithException(new String(leaderData, "UTF-8"));
                        Integer leader = ((Number) value.get("leader")).intValue();
                        byte[] brokerData = curatorFramework.getData().forPath(brokersZkNode + "ids/" + leader);
                        Map<Object, Object> broker = (Map<Object, Object>) JSONValue.parseWithException(new String(brokerData, "UTF-8"));
                        String host = (String) broker.get("host");
                        Integer port = ((Long) broker.get("port")).intValue();
                        String leaderBroker = host + ":" + port;
                        if (!result.containsKey(leaderBroker)) {
                            result.put(leaderBroker, new ArrayList<TopicPartition>());
                        }
                        result.get(leaderBroker).add(new TopicPartition(topic, i));
                    }
                }
            } finally {
                if (curatorFramework != null) {
                    curatorFramework.close();
                }
            }
        }
        return result;
    }

    private static Map<String, Map<Integer, Long>> getLogHeadOffsets (Map<String, List<TopicPartition>> leadersAndTopicPartitions) {
        Map<String, Map<Integer, Long>> result = new HashMap<>();
        if (leadersAndTopicPartitions != null) {
            PartitionOffsetRequestInfo partitionOffsetRequestInfo = new PartitionOffsetRequestInfo(OffsetRequest.LatestTime(), 1);
            SimpleConsumer simpleConsumer  = null;
            for (Map.Entry<String, List<TopicPartition>> leader: leadersAndTopicPartitions.entrySet()) {
                try {
                    simpleConsumer = new SimpleConsumer(leader.getKey().split(":")[0], Integer.parseInt(leader.getKey().split(":")[1]), 10000, 64 *
                            1024, "LogHeadOffsetRequest");
                    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
                    for (TopicPartition topicPartition : leader.getValue()) {
                        requestInfo.put(new TopicAndPartition(topicPartition.topic(), topicPartition.partition()), partitionOffsetRequestInfo);
                        if (!result.containsKey(topicPartition.topic())) {
                            result.put(topicPartition.topic(), new HashMap<Integer, Long>());
                        }
                    }
                    kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
                            "LogHeadOffsetRequest");
                    OffsetResponse response = simpleConsumer.getOffsetsBefore(request);
                    for (TopicPartition topicPartition : leader.getValue()) {
                        result.get(topicPartition.topic()).put(topicPartition.partition(), response.offsets(topicPartition.topic(), topicPartition.partition())[0]);
                    }
                } finally {
                    if (simpleConsumer != null) {
                        simpleConsumer.close();
                    }
                }
            }
        }
        return result;
    }

    private static Map<String, Map<Integer, Long>> getOldConsumerOffsetsFromZk (Map<String, List<Integer>> topicPartitions, OldKafkaSpoutOffsetQuery
            oldKafkaSpoutOffsetQuery) throws Exception {
        Map<String, Map<Integer, Long>> result = new HashMap<>();
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(oldKafkaSpoutOffsetQuery.getZkServers(), 20000, 15000, new RetryOneTime(1000));
        curatorFramework.start();
        String partitionPrefix = "partition_";
        String zkPath = oldKafkaSpoutOffsetQuery.getZkPath();
        if (zkPath.endsWith("/")) {
            zkPath = zkPath.substring(0, zkPath.length()-1);
        }
        if (curatorFramework.checkExists().forPath(zkPath) == null) {
            throw new IllegalArgumentException(OPTION_ZK_COMMITTED_NODE_LONG+" '"+zkPath+"' dose not exists.");
        }
        byte[] zkData;
        try {
            if (topicPartitions != null) {
                for (Map.Entry<String, List<Integer>> topicEntry: topicPartitions.entrySet()) {
                    Map<Integer, Long> partitionOffsets = new HashMap<>();
                    for (Integer partition: topicEntry.getValue()) {
                        String path = zkPath + "/" + (oldKafkaSpoutOffsetQuery.isWildCardTopic() ? topicEntry.getKey() + "/" : "") + partitionPrefix + partition;
                        if (curatorFramework.checkExists().forPath(path) != null) {
                            zkData = curatorFramework.getData().forPath(path);
                            Map<Object, Object> offsetData = (Map<Object, Object>) JSONValue.parseWithException(new String(zkData, "UTF-8"));
                            partitionOffsets.put(partition, (Long) offsetData.get("offset"));
                        }
                    }
                    result.put(topicEntry.getKey(), partitionOffsets);
                }
            }
        } finally {
            if (curatorFramework != null) {
                curatorFramework.close();
            }
        }
        return result;
    }

}
