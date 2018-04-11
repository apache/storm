/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.kafka.migration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSpoutMigration {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutMigration.class);

    private static class Configuration {

        private String zkHosts;
        private String zkRoot;
        private String spoutId;
        private String topic;
        private boolean isWildcardTopic;
        private String kafkaBootstrapServers;
        private String newSpoutConsumerGroup;
        private int zkSessionTimeoutMs;
        private int zkConnectionTimeoutMs;
        private int zkRetryTimes;
        private int zkRetryIntervalMs;
    }

    /**
     * Migrates offsets from the Zookeeper store used by the storm-kafka non-Trident spouts, to Kafka's offset store used by the
     * storm-kafka-client non-Trident spout.
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Args: confFile");
            System.exit(1);
        }

        Map<String, Object> conf = Utils.findAndReadConfigFile(args[0]);
        Configuration configuration = new Configuration();

        configuration.zkHosts = MapUtil.getOrError(conf, "zookeeper.servers");
        configuration.zkRoot = MapUtil.getOrError(conf, "zookeeper.root");
        configuration.spoutId = MapUtil.getOrError(conf, "spout.id");
        configuration.topic = MapUtil.getOrError(conf, "topic");
        configuration.isWildcardTopic = MapUtil.getOrError(conf, "is.wildcard.topic");
        configuration.kafkaBootstrapServers = MapUtil.getOrError(conf, "kafka.bootstrap.servers");
        configuration.newSpoutConsumerGroup = MapUtil.getOrError(conf, "new.spout.consumer.group");
        configuration.zkSessionTimeoutMs = MapUtil.getOrError(conf, "zookeeper.session.timeout.ms");
        configuration.zkConnectionTimeoutMs = MapUtil.getOrError(conf, "zookeeper.connection.timeout.ms");
        configuration.zkRetryTimes = MapUtil.getOrError(conf, "zookeeper.retry.times");
        configuration.zkRetryIntervalMs = MapUtil.getOrError(conf, "zookeeper.retry.interval.ms");

        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = getOffsetsToCommit(configuration);

        LOG.info("Migrating offsets {}", offsetsToCommit);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.kafkaBootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, configuration.newSpoutConsumerGroup);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        try (KafkaConsumer<?, ?> consumer = new KafkaConsumer<>(props)) {
            consumer.assign(offsetsToCommit.keySet());
            consumer.commitSync(offsetsToCommit);
        }

        LOG.info("Migrated offsets {} to consumer group {}", offsetsToCommit, configuration.newSpoutConsumerGroup);
    }

    private static Map<TopicPartition, OffsetAndMetadata> getOffsetsAtPath(
        CuratorFramework curator, ObjectMapper objectMapper, String partitionsRoot) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

        if (curator.checkExists().forPath(partitionsRoot) == null) {
            throw new RuntimeException("No such path " + partitionsRoot);
        }
        List<String> partitionPaths = curator.getChildren().forPath(partitionsRoot);
        for (String partitionPath : partitionPaths) {
            String absPartitionPath = partitionsRoot + "/" + partitionPath;
            LOG.info("Reading offset data from path {}", absPartitionPath);
            byte[] partitionBytes = curator.getData().forPath(absPartitionPath);
            Map<String, Object> partitionMetadata = objectMapper.readValue(partitionBytes, new TypeReference<Map<String, Object>>() {
            });
            String topic = (String) partitionMetadata.get("topic");
            int partition = ((Number) partitionMetadata.get("partition")).intValue();
            long offset = ((Number) partitionMetadata.get("offset")).longValue();
            offsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset));
        }
        return offsets;
    }

    private static Map<TopicPartition, OffsetAndMetadata> getOffsetsToCommit(Configuration configuration) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();

        try (CuratorFramework curator = newCurator(configuration)) {
            curator.start();
            ObjectMapper objectMapper = new ObjectMapper();

            String spoutRoot = configuration.zkRoot + "/" + configuration.spoutId;
            if (curator.checkExists().forPath(spoutRoot) == null) {
                throw new RuntimeException("No such path " + spoutRoot);
            }

            if (configuration.isWildcardTopic) {
                LOG.info("Expecting wildcard topics, looking for topics in {}", spoutRoot);
                List<String> topicPaths = curator.getChildren().forPath(spoutRoot);
                for (String topicPath : topicPaths) {
                    if (!topicPath.matches(configuration.topic)) {
                        LOG.info("Skipping directory {} because it doesn't match the topic pattern {}", topicPath, configuration.topic);
                    } else {
                        String absTopicPath = spoutRoot + "/" + topicPath;
                        LOG.info("Looking for partitions in {}", absTopicPath);
                        offsetsToCommit.putAll(getOffsetsAtPath(curator, objectMapper, absTopicPath));
                    }
                }
            } else {
                LOG.info("Expecting exact topic match, looking for offsets in {}", spoutRoot);
                offsetsToCommit.putAll(getOffsetsAtPath(curator, objectMapper, spoutRoot));
            }

        }
        return offsetsToCommit;
    }

    private static CuratorFramework newCurator(Configuration config) throws Exception {
        return CuratorFrameworkFactory.newClient(config.zkHosts,
            config.zkSessionTimeoutMs,
            config.zkConnectionTimeoutMs,
            new RetryNTimes(config.zkRetryTimes,
                config.zkRetryIntervalMs));
    }

}
