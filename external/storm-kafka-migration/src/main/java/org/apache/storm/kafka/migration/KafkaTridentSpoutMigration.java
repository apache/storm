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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.PathAndBytesable;
import org.apache.curator.retry.RetryNTimes;
import org.apache.kafka.common.TopicPartition;

import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTridentSpoutMigration {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutMigration.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static class Configuration {

        private String zkHosts;
        private String zkRoot;
        private String txId;
        private String topic;
        private boolean isWildcardTopic;
        private String newTopologyTxId;
        private int zkSessionTimeoutMs;
        private int zkConnectionTimeoutMs;
        private int zkRetryTimes;
        private int zkRetryIntervalMs;
    }

    private static class PartitionMetadata {

        private final long firstOffset;
        private final long lastOffset;

        PartitionMetadata(long firstOffset, long lastOffset) {
            this.firstOffset = firstOffset;
            this.lastOffset = lastOffset;
        }

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this, ToStringStyle.SIMPLE_STYLE);
        }
    }
    
    /**
     * Migrates offsets from the Zookeeper store used by the storm-kafka Trident spouts, to the Zookeeper store used by the
     * storm-kafka-clients Trident spout.
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
        configuration.txId = MapUtil.getOrError(conf, "txid");
        configuration.topic = MapUtil.getOrError(conf, "topic");
        configuration.isWildcardTopic = MapUtil.getOrError(conf, "is.wildcard.topic");
        configuration.newTopologyTxId = MapUtil.getOrError(conf, "new.topology.txid");
        configuration.zkSessionTimeoutMs = MapUtil.getOrError(conf, "zookeeper.session.timeout.ms");
        configuration.zkConnectionTimeoutMs = MapUtil.getOrError(conf, "zookeeper.connection.timeout.ms");
        configuration.zkRetryTimes = MapUtil.getOrError(conf, "zookeeper.retry.times");
        configuration.zkRetryIntervalMs = MapUtil.getOrError(conf, "zookeeper.retry.interval.ms");

        try (CuratorFramework curator = newCurator(configuration)) {
            curator.start();

            Map<TopicPartition, Map<Long, PartitionMetadata>> offsetsToMigrate = getOffsetsToMigrate(curator, configuration);

            LOG.info("Migrating offsets {}", offsetsToMigrate);

            migrateOffsets(curator, configuration, offsetsToMigrate);

            migrateCoordinator(curator, configuration, new ArrayList<>(offsetsToMigrate.keySet()));
        }
    }

    private static Map<TopicPartition, Map<Long, PartitionMetadata>> getOffsetsAtPath(
        CuratorFramework curator, ObjectMapper objectMapper, String partitionsRoot) throws Exception {
        Map<TopicPartition, Map<Long, PartitionMetadata>> offsets = new HashMap<>();

        if (curator.checkExists().forPath(partitionsRoot) == null) {
            throw new RuntimeException("No such path " + partitionsRoot);
        }

        List<String> partitionPaths = curator.getChildren().forPath(partitionsRoot);
        for (String partitionPath : partitionPaths) {
            String absPartitionPath = partitionsRoot + "/" + partitionPath;
            List<String> transactions = curator.getChildren().forPath(absPartitionPath);
            Map<Long, PartitionMetadata> partitionMeta = new HashMap<>();
            TopicPartition tp = null;
            for (String transaction : transactions) {
                String absTransactionPath = absPartitionPath + "/" + transaction;
                LOG.info("Reading offset data from path {}", absTransactionPath);
                byte[] partitionBytes = curator.getData().forPath(absTransactionPath);
                Map<String, Object> partitionMetadata = objectMapper.readValue(partitionBytes, new TypeReference<Map<String, Object>>() {
                });
                tp = new TopicPartition((String) partitionMetadata.get("topic"), ((Number) partitionMetadata.get("partition")).intValue());
                PartitionMetadata meta = new PartitionMetadata(
                    ((Number) partitionMetadata.get("offset")).longValue(),
                    ((Number) partitionMetadata.get("nextOffset")).longValue() - 1); //nextOffset is the last offset from last batch + 1
                partitionMeta.put(Long.parseLong(transaction), meta);
            }
            if (tp != null) {
                offsets.put(tp, partitionMeta);
            }
        }
        return offsets;
    }

    private static Map<TopicPartition, Map<Long, PartitionMetadata>> getOffsetsToMigrate(
        CuratorFramework curator, Configuration configuration) throws Exception {
        //Read the partitions, transaction ids and offsets from the old storm-kafka /user path
        Map<TopicPartition, Map<Long, PartitionMetadata>> offsetsToMigrate = new HashMap<>();

        String streamRoot = configuration.zkRoot + "/" + configuration.txId + "/user";
        if (curator.checkExists().forPath(streamRoot) == null) {
            throw new RuntimeException("No such path " + streamRoot);
        }

        if (configuration.isWildcardTopic) {
            LOG.info("Expecting wildcard topics, looking for topics in {}", streamRoot);
            List<String> topics = curator.getChildren().forPath(streamRoot);
            for (String topic : topics) {
                if (!topic.matches(configuration.topic)) {
                    LOG.info("Skipping directory {} because it does not match topic pattern {}", topic, configuration.topic);
                } else {
                    String partitionsRoot = streamRoot + "/" + topic;
                    LOG.info("Looking for partitions in {}", partitionsRoot);
                    offsetsToMigrate.putAll(getOffsetsAtPath(curator, objectMapper, partitionsRoot));
                }
            }
        } else {
            LOG.info("Expecting exact topic match, looking for offsets in {}", streamRoot);
            offsetsToMigrate.putAll(getOffsetsAtPath(curator, objectMapper, streamRoot));
        }
        return offsetsToMigrate;
    }

    private static String coordinatorPath(Configuration configuration, String txid) {
        return configuration.zkRoot + "/" + txid + "/coordinator";
    }

    private static void migrateCoordinator(
        CuratorFramework curator, Configuration configuration, List<TopicPartition> topics) throws Exception {
        //Migrate the /coordinator currtx, currattempts and meta directories.
        //The new spout expects the list of topic partitions as coordinator meta.
        String oldCoordinatorRoot = coordinatorPath(configuration, configuration.txId);
        String newCoordinatorRoot = coordinatorPath(configuration, configuration.newTopologyTxId);

        String oldTxPath = oldCoordinatorRoot + "/currtx";
        String newTxPath = newCoordinatorRoot + "/currtx";
        createOrUpdate(curator, newTxPath).forPath(newTxPath, curator.getData().forPath(oldTxPath));

        String oldAttemptsPath = oldCoordinatorRoot + "/currattempts";
        String newAttemptsPath = newCoordinatorRoot + "/currattempts";
        createOrUpdate(curator, newAttemptsPath).forPath(newAttemptsPath, curator.getData().forPath(oldAttemptsPath));

        List<String> transactions = curator.getChildren().forPath(oldCoordinatorRoot + "/meta");
        List<Map<String, Object>> coordinatorMeta = new ArrayList<>();
        for (TopicPartition tp : topics) {
            coordinatorMeta.add(tpMeta(tp));
        }
        for (String transaction : transactions) {
            String newMetaPath = newCoordinatorRoot + "/meta/" + transaction;
            createOrUpdate(curator, newMetaPath).forPath(newMetaPath, objectMapper.writeValueAsBytes(coordinatorMeta));
        }
        LOG.info("Migrated coordinator data to new path {}", newCoordinatorRoot);
    }

    private static PathAndBytesable<?> createOrUpdate(CuratorFramework curator, String path) throws Exception {
        if (curator.checkExists().forPath(path) == null) {
            return curator.create().creatingParentsIfNeeded();
        } else {
            return curator.setData();
        }
    }

    private static Map<String, Object> tpMeta(TopicPartition tp) {
        Map<String, Object> tpMeta = new HashMap<>();
        tpMeta.put("topic", tp.topic());
        tpMeta.put("partition", tp.partition());
        return tpMeta;
    }

    private static void migrateOffsets(
        CuratorFramework curator, Configuration configuration, Map<TopicPartition, Map<Long, PartitionMetadata>> offsets) throws Exception {
        //Writes the offsets in the new format to the /user partitions paths
        String streamRoot = configuration.zkRoot + "/" + configuration.newTopologyTxId + "/user";

        for (Entry<TopicPartition, Map<Long, PartitionMetadata>> offset : offsets.entrySet()) {
            TopicPartition tp = offset.getKey();
            for (Entry<Long, PartitionMetadata> transaction : offset.getValue().entrySet()) {
                PartitionMetadata meta = transaction.getValue();
                Map<String, Object> metadataToWrite = new HashMap<>();
                metadataToWrite.put("firstOffset", meta.firstOffset);
                metadataToWrite.put("lastOffset", meta.lastOffset);
                metadataToWrite.put("tp", tpMeta(tp));
                String partitionPath = streamRoot + "/" + tp.topic() + "@" + tp.partition() + "/" + transaction.getKey();
                LOG.info("Writing {} to path {}", metadataToWrite, partitionPath);
                createOrUpdate(curator, partitionPath).forPath(partitionPath, objectMapper.writeValueAsBytes(metadataToWrite));
            }
        }

        LOG.info("Migrated offsets {} to new root {}", offsets, streamRoot);
    }

    private static CuratorFramework newCurator(Configuration config) throws Exception {
        return CuratorFrameworkFactory.newClient(config.zkHosts,
            config.zkSessionTimeoutMs,
            config.zkConnectionTimeoutMs,
            new RetryNTimes(config.zkRetryTimes,
                config.zkRetryIntervalMs));
    }

}
