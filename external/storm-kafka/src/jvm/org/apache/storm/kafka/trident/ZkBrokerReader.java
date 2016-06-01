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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.kafka.DynamicBrokersReader;
import org.apache.storm.kafka.ZkHosts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.storm.kafka.KafkaConfig;
import org.apache.storm.kafka.ZkMetadataReaderFactory;

public class ZkBrokerReader implements IBrokerReader {

    public static final Logger LOG = LoggerFactory.getLogger(ZkBrokerReader.class);

    List<GlobalPartitionInformation> cachedBrokers = new ArrayList<GlobalPartitionInformation>();
    DynamicBrokersReader reader;
    long lastRefreshTimeMs;

    long refreshMillis;

    public ZkBrokerReader(Map conf, KafkaConfig kafkaConfig) {
        reader = new DynamicBrokersReader(conf, kafkaConfig, new ZkMetadataReaderFactory());
        refresh();
        lastRefreshTimeMs = System.currentTimeMillis();
        ZkHosts zkHosts = (ZkHosts) kafkaConfig.hosts;
        refreshMillis = zkHosts.refreshFreqSecs * 1000L;
    }

    private void refresh() {
        long currTime = System.currentTimeMillis();
        if (currTime > lastRefreshTimeMs + refreshMillis) {
            try {
                LOG.info("brokers need refreshing because " + refreshMillis + "ms have expired");
                cachedBrokers = reader.getBrokerInfo();
                lastRefreshTimeMs = currTime;
            } catch (java.net.SocketTimeoutException e) {
                LOG.warn("Failed to update brokers", e);
            }
        }
    }

    @Override
    public GlobalPartitionInformation getBrokerForTopic(String topic) {
        refresh();
        for (GlobalPartitionInformation partitionInformation : cachedBrokers) {
            if (partitionInformation.topic.equals(topic)) {
                return partitionInformation;
            }
        }
        return null;
    }

    @Override
    public List<GlobalPartitionInformation> getAllBrokers() {
        refresh();
        return cachedBrokers;
    }

    @Override
    public void close() {
        reader.close();
    }
}
