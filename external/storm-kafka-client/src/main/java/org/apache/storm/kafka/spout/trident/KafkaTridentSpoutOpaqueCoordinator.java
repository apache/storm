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

package org.apache.storm.kafka.spout.trident;

import org.apache.kafka.common.TopicPartition;
import org.apache.storm.trident.spout.IOpaquePartitionedTridentSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class KafkaTridentSpoutOpaqueCoordinator<K,V> implements IOpaquePartitionedTridentSpout.Coordinator<List<TopicPartition>>, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTridentSpoutOpaqueCoordinator.class);

    private KafkaTridentSpoutManager<K,V> kafkaManager;

    public KafkaTridentSpoutOpaqueCoordinator(KafkaTridentSpoutManager<K, V> kafkaManager) {
        this.kafkaManager = kafkaManager;
        LOG.debug("Created {}", this);
    }

    @Override
    public boolean isReady(long txid) {
        LOG.debug("isReady = true");
        return true;    // the "old" trident kafka spout always returns true, like this
    }

    @Override
    public List<TopicPartition> getPartitionsForBatch() {
        final ArrayList<TopicPartition> topicPartitions = new ArrayList<>(kafkaManager.getTopicPartitions());
        LOG.debug("TopicPartitions for batch {}", topicPartitions);
        return topicPartitions;
    }

    @Override
    public void close() {
        LOG.debug("Closed"); // the "old" trident kafka spout is no op like this
    }

    @Override
    public String toString() {
        return super.toString() +
                "{kafkaManager=" + kafkaManager +
                '}';
    }
}
