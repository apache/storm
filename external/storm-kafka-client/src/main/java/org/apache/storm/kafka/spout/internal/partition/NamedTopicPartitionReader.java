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

package org.apache.storm.kafka.spout.internal.partition;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class NamedTopicPartitionReader implements KafkaPartitionReader {
    private final Set<String > topics;

    public NamedTopicPartitionReader(Set<String> topics) {
        this.topics = topics;
    }

    @Override
    public List<TopicPartition> readPartitions(KafkaConsumer<?, ?> consumer) {
        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (String topic : topics) {
            for (PartitionInfo partitionInfo: consumer.partitionsFor(topic)) {
                topicPartitions.add(KafkaPartitionReaders.toTopicPartition(partitionInfo));
            }
        }

        return topicPartitions;
    }
}
