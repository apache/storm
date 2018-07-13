/*
 * Copyright 2017 The Apache Software Foundation.
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

package org.apache.storm.kafka.spout.subscription;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter that returns all partitions for the specified topics.
 */
public class NamedTopicFilter implements TopicFilter {

    private static final Logger LOG = LoggerFactory.getLogger(NamedTopicFilter.class);
    private final Set<String> topics;
    
    /**
     * Create filter based on a set of topic names.
     * @param topics The topic names the filter will pass.
     */
    public NamedTopicFilter(Set<String> topics) {
        this.topics = Collections.unmodifiableSet(topics);
    }
    
    /**
     * Convenience constructor.
     * @param topics The topic names the filter will pass.
     */
    public NamedTopicFilter(String... topics) {
        this(new HashSet<>(Arrays.asList(topics)));
    }
    
    @Override
    public Set<TopicPartition> getAllSubscribedPartitions(Consumer<?, ?> consumer) {
        Set<TopicPartition> allPartitions = new HashSet<>();
        for (String topic : topics) {
            List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
            if (partitionInfoList != null) {
                for (PartitionInfo partitionInfo : partitionInfoList) {
                    allPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
                }
            } else {
                LOG.warn("Topic {} not found, skipping addition of the topic", topic);
            }
        }
        return allPartitions;
    }

    @Override
    public String getTopicsString() {
        return String.join(",", topics);
    }
}
