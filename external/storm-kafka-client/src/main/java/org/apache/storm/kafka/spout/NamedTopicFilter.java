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

package org.apache.storm.kafka.spout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/**
 * Filter that returns all partitions for the specified topics.
 */
public class NamedTopicFilter implements TopicFilter {

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
    public List<TopicPartition> getFilteredTopicPartitions(KafkaConsumer<?, ?> consumer) {
        List<TopicPartition> allPartitions = new ArrayList<>();
        for (String topic : topics) {
            for (PartitionInfo partitionInfo: consumer.partitionsFor(topic)) {
                allPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
            }
        }
        return allPartitions;
    }

    @Override
    public String getTopicsString() {
        return StringUtils.join(topics, ",");
    }
}
