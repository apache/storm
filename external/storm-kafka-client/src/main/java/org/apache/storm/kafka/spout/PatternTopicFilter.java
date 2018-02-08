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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/**
 * Filter that returns all partitions for topics matching the given {@link Pattern}.
 */
public class PatternTopicFilter implements TopicFilter {

    private final Pattern pattern;
    private final Set<String> topics = new HashSet<>();

    /**
     * Creates filter based on a Pattern. Only topic names matching the Pattern are passed by the filter.
     *
     * @param pattern The Pattern to use.
     */
    public PatternTopicFilter(Pattern pattern) {
        this.pattern = pattern;
    }

    @Override
    public List<TopicPartition> getFilteredTopicPartitions(KafkaConsumer<?, ?> consumer) {
        topics.clear();
        List<TopicPartition> allPartitions = new ArrayList<>();
        for (Map.Entry<String, List<PartitionInfo>> entry : consumer.listTopics().entrySet()) {
            if (pattern.matcher(entry.getKey()).matches()) {
                for (PartitionInfo partitionInfo : entry.getValue()) {
                    allPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
                    topics.add(partitionInfo.topic());
                }
            }
        }
        return allPartitions;
    }

    @Override
    public String getTopicsString() {
        return StringUtils.join(topics, ",");
    }

    public String getTopicsPattern() {
        return pattern.pattern();
    }
}
