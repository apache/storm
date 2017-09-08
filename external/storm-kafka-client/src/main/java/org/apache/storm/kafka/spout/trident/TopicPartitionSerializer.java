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

package org.apache.storm.kafka.spout.trident;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;

public class TopicPartitionSerializer {

    public static final String TOPIC_PARTITION_TOPIC_KEY = "topic";
    public static final String TOPIC_PARTITION_PARTITION_KEY = "partition";

    /**
     * Serializes the given TopicPartition to Map so Trident can serialize it to JSON.
     */
    public Map<String, Object> toMap(TopicPartition topicPartition) {
        Map<String, Object> topicPartitionMap = new HashMap<>();
        topicPartitionMap.put(TOPIC_PARTITION_TOPIC_KEY, topicPartition.topic());
        topicPartitionMap.put(TOPIC_PARTITION_PARTITION_KEY, topicPartition.partition());
        return topicPartitionMap;
    }

    /**
     * Deserializes the given map into a TopicPartition. The map keys are expected to be those produced by
     * {@link #toMap(org.apache.kafka.common.TopicPartition)}.
     */
    public TopicPartition fromMap(Map<String, Object> map) {
        return new TopicPartition((String) map.get(TOPIC_PARTITION_TOPIC_KEY),
            ((Number) map.get(TOPIC_PARTITION_PARTITION_KEY)).intValue());
    }

}
