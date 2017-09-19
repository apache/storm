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

import java.io.Serializable;
import java.util.List;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public interface TopicFilter extends Serializable {
    
    /**
     * Get the Kafka TopicPartitions passed by this filter. 
     * @param consumer The Kafka consumer to use to read the list of existing partitions
     * @return The Kafka partitions passed by this filter.
     */
    List<TopicPartition> getFilteredTopicPartitions(KafkaConsumer<?, ?> consumer);
    
    /**
     * Get the topics string.
     * @return A human-readable string representing the topics that pass the filter.
     */
    String getTopicsString();

}
