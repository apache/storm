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
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

/**
 * Handles assigning partitions to the consumer and updating the rebalance listener.
 */
public class TopicAssigner implements Serializable {

    private static final long serialVersionUID = 5423018073527583826L;
    
    /**
     * Assign partitions to the KafkaConsumer.
     * @param <K> The consumer key type
     * @param <V> The consumer value type
     * @param consumer The Kafka consumer to assign partitions to
     * @param newAssignment The partitions to assign.
     * @param listener The rebalance listener to call back on when the assignment changes
     */
    public <K, V> void assignPartitions(Consumer<K, V> consumer, Set<TopicPartition> newAssignment,
        ConsumerRebalanceListener listener) {
        Set<TopicPartition> currentAssignment = consumer.assignment();
        if (!newAssignment.equals(currentAssignment)) {
            listener.onPartitionsRevoked(currentAssignment);
            consumer.assign(newAssignment);
            listener.onPartitionsAssigned(newAssignment);
        }
    }
    
}
