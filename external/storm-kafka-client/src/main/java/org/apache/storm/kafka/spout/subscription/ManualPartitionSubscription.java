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

package org.apache.storm.kafka.spout.subscription;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.TopicPartitionComparator;
import org.apache.storm.task.TopologyContext;

public class ManualPartitionSubscription extends Subscription {
    private static final long serialVersionUID = 5633018073527583826L;
    private final ManualPartitioner partitioner;
    private final TopicFilter partitionFilter;
    private transient Set<TopicPartition> currentAssignment = null;
    private transient KafkaConsumer<?, ?> consumer = null;
    private transient ConsumerRebalanceListener listener = null;
    private transient TopologyContext context = null;

    public ManualPartitionSubscription(ManualPartitioner parter, TopicFilter partitionFilter) {
        this.partitionFilter = partitionFilter;
        this.partitioner = parter;
    }
    
    @Override
    public <K, V> void subscribe(KafkaConsumer<K, V> consumer, ConsumerRebalanceListener listener, TopologyContext context) {
        this.consumer = consumer;
        this.listener = listener;
        this.context = context;
        refreshAssignment();
    }
    
    @Override
    public void refreshAssignment() {
        List<TopicPartition> allPartitions = partitionFilter.getFilteredTopicPartitions(consumer);
        Collections.sort(allPartitions, TopicPartitionComparator.INSTANCE);
        Set<TopicPartition> newAssignment = new HashSet<>(partitioner.partition(allPartitions, context));
        if (!newAssignment.equals(currentAssignment)) {
            if (currentAssignment != null) {
                listener.onPartitionsRevoked(currentAssignment);
            }
            currentAssignment = newAssignment;
            consumer.assign(newAssignment);
            listener.onPartitionsAssigned(newAssignment);
        }
    }
    
    @Override
    public String getTopicsString() {
        return partitionFilter.getTopicsString();
    }
}
