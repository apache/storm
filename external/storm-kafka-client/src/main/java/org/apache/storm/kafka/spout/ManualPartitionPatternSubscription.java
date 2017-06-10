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
package org.apache.storm.kafka.spout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.task.TopologyContext;

public class ManualPartitionPatternSubscription extends PatternSubscription {
    private static final long serialVersionUID = 5633018073527583826L;
    private final ManualPartitioner parter;
    private Set<TopicPartition> currentAssignment = null;
    private KafkaConsumer<?, ?> consumer = null;
    private ConsumerRebalanceListener listener = null;
    private TopologyContext context = null;

    public ManualPartitionPatternSubscription(ManualPartitioner parter, Pattern pattern) {
        super(pattern);
        this.parter = parter;
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
        List<TopicPartition> allPartitions = new ArrayList<>();
        for(Map.Entry<String, List<PartitionInfo>> entry: consumer.listTopics().entrySet()) {
            if (pattern.matcher(entry.getKey()).matches()) {
                for (PartitionInfo partitionInfo: entry.getValue()) {
                    allPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
                }
            }
        }
        Collections.sort(allPartitions, TopicPartitionComparator.INSTANCE);
        Set<TopicPartition> newAssignment = new HashSet<>(parter.partition(allPartitions, context));
        if (!newAssignment.equals(currentAssignment)) {
            if (currentAssignment != null) {
                listener.onPartitionsRevoked(currentAssignment);
                listener.onPartitionsAssigned(newAssignment);
            }
            currentAssignment = newAssignment;
            consumer.assign(currentAssignment);
        }
    }
}