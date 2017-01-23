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

package org.apache.storm.kafka.spout.internal.fetcher;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.internal.Timer;
import org.apache.storm.kafka.spout.internal.partition.KafkaPartitionReader;
import org.apache.storm.kafka.spout.TopicPartitionComparator;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class ManualKafkaRecordsFetcher<K, V> implements KafkaRecordsFetcher<K, V> {
    private static final Comparator<TopicPartition> KAFKA_TOPIC_PARTITION_COMPARATOR = new TopicPartitionComparator();

    private final KafkaConsumer<K, V> consumer;
    private final int thisTaskIndex;
    private final int totalTaskCount;
    private final KafkaPartitionReader partitionReader;
    private final Timer partitionRefreshTimer;
    private final PartitionAssignmentChangeListener partitionAssignmentChangeListener;
    private Set<TopicPartition> myPartitions = Collections.emptySet();

    public ManualKafkaRecordsFetcher(KafkaConsumer<K, V> consumer,
                                     int thisTaskIndex,
                                     int totalTaskCount,
                                     KafkaPartitionReader partitionReader,
                                     Timer partitionRefreshTimer,
                                     PartitionAssignmentChangeListener partitionAssignmentChangeListener) {
        this.consumer = consumer;
        this.thisTaskIndex = thisTaskIndex;
        this.totalTaskCount = totalTaskCount;
        this.partitionReader = partitionReader;
        this.partitionRefreshTimer = partitionRefreshTimer;
        this.partitionAssignmentChangeListener = partitionAssignmentChangeListener;

        doRefreshMyPartitions();
    }

    private void refreshMyPartitionsIfNeeded() {
        if (!partitionRefreshTimer.isExpiredResetOnTrue()) {
            return;
        }

        doRefreshMyPartitions();
    }

    private void doRefreshMyPartitions() {
        List<TopicPartition> topicPartitions = partitionReader.readPartitions(consumer);
        Collections.sort(topicPartitions, KAFKA_TOPIC_PARTITION_COMPARATOR);

        Set<TopicPartition> curPartitions = new HashSet<>(topicPartitions.size()/totalTaskCount+1);
        for (int i=thisTaskIndex; i<topicPartitions.size(); i+=totalTaskCount) {
            curPartitions.add(topicPartitions.get(i));
        }

        if (!myPartitions.equals(curPartitions) && myPartitions!=null) {
            partitionAssignmentChangeListener.onPartitionAssignmentChange(myPartitions, curPartitions);
        }

        myPartitions = curPartitions;

        consumer.assign(myPartitions);
    }

    @Override
    public ConsumerRecords<K, V> fetchRecords(long fetchTimeoutMs) {
        refreshMyPartitionsIfNeeded();

        return consumer.poll(fetchTimeoutMs);
    }

    public interface PartitionAssignmentChangeListener {
        void onPartitionAssignmentChange(Set<TopicPartition> oldPartitions, Set<TopicPartition> newPartitions);
    }

    public static PartitionAssignmentChangeListener listenerOf(final ConsumerRebalanceListener consumerRebalanceListener) {
        return new PartitionAssignmentChangeListener() {
            @Override
            public void onPartitionAssignmentChange(Set<TopicPartition> oldPartitions, Set<TopicPartition> newPartitions) {
                consumerRebalanceListener.onPartitionsRevoked(oldPartitions);
                consumerRebalanceListener.onPartitionsAssigned(newPartitions);
            }
        };
    }
}

