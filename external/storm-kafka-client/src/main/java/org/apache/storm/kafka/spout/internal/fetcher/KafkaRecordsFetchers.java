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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.internal.Timer;
import org.apache.storm.kafka.spout.internal.partition.KafkaPartitionReader;
import org.apache.storm.kafka.spout.internal.partition.KafkaPartitionReaders;
import org.apache.storm.task.TopologyContext;

import java.util.concurrent.TimeUnit;

public final class KafkaRecordsFetchers {
    public static <K, V> KafkaRecordsFetcher<K, V> create(KafkaSpoutConfig kafkaSpoutConfig,
                                                          KafkaConsumer<K, V> consumer,
                                                          TopologyContext context,
                                                          ConsumerRebalanceListener rebalanceListener) {
        if (kafkaSpoutConfig.isManualPartitionAssignment()) {
            int thisTaskIndex = context.getThisTaskIndex();
            int totalTaskCount = context.getComponentTasks(context.getThisComponentId()).size();
            KafkaPartitionReader partitionReader = KafkaPartitionReaders.create(
                kafkaSpoutConfig.getKafkaSpoutStreams());
            Timer partitionRefreshTimer = new Timer(500,
                kafkaSpoutConfig.getPartitionRefreshPeriodMs(), TimeUnit.MILLISECONDS);

            ManualKafkaRecordsFetcher.PartitionAssignmentChangeListener partitionAssignmentChangeListener =
                ManualKafkaRecordsFetcher.listenerOf(rebalanceListener);

            return new ManualKafkaRecordsFetcher<>(consumer, thisTaskIndex, totalTaskCount, partitionReader,
                partitionRefreshTimer, partitionAssignmentChangeListener);
        } else {
            return new AutomaticKafkaRecordsFetcher<>(consumer, rebalanceListener,
                kafkaSpoutConfig.getKafkaSpoutStreams());
        }
    }
}
