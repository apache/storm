/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.trident.spout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.tuple.Fields;

/**
 * This interface defines a transactional spout that reads its tuples from a partitioned set of brokers. It automates the storing of
 * metadata for each partition to ensure that the same batch is always emitted for the same transaction id. The partition metadata is stored
 * in Zookeeper.
 */
public interface IPartitionedTridentSpout<PartitionsT, PartitionT extends ISpoutPartition, T> extends ITridentDataSource {
    Coordinator<PartitionsT> getCoordinator(Map<String, Object> conf, TopologyContext context);

    Emitter<PartitionsT, PartitionT, T> getEmitter(Map<String, Object> conf, TopologyContext context);

    Map<String, Object> getComponentConfiguration();

    Fields getOutputFields();

    interface Coordinator<PartitionsT> {
        /**
         * Return the partitions currently in the source of data. The idea is is that if a new partition is added and a prior transaction is
         * replayed, it doesn't emit tuples for the new partition because it knows what partitions were in that transaction.
         */
        PartitionsT getPartitionsForBatch();

        boolean isReady(long txid);

        void close();
    }

    interface Emitter<PartitionsT, PartitionT extends ISpoutPartition, X> {

        /**
         * Sorts given partition info to produce an ordered list of partitions.
         *
         * @param allPartitionInfo  The partition info for all partitions being processed by all spout tasks
         * @return sorted list of partitions being processed by all the tasks. The ordering must be consistent for all tasks.
         */
        List<PartitionT> getOrderedPartitions(PartitionsT allPartitionInfo);

        /**
         * Emit a batch of tuples for a partition/transaction that's never been emitted before. Return the metadata that can be used to
         * reconstruct this partition/batch in the future.
         */
        X emitPartitionBatchNew(TransactionAttempt tx, TridentCollector collector, PartitionT partition, X lastPartitionMeta);

        /**
         * This method is called when this task is responsible for a new set of partitions. Should be used to manage things like connections
         * to brokers.
         */
        void refreshPartitions(List<PartitionT> partitionResponsibilities);

        /**
         * Emit a batch of tuples for a partition/transaction that has been emitted before, using the metadata created when it was first
         * emitted.
         */
        void emitPartitionBatch(TransactionAttempt tx, TridentCollector collector, PartitionT partition, X partitionMeta);

        /**
         * Get the partitions assigned to the given task.
         *
         * @param taskId                 The id of the task
         * @param numTasks               The number of tasks for the spout
         * @param allPartitionInfoSorted The partition info of all partitions being processed by all spout tasks, sorted according to
         *                               {@link #getOrderedPartitions(java.lang.Object)}
         * @return The list of partitions that are to be processed by the task with {@code taskId}
         */
        default List<PartitionT> getPartitionsForTask(int taskId, int numTasks, List<PartitionT> allPartitionInfoSorted) {
            List<PartitionT> taskPartitions = new ArrayList<>(allPartitionInfoSorted == null ? 0 : allPartitionInfoSorted.size());
            if (allPartitionInfoSorted != null) {
                for (int i = taskId; i < allPartitionInfoSorted.size(); i += numTasks) {
                    taskPartitions.add(allPartitionInfoSorted.get(i));
                }
            }
            return taskPartitions;
        }

        void close();
    }
}
