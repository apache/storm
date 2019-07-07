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
 * This defines a transactional spout which does *not* necessarily replay the same batch every time it emits a batch for a transaction id.
 *
 * @param <M>          The type of metadata object passed to the Emitter when emitting a new batch based on a previous batch. This type must
 *                     be JSON serializable by json-simple.
 * @param <PartitionsT> The type of metadata object used by the coordinator to describe partitions. This type must be JSON serializable by
 *                     json-simple.
 */
public interface IOpaquePartitionedTridentSpout<PartitionsT, PartitionT extends ISpoutPartition, M>
    extends ITridentDataSource {

    Emitter<PartitionsT, PartitionT, M> getEmitter(Map<String, Object> conf, TopologyContext context);

    Coordinator<PartitionsT> getCoordinator(Map<String, Object> conf, TopologyContext context);

    Map<String, Object> getComponentConfiguration();

    Fields getOutputFields();

    /**
     * Coordinator for batches. Trident will only begin committing once at least one coordinator is ready.
     *
     * @param <PartitionsT> The type of metadata object used by the coordinator to describe partitions. This type must be JSON serializable
     *                     by json-simple.
     */
    interface Coordinator<PartitionsT> {
        /**
         * Indicates whether this coordinator is ready to commit the given transaction. The master batch coordinator will only begin
         * committing if at least one coordinator indicates it is ready to commit.
         *
         * @param txid The transaction id
         * @return true if this coordinator is ready to commit, false otherwise.
         */
        boolean isReady(long txid);

        /**
         * Gets the partitions for the following batches. The emitter will be asked to refresh partitions when this value changes.
         *
         * @return The partitions for the following batches.
         */
        PartitionsT getPartitionsForBatch();

        void close();
    }

    interface Emitter<PartitionsT, PartitionT extends ISpoutPartition, M> {
        /**
         * Emit a batch of tuples for a partition/transaction.
         *
         * <p>Return the metadata describing this batch that will be used as lastPartitionMeta for defining the
         * parameters of the next batch.
         */
        M emitPartitionBatch(TransactionAttempt tx, TridentCollector collector, PartitionT partition, M lastPartitionMeta);

        /**
         * This method is called when this task is responsible for a new set of partitions. Should be used to manage things like connections
         * to brokers.
         *
         * @param partitionResponsibilities The partitions assigned to this task
         */
        void refreshPartitions(List<PartitionT> partitionResponsibilities);

        /**
         * Sorts the partition info to produce an ordered list of partition.
         *
         * @param allPartitionInfo The partition info for all partitions being processed by all spout tasks
         * @return The ordered list of partitions being processed by all the tasks. The ordering must be consistent for all tasks.
         */
        List<PartitionT> getOrderedPartitions(PartitionsT allPartitionInfo);

        /**
         * Get the partitions assigned to this task.
         *
         * @param taskId                 The id of this task
         * @param numTasks               The number of tasks for this spout
         * @param allPartitionInfoSorted The partition info for all partitions being processed by all spout tasks, sorted according to
         *                               {@link #getOrderedPartitions(java.lang.Object)}
         * @return The list of partitions that are to be processed by the task with id {@code taskId}
         */
        default List<PartitionT> getPartitionsForTask(int taskId, int numTasks, List<PartitionT> allPartitionInfoSorted) {
            final List<PartitionT> taskPartitions = new ArrayList<>(allPartitionInfoSorted == null ? 0 : allPartitionInfoSorted.size());
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
