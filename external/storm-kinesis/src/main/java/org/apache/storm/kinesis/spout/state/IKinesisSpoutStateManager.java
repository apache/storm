/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.kinesis.spout.state;

import com.amazonaws.services.kinesis.model.Record;
import org.apache.storm.kinesis.spout.IShardGetter;

/**
 * State manager for storing Amazon Kinesis Storm Spout specific state.
 *
 * A class implementing IKinesisSpoutStateManager is in charge of handling the assignment of
 * getters, as well as handling shard state.
 *
 * An implementation may or may not be backed by a persistent store, and can optionally
 * handle retrying failed records.
 */
public interface IKinesisSpoutStateManager {

    /**
     * Activate the state manager. This should be called before other calls.
     */
    void activate();

    /**
     * Check if the task handles any getters (e.g. responsible for shards).
     * 
     * @return true if the task handles at least one getter.
     */
    boolean hasGetters();

    /**
     * Get the next getter for the spout task to read from.
     * Should only be called if hasGetters() returned true.
     * 
     * @return a shard getter.
     */
    IShardGetter getNextGetter();

    /**
     * Inform the state of a spout rebalance by Storm. This will cause the state to re-assign its
     * getters to match its new task index and the new number of tasks.
     * 
     * @param taskIndex New task index (assigned by Storm).
     * @param totalNumTasks New total number of tasks (assigned by Storm).
     */
    void rebalance(int taskIndex, int totalNumTasks);

    /**
     * Mark a record as acknowledged by the topology. It will not be retried.
     * Implementations that don't support retries, should silently return
     * 
     * @param shardId Shard containing the acknowledged record.
     * @param seqNum Sequence number uniquely identifying the record.
     */
    void ack(String shardId, String seqNum);

    /**
     * Mark a record as failed by the topology. If implemented, this operation should trigger a
     * retry.
     * 
     * Implementations that don't support retries, should silently return.
     * 
     * @param shardId Shard containing the failed record.
     * @param seqNum Sequence number uniquely identifying the record.
     */
    void fail(String shardId, String seqNum);

    /**
     * Mark a record as emitted into the topology.
     * 
     * Implementations that don't support emit should silently return.
     * 
     * @param shardId Shard containing the emitted record.
     * @param record Record emitted.
     * @param isRetry Is the record a retry attempt?
     */
    void emit(String shardId, Record record, boolean isRetry);

    /**
     * Checks whether there is a record pending retry in a shard.
     * 
     * Implementations that don't support retries should return false.
     * 
     * @param shardId Shard containing the record to retry.
     * @return true if there is a record to retry.
     */
    boolean shouldRetry(String shardId);
    
    /**
     * Amazon Kinesis record to retry.
     * This should be called only if shouldRetry returned true.
     * 
     * @param shardId Shard to get record to retry from.
     * @return record to retry.
     */
    Record recordToRetry(String shardId);

    /**
     * Commit shard states into the persistent backing store of the implementation.
     */
    void commitShardStates();

    /**
     * Can be used to close the connection to the persistent store of the implementation.
     * 
     * @throws Exception if the close failed.
     */
    void deactivate() throws Exception;

}
