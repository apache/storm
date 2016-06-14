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

package org.apache.storm.kinesis.spout.state.zookeeper;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.model.Record;

/**
 * This class tracks the state of a shard (e.g. current shard position).
 */
class LocalShardState {
    private static final Logger LOG = LoggerFactory.getLogger(LocalShardState.class);

    private final String shardId;

    private InflightRecordTracker tracker;
    private String committedSequenceNumber;

    /**
     * Constructor.
     * 
     * @param shardId ID of the shard this LocalShardState is tracking.
     * @param latestZookeeperSeqNum the last checkpoint stored in Zookeeper.
     * @param recordRetryLimit Number of times a failed record should be retried.
     */
    LocalShardState(final String shardId, final String latestZookeeperSeqNum, final int recordRetryLimit) {
        this.shardId = shardId;
        this.tracker = new InflightRecordTracker(shardId, latestZookeeperSeqNum, recordRetryLimit);
        this.committedSequenceNumber = latestZookeeperSeqNum;
    }

    /**
     * Call when a record is emitted in nextTuple.
     *
     * @param record the Kinesis record emitted.
     * @param isRetry Is this a retry attempt of a previously emitted record.
     */
    void emit(final Record record, boolean isRetry) {
        tracker.onEmit(record, isRetry);
    }

    /**
     * Call when a record is acknowledged. This will try to update the latest offset to be
     * stored in Zookeeper, if possible.
     *
     * @param seqNum  the sequence number of the record.
     */
    void ack(final String seqNum) {
        tracker.onAck(seqNum);
    }

    /** 
     * Call when a record is failed. It is then added to a retry queue that is queried by
     * nextTuple().
     *
     * @param failedSequenceNumber  sequence number of failed record.
     */
    void fail(final String failedSequenceNumber) {
        tracker.onFail(failedSequenceNumber);
    }

    /**
     * Get a record to retry.
     *
     * Pre : shouldRetry().
     * @return a record to retry - may be null if we can't find a record to retry.
     */
    Record recordToRetry() {
        assert shouldRetry() : "Nothing to retry.";
        return tracker.recordToRetry();
    }

    /**
     * @return true if there are sequence numbers that need to be retried.
     */
    boolean shouldRetry() {
        return tracker.shouldRetry();
    }

    /**
     * Get the latest sequence number validated by the shard state. This should be stored
     * periodically in Zookeeper.
     *
     * @return a sequence number.
     */
    String getLatestValidSeqNum() {
        return tracker.getCheckpointSequenceNumber();
    }

    /**
     * @return true if the checkpoint sequence number has changed (new records were processed).
     */
    boolean isDirty() {
        return !committedSequenceNumber.equals(tracker.getCheckpointSequenceNumber());
    }

    /**
     * Record the sequenced number we checkpointed.
     * @param checkpointSequenceNumber Sequence number we used to checkpoint.
     */
    void commit(String checkpointSequenceNumber) {
        this.committedSequenceNumber = checkpointSequenceNumber;
    }

    /**
     * Helper log function. Checks that debug logging is enabled before evaluating the
     * detailedToString() function.
     *
     * @param prefix  prefix to prepend to log message.
     */
    void logMe(String prefix) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(this + " " + prefix + " " + detailedToString());
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
            .append("shardId", shardId)
            .toString();
    }

    private String detailedToString() {
        return ReflectionToStringBuilder.toString(this);
    }
}
