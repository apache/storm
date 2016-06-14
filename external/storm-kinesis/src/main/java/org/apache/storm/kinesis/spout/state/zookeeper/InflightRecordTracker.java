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

import java.math.BigInteger;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.model.Record;

// @formatter:off
/**
 * This class helps track in-flight records/tuples so we can provide record level ack/fail semantics,
 * while fetching records from Kinesis in batches. This class computes and tracks the checkpoint sequence number
 * based on messages that have been acked, failed, or are inflight.
 * We will checkpoint at the highest sequence number for a record which has been acked or retried up to the retry limit.
 * 
 * We temporarily buffer records which have not been acked or exhausted retries, so we can re-emit them upon failure
 * without having to refetch them from Kinesis.
 * Since acks/fails can arrive in an order different from the order in which the records were
 * emitted and sequence numbers are not dense (consecutive numbers), we need to temporarily keep some additional
 * (acked/retried) records. For example, let's say we emit records with sequence numbers [101, 125, 132] and we get
 * acks in the order [125, 101, 132]. When we get the ack for 125, we keep keep it around. When we get an ack for 101,
 * we remove both 101 and 125 and set checkpointSequenceNumber to 125. When we get the ack for 132, we remove it and
 * set the checkpointSequenceNumber to 132.
 * 
 * Here's the table with info on processing acks. We maintain an ordered list of records (asc seq num).
 * In the table below we describe how we process an ack for a ("current") record based on the status of the previous
 * and next records in the ordered list (none means there is no previous/next record, keep means we don't delete
 * the current record).
 * 
 * 
 * +---------+-------------------------------+-------------------------------+-------------------------------+
 * |Next---> |                               |                               |                               |
 * |Previous |             acked             |            pending            |           none                |
 * |   |     |                               |                               |                               |
 * |   V     |                               |                               |                               |
 * +---------+-------------------------------+-------------------------------+-------------------------------+
 * | acked   | delete and delete previous    | keep and delete previous      | keep and delete previous      |
 * +---------+-------------------------------+-------------------------------+-------------------------------+
 * | pending | delete                        | keep                          | keep                          |
 * +---------+-------------------------------+-------------------------------+-------------------------------+
 * | none    | delete, delete next, and set  | delete and advance checkpoint | delete and advance checkpoint |
 * |         | checkpoint to seq num of next | to seq num of current record  | to seq num of current record  |
 * +---------+-------------------------------+-------------------------------+-------------------------------+
 * 
 */
// @formatter:on
class InflightRecordTracker {

    private static final Logger LOG = LoggerFactory.getLogger(InflightRecordTracker.class);

    private transient final String shardId;
    // All records up to (and including) this sequence number have been acked or retried up to the retry limit.
    private transient String checkpointSequenceNumber;

    // Used for efficient (random) access to the record info in the ordered record list.
    private transient Map<String, RecordNode> seqNumToRecordInfoMap;
    // Ordered list of records (ascending order of sequence numbers)
    private transient RecordNodeList recordNodeList;
    // Used to keep a queue of sequence numbers (corresponding to failed records that should be retried).
    private transient Queue<String> retryQueue;

    // Max number of retries for a record.
    private final int recordRetryLimit;

    /**
     * @param initialSequenceNumber Initial sequence number (e.g. from stored checkpoint)
     */
    InflightRecordTracker(final String shardId, final String initialSequenceNumber, final int recordRetryLimit) {
        this.shardId = shardId;
        checkpointSequenceNumber = initialSequenceNumber;
        seqNumToRecordInfoMap = new HashMap<>();
        recordNodeList = new RecordNodeList();
        retryQueue = new LinkedList<>();
        this.recordRetryLimit = recordRetryLimit;
    }

    /**
     * @return the checkpointSequenceNumber
     */
    String getCheckpointSequenceNumber() {
        return checkpointSequenceNumber;
    }

    void onEmit(final Record record, boolean isRetry) {
        // Only track records if we are going to retry/redrive upon failure.
        String sequenceNumber = record.getSequenceNumber();
        if (recordRetryLimit > 0) {
            int retryNum = 0;
            RecordNode node = seqNumToRecordInfoMap.get(sequenceNumber);
            if (node == null) {
                // Add to in-flight records being tracked, if this is not a retry
                if (!isRetry) {
                    node = recordNodeList.addToList(record);
                    seqNumToRecordInfoMap.put(sequenceNumber, node);
                }
            } else {
                // For retries, increment retry count and remove from queue of records that need to be redriven.
                if (isRetry) {
                    node.incrementRetryCount();
                    retryQueue.remove(sequenceNumber);
                    retryNum = node.getRetryCount();
                }
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Shard " + shardId + ": Recorded emit for seq num " + sequenceNumber + ", isRetry = " + isRetry
                        + ", retryNum = " + retryNum);       
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Shard " + shardId + ": Not tracking seq num " + sequenceNumber + " since recordRetryLimit "
                        + recordRetryLimit + " <= 0");
            }
        }
    }

    void onAck(final String sequenceNumber) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Shard " + shardId + ": Processing ack for sequence number " + sequenceNumber);
        }
        RecordNode node = seqNumToRecordInfoMap.get(sequenceNumber);
        // Ignore if we already removed it from the map (e.g. acked or exhausted retries).
        if (node != null) {
            node.setAcked(true);
            RecordNode previous = node.getPrev();
            RecordNode next = node.getNext();
            handleAck(previous, node, next);
        }
    }

    private void handleAck(RecordNode previous, RecordNode node, RecordNode next) {
        if (previous == null) {
            removeNodeAndUpdateCheckpoint(node);
            if ((next != null) && (next.isAcked())) {
                removeNodeAndUpdateCheckpoint(next);
            }
        } else {
            if (previous.isAcked()) {
                removeNodeAndUpdateCheckpoint(previous);
                if ((next != null) && (next.isAcked())) {
                    removeNodeAndUpdateCheckpoint(node);
                }
            } else {
                // previous is still pending
                if ((next != null) && (next.isAcked())) {
                    removeNodeAndUpdateCheckpoint(node);
                }
            }
        }
    }

    private void removeNodeAndUpdateCheckpoint(RecordNode node) {
        if (recordNodeList.getFirst() == node) {
            checkpointSequenceNumber = node.getRecord().getSequenceNumber();
        }
        recordNodeList.remove(node);
        seqNumToRecordInfoMap.remove(node.getRecord().getSequenceNumber());
    }

    void onFail(final String sequenceNumber) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Shard " + shardId + ": Processing failed for record with sequence number " + sequenceNumber);
        }
        RecordNode node = seqNumToRecordInfoMap.get(sequenceNumber);
        if ((node != null) && (!node.isAcked())) {
            if (node.getRetryCount() < recordRetryLimit) {
                retryQueue.add(node.getRecord().getSequenceNumber());
            } else {
                if (recordRetryLimit > 0) {
                    LOG.error("Record with sequence number " + node.getRecord().getSequenceNumber() + " was retried "
                            + node.getRetryCount() + " time(s). It has exceeded the retry limit " + recordRetryLimit
                            + ". Skipping the record.");
                }
                onAck(sequenceNumber);
            }
        }

    }

    boolean shouldRetry() {
        return !retryQueue.isEmpty();
    }

    Record recordToRetry() {
        Record recordToRetry = null;
        String sequenceNumber = retryQueue.peek();
        if (sequenceNumber != null) {
            RecordNode node = seqNumToRecordInfoMap.get(sequenceNumber);
            if (node != null) {
                recordToRetry = node.getRecord();
                if (LOG.isInfoEnabled()) {
                    LOG.info("Retrying record with partition key " + recordToRetry.getPartitionKey() + " sequence number "
                            + recordToRetry.getSequenceNumber() + ". Retry attempt " + (node.getRetryCount() + 1));
                }
            }
        }
        return recordToRetry;
    }

    /**
     * Note: This has package level access solely for testing purposes.
     * 
     * @return List of in-flight records
     */
    RecordNodeList getRecordNodeList() {
        return recordNodeList;
    }

    /**
     * Note: This method has package level access solely for testing purposes.
     * 
     * @return SequenceNumber->RecordNode map of in-flight records.
     */
    Map<String, RecordNode> getSequenceNumberToRecordNodeMap() {
        return seqNumToRecordInfoMap;
    }

    /**
     * Note: This method has package level access solely for testing purposes.
     * 
     * @return Queue of records to retry
     */
    Queue<String> getRetryQueue() {
        return retryQueue;
    }

    /**
     * Node in the ordered list of records. Tracks record and some related information.
     */
    class RecordNode {
        private final Record record;
        private int retryCount;
        private boolean isAcked;
        private RecordNode next;
        private RecordNode prev;

        RecordNode(Record record) {
            this.record = record;
            this.retryCount = 0;
            this.isAcked = false;
        }

        /**
         * @return the isAcked
         */
        boolean isAcked() {
            return isAcked;
        }

        /**
         * @param isAcked the isAcked to set
         */
        void setAcked(boolean isAcked) {
            this.isAcked = isAcked;
        }

        /**
         * @return the next
         */
        RecordNode getNext() {
            return next;
        }

        /**
         * @param next the next to set
         */
        void setNext(RecordNode next) {
            this.next = next;
        }

        /**
         * @return the prev
         */
        RecordNode getPrev() {
            return prev;
        }

        /**
         * @param prev the prev to set
         */
        void setPrev(RecordNode prev) {
            this.prev = prev;
        }

        /**
         * @return the record
         */
        Record getRecord() {
            return record;
        }

        /**
         * @return the retryCount
         */
        int getRetryCount() {
            return retryCount;
        }

        void incrementRetryCount() {
            retryCount++;
        }
    }

    /**
     * Doubly linked list used to keep an ordered list of records (ascending order of sequence numbers).
     */
    class RecordNodeList {
        private transient RecordNode first;
        private transient RecordNode last;
        private transient int size = 0;

        /**
         * Adds record to the end of the list.
         * 
         * @param record Record will be added to end of the list
         * @return Newly added node
         * @throws IllegalArgumentException Thrown if the sequence number of the record is lower than the (current) last
         *         node of the list
         */
        RecordNode addToList(Record record) {
            RecordNode node = new RecordNode(record);

            node.setPrev(last);
            node.setNext(null);

            if (last != null) {
                // Assert that sequence number of node is > sequence number of last node in list
                BigInteger currentLastSeqNum = new BigInteger(last.getRecord().getSequenceNumber());
                BigInteger nodeSeqNum = new BigInteger(node.getRecord().getSequenceNumber());
                if (currentLastSeqNum.compareTo(nodeSeqNum) > 0) {
                    throw new IllegalArgumentException("OUT OF ORDER INSERT: ShardId " + shardId
                            + " Inserting record with seq num " + nodeSeqNum + " after " + currentLastSeqNum);
                }
                last.setNext(node);
            }

            last = node;
            if (first == null) {
                first = node;
            }

            size++;

            return node;
        }

        void remove(RecordNode node) {
            RecordNode previousNode = node.getPrev();
            RecordNode nextNode = node.getNext();
            if (previousNode == null) {
                if (first != node) {
                    throw new IllegalArgumentException("Node to be removed is not first, yet also doesn't have a previous node. ShardId "
                            + shardId + " seq num " + node.getRecord().getSequenceNumber());
                }
                first = nextNode;
            } else {
                previousNode.setNext(nextNode);
            }

            if (nextNode != null) {
                nextNode.setPrev(previousNode);
            }

            if (last == node) {
                if (nextNode != null) {
                    throw new IllegalArgumentException("Node to be removed is last, yet also has a next node. ShardId "
                            + shardId + " seq num " + node.getRecord().getSequenceNumber() + " next node seq num "
                            + node.getNext().getRecord().getSequenceNumber());
                }
                last = null;
            }
            size--;
        }

        RecordNode getFirst() {
            return first;
        }

        RecordNode getLast() {
            return last;
        }

        int size() {
            return size;
        }

    }

}
