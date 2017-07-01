/*
 * Copyright 2016 The Apache Software Foundation.
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

package org.apache.storm.kafka.spout.internal;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.TreeSet;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.KafkaSpoutMessageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages acked and committed offsets for a TopicPartition. This class is not thread safe
 */
public class OffsetManager {

    private static final Comparator<KafkaSpoutMessageId> OFFSET_COMPARATOR = new OffsetComparator();
    private static final Logger LOG = LoggerFactory.getLogger(OffsetManager.class);
    private final TopicPartition tp;
    /* First offset to be fetched. It is either set to the beginning, end, or to the first uncommitted offset.
    * Initial value depends on offset strategy. See KafkaSpoutConsumerRebalanceListener */
    private final long initialFetchOffset;
    // Earliest uncommitted offset, i.e. the last committed offset + 1. Initially it is set to fetchOffset
    private long earliestUncommittedOffset;
    // Emitted Offsets List
    private final NavigableSet<Long> emittedOffsets = new TreeSet<>();
    // Acked messages sorted by ascending order of offset
    private final NavigableSet<KafkaSpoutMessageId> ackedMsgs = new TreeSet<>(OFFSET_COMPARATOR);

    /**
     * Creates a new OffsetManager.
     * @param tp The TopicPartition 
     * @param initialFetchOffset The initial fetch offset for the given TopicPartition
     */
    public OffsetManager(TopicPartition tp, long initialFetchOffset) {
        this.tp = tp;
        this.initialFetchOffset = initialFetchOffset;
        this.earliestUncommittedOffset = initialFetchOffset;
        LOG.debug("Instantiated {}", this);
    }

    public void addToAckMsgs(KafkaSpoutMessageId msgId) {          // O(Log N)
        ackedMsgs.add(msgId);
    }

    public void addToEmitMsgs(long offset) {
        this.emittedOffsets.add(offset);                  // O(Log N)
    }

    /**
     * An offset can only be committed when all emitted records with lower offset have been
     * acked. This guarantees that all offsets smaller than the committedOffset
     * have been delivered, or that those offsets no longer exist in Kafka. 
     * <p/>
     * The returned offset points to the earliest uncommitted offset, and matches the semantics of the KafkaConsumer.commitSync API.
     *
     * @return the next OffsetAndMetadata to commit, or null if no offset is
     *     ready to commit.
     */
    public OffsetAndMetadata findNextCommitOffset() {
        long currOffset;
        long nextEarliestUncommittedOffset = earliestUncommittedOffset;
        KafkaSpoutMessageId nextCommitMsg = null;     // this is a convenience variable to make it faster to create OffsetAndMetadata

        for (KafkaSpoutMessageId currAckedMsg : ackedMsgs) {  // complexity is that of a linear scan on a TreeMap
            currOffset = currAckedMsg.offset();
            if (currOffset == nextEarliestUncommittedOffset) {            // found the next offset to commit
                nextCommitMsg = currAckedMsg;
                nextEarliestUncommittedOffset = currOffset + 1;
            } else if (currOffset > nextEarliestUncommittedOffset) {
                if (emittedOffsets.contains(nextEarliestUncommittedOffset)) {
                    LOG.debug("topic-partition [{}] has non-contiguous offset [{}]."
                        + " It will be processed in a subsequent batch.", tp, currOffset);
                    break;
                } else {
                    /*
                        This case will arise in case of non contiguous offset being processed.
                        So, if the topic doesn't contain offset = committedOffset + 1 (possible
                        if the topic is compacted or deleted), the consumer should jump to
                        the next logical point in the topic. Next logical offset should be the
                        first element after committedOffset in the ascending ordered emitted set.
                     */
                    LOG.debug("Processed non-contiguous offset."
                        + " The earliest uncommitted offset is no longer part of the topic."
                        + " Missing uncommitted offset: [{}], Processed: [{}]", nextEarliestUncommittedOffset, currOffset);
                    final Long nextEmittedOffset = emittedOffsets.ceiling(nextEarliestUncommittedOffset);
                    if (nextEmittedOffset != null && currOffset == nextEmittedOffset) {
                        LOG.debug("Found committable offset: [{}] after missing offset: [{}], skipping to the committable offset",
                            currOffset, nextEarliestUncommittedOffset);
                        nextCommitMsg = currAckedMsg;
                        nextEarliestUncommittedOffset = currOffset + 1;
                    } else {
                        LOG.debug("topic-partition [{}] has non-contiguous offset [{}]."
                            + " Next Offset to commit should be [{}]", tp, currOffset, nextEarliestUncommittedOffset - 1);
                        break;
                    }
                }
            } else {
                //Received a redundant ack. Ignore and continue processing.
                LOG.warn("topic-partition [{}] has unexpected offset [{}]. Current earliest uncommitted offset [{}]",
                    tp, currOffset, earliestUncommittedOffset);
            }
        }

        OffsetAndMetadata nextCommitOffsetAndMetadata = null;
        if (nextCommitMsg != null) {
            nextCommitOffsetAndMetadata = new OffsetAndMetadata(nextEarliestUncommittedOffset,
                nextCommitMsg.getMetadata(Thread.currentThread()));
            LOG.debug("topic-partition [{}] has offsets [{}-{}] ready to be committed",
                tp, earliestUncommittedOffset, nextCommitOffsetAndMetadata.offset());
        } else {
            LOG.debug("topic-partition [{}] has NO offsets ready to be committed", tp);
        }
        LOG.trace("{}", this);
        return nextCommitOffsetAndMetadata;
    }

    /**
     * Marks an offset as committed. This method has side effects - it sets the
     * internal state in such a way that future calls to
     * {@link #findNextCommitOffset()} will return offsets greater than or equal to the
     * offset specified, if any.
     *
     * @param earliestUncommittedOffset Earliest uncommitted offset. All lower offsets are expected to have been committed.
     * @return Number of offsets committed in this commit
     */
    public long commit(OffsetAndMetadata earliestUncommittedOffset) {
        final long preCommitEarliestUncommittedOffset = this.earliestUncommittedOffset;
        final long numCommittedOffsets = earliestUncommittedOffset.offset() - this.earliestUncommittedOffset;
        this.earliestUncommittedOffset = earliestUncommittedOffset.offset();
        for (Iterator<KafkaSpoutMessageId> iterator = ackedMsgs.iterator(); iterator.hasNext();) {
            if (iterator.next().offset() < earliestUncommittedOffset.offset()) {
                iterator.remove();
            } else {
                break;
            }
        }

        for (Iterator<Long> iterator = emittedOffsets.iterator(); iterator.hasNext();) {
            if (iterator.next() < earliestUncommittedOffset.offset()) {
                iterator.remove();
            } else {
                break;
            }
        }

        LOG.trace("{}", this);
        
        LOG.debug("Committed offsets [{}-{} = {}] for topic-partition [{}].",
                    preCommitEarliestUncommittedOffset, this.earliestUncommittedOffset - 1, numCommittedOffsets, tp);
        
        return numCommittedOffsets;
    }

    public long getEarliestUncommittedOffset() {
        return earliestUncommittedOffset;
    }

    public boolean isEmpty() {
        return ackedMsgs.isEmpty();
    }

    public boolean contains(ConsumerRecord record) {
        return contains(new KafkaSpoutMessageId(record));
    }

    public boolean contains(KafkaSpoutMessageId msgId) {
        return ackedMsgs.contains(msgId);
    }
    
    public boolean containsEmitted(long offset) {
        return emittedOffsets.contains(offset);
    }

    @Override
    public String toString() {
        return "OffsetManager{"
            + "topic-partition=" + tp
            + ", fetchOffset=" + initialFetchOffset
            + ", earliestUncommittedOffset=" + earliestUncommittedOffset
            + ", emittedOffsets=" + emittedOffsets
            + ", ackedMsgs=" + ackedMsgs
            + '}';
    }

    private static class OffsetComparator implements Comparator<KafkaSpoutMessageId> {

        @Override
        public int compare(KafkaSpoutMessageId m1, KafkaSpoutMessageId m2) {
            return m1.offset() < m2.offset() ? -1 : m1.offset() == m2.offset() ? 0 : 1;
        }
    }
}
