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
    // Last offset committed to Kafka. Initially it is set to fetchOffset - 1
    private long committedOffset;
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
        this.committedOffset = initialFetchOffset - 1;
        LOG.debug("Instantiated {}", this);
    }

    public void addToAckMsgs(KafkaSpoutMessageId msgId) {          // O(Log N)
        ackedMsgs.add(msgId);
    }

    public void addToEmitMsgs(long offset) {
        this.emittedOffsets.add(offset);                  // O(Log N)
    }

    /**
     * An offset is only committed when all records with lower offset have been
     * acked. This guarantees that all offsets smaller than the committedOffset
     * have been delivered.
     *
     * @return the next OffsetAndMetadata to commit, or null if no offset is
     *     ready to commit.
     */
    public OffsetAndMetadata findNextCommitOffset() {
        long currOffset;
        long nextCommitOffset = committedOffset;
        KafkaSpoutMessageId nextCommitMsg = null;     // this is a convenience variable to make it faster to create OffsetAndMetadata

        for (KafkaSpoutMessageId currAckedMsg : ackedMsgs) {  // complexity is that of a linear scan on a TreeMap
            currOffset = currAckedMsg.offset();
            if (currOffset == nextCommitOffset + 1) {            // found the next offset to commit
                nextCommitMsg = currAckedMsg;
                nextCommitOffset = currOffset;
            } else if (currOffset > nextCommitOffset + 1) {
                if (emittedOffsets.contains(nextCommitOffset + 1)) {
                    LOG.debug("topic-partition [{}] has non-continuous offset [{}]."
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
                    LOG.debug("Processed non contiguous offset."
                        + " (committedOffset+1) is no longer part of the topic."
                        + " Committed: [{}], Processed: [{}]", committedOffset, currOffset);
                    final Long nextEmittedOffset = emittedOffsets.ceiling(nextCommitOffset);
                    if (nextEmittedOffset != null && currOffset == nextEmittedOffset) {
                        nextCommitMsg = currAckedMsg;
                        nextCommitOffset = currOffset;
                    } else {
                        LOG.debug("topic-partition [{}] has non-continuous offset [{}]."
                            + " Next Offset to commit should be [{}]", tp, currOffset, nextEmittedOffset);
                        break;
                    }
                }
            } else {
                //Received a redundant ack. Ignore and continue processing.
                LOG.warn("topic-partition [{}] has unexpected offset [{}]. Current committed Offset [{}]",
                    tp, currOffset, committedOffset);
            }
        }

        OffsetAndMetadata nextCommitOffsetAndMetadata = null;
        if (nextCommitMsg != null) {
            nextCommitOffsetAndMetadata = new OffsetAndMetadata(nextCommitOffset, nextCommitMsg.getMetadata(Thread.currentThread()));
            LOG.debug("topic-partition [{}] has offsets [{}-{}] ready to be committed",
                tp, committedOffset + 1, nextCommitOffsetAndMetadata.offset());
        } else {
            LOG.debug("topic-partition [{}] has NO offsets ready to be committed", tp);
        }
        LOG.trace("{}", this);
        return nextCommitOffsetAndMetadata;
    }

    /**
     * Marks an offset has committed. This method has side effects - it sets the
     * internal state in such a way that future calls to
     * {@link #findNextCommitOffset()} will return offsets greater than the
     * offset specified, if any.
     *
     * @param committedOffset offset to be marked as committed
     * @return Number of offsets committed in this commit
     */
    public long commit(OffsetAndMetadata committedOffset) {
        final long preCommitCommittedOffsets = this.committedOffset;
        long numCommittedOffsets = 0;
        this.committedOffset = committedOffset.offset();
        for (Iterator<KafkaSpoutMessageId> iterator = ackedMsgs.iterator(); iterator.hasNext();) {
            if (iterator.next().offset() <= committedOffset.offset()) {
                iterator.remove();
                numCommittedOffsets++;
            } else {
                break;
            }
        }

        for (Iterator<Long> iterator = emittedOffsets.iterator(); iterator.hasNext();) {
            if (iterator.next() <= committedOffset.offset()) {
                iterator.remove();
            } else {
                break;
            }
        }

        LOG.trace("{}", this);
        
        LOG.debug("Committed [{}] offsets in the range [{}-{}] for topic-partition [{}].",
                numCommittedOffsets, preCommitCommittedOffsets + 1, this.committedOffset, tp);
        
        return numCommittedOffsets;
    }

    public long getCommittedOffset() {
        return committedOffset;
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

    @Override
    public String toString() {
        return "OffsetManager{"
            + "topic-partition=" + tp
            + ", fetchOffset=" + initialFetchOffset
            + ", committedOffset=" + committedOffset
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
