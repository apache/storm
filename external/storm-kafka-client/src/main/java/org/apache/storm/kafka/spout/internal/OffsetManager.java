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

import com.google.common.annotations.VisibleForTesting;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.TreeSet;
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
    // Emitted Offsets List
    private final NavigableSet<Long> emittedOffsets = new TreeSet<>();
    // Acked messages sorted by ascending order of offset
    private final NavigableSet<KafkaSpoutMessageId> ackedMsgs = new TreeSet<>(OFFSET_COMPARATOR);
    // Committed offset, i.e. the offset where processing will resume upon spout restart. Initially it is set to fetchOffset.
    private long committedOffset;
    // True if this OffsetManager has made at least one commit to Kafka
    private boolean committed;
    private long latestEmittedOffset;

    /**
     * Creates a new OffsetManager.
     * @param tp The TopicPartition
     * @param initialFetchOffset The initial fetch offset for the given TopicPartition
     */
    public OffsetManager(TopicPartition tp, long initialFetchOffset) {
        this.tp = tp;
        this.committedOffset = initialFetchOffset;
        LOG.debug("Instantiated {}", this.toString());
    }

    public void addToAckMsgs(KafkaSpoutMessageId msgId) {          // O(Log N)
        ackedMsgs.add(msgId);
    }

    public void addToEmitMsgs(long offset) {
        this.emittedOffsets.add(offset);  // O(Log N)
        this.latestEmittedOffset = Math.max(latestEmittedOffset, offset);
    }
    
    public int getNumUncommittedOffsets() {
        return this.emittedOffsets.size();
    }
    
    /**
     * Gets the offset of the nth emitted message after the committed offset. 
     * Example: If the committed offset is 0 and offsets 1, 2, 8, 10 have been emitted,
     * getNthUncommittedOffsetAfterCommittedOffset(3) returns 8.
     * 
     * @param index The index of the message to get the offset for
     * @return The offset
     * @throws NoSuchElementException if the index is out of range
     */
    public long getNthUncommittedOffsetAfterCommittedOffset(int index) {
        Iterator<Long> offsetIter = emittedOffsets.iterator();
        for (int i = 0; i < index - 1; i++) {
            offsetIter.next();
        }
        return offsetIter.next();
    }

    /**
     * An offset can only be committed when all emitted records with lower offset have been
     * acked. This guarantees that all offsets smaller than the committedOffset
     * have been delivered, or that those offsets no longer exist in Kafka. 
     * <p/>
     * The returned offset points to the earliest uncommitted offset, and matches the semantics of the KafkaConsumer.commitSync API.
     *
     * @param commitMetadata Metadata information to commit to Kafka. It is constant per KafkaSpout instance per topology
     * @return the next OffsetAndMetadata to commit, or null if no offset is
     *     ready to commit.
     */
    public OffsetAndMetadata findNextCommitOffset(final String commitMetadata) {
        boolean found = false;
        long currOffset;
        long nextCommitOffset = committedOffset;

        for (KafkaSpoutMessageId currAckedMsg : ackedMsgs) {  // complexity is that of a linear scan on a TreeMap
            currOffset = currAckedMsg.offset();
            if (currOffset == nextCommitOffset) {
                // found the next offset to commit
                found = true;
                nextCommitOffset = currOffset + 1;
            } else if (currOffset > nextCommitOffset) {
                if (emittedOffsets.contains(nextCommitOffset)) {
                    LOG.debug("topic-partition [{}] has non-sequential offset [{}]."
                        + " It will be processed in a subsequent batch.", tp, currOffset);
                    break;
                } else {
                    /*
                        This case will arise in case of non-sequential offset being processed.
                        So, if the topic doesn't contain offset = nextCommitOffset (possible
                        if the topic is compacted or deleted), the consumer should jump to
                        the next logical point in the topic. Next logical offset should be the
                        first element after nextCommitOffset in the ascending ordered emitted set.
                     */
                    LOG.debug("Processed non-sequential offset."
                        + " The earliest uncommitted offset is no longer part of the topic."
                        + " Missing offset: [{}], Processed: [{}]", nextCommitOffset, currOffset);
                    final Long nextEmittedOffset = emittedOffsets.ceiling(nextCommitOffset);
                    if (nextEmittedOffset != null && currOffset == nextEmittedOffset) {
                        LOG.debug("Found committable offset: [{}] after missing offset: [{}], skipping to the committable offset",
                            currOffset, nextCommitOffset);
                        found = true;
                        nextCommitOffset = currOffset + 1;
                    } else {
                        LOG.debug("Topic-partition [{}] has non-sequential offset [{}]."
                            + " Next offset to commit should be [{}]", tp, currOffset, nextCommitOffset);
                        break;
                    }
                }
            } else {
                throw new IllegalStateException("The offset [" + currOffset + "] is below the current nextCommitOffset "
                    + "[" + nextCommitOffset + "] for [" + tp + "]."
                    + " This should not be possible, and likely indicates a bug in the spout's acking or emit logic.");
            }
        }

        OffsetAndMetadata nextCommitOffsetAndMetadata = null;
        if (found) {
            nextCommitOffsetAndMetadata = new OffsetAndMetadata(nextCommitOffset, commitMetadata);

            LOG.debug("Topic-partition [{}] has offsets [{}-{}] ready to be committed."
                + " Processing will resume at offset [{}] upon spout restart",
                tp, committedOffset, nextCommitOffsetAndMetadata.offset() - 1, nextCommitOffsetAndMetadata.offset());
        } else {
            LOG.debug("Topic-partition [{}] has no offsets ready to be committed", tp);
        }
        LOG.trace("{}", this);
        return nextCommitOffsetAndMetadata;
    }

    /**
     * Marks an offset as committed. This method has side effects - it sets the
     * internal state in such a way that future calls to
     * {@link #findNextCommitOffset(String)} will return offsets greater than or equal to the
     * offset specified, if any.
     *
     * @param committedOffsetAndMeta The committed offset. All lower offsets are expected to have been committed.
     * @return Number of offsets committed in this commit
     */
    public long commit(OffsetAndMetadata committedOffsetAndMeta) {
        committed = true;
        final long preCommitCommittedOffset = this.committedOffset;
        long numCommittedOffsets = 0;
        this.committedOffset = committedOffsetAndMeta.offset();
        for (Iterator<KafkaSpoutMessageId> iterator = ackedMsgs.iterator(); iterator.hasNext();) {
            if (iterator.next().offset() < committedOffsetAndMeta.offset()) {
                iterator.remove();
                numCommittedOffsets++;
            } else {
                break;
            }
        }

        for (Iterator<Long> iterator = emittedOffsets.iterator(); iterator.hasNext();) {
            if (iterator.next() < committedOffsetAndMeta.offset()) {
                iterator.remove();
            } else {
                break;
            }
        }

        LOG.trace("{}", this);
        
        LOG.debug("Committed [{}] offsets in the range [{}-{}] for topic-partition [{}]."
            + " Processing will resume at [{}] upon spout restart",
                numCommittedOffsets, preCommitCommittedOffset, this.committedOffset - 1, tp, this.committedOffset);
        
        return numCommittedOffsets;
    }

    /**
     * Checks if this OffsetManager has committed to Kafka.
     *
     * @return true if this OffsetManager has made at least one commit to Kafka, false otherwise
     */
    public boolean hasCommitted() {
        return committed;
    }

    public boolean contains(KafkaSpoutMessageId msgId) {
        return ackedMsgs.contains(msgId);
    }

    @VisibleForTesting
    boolean containsEmitted(long offset) {
        return emittedOffsets.contains(offset);
    }

    public long getLatestEmittedOffset() {
        return latestEmittedOffset;
    }

    public long getCommittedOffset() {
        return committedOffset;
    }

    @Override
    public final String toString() {
        return "OffsetManager{"
            + "topic-partition=" + tp
            + ", committedOffset=" + committedOffset
            + ", emittedOffsets=" + emittedOffsets
            + ", ackedMsgs=" + ackedMsgs
            + ", latestEmittedOffset=" + latestEmittedOffset
            + '}';
    }

    private static class OffsetComparator implements Comparator<KafkaSpoutMessageId> {

        @Override
        public int compare(KafkaSpoutMessageId m1, KafkaSpoutMessageId m2) {
            return m1.offset() < m2.offset() ? -1 : m1.offset() == m2.offset() ? 0 : 1;
        }
    }
}
