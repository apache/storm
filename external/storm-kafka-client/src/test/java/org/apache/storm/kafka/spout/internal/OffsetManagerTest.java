/*
 * Copyright 2017 The Apache Software Foundation.
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.NoSuchElementException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.KafkaSpoutMessageId;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class OffsetManagerTest {
    private static final String COMMIT_METADATA = "{\"topologyId\":\"tp1\",\"taskId\":3,\"threadName\":\"Thread-20\"}";

    @Rule
    public ExpectedException expect = ExpectedException.none();
    
    private final long initialFetchOffset = 0;
    private final TopicPartition testTp = new TopicPartition("testTopic", 0);
    private final OffsetManager manager = new OffsetManager(testTp, initialFetchOffset);

    @Test
    public void testSkipMissingOffsetsWhenFindingNextCommitOffsetWithGapInMiddleOfAcked() {
        /* If topic compaction is enabled in Kafka, we sometimes need to commit past a gap of deleted offsets
         * Since the Kafka consumer should return offsets in order, we can assume that if a message is acked
         * then any prior message will have been emitted at least once.
         * If we see an acked message and some of the offsets preceding it were not emitted, they must have been compacted away and should be skipped.
         */
        manager.addToEmitMsgs(0);
        manager.addToEmitMsgs(1);
        manager.addToEmitMsgs(2);
        //3, 4 compacted away
        manager.addToEmitMsgs(initialFetchOffset + 5);
        manager.addToEmitMsgs(initialFetchOffset + 6);
        manager.addToAckMsgs(getMessageId(initialFetchOffset));
        manager.addToAckMsgs(getMessageId(initialFetchOffset + 1));
        manager.addToAckMsgs(getMessageId(initialFetchOffset + 2));
        manager.addToAckMsgs(getMessageId(initialFetchOffset + 6));
        
        assertThat("The offset manager should not skip past offset 5 which is still pending", manager.findNextCommitOffset(COMMIT_METADATA).offset(), is(initialFetchOffset + 3));
        
        manager.addToAckMsgs(getMessageId(initialFetchOffset + 5));
        
        assertThat("The offset manager should skip past the gap in acked messages, since the messages were not emitted",
            manager.findNextCommitOffset(COMMIT_METADATA), is(new OffsetAndMetadata(initialFetchOffset + 7, COMMIT_METADATA)));
    }
    
    @Test
    public void testSkipMissingOffsetsWhenFindingNextCommitOffsetWithGapBeforeAcked() {
        //0-4 compacted away
        manager.addToEmitMsgs(initialFetchOffset + 5);
        manager.addToEmitMsgs(initialFetchOffset + 6);
        manager.addToAckMsgs(getMessageId(initialFetchOffset + 6));
        
        assertThat("The offset manager should not skip past offset 5 which is still pending", manager.findNextCommitOffset(COMMIT_METADATA), is(nullValue()));
        
        manager.addToAckMsgs(getMessageId(initialFetchOffset + 5));
        
        assertThat("The offset manager should skip past the gap in acked messages, since the messages were not emitted", 
            manager.findNextCommitOffset(COMMIT_METADATA), is(new OffsetAndMetadata(initialFetchOffset + 7, COMMIT_METADATA)));
    }

    @Test
    public void testFindNextCommittedOffsetWithNoAcks() {
        OffsetAndMetadata nextCommitOffset = manager.findNextCommitOffset(COMMIT_METADATA);
        assertThat("There shouldn't be a next commit offset when nothing has been acked", nextCommitOffset, is(nullValue()));
    }

    @Test
    public void testFindNextCommitOffsetWithOneAck() {
        /*
         * The KafkaConsumer commitSync API docs: "The committed offset should be the next message your application will consume, i.e.
         * lastProcessedMessageOffset + 1. "
         */
        emitAndAckMessage(getMessageId(initialFetchOffset));
        OffsetAndMetadata nextCommitOffset = manager.findNextCommitOffset(COMMIT_METADATA);
        assertThat("The next commit offset should be one past the processed message offset", nextCommitOffset.offset(), is(initialFetchOffset + 1));
    }

    @Test
    public void testFindNextCommitOffsetWithMultipleOutOfOrderAcks() {
        emitAndAckMessage(getMessageId(initialFetchOffset + 1));
        emitAndAckMessage(getMessageId(initialFetchOffset));
        OffsetAndMetadata nextCommitOffset = manager.findNextCommitOffset(COMMIT_METADATA);
        assertThat("The next commit offset should be one past the processed message offset", nextCommitOffset.offset(), is(initialFetchOffset + 2));
    }

    @Test
    public void testFindNextCommitOffsetWithAckedOffsetGap() {
        emitAndAckMessage(getMessageId(initialFetchOffset + 2));
        manager.addToEmitMsgs(initialFetchOffset + 1);
        emitAndAckMessage(getMessageId(initialFetchOffset));
        OffsetAndMetadata nextCommitOffset = manager.findNextCommitOffset(COMMIT_METADATA);
        assertThat("The next commit offset should cover the sequential acked offsets", nextCommitOffset.offset(), is(initialFetchOffset + 1));
    }

    @Test
    public void testFindNextOffsetWithAckedButNotEmittedOffsetGap() {
        /**
         * If topic compaction is enabled in Kafka some offsets may be deleted.
         * We distinguish this case from regular gaps in the acked offset sequence caused by out of order acking
         * by checking that offsets in the gap have been emitted at some point previously. 
         * If they haven't then they can't exist in Kafka, since the spout emits tuples in order.
         */
        emitAndAckMessage(getMessageId(initialFetchOffset + 2));
        emitAndAckMessage(getMessageId(initialFetchOffset));
        OffsetAndMetadata nextCommitOffset = manager.findNextCommitOffset(COMMIT_METADATA);
        assertThat("The next commit offset should cover all the acked offsets, since the offset in the gap hasn't been emitted and doesn't exist",
            nextCommitOffset.offset(), is(initialFetchOffset + 3));
    }
    
    @Test
    public void testFindNextCommitOffsetWithUnackedOffsetGap() {
        manager.addToEmitMsgs(initialFetchOffset + 1);
        emitAndAckMessage(getMessageId(initialFetchOffset));
        OffsetAndMetadata nextCommitOffset = manager.findNextCommitOffset(COMMIT_METADATA);
        assertThat("The next commit offset should cover the contiguously acked offsets", nextCommitOffset.offset(), is(initialFetchOffset + 1));
    }
    
    @Test
    public void testFindNextCommitOffsetWhenTooLowOffsetIsAcked() {
        OffsetManager startAtHighOffsetManager = new OffsetManager(testTp, 10);
        emitAndAckMessage(getMessageId(0));
        OffsetAndMetadata nextCommitOffset = startAtHighOffsetManager.findNextCommitOffset(COMMIT_METADATA);
        assertThat("Acking an offset earlier than the committed offset should have no effect", nextCommitOffset, is(nullValue()));
    }
    
    @Test
    public void testCommit() {
        emitAndAckMessage(getMessageId(initialFetchOffset));
        emitAndAckMessage(getMessageId(initialFetchOffset + 1));
        emitAndAckMessage(getMessageId(initialFetchOffset + 2));
        
        long committedMessages = manager.commit(new OffsetAndMetadata(initialFetchOffset + 2));
        
        assertThat("Should have committed all messages to the left of the earliest uncommitted offset", committedMessages, is(2L));
        assertThat("The committed messages should not be in the acked list anymore", manager.contains(getMessageId(initialFetchOffset)), is(false));
        assertThat("The committed messages should not be in the emitted list anymore", manager.containsEmitted(initialFetchOffset), is(false));
        assertThat("The committed messages should not be in the acked list anymore", manager.contains(getMessageId(initialFetchOffset + 1)), is(false));
        assertThat("The committed messages should not be in the emitted list anymore", manager.containsEmitted(initialFetchOffset + 1), is(false));
        assertThat("The uncommitted message should still be in the acked list", manager.contains(getMessageId(initialFetchOffset + 2)), is(true));
        assertThat("The uncommitted message should still be in the emitted list", manager.containsEmitted(initialFetchOffset + 2), is(true));
    }

    private KafkaSpoutMessageId getMessageId(long offset) {
        return new KafkaSpoutMessageId(testTp, offset);
    }
    
    private void emitAndAckMessage(KafkaSpoutMessageId msgId) {
        manager.addToEmitMsgs(msgId.offset());
        manager.addToAckMsgs(msgId);
    }

    @Test
    public void testGetNthUncommittedOffsetAfterCommittedOffset() { 
        manager.addToEmitMsgs(initialFetchOffset + 1);
        manager.addToEmitMsgs(initialFetchOffset + 2);
        manager.addToEmitMsgs(initialFetchOffset + 5);
        manager.addToEmitMsgs(initialFetchOffset + 30);
        
        assertThat("The third uncommitted offset should be 5", manager.getNthUncommittedOffsetAfterCommittedOffset(3), is(initialFetchOffset + 5L));
        assertThat("The fourth uncommitted offset should be 30", manager.getNthUncommittedOffsetAfterCommittedOffset(4), is(initialFetchOffset + 30L));
        
        expect.expect(NoSuchElementException.class);
        manager.getNthUncommittedOffsetAfterCommittedOffset(5);
    }

    @Test
    public void testCommittedFlagSetOnCommit() throws Exception {
        assertFalse(manager.hasCommitted());
        manager.commit(mock(OffsetAndMetadata.class));
        assertTrue(manager.hasCommitted());
    }
}
