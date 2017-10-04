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
import static org.junit.Assert.assertThat;

import java.util.NoSuchElementException;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.KafkaSpoutMessageId;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class OffsetManagerTest {

    @Rule
    public ExpectedException expect = ExpectedException.none();

    @Test
    public void testSkipMissingOffsetsWhenFindingNextCommitOffsetWithGapInMiddleOfAcked() {
        /*If topic compaction is enabled in Kafka, we sometimes need to commit past a gap of deleted offsets
         * Since the Kafka consumer should return offsets in order, we can assume that if a message is acked
         * then any prior message will have been emitted at least once.
         * If we see an acked message and some of the offsets preceding it were not emitted, they must have been compacted away and should be skipped.
         */
        
        TopicPartition tp = new TopicPartition("test", 0);
        OffsetManager manager = new OffsetManager(tp, 0);
        
        manager.addToEmitMsgs(0);
        manager.addToEmitMsgs(1);
        manager.addToEmitMsgs(2);
        //3, 4 compacted away
        manager.addToEmitMsgs(5);
        manager.addToEmitMsgs(6);
        manager.addToAckMsgs(new KafkaSpoutMessageId(tp, 0));
        manager.addToAckMsgs(new KafkaSpoutMessageId(tp, 1));
        manager.addToAckMsgs(new KafkaSpoutMessageId(tp, 2));
        manager.addToAckMsgs(new KafkaSpoutMessageId(tp, 6));
        
        assertThat("The offset manager should not skip past offset 5 which is still pending", manager.findNextCommitOffset().offset(), is(2L));
        
        manager.addToAckMsgs(new KafkaSpoutMessageId(tp, 5));
        
        assertThat("The offset manager should skip past the gap in acked messages, since the messages were not emitted", 
            manager.findNextCommitOffset().offset(), is(6L));
    }
    
    @Test
    public void testSkipMissingOffsetsWhenFindingNextCommitOffsetWithGapBeforeAcked() {
        
        TopicPartition tp = new TopicPartition("test", 0);
        OffsetManager manager = new OffsetManager(tp, 0);
        
        //0-4 compacted away
        manager.addToEmitMsgs(5);
        manager.addToEmitMsgs(6);
        manager.addToAckMsgs(new KafkaSpoutMessageId(tp, 6));
        
        assertThat("The offset manager should not skip past offset 5 which is still pending", manager.findNextCommitOffset(), is(nullValue()));
        
        manager.addToAckMsgs(new KafkaSpoutMessageId(tp, 5));
        
        assertThat("The offset manager should skip past the gap in acked messages, since the messages were not emitted", 
            manager.findNextCommitOffset().offset(), is(6L));
    }

}
