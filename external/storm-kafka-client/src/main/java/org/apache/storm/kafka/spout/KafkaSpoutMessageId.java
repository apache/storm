/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.kafka.spout;

import java.io.Serializable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

public class KafkaSpoutMessageId implements Serializable {
    private final TopicPartition topicPart;
    private final long offset;
    private int numFails = 0;
    /**
     * true if the record was emitted using a form of collector.emit(...). false
     * when skipping null tuples as configured by the user in KafkaSpoutConfig
     */
    private boolean emitted;

    public KafkaSpoutMessageId(ConsumerRecord<?, ?> consumerRecord) {
        this(consumerRecord, true);
    }

    public KafkaSpoutMessageId(ConsumerRecord<?, ?> consumerRecord, boolean emitted) {
        this(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()), consumerRecord.offset(), emitted);
    }

    public KafkaSpoutMessageId(TopicPartition topicPart, long offset) {
        this(topicPart, offset, true);
    }

    /**
     * Creates a new KafkaSpoutMessageId.
     * @param topicPart The topic partition this message belongs to
     * @param offset The offset of this message
     * @param emitted True iff this message is not being skipped as a null tuple
     */
    public KafkaSpoutMessageId(TopicPartition topicPart, long offset, boolean emitted) {
        this.topicPart = topicPart;
        this.offset = offset;
        this.emitted = emitted;
    }

    public int partition() {
        return topicPart.partition();
    }

    public String topic() {
        return topicPart.topic();
    }

    public long offset() {
        return offset;
    }

    public int numFails() {
        return numFails;
    }

    public void incrementNumFails() {
        ++numFails;
    }

    public TopicPartition getTopicPartition() {
        return topicPart;
    }

    public boolean isEmitted() {
        return emitted;
    }

    public void setEmitted(boolean emitted) {
        this.emitted = emitted;
    }

    @Override
    public String toString() {
        return "{"
            + "topic-partition=" + topicPart
            + ", offset=" + offset
            + ", numFails=" + numFails
            + ", emitted=" + emitted
            + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KafkaSpoutMessageId messageId = (KafkaSpoutMessageId) o;
        if (offset != messageId.offset) {
            return false;
        }
        return topicPart.equals(messageId.topicPart);
    }

    @Override
    public int hashCode() {
        int result = topicPart.hashCode();
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        return result;
    }
}
