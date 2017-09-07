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
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Represents the logic that manages the retrial of failed tuples.
 */
public interface KafkaSpoutRetryService extends Serializable {
    /**
     * Schedules this {@link KafkaSpoutMessageId} if not yet scheduled, or
     * updates retry time if it has already been scheduled. It may also indicate
     * that the message should not be retried, in which case the message will not be scheduled.
     * @param msgId message to schedule for retrial
     * @return true if the message will be retried, false otherwise
     */
    boolean schedule(KafkaSpoutMessageId msgId);

    /**
     * Removes a message from the list of messages scheduled for retrial.
     * @param msgId message to remove from retrial
     * @return true if the message was scheduled for retrial, false otherwise
     */
    boolean remove(KafkaSpoutMessageId msgId);

    /**
     * Retains all the messages whose {@link TopicPartition} belongs to the specified {@code Collection<TopicPartition>}.
     * All messages that come from a {@link TopicPartition} NOT existing in the collection will be removed.
     * This method is useful to cleanup state following partition rebalance.
     * @param topicPartitions Collection of {@link TopicPartition} for which to keep messages
     * @return true if at least one message was removed, false otherwise
     */
    boolean retainAll(Collection<TopicPartition> topicPartitions);

    /**
     * Gets the earliest retriable offsets.
     * @return The earliest retriable offset for each TopicPartition that has
     *     offsets ready to be retried, i.e. for which a tuple has failed
     *     and has retry time less than current time.
     */
    Map<TopicPartition, Long> earliestRetriableOffsets();

    /**
     * Checks if a specific failed {@link KafkaSpoutMessageId} is ready to be retried,
     * i.e is scheduled and has retry time that is less than current time.
     * @param msgId message to check for readiness
     * @return true if message is ready to be retried, false otherwise
     */
    boolean isReady(KafkaSpoutMessageId msgId);

    /**
     * Checks if a specific failed {@link KafkaSpoutMessageId} is scheduled to be retried.
     * The message may or may not be ready to be retried yet.
     * @param msgId message to check for scheduling status
     * @return true if the message is scheduled to be retried, regardless of being or not ready to be retried.
     *     Returns false is this message is not scheduled for retrial
     */
    boolean isScheduled(KafkaSpoutMessageId msgId);

    /**
     * Get the number of messages ready for retry.
     * @return The number of messages that are ready for retry
     */
    int readyMessageCount();

    /**
     * Gets the {@link KafkaSpoutMessageId} for the given record.
     * @param record The record to fetch the id for
     * @return The id the record was scheduled for retry with,
     *     or a new {@link KafkaSpoutMessageId} if the record was not scheduled for retry.
     */
    KafkaSpoutMessageId getMessageId(ConsumerRecord<?, ?> record);
}
