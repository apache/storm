/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.kinesis.spout;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExponentialBackoffRetrier implements FailedMessageRetryHandler, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(ExponentialBackoffRetrier.class);
    // Wait interfal for retrying after first failure
    private final Long initialDelayMillis;
    // Base for exponential function in seconds for retrying for second, third and so on failures
    private final Long baseSeconds;
    // Maximum number of retries
    private final Long maxRetries;
    // map to track number of failures for each kinesis message that failed
    private Map<KinesisMessageId, Long> failCounts = new HashMap<>();
    // map to track next retry time for each kinesis message that failed
    private Map<KinesisMessageId, Long> retryTimes = new HashMap<>();
    // sorted set of records to be retrued based on retry time. earliest retryTime record comes first
    private SortedSet<KinesisMessageId> retryMessageSet = new TreeSet<>(new RetryTimeComparator());

    /**
     * No args constructor that uses defaults of 100 ms for first retry, max retries of Long.MAX_VALUE and an
     * exponential backoff of {@code Math.pow(2,i-1)} secs for retry {@code i} where {@code i = 2,3,...}.
     */
    public ExponentialBackoffRetrier() {
        this(100L, 2L, Long.MAX_VALUE);
    }

    /**
     * Creates a new exponential backoff retrier.
     * @param initialDelayMillis delay in milliseconds for first retry
     * @param baseSeconds base for exponent function in seconds
     * @param maxRetries maximum number of retries before the record is discarded/acked
     */
    public ExponentialBackoffRetrier(Long initialDelayMillis, Long baseSeconds, Long maxRetries) {
        this.initialDelayMillis = initialDelayMillis;
        this.baseSeconds = baseSeconds;
        this.maxRetries = maxRetries;
        validate();
    }

    private void validate() {
        if (initialDelayMillis < 0) {
            throw new IllegalArgumentException("initialDelayMillis cannot be negative.");
        }
        if (baseSeconds < 0) {
            throw new IllegalArgumentException("baseSeconds cannot be negative.");
        }
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries cannot be negative.");
        }
    }

    @Override
    public boolean failed(KinesisMessageId messageId) {
        LOG.debug("Handling failed message {}", messageId);
        // if maxRetries is 0, dont retry and return false as per interface contract
        if (maxRetries == 0) {
            LOG.warn("maxRetries set to 0. Hence not queueing " + messageId);
            return false;
        }
        // if first failure add it to the count map
        if (!failCounts.containsKey(messageId)) {
            failCounts.put(messageId, 0L);
        }
        // increment the fail count as we started with 0
        Long failCount = failCounts.get(messageId);
        failCounts.put(messageId, ++failCount);
        // if fail count is greater than maxRetries, discard or ack. for e.g. for maxRetries 3, 4 failures are allowed at maximum
        if (failCount > maxRetries) {
            LOG.warn("maxRetries reached so dropping " + messageId);
            failCounts.remove(messageId);
            return false;
        }
        // if reached so far, add it to the set of messages waiting to be retried with next retry time based on how many times it failed
        retryTimes.put(messageId, getRetryTime(failCount));
        retryMessageSet.add(messageId);
        LOG.debug("Scheduled {} for retry at {} and retry attempt {}", messageId, retryTimes.get(messageId), failCount);
        return true;
    }

    @Override
    public void acked(KinesisMessageId messageId) {
        // message was acked after being retried. so clear the state for that message
        LOG.debug("Ack received for {}. Hence cleaning state.", messageId);
        failCounts.remove(messageId);
    }

    @Override
    public KinesisMessageId getNextFailedMessageToRetry() {
        KinesisMessageId result = null;
        // return the first message to be retried from the set. It will return the message with the earliest retry time <= current time
        if (!retryMessageSet.isEmpty()) {
            result = retryMessageSet.first();
            if (!(retryTimes.get(result) <= System.nanoTime())) {
                result = null;
            }
        }
        LOG.debug("Returning {} to spout for retrying.", result);
        return result;
    }

    @Override
    public void failedMessageEmitted(KinesisMessageId messageId) {
        // spout notified that message returned by us for retrying was actually emitted. hence remove it from set and
        // wait for its ack or fail
        // but still keep it in counts map to retry again on failure or remove on ack
        LOG.debug("Spout says {} emitted. Hence removing it from queue and wait for its ack or fail", messageId);
        retryMessageSet.remove(messageId);
        retryTimes.remove(messageId);
    }

    /**
     * private helper method to get next retry time for retry attempt i (handles long overflow as well by capping it to
     * Long.MAX_VALUE).
     */
    private Long getRetryTime(Long retryNum) {
        Long retryTime = System.nanoTime();
        Long nanoMultiplierForMillis = 1000000L;
        // if first retry then retry time  = current time  + initial delay
        if (retryNum == 1) {
            retryTime += initialDelayMillis * nanoMultiplierForMillis;
        } else {
            // else use the exponential backoff logic and handle long overflow
            Long maxValue = Long.MAX_VALUE;
            double time = Math.pow(baseSeconds, retryNum - 1) * 1000 * nanoMultiplierForMillis;
            // if delay or delay + current time are bigger than long max value
            // second predicate for or condition uses the fact that long addition over the limit circles back
            if ((time >= maxValue.doubleValue()) || ((retryTime + (long) time) < retryTime)) {
                retryTime = maxValue;
            } else {
                retryTime += (long) time;
            }
        }
        return retryTime;
    }

    private class RetryTimeComparator implements Serializable, Comparator<KinesisMessageId> {
        @Override
        public int compare(KinesisMessageId o1, KinesisMessageId o2) {
            return retryTimes.get(o1).compareTo(retryTimes.get(o2));
        }
    }
}
