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
package org.apache.storm.kafka.spout.internal;

import java.util.concurrent.TimeUnit;
import org.apache.storm.utils.Time;

public class Timer {
    private final long delay;
    private final long period;
    private final TimeUnit timeUnit;
    private final long periodNanos;
    private long start;

    /**
     * Creates a class that mimics a single threaded timer that expires periodically. If a call to {@link
     * #isExpiredResetOnTrue()} occurs later than {@code period} since the timer was initiated or reset, this method returns
     * true. Each time the method returns true the counter is reset. The timer starts with the specified time delay.
     *
     * @param delay    the initial delay before the timer starts
     * @param period   the period between calls {@link #isExpiredResetOnTrue()}
     * @param timeUnit the time unit of delay and period
     */
    public Timer(long delay, long period, TimeUnit timeUnit) {
        this.delay = delay;
        this.period = period;
        this.timeUnit = timeUnit;

        periodNanos = timeUnit.toNanos(period);
        start = Time.nanoTime() + timeUnit.toNanos(delay);
    }

    public long period() {
        return period;
    }

    public long delay() {
        return delay;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    /**
     * Checks if a call to this method occurs later than {@code period} since the timer was initiated or reset. If that is the
     * case the method returns true, otherwise it returns false. Each time this method returns true, the counter is reset
     * (re-initiated) and a new cycle will start.
     *
     * @return true if the time elapsed since the last call returning true is greater than {@code period}. Returns false
     * otherwise.
     */
    public boolean isExpiredResetOnTrue() {
        final boolean expired = Time.nanoTime() - start >= periodNanos;
        if (expired) {
            start = Time.nanoTime();
        }
        return expired;
    }
}