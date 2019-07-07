/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.metric.internal;

import java.io.Closeable;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is a utility to track the rate of something.
 */
public class RateTracker implements Closeable {
    private final int bucketSizeMillis;
    //Old Buckets and their length are only touched when rotating or gathering the metrics, which should not be that frequent
    // As such all access to them should be protected by synchronizing with the RateTracker instance
    private final long[] bucketTime;
    private final long[] oldBuckets;

    private final AtomicLong bucketStart;
    private final AtomicLong currentBucket;

    private final TimerTask task;

    /**
     * Constructor.
     *
     * @param validTimeWindowInMils events that happened before validTimeWindowInMils are not considered when reporting the rate.
     * @param numBuckets            the number of time sildes to divide validTimeWindows. The more buckets, the smother the reported results
     *                              will be.
     */
    public RateTracker(int validTimeWindowInMils, int numBuckets) {
        this(validTimeWindowInMils, numBuckets, -1);
    }

    /**
     * Constructor.
     *
     * @param validTimeWindowInMils events that happened before validTimeWindow are not considered when reporting the rate.
     * @param numBuckets            the number of time sildes to divide validTimeWindows. The more buckets, the smother the reported results
     *                              will be.
     * @param startTime             if positive the simulated time to start the first bucket at.
     */
    RateTracker(int validTimeWindowInMils, int numBuckets, long startTime) {
        numBuckets = Math.max(numBuckets, 1);
        bucketSizeMillis = validTimeWindowInMils / numBuckets;
        if (bucketSizeMillis < 1) {
            throw new IllegalArgumentException(
                "validTimeWindowInMilis and numOfSildes cause each slide to have a window that is too small");
        }
        bucketTime = new long[numBuckets - 1];
        oldBuckets = new long[numBuckets - 1];

        bucketStart = new AtomicLong(startTime >= 0 ? startTime : System.currentTimeMillis());
        currentBucket = new AtomicLong(0);
        if (startTime < 0) {
            task = new Fresher();
            MetricStatTimer.timer.scheduleAtFixedRate(task, bucketSizeMillis, bucketSizeMillis);
        } else {
            task = null;
        }
    }

    /**
     * Notify the tracker upon new arrivals.
     *
     * @param count number of arrivals
     */
    public void notify(long count) {
        currentBucket.addAndGet(count);
    }

    /**
     * Get report rate.
     * @return the approximate average rate per second.
     */
    public synchronized double reportRate() {
        return reportRate(System.currentTimeMillis());
    }

    synchronized double reportRate(long currentTime) {
        long duration = Math.max(1L, currentTime - bucketStart.get());
        long events = currentBucket.get();
        for (int i = 0; i < oldBuckets.length; i++) {
            events += oldBuckets[i];
            duration += bucketTime[i];
        }

        return events * 1000.0 / duration;
    }

    @Override
    public void close() {
        if (task != null) {
            task.cancel();
        }
    }

    /**
     * Rotate the buckets a set number of times for testing purposes.
     *
     * @param numToEclipse the number of rotations to perform.
     */
    final void forceRotate(int numToEclipse, long interval) {
        long time = bucketStart.get();
        for (int i = 0; i < numToEclipse; i++) {
            time += interval;
            rotateBuckets(time);
        }
    }

    private synchronized void rotateBuckets(long time) {
        long timeSpent = time - bucketStart.getAndSet(time);
        long currentVal = currentBucket.getAndSet(0);
        for (int i = 0; i < oldBuckets.length; i++) {
            long tmpTime = bucketTime[i];
            bucketTime[i] = timeSpent;
            timeSpent = tmpTime;

            long cnt = oldBuckets[i];
            oldBuckets[i] = currentVal;
            currentVal = cnt;
        }
    }

    private class Fresher extends TimerTask {
        @Override
        public void run() {
            rotateBuckets(System.currentTimeMillis());
        }
    }
}
