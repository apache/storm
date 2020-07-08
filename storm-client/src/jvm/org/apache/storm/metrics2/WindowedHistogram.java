/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.metrics2;

import com.codahale.metrics.Histogram;
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;
import org.apache.storm.metric.internal.MetricStatTimer;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;

/**
 * Acts as a Histogram, but keeps track of approximate counts for the last 10 mins, 3 hours, 1 day, and all time.
 */
public class WindowedHistogram {
    private Histogram histogram;

    //10 min values
    private final int tmSize;
    private final long[] tmLatBuckets;
    private final long[] tmCountBuckets;
    private final long[] tmTime;
    //3 hour values
    private final int thSize;
    private final long[] thLatBuckets;
    private final long[] thCountBuckets;
    private final long[] thTime;
    //1 day values
    private final int odSize;
    private final long[] odLatBuckets;
    private final long[] odCountBuckets;
    private final long[] odTime;
    private final TimerTask task;
    private long currentLatBucket;
    private long currentCountBucket;
    // All internal state except for the current buckets are
    // protected using the Object Lock
    private long bucketStart;

    //all time
    private long allTimeLat;
    private long allTimeCount;

    /**
     * Constructor.
     *
     * @param numBuckets the number of buckets to divide the time periods into.
     */
    WindowedHistogram(String name, StormMetricRegistry metricRegistry, String topologyId,
                      String componentId, String streamId, int taskId, int port,
                      int numBuckets) {
        this.histogram = metricRegistry.histogram(name, topologyId, componentId, taskId, port, streamId);
        numBuckets = Math.max(numBuckets, 2);
        //We want to capture the full time range, so the target size is as
        // if we had one bucket less, then we do
        tmSize = 10 * 60 * 1000 / (numBuckets - 1);
        thSize = 3 * 60 * 60 * 1000 / (numBuckets - 1);
        odSize = 24 * 60 * 60 * 1000 / (numBuckets - 1);
        if (tmSize < 1 || thSize < 1 || odSize < 1) {
            throw new IllegalArgumentException("number of buckets is too large to be supported");
        }
        tmLatBuckets = new long[numBuckets];
        tmCountBuckets = new long[numBuckets];
        tmTime = new long[numBuckets];
        thLatBuckets = new long[numBuckets];
        thCountBuckets = new long[numBuckets];
        thTime = new long[numBuckets];
        odLatBuckets = new long[numBuckets];
        odCountBuckets = new long[numBuckets];
        odTime = new long[numBuckets];
        allTimeLat = 0;
        allTimeCount = 0;

        bucketStart = Time.currentTimeMillis();
        currentLatBucket = 0;
        currentCountBucket = 0;
        task = new Fresher();
        MetricStatTimer.scheduleMetricTask(task, tmSize, tmSize);
    }

    /**
     * Record a specific latency.
     *
     * @param latency what we are recording
     */
    public void record(long latency) {
        this.histogram.update(latency);

        synchronized (this) {
            currentLatBucket += latency;
            currentCountBucket++;
        }
    }

    private void rotateSched() {
        long lat;
        long count;
        long now = Time.currentTimeMillis();
        synchronized (this) {
            lat = currentLatBucket;
            count = currentCountBucket;
            currentLatBucket = 0;
            currentCountBucket = 0;

            long timeSpent = now - bucketStart;
            bucketStart = now;
            rotateBuckets(lat, count, timeSpent);
        }
    }

    private void rotateBuckets(long lat, long count, long timeSpent) {
        rotate(lat, count, timeSpent, tmSize, tmTime, tmLatBuckets, tmCountBuckets);
        rotate(lat, count, timeSpent, thSize, thTime, thLatBuckets, thCountBuckets);
        rotate(lat, count, timeSpent, odSize, odTime, odLatBuckets, odCountBuckets);
        allTimeLat += lat;
        allTimeCount += count;
    }

    private void rotate(long lat, long count, long timeSpent, long targetSize,
                        long[] times, long[] latBuckets, long[] countBuckets) {
        times[0] += timeSpent;
        latBuckets[0] += lat;
        countBuckets[0] += count;

        long currentTime = 0;
        long currentLat = 0;
        long currentCount = 0;
        if (times[0] >= targetSize) {
            for (int i = 0; i < latBuckets.length; i++) {
                long tmpTime = times[i];
                times[i] = currentTime;
                currentTime = tmpTime;

                long lt = latBuckets[i];
                latBuckets[i] = currentLat;
                currentLat = lt;

                long cnt = countBuckets[i];
                countBuckets[i] = currentCount;
                currentCount = cnt;
            }
        }
    }

    /**
     * Get time latency average.
     * @return a map of time window to average latency. Keys are "600" for last 10 mins "10800" for the last 3 hours "86400" for the last
     *     day ":all-time" for all time
     */
    public Map<String, Double> getTimeLatAvg() {
        Map<String, Double> ret = new HashMap<>();
        long lat;
        long count;
        long now = Time.currentTimeMillis();
        synchronized (this) {
            lat = currentLatBucket;
            count = currentCountBucket;
            long timeSpent = now - bucketStart;
            ret.put("600", readApproximateLatAvg(lat, count, timeSpent, tmTime, tmLatBuckets, tmCountBuckets, 600 * 1000));
            ret.put("10800", readApproximateLatAvg(lat, count, timeSpent, thTime, thLatBuckets, thCountBuckets, 10800 * 1000));
            ret.put("86400", readApproximateLatAvg(lat, count, timeSpent, odTime, odLatBuckets, odCountBuckets, 86400 * 1000));
            long allTimeCountSum = count + allTimeCount;
            ret.put(":all-time", Utils.zeroIfNaNOrInf(
                    (double) lat + allTimeLat) / allTimeCountSum);
        }
        return ret;
    }

    private double readApproximateLatAvg(long lat, long count, long timeSpent, long[] bucketTime,
                                 long[] latBuckets, long[] countBuckets, long desiredTime) {
        long timeNeeded = desiredTime - timeSpent;
        long totalLat = lat;
        long totalCount = count;
        for (int i = 0; i < bucketTime.length && timeNeeded > 0; i++) {
            //Don't pro-rate anything, it is all approximate so an extra bucket is not that bad.
            totalLat += latBuckets[i];
            totalCount += countBuckets[i];
            timeNeeded -= bucketTime[i];
        }
        return Utils.zeroIfNaNOrInf(((double) totalLat) / totalCount);
    }

    public void close() {
        if (task != null) {
            task.cancel();
        }
    }

    private class Fresher extends TimerTask {
        @Override
        public void run() {
            rotateSched();
        }
    }
}
