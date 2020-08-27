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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps track of approximate latency for the last 10 mins, 3 hours, 1 day, and all time. for
 * the same keys
 */
public class MultiLatencyStat<T> {
    private final int numBuckets;
    private ConcurrentHashMap<T, LatencyStat> lat = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param numBuckets the number of buckets to divide the time periods into.
     */
    public MultiLatencyStat(int numBuckets) {
        this.numBuckets = numBuckets;
    }

    LatencyStat get(T key) {
        LatencyStat c = lat.get(key);
        if (c == null) {
            synchronized (this) {
                c = lat.get(key);
                if (c == null) {
                    c = new LatencyStat(numBuckets);
                    lat.put(key, c);
                }
            }
        }
        return c;
    }

    /**
     * Record a latency value.
     *
     * @param latency the measurement to record
     */
    public void record(T key, long latency) {
        get(key).record(latency);
    }

    public Map<String, Map<T, Double>> getTimeLatAvg() {
        Map<String, Map<T, Double>> ret = new HashMap<>();
        for (Map.Entry<T, LatencyStat> entry : lat.entrySet()) {
            T key = entry.getKey();
            Map<String, Double> toFlip = entry.getValue().getTimeLatAvg();
            for (Map.Entry<String, Double> subEntry : toFlip.entrySet()) {
                String time = subEntry.getKey();
                Map<T, Double> tmp = ret.get(time);
                if (tmp == null) {
                    tmp = new HashMap<>();
                    ret.put(time, tmp);
                }
                tmp.put(key, subEntry.getValue());
            }
        }
        return ret;
    }

    public void close() {
        for (LatencyStat l : lat.values()) {
            l.close();
        }
    }
}
