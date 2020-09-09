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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Acts as a MultiCount Stat, but keeps track of approximate counts for the last 10 mins, 3 hours, 1 day, and all time. for the same keys
 */
public class MultiCountStat<T> {
    public static final int TEN_MIN_IN_SECONDS = 60 * 10;
    public static final String TEN_MIN_IN_SECONDS_STR = TEN_MIN_IN_SECONDS + "";
    private final int numBuckets;
    private ConcurrentHashMap<T, CountStat> counts = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param numBuckets the number of buckets to divide the time periods into.
     */
    public MultiCountStat(int numBuckets) {
        this.numBuckets = numBuckets;
    }

    CountStat get(T key) {
        CountStat c = counts.get(key);
        if (c == null) {
            synchronized (this) {
                c = counts.get(key);
                if (c == null) {
                    c = new CountStat(numBuckets);
                    counts.put(key, c);
                }
            }
        }
        return c;
    }

    /**
     * Increase the count by the given value.
     *
     * @param count number to count
     */
    public void incBy(T key, long count) {
        get(key).incBy(count);
    }

    protected String keyToString(T key) {
        if (key instanceof List) {
            //This is a bit of a hack.  If it is a list, then it is [component, stream]
            //we want to format this as component:stream
            List<String> lk = (List<String>) key;
            return lk.get(0) + ":" + lk.get(1);
        }
        return key.toString();
    }

    public Map<String, Map<T, Long>> getTimeCounts() {
        Map<String, Map<T, Long>> ret = new HashMap<>();
        for (Map.Entry<T, CountStat> entry : counts.entrySet()) {
            T key = entry.getKey();
            Map<String, Long> toFlip = entry.getValue().getTimeCounts();
            for (Map.Entry<String, Long> subEntry : toFlip.entrySet()) {
                String time = subEntry.getKey();
                Map<T, Long> tmp = ret.get(time);
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
        for (CountStat cc : counts.values()) {
            cc.close();
        }
    }
}
