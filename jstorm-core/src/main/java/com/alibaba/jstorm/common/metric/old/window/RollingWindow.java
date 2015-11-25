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
package com.alibaba.jstorm.common.metric.old.window;

import com.alibaba.jstorm.common.metric.old.operator.Sampling;
import com.alibaba.jstorm.common.metric.old.operator.StartTime;
import com.alibaba.jstorm.common.metric.old.operator.merger.Merger;
import com.alibaba.jstorm.common.metric.old.operator.updater.Updater;
import com.alibaba.jstorm.utils.IntervalCheck;
import com.alibaba.jstorm.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;

public class RollingWindow<V> implements Sampling<V>, StartTime {
    private static final long serialVersionUID = 3794478417380003279L;
    private static final Logger LOG = LoggerFactory.getLogger(RollingWindow.class);

    protected long startTime;
    protected Integer currBucketTime;
    protected int interval; // unit is second
    protected int windowSecond;
    protected IntervalCheck intervalCheck;

    protected TreeMap<Integer, V> buckets;
    protected Integer bucketNum;
    protected V unflushed;
    protected V defaultValue;

    protected Updater<V> updater;
    protected Merger<V> merger;

    RollingWindow(V defaultValue, int interval, int windowSecond, Updater<V> updater, Merger<V> merger) {
        this.startTime = System.currentTimeMillis();
        this.interval = interval;
        this.intervalCheck = new IntervalCheck();
        this.intervalCheck.setInterval(interval);
        this.currBucketTime = getCurrBucketTime();

        this.bucketNum = windowSecond / interval;
        this.windowSecond = (bucketNum) * interval;

        this.buckets = new TreeMap<Integer, V>();

        this.updater = updater;
        this.merger = merger;

        this.defaultValue = defaultValue;

    }

    @Override
    public void update(Number obj) {
        // TODO Auto-generated method stub

        if (intervalCheck.check()) {
            rolling();
        }
        synchronized (this) {
            unflushed = updater.update(obj, unflushed);

        }

    }

    /**
     * In order to improve performance Flush one batch to rollingWindow
     * 
     */
    public void updateBatch(V batch) {

        if (intervalCheck.check()) {
            rolling();
        }
        synchronized (this) {
            unflushed = updater.updateBatch(batch, unflushed);
        }

    }

    @Override
    public V getSnapshot() {
        // TODO Auto-generated method stub
        if (intervalCheck.check()) {
            rolling();
        }

        cleanExpiredBuckets();
        // @@@ Testing
        // LOG.info("Raw Data:" + buckets + ",unflushed:" + unflushed);

        Collection<V> values = buckets.values();

        V ret = merger.merge(values, unflushed, this);
        if (ret == null) {

            // @@@ testing
            // LOG.warn("!!!!Exist null data !!!!!");
            return defaultValue;
        }
        return ret;
    }

    /*
     * Move the "current bucket time" index and clean the expired buckets
     */
    protected void rolling() {
        synchronized (this) {
            if (unflushed != null) {
                buckets.put(currBucketTime, unflushed);
                unflushed = null;
            }

            currBucketTime = getCurrBucketTime();

            return;
        }
    }

    protected void cleanExpiredBuckets() {
        int nowSec = TimeUtils.current_time_secs();
        int startRemove = nowSec - (interval - 1) - windowSecond;

        List<Integer> removeList = new ArrayList<Integer>();

        for (Integer keyTime : buckets.keySet()) {
            if (keyTime < startRemove) {
                removeList.add(keyTime);
            } else if (keyTime >= startRemove) {
                break;
            }
        }

        for (Integer removeKey : removeList) {
            buckets.remove(removeKey);
            // @@@ Testing
            // LOG.info("Remove key:" + removeKey + ", diff:" + (nowSec - removeKey));

        }

        if (buckets.isEmpty() == false) {
            Integer first = buckets.firstKey();
            startTime = first.longValue() * 1000;
        }
    }

    public int getWindowSecond() {
        return windowSecond;
    }

    public long getStartTime() {
        return startTime;
    }

    public int getInterval() {
        return interval;
    }

    public Integer getBucketNum() {
        return bucketNum;
    }

    public V getDefaultValue() {
        return defaultValue;
    }

    private Integer getCurrBucketTime() {
        return (TimeUtils.current_time_secs() / interval) * interval;
    }
}
