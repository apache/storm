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
package com.alibaba.jstorm.common.metric;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.common.metric.snapshot.AsmSnapshot;
import com.alibaba.jstorm.metric.AsmWindow;
import com.alibaba.jstorm.metric.MetaType;
import com.alibaba.jstorm.metric.MetricType;
import com.alibaba.jstorm.utils.TimeUtils;
import com.codahale.metrics.Metric;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AsmMetric<T extends Metric> {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private static final Joiner JOINER = Joiner.on(".");

    protected static final List<Integer> windowSeconds = Lists
            .newArrayList(AsmWindow.M1_WINDOW, AsmWindow.M10_WINDOW, AsmWindow.H2_WINDOW, AsmWindow.D1_WINDOW);
    protected static final List<Integer> nettyWindows = Lists.newArrayList(AsmWindow.M1_WINDOW);

    protected static int minWindow = AsmWindow.M1_WINDOW;
    protected static final List<Integer> EMPTY_WIN = Lists.newArrayListWithCapacity(0);
    /**
     * sample rate for meter, histogram and timer, note that counter & gauge are not sampled.
     */
    private static double sampleRate = ConfigExtension.DEFAULT_METRIC_SAMPLE_RATE;

    protected int op = MetricOp.REPORT;
    protected volatile long metricId = 0L;
    protected String metricName;
    protected boolean aggregate = true;
    protected volatile long lastFlushTime = TimeUtils.current_time_secs() - AsmWindow.M1_WINDOW;
    protected Map<Integer, Long> rollingTimeMap = new ConcurrentHashMap<>();
    protected Map<Integer, Boolean> rollingDirtyMap = new ConcurrentHashMap<>();

    protected final Map<Integer, AsmSnapshot> snapshots = new ConcurrentHashMap<Integer, AsmSnapshot>();

    protected Set<AsmMetric> assocMetrics = new HashSet<AsmMetric>();

    public AsmMetric() {
        for (Integer win : windowSeconds) {
            rollingTimeMap.put(win, lastFlushTime);
            rollingDirtyMap.put(win, false);
        }
    }

    /**
     * keep a random for each instance to avoid competition (although it's thread-safe).
     */
    private final Random rand = new Random();

    protected boolean sample() {
        return rand.nextDouble() <= sampleRate;
    }

    public static void setSampleRate(double sampleRate) {
        AsmMetric.sampleRate = sampleRate;
    }

    /**
     * In order to improve performance
     */
    public abstract void update(Number obj);


    public void updateDirectly(Number obj) {
        update(obj);
    }

    public abstract AsmMetric clone();

    public AsmMetric setOp(int op) {
        this.op = op;
        return this;
    }

    public int getOp() {
        return this.op;
    }

    /**
     * for test
     */
    public static void setWindowSeconds(List<Integer> windows) {
        synchronized (windowSeconds) {
            windowSeconds.clear();
            windowSeconds.addAll(windows);

            minWindow = getMinWindow(windows);
        }
    }

    public static int getMinWindow(List<Integer> windows) {
        int min = Integer.MAX_VALUE;
        for (int win : windows) {
            if (win < min) {
                min = win;
            }
        }
        return min;
    }

    public void addAssocMetrics(AsmMetric... metrics) {
        Collections.addAll(assocMetrics, metrics);
    }

    public long getMetricId() {
        return metricId;
    }

    public void setMetricId(long metricId) {
        this.metricId = metricId;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public void flush() {
        long time = TimeUtils.current_time_secs();
        List<Integer> windows = getValidWindows();
        if (windows.size() == 0) {
            return;
        }

        doFlush();

        List<Integer> rollwindows = rollWindows(time, windows);

        for (int win : windows) {
            if (rollwindows.contains(win)) {
                updateSnapshot(win);

                Map<Integer, T> metricMap = getWindowMetricMap();
                if (metricMap != null) {
                    metricMap.put(win, mkInstance());
                }
            } else if (!rollingDirtyMap.get(win)) {
                //if this window has never been passed, we still update this window snapshot
                updateSnapshot(win);
            }
        }
        this.lastFlushTime = TimeUtils.current_time_secs();
    }

    public List<Integer> rollWindows(long time, List<Integer> windows) {
        List<Integer> rolling = new ArrayList<>();
        for (Integer win : windows) {
            long rollingTime = rollingTimeMap.get(win);
            // might delay somehow, so add extra 5 sec bias
            if (time - rollingTime >= win - 5) {
                rolling.add(win);
                rollingDirtyMap.put(win, true);     //mark this window has been passed
                rollingTimeMap.put(win, (long) TimeUtils.current_time_secs());
            }
        }
        return rolling;
    }

    /**
     * flush temp data to all windows & assoc metrics.
     */
    protected abstract void doFlush();

    public abstract Map<Integer, T> getWindowMetricMap();

    public abstract T mkInstance();

    protected abstract void updateSnapshot(int window);

    public Map<Integer, AsmSnapshot> getSnapshots() {
        return snapshots;
    }

    /**
     * DO NOT judge whether to flush by 60sec because there might be nuance by the alignment of time(maybe less than 1 sec?)
     * so we subtract 5 sec from a min flush window.
     */
    public List<Integer> getValidWindows() {
        long diff = TimeUtils.current_time_secs() - this.lastFlushTime + 5;
        if (diff < minWindow) {
            // logger.warn("no valid windows for metric:{}, diff:{}", this.metricName, diff);
            return EMPTY_WIN;
        }
        // for netty metrics, use only 1min window
        if (this.metricName.startsWith(MetaType.NETTY.getV())) {
            return nettyWindows;
        }

        return windowSeconds;
    }

    public boolean isAggregate() {
        return aggregate;
    }

    public void setAggregate(boolean aggregate) {
        this.aggregate = aggregate;
    }

    public static String mkName(Object... parts) {
        return JOINER.join(parts);
    }

    public static class MetricOp {
        public static final int LOG = 1;
        public static final int REPORT = 2;
    }

    public static class Builder {
        public static AsmMetric build(MetricType metricType) {
            AsmMetric metric;
            if (metricType == MetricType.COUNTER) {
                metric = new AsmCounter();
            } else if (metricType == MetricType.METER) {
                metric = new AsmMeter();
            } else if (metricType == MetricType.HISTOGRAM) {
                metric = new AsmHistogram();
            } else if (metricType == MetricType.TIMER) {
                metric = new AsmTimer();
            } else {
                throw new IllegalArgumentException("invalid metric type:" + metricType);
            }
            return metric;
        }
    }

    public static void main(String[] args) throws Exception {
        AsmMeter meter = new AsmMeter();
        int t = 0, f = 0;
        for (int i = 0; i < 100; i++) {
            if (meter.sample()) {
                t++;
            } else {
                f++;
            }
        }
        System.out.println(t + "," + f);
    }
}
