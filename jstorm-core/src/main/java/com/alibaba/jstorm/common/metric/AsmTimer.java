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

import com.alibaba.jstorm.common.metric.snapshot.AsmMeterSnapshot;
import com.alibaba.jstorm.common.metric.snapshot.AsmTimerSnapshot;
import com.codahale.metrics.*;
import com.codahale.metrics.Timer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * same as histogram, each window has a separate timer, which is recreated after the window cycle. note that all data in a timer are measured by nanoseconds. so
 * for most cases, you can replace with histograms.
 */
public class AsmTimer extends AsmMetric<Timer> {
    private final Map<Integer, Timer> timerMap = new ConcurrentHashMap<Integer, Timer>();
    private Timer unFlushed = newTimer();

    public AsmTimer() {
        super();
        for (int win : windowSeconds) {
            timerMap.put(win, newTimer());
        }
    }

    @Override
    public void update(Number obj) {
        if (sample()) {
            this.unFlushed.update(obj.longValue(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void updateDirectly(Number obj) {
        this.unFlushed.update(obj.longValue(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Map<Integer, Timer> getWindowMetricMap() {
        return timerMap;
    }

    @Override
    public Timer mkInstance() {
        return newTimer();
    }

    @Override
    protected void updateSnapshot(int window) {
        Timer timer = timerMap.get(window);
        if (timer != null){
            AsmTimerSnapshot timerSnapshot = new AsmTimerSnapshot();
            timerSnapshot.setHistogram(timer.getSnapshot());
            timerSnapshot.setMeter(new AsmMeterSnapshot().setM1(timer.getOneMinuteRate()).setM5(timer.getFiveMinuteRate())
                    .setM15(timer.getFifteenMinuteRate()).setMean(timer.getMeanRate()));
            if (metricId > 0) {
                timerSnapshot.setMetricId(metricId);
            }
            timerSnapshot.setTs(System.currentTimeMillis());
            snapshots.put(window, timerSnapshot);
        }
    }

    /**
     * flush temp timer data to all windows & assoc metrics.
     */
    protected void doFlush() {
        long[] values = unFlushed.getSnapshot().getValues();
        for (Timer timer : timerMap.values()) {
            for (long val : values) {
                timer.update(val, TimeUnit.MILLISECONDS);

                for (AsmMetric metric : this.assocMetrics) {
                    metric.updateDirectly(val);
                }
            }
        }
        this.unFlushed = newTimer();
    }

    @Override
    public AsmMetric clone() {
        return new AsmTimer();
    }

    private Timer newTimer() {
        return new Timer(new ExponentiallyDecayingReservoir());
    }
}
