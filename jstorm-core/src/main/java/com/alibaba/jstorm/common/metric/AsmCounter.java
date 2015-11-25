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


import com.alibaba.jstorm.common.metric.snapshot.AsmCounterSnapshot;
import com.alibaba.jstorm.common.metric.snapshot.AsmSnapshot;
import com.codahale.metrics.Counter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * counter wrapper. note that counter is a little special, every snapshot we only return the delta value instead of
 * total value, which prevents data loss if certain tasks are killed.
 */
public class AsmCounter extends AsmMetric<Counter> {

    private final Map<Integer, Counter> counterMap = new ConcurrentHashMap<>();
    private Counter unFlushed = new Counter();

    public AsmCounter() {
        super();
        for (int win : windowSeconds) {
            counterMap.put(win, new Counter());
        }
    }

    public void inc() {
        update(1);
    }

    @Override
    public void update(Number val) {
        this.unFlushed.inc(val.longValue());
    }

    /**
     * flush temp counter data to all windows & assoc metrics.
     */
    protected void doFlush() {
        long v = unFlushed.getCount();
        for (Counter counter : counterMap.values()) {
            counter.inc(v);
        }
        for (AsmMetric assocMetric : assocMetrics) {
            assocMetric.updateDirectly(v);
        }

        this.unFlushed.dec(v);
    }

    @Override
    public Map<Integer, Counter> getWindowMetricMap() {
        return counterMap;
    }

    @Override
    public Counter mkInstance() {
        return new Counter();
    }

    @Override
    protected void updateSnapshot(int window) {
        Counter counter = counterMap.get(window);
        if (counter != null) {
            AsmSnapshot snapshot = new AsmCounterSnapshot().setValue(counter.getCount())
                    .setTs(System.currentTimeMillis()).setMetricId(metricId);
            snapshots.put(window, snapshot);
        }
    }

    @Override
    public AsmMetric clone() {
        return new AsmCounter();
    }
}
