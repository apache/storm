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
import com.alibaba.jstorm.common.metric.snapshot.AsmSnapshot;
import com.codahale.metrics.Meter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * one meter & one snapshot for all windows. since meter is window-sliding, there's no need to recreate new ones.
 */
public class AsmMeter extends AsmMetric<Meter> {
    private final Meter meter = new Meter();

    public void mark() {
        meter.mark(1l);
    }

    @Override
    public void update(Number obj) {
        meter.mark(obj.longValue());
        for (AsmMetric metric : this.assocMetrics) {
            metric.update(obj);
        }
    }


    @Override
    public AsmMetric clone() {
        return new AsmMeter();
    }

    @Override
    public Map<Integer, Meter> getWindowMetricMap() {
        return null;
    }

    @Override
    protected void doFlush() {
        // nothing to do for meters.
    }

    @Override
    protected void updateSnapshot(int window) {
        AsmMeterSnapshot meterSnapshot = new AsmMeterSnapshot();
        meterSnapshot.setM1(meter.getOneMinuteRate()).setM5(meter.getFiveMinuteRate()).setM15(meter.getFifteenMinuteRate()).setMean(meter.getMeanRate())
                .setTs(System.currentTimeMillis()).setMetricId(metricId);
        snapshots.put(window, meterSnapshot);
    }

    @Override
    public Meter mkInstance() {
        return null;
    }
}
