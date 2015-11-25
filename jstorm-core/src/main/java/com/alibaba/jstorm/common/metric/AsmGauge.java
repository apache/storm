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

import com.alibaba.jstorm.common.metric.snapshot.AsmGaugeSnapshot;
import com.alibaba.jstorm.common.metric.snapshot.AsmSnapshot;
import com.codahale.metrics.Gauge;

import java.util.Map;

/**
 * gauges cannot be aggregated.
 */
public class AsmGauge extends AsmMetric<Gauge> {

    private Gauge gauge;

    public AsmGauge(Gauge<Double> gauge) {
        this.aggregate = false;
        this.gauge = gauge;
    }

    @Override
    public void update(Number obj) {
        // nothing to do for gauges.
    }

    @Override
    public AsmMetric clone() {
        AsmMetric metric = new AsmGauge(this.gauge);
        metric.setMetricName(this.getMetricName());
        return metric;
    }

    @Override
    public Map<Integer, Gauge> getWindowMetricMap() {
        return null;
    }

    @Override
    public Gauge mkInstance() {
        return null;
    }

    @Override
    protected void doFlush() {
        // nothing to do for gauges.
    }

    @Override
    protected void updateSnapshot(int window) {
        double v = (Double) gauge.getValue();
        AsmSnapshot snapshot =  new AsmGaugeSnapshot().setValue(v)
                .setTs(System.currentTimeMillis()).setMetricId(metricId);
        snapshots.put(window, snapshot);
    }
}
