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
package com.alibaba.jstorm.metric;

import com.alibaba.jstorm.common.metric.*;
import com.codahale.metrics.Gauge;

/**
 * simplified metrics, only for worker metrics. all metrics are logged locally without reporting to TM or nimbus.
 *
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
public class SimpleJStormMetric extends JStormMetrics {
    private static final long serialVersionUID = 7468005641982249536L;

    protected static final AsmMetricRegistry metrics = JStormMetrics.getWorkerMetrics();

    public static void updateNimbusHistogram(String name, Number obj) {
        updateHistogram(NIMBUS_METRIC_KEY, name, obj);
    }

    public static void updateSupervisorHistogram(String name, Number obj) {
        updateHistogram(SUPERVISOR_METRIC_KEY, name, obj);
    }

    public static void updateNimbusMeter(String name, Number obj) {
        updateMeter(NIMBUS_METRIC_KEY, name, obj);
    }

    public static void updateSupervisorMeter(String name, Number obj) {
        updateMeter(SUPERVISOR_METRIC_KEY, name, obj);
    }

    public static void updateNimbusCounter(String name, Number obj) {
        updateCounter(NIMBUS_METRIC_KEY, name, obj);
    }

    public static void updateSupervisorCounter(String name, Number obj) {
        updateCounter(SUPERVISOR_METRIC_KEY, name, obj);
    }

    public static void updateHistogram(String key, String name, Number obj) {
        String formalName = MetricUtils.workerMetricName(key, host, 0, name, MetricType.HISTOGRAM);
        AsmHistogram histogram = (AsmHistogram) metrics.getMetric(formalName);
        if (histogram == null) {
            histogram = registerHistogram(name);
        }

        histogram.update(obj);
    }

    public static void updateMeter(String key, String name, Number obj) {
        String formalName = MetricUtils.workerMetricName(key, host, 0, name, MetricType.METER);
        AsmMeter meter = (AsmMeter) metrics.getMetric(formalName);
        if (meter == null) {
            meter = registerMeter(name);
        }

        meter.update(obj);
    }

    public static void updateCounter(String key, String name, Number obj) {
        String formalName = MetricUtils.workerMetricName(key, host, 0, name, MetricType.COUNTER);
        AsmCounter counter = (AsmCounter) metrics.getMetric(formalName);
        if (counter == null) {
            counter = registerCounter(name);
        }

        counter.update(obj);
    }

    private static AsmGauge registerGauge(Gauge<Double> gauge, String name) {
        AsmGauge gauge1 = new AsmGauge(gauge);
        gauge1.setOp(AsmMetric.MetricOp.LOG);

        return registerWorkerGauge(topologyId, name, gauge1);
    }

    private static AsmHistogram registerHistogram(String name) {
        AsmHistogram histogram = new AsmHistogram();
        histogram.setOp(AsmMetric.MetricOp.LOG);

        return registerWorkerHistogram(NIMBUS_METRIC_KEY, name, histogram);
    }

    public static AsmMeter registerMeter(String name) {
        AsmMeter meter = new AsmMeter();
        meter.setOp(AsmMetric.MetricOp.LOG);

        return registerWorkerMeter(NIMBUS_METRIC_KEY, name, meter);
    }

    public static AsmCounter registerCounter(String name) {
        AsmCounter counter = new AsmCounter();
        counter.setOp(AsmMetric.MetricOp.LOG);

        return registerWorkerCounter(NIMBUS_METRIC_KEY, name, counter);
    }
}
