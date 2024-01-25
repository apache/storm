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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;

/**
 * A Counter metric that also implements a Gauge to report the average rate of events per second over 1 minute.  This class
 * was added as a compromise to using a Meter, which has a much larger performance impact.
 */
public class RateCounter implements Gauge<Double> {
    private Counter counter;
    private double currentRate = 0;
    private int time = 0;
    private final long[] values;
    private final int timeSpanInSeconds;

    RateCounter(StormMetricRegistry metricRegistry, String metricName, String topologyId,
                String componentId, int taskId, int workerPort, String streamId) {
        if (streamId != null) {
            counter = metricRegistry.counter(metricName, topologyId, componentId,
                    taskId, workerPort, streamId);
            metricRegistry.gauge(metricName + ".m1_rate", this, topologyId, componentId, streamId,
                    taskId, workerPort);
        } else {
            counter = metricRegistry.counter(metricName, componentId, taskId);
            metricRegistry.gauge(metricName + ".m1_rate", this, componentId, taskId);
        }

        this.timeSpanInSeconds = Math.max(60 - (60 % metricRegistry.getRateCounterUpdateIntervalSeconds()),
                metricRegistry.getRateCounterUpdateIntervalSeconds());
        this.values = new long[this.timeSpanInSeconds / metricRegistry.getRateCounterUpdateIntervalSeconds() + 1];

    }

    RateCounter(StormMetricRegistry metricRegistry, String metricName, String topologyId,
                String componentId, int taskId, int workerPort) {
        this(metricRegistry, metricName, topologyId, componentId, taskId, workerPort, null);
    }

    /**
     * Reports the the average rate of events per second over 1 minute for the metric.
     * @return the rate
     */
    @Override
    public Double getValue() {
        return currentRate;
    }

    public void inc(long n) {
        counter.inc(n);
    }

    /**
     * Updates the rate in a background thread by a StormMetricRegistry at a fixed frequency.
     */
    void update() {
        time = (time + 1) % values.length;
        values[time] = counter.getCount();
        currentRate =  ((double) (values[time] - values[(time + 1) % values.length]) / timeSpanInSeconds);
    }

    Counter getCounter() {
        return counter;
    }
}
