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

package org.apache.storm.metrics2;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metric repository to allow reporting of task-specific metrics.
 */
public class TaskMetricRepo {
    private ConcurrentHashMap<String, Gauge> gauges = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Counter> counters = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Histogram> histograms = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Meter> meters = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Timer> timers = new ConcurrentHashMap<>();

    public void addCounter(String name, Counter counter) {
        counters.put(name, counter);
    }

    public void addGauge(String name, Gauge gauge) {
        gauges.put(name, gauge);
    }

    public void addMeter(String name, Meter meter) {
        meters.put(name, meter);
    }

    public void addHistogram(String name, Histogram histogram) {
        histograms.put(name, histogram);
    }

    public void addTimer(String name, Timer timer) {
        timers.put(name, timer);
    }

    public void report(ScheduledReporter reporter, MetricFilter filter) {
        if (filter == null) {
            filter = MetricFilter.ALL;
        }

        SortedMap<String, Gauge> filteredGauges = new TreeMap<>();
        SortedMap<String, Counter> filteredCounters = new TreeMap<>();
        SortedMap<String, Histogram> filteredHistograms = new TreeMap<>();
        SortedMap<String, Meter> filteredMeters = new TreeMap<>();
        SortedMap<String, Timer> filteredTimers = new TreeMap<>();

        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            if (filter.matches(entry.getKey(), entry.getValue())) {
                filteredGauges.put(entry.getKey(), entry.getValue());
            }
        }
        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            if (filter.matches(entry.getKey(), entry.getValue())) {
                filteredCounters.put(entry.getKey(), entry.getValue());
            }
        }
        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            if (filter.matches(entry.getKey(), entry.getValue())) {
                filteredHistograms.put(entry.getKey(), entry.getValue());
            }
        }
        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            if (filter.matches(entry.getKey(), entry.getValue())) {
                filteredMeters.put(entry.getKey(), entry.getValue());
            }
        }
        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            if (filter.matches(entry.getKey(), entry.getValue())) {
                filteredTimers.put(entry.getKey(), entry.getValue());
            }
        }
        reporter.report(filteredGauges, filteredCounters, filteredHistograms, filteredMeters, filteredTimers);
    }

    void degister(MetricFilter metricFilter) {
        gauges.entrySet().removeIf(entry -> metricFilter.matches(entry.getKey(), entry.getValue()));
        counters.entrySet().removeIf(entry -> metricFilter.matches(entry.getKey(), entry.getValue()));
        histograms.entrySet().removeIf(entry -> metricFilter.matches(entry.getKey(), entry.getValue()));
        meters.entrySet().removeIf(entry -> metricFilter.matches(entry.getKey(), entry.getValue()));
        timers.entrySet().removeIf(entry -> metricFilter.matches(entry.getKey(), entry.getValue()));
    }
}