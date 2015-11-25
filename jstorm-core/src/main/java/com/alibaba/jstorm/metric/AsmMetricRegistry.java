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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * metric registry. generally methods of this class should not be exposed, wrapper methods in @see JStormMonitorMetrics should be called.
 *
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
public class AsmMetricRegistry implements AsmMetricSet {
    private static final long serialVersionUID = 8184106900230111064L;
    private static final Logger LOG = LoggerFactory.getLogger(AsmMetricRegistry.class);

    protected final ConcurrentMap<String, AsmMetric> metrics = new ConcurrentHashMap<String, AsmMetric>();

    public int size() {
        return metrics.size();
    }

    /**
     * Given a {@link com.alibaba.jstorm.common.metric.old.window.Metric}, registers it under the given name.
     *
     * @param name   the metric node
     * @param metric the metric
     * @param <T>    the type of the metric
     * @return {@code metric}
     * @throws IllegalArgumentException if the name is already registered
     */
    @SuppressWarnings("unchecked")
    public <T extends AsmMetric> AsmMetric register(String name, T metric) throws IllegalArgumentException {
        metric.setMetricName(name);
        final AsmMetric existing = metrics.putIfAbsent(name, metric);
        if (existing == null) {
            LOG.info("Successfully register metric of {}", name);
            return metric;
        } else {
            LOG.warn("duplicate metric: {}", name);
            return existing;
        }
    }

    /**
     * Removes the metric with the given name.
     *
     * @param name the metric node
     * @return whether or not the metric was removed
     */
    public boolean remove(String name) {
        final AsmMetric metric = metrics.remove(name);
        if (metric != null) {
            LOG.info("Successfully unregister metric of {}", name);
            return true;
        }
        return false;
    }

    public AsmMetric getMetric(String name) {
        return metrics.get(name);
    }

    /**
     * Returns a set of the names of all the metrics in the registry.
     *
     * @return the names of all the metrics
     */
    public SortedSet<String> getMetricNames() {
        return Collections.unmodifiableSortedSet(new TreeSet<String>(metrics.keySet()));
    }

    /**
     * Returns a map of all the gauges in the registry and their names.
     *
     * @return all the gauges in the registry
     */
    public SortedMap<String, AsmGauge> getGauges() {
        return getGauges(AsmMetricFilter.ALL);
    }

    /**
     * Returns a map of all the gauges in the registry and their names which match the given filter.
     *
     * @param filter the metric filter to match
     * @return all the gauges in the registry
     */
    public SortedMap<String, AsmGauge> getGauges(AsmMetricFilter filter) {
        return getMetrics(AsmGauge.class, filter);
    }

    /**
     * Returns a map of all the counters in the registry and their names.
     *
     * @return all the counters in the registry
     */
    public SortedMap<String, AsmCounter> getCounters() {
        return getCounters(AsmMetricFilter.ALL);
    }

    /**
     * Returns a map of all the counters in the registry and their names which match the given filter.
     *
     * @param filter the metric filter to match
     * @return all the counters in the registry
     */
    public SortedMap<String, AsmCounter> getCounters(AsmMetricFilter filter) {
        return getMetrics(AsmCounter.class, filter);
    }

    /**
     * Returns a map of all the histograms in the registry and their names.
     *
     * @return all the histograms in the registry
     */
    public SortedMap<String, AsmHistogram> getHistograms() {
        return getHistograms(AsmMetricFilter.ALL);
    }

    /**
     * Returns a map of all the histograms in the registry and their names which match the given filter.
     *
     * @param filter the metric filter to match
     * @return all the histograms in the registry
     */
    public SortedMap<String, AsmHistogram> getHistograms(AsmMetricFilter filter) {
        return getMetrics(AsmHistogram.class, filter);
    }

    /**
     * Returns a map of all the meters in the registry and their names.
     *
     * @return all the meters in the registry
     */
    public SortedMap<String, AsmMeter> getMeters() {
        return getMeters(AsmMetricFilter.ALL);
    }

    /**
     * Returns a map of all the meters in the registry and their names which match the given filter.
     *
     * @param filter the metric filter to match
     * @return all the meters in the registry
     */
    public SortedMap<String, AsmMeter> getMeters(AsmMetricFilter filter) {
        return getMetrics(AsmMeter.class, filter);
    }

    /**
     * Returns a map of all the timers in the registry and their names.
     *
     * @return all the timers in the registry
     */
    public SortedMap<String, AsmTimer> getTimers() {
        return getTimers(AsmMetricFilter.ALL);
    }

    /**
     * Returns a map of all the timers in the registry and their names which match the given filter.
     *
     * @param filter the metric filter to match
     * @return all the timers in the registry
     */
    public SortedMap<String, AsmTimer> getTimers(AsmMetricFilter filter) {
        return getMetrics(AsmTimer.class, filter);
    }

    @SuppressWarnings("unchecked")
    private <T extends AsmMetric> SortedMap<String, T> getMetrics(Class<T> klass, AsmMetricFilter filter) {
        final TreeMap<String, T> timers = new TreeMap<String, T>();
        for (Map.Entry<String, AsmMetric> entry : metrics.entrySet()) {
            if (klass.isInstance(entry.getValue()) && filter.matches(entry.getKey(), entry.getValue())) {
                timers.put(entry.getKey(), (T) entry.getValue());
            }
        }
        return timers;
    }

    @Override
    public Map<String, AsmMetric> getMetrics() {
        return metrics;
    }

}
