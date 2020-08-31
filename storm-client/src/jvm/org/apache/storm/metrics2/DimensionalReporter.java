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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Class that allows using a ScheduledReporter to report V2 task metrics with support for dimensions.
 * <p></p>
 * This reporter will be started and scheduled with the MetricRegistry.  Once it is called on to report,
 * it will query the StormMetricRegistry for various sets of task metrics with unique dimensions.  The
 * underlying ScheduledReporter will perform the actual reporting with the help of a DimensionHandler to
 * deal with the dimensions.
 */
public class DimensionalReporter extends ScheduledReporter {
    private ScheduledReporter underlyingReporter;
    private MetricRegistryProvider metricRegistryProvider;
    private MetricFilter filter;
    private DimensionHandler dimensionHandler;

    /**
     * Constructor.
     *
     * @param metricRegistryProvider MetricRegistryProvider tracking task-specific metrics.
     * @param unstartedReporter      ScheduledReporter to perform the actual reporting.  It should NOT be started.
     * @param dimensionHandler       class to handle setting dimensions before reporting a set of metrics.
     * @param name                   the reporter's name.
     * @param filter                 the filter for which metrics to report.
     * @param rateUnit               rate unit for the reporter.
     * @param durationUnit           duration unit for the reporter.
     * @param executor               the executor to use while scheduling reporting of metrics.
     * @param shutdownExecutorOnStop if true, then executor will be stopped in same time with this reporter.
     */
    public DimensionalReporter(MetricRegistryProvider metricRegistryProvider,
                               ScheduledReporter unstartedReporter,
                               DimensionHandler dimensionHandler,
                               String name,
                               MetricFilter filter,
                               TimeUnit rateUnit,
                               TimeUnit durationUnit,
                               ScheduledExecutorService executor,
                               boolean shutdownExecutorOnStop) {
        super(metricRegistryProvider.getRegistry(), name, filter, rateUnit, durationUnit, executor, shutdownExecutorOnStop);
        underlyingReporter = unstartedReporter;
        this.metricRegistryProvider = metricRegistryProvider;
        this.filter = filter;
        this.dimensionHandler = dimensionHandler;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        report();
    }

    @Override
    public void report() {
        for (Map.Entry<TaskMetricDimensions, TaskMetricRepo> entry : metricRegistryProvider.getTaskMetrics().entrySet()) {
            TaskMetricRepo repo = entry.getValue();
            if (dimensionHandler != null) {
                TaskMetricDimensions dimensions = entry.getKey();
                dimensionHandler.setDimensions(dimensions.getDimensions());
            }
            repo.report(underlyingReporter, filter);
        }
    }

    public interface DimensionHandler {
        /**
         * Sets dimensions to be used for reporting on the next batch of metrics.
         *
         * @param dimensions    dimensions valid for use in the next scheduled report.
         */
        void setDimensions(Map<String, String> dimensions);
    }
}