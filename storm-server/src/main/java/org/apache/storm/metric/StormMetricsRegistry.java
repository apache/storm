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

package org.apache.storm.metric;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reservoir;

import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.Callable;

import org.apache.commons.lang.NullArgumentException;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.daemon.metrics.MetricsUtils;
import org.apache.storm.daemon.metrics.reporters.PreparableReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
public class StormMetricsRegistry {
    // TODO: Adding a secondary reporting_registry for filtering?
    public static final MetricRegistry DEFAULT_REGISTRY = new MetricRegistry();
    private static final Logger LOG = LoggerFactory.getLogger(StormMetricsRegistry.class);
    private static String source = null;

    public static boolean setSource(DaemonType source) {
        return setSource(source.toString());
    }

    /**
     * Specify source of metrics to filter out accidental registry.
     * This should be called as early as possible
     * @param source Components all the metrics should belong to
     * @return whether the source has been set successfully
     */
    public static boolean setSource(String source) throws NullArgumentException {
        if (StormMetricsRegistry.source == null) {
            if (source != null) {
                StormMetricsRegistry.source = source;
                //#getNames returns a snapshot of the registry,
                // hence safe to perform remove
                for (String name : DEFAULT_REGISTRY.getNames()) {
                    if (!name.startsWith(source)) {
                        DEFAULT_REGISTRY.remove(name);
                    }
                }
                return true;
            } else {
                throw new NullArgumentException("Registry source");
            }
        } else {
            //Ignore all subsequent changes if the source has been initialized
            return false;
        }
    }

    public static String name(DaemonType source, String metrics) {
        return name(source.toString(), metrics);
    }

    /**
     * Helper function for naming metrics.
     * @param source Daemon/component the metrics belong to
     * @param metrics name of the metrics
     * @return the name of the metrics following its parenting component
     * @throws NullArgumentException source shouldn't be null
     */
    public static String name(String source, String metrics) throws NullArgumentException {
        if (source != null) {
            return source + ":" + metrics;
        } else {
            throw new NullArgumentException("Registry source");
        }
    }

    public static Meter registerMeter(String name) {
        Meter meter = new Meter();
        return register(name, meter);
    }

    // TODO: should replace Callable to Gauge<Integer> when nimbus.clj is translated to java
    public static Gauge<Integer> registerGauge(final String name, final Callable fn) {
        Gauge<Integer> gauge = new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                try {
                    return (Integer) fn.call();
                } catch (Exception e) {
                    LOG.error("Error getting gauge value for {}", name, e);
                }
                return 0;
            }
        };
        return register(name, gauge);
    }

    public static void registerProvidedGauge(final String name, Gauge gauge) {
        register(name, gauge);
    }

    public static Histogram registerHistogram(String name, Reservoir reservoir) {
        Histogram histogram = new Histogram(reservoir);
        return register(name, histogram);
    }

    public static void startMetricsReporters(Map<String, Object> topoConf) {
        for (PreparableReporter reporter : MetricsUtils.getPreparableReporters(topoConf)) {
            startMetricsReporter(reporter, topoConf);
        }
    }

    private static void startMetricsReporter(PreparableReporter reporter, Map<String, Object> topoConf) {
        reporter.prepare(StormMetricsRegistry.DEFAULT_REGISTRY, topoConf);
        reporter.start();
        LOG.info("Started statistics report plugin...");
    }

    private static <T extends Metric> T register(String name, T metric) {
        if (source != null && !name.startsWith(source)) {
            LOG.warn("Metric {} accidentally registered and filtered", name);
            //Mute metrics by not registering it. This is a band-aid fix
            // TODO: Find a better solution
            return metric;
        }
        T ret;
        try {
            ret = DEFAULT_REGISTRY.register(name, metric);
        } catch (IllegalArgumentException e) {
            // swallow IllegalArgumentException when the metric exists already
            ret = (T) DEFAULT_REGISTRY.getMetrics().get(name);
            if (ret == null) {
                throw e;
            } else {
                LOG.warn("Metric {} has already been registered", name);
            }
        }
        return ret;
    }
}
