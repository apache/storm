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
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reservoir;
import java.util.List;
import java.util.Map;
import org.apache.storm.daemon.metrics.MetricsUtils;
import org.apache.storm.daemon.metrics.reporters.PreparableReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
public class StormMetricsRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(StormMetricsRegistry.class);
    private final MetricRegistry registry = new MetricRegistry();
    private List<PreparableReporter> reporters;
    private boolean reportersStarted = false;

    public Meter registerMeter(String name) {
        return registry.meter(name);
    }

    public <V> Gauge<V> registerGauge(final String name, Gauge<V> gauge) {
        return registry.gauge(name, () -> gauge);
    }

    public Histogram registerHistogram(String name, Reservoir reservoir) {
        Histogram histogram = new Histogram(reservoir);
        return registry.histogram(name, () -> histogram);
    }

    public void startMetricsReporters(Map<String, Object> daemonConf) {
        reporters = MetricsUtils.getPreparableReporters(daemonConf);
        for (PreparableReporter reporter : reporters) {
            reporter.prepare(registry, daemonConf);
            reporter.start();
            LOG.info("Started statistics report plugin...");
        }
        reportersStarted = true;
    }
    
    public void stopMetricsReporters(Map<String, Object> daemonConf) {
        if (reportersStarted) {
            for (PreparableReporter reporter : reporters) {
                reporter.stop();
            }
            reportersStarted = false;
        }
    }
}
