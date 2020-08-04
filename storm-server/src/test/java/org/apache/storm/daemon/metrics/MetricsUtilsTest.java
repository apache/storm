/*
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

package org.apache.storm.daemon.metrics;

import static org.junit.Assert.*;

import java.io.File;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.daemon.metrics.reporters.ConsolePreparableReporter;
import org.apache.storm.daemon.metrics.reporters.CsvPreparableReporter;
import org.apache.storm.daemon.metrics.reporters.JmxPreparableReporter;
import org.apache.storm.daemon.metrics.reporters.PreparableReporter;
import org.junit.Test;

public class MetricsUtilsTest {

    @Test
    public void getPreparableReporters() {
        Map<String, Object> daemonConf = new HashMap<>();

        List<PreparableReporter> reporters = MetricsUtils.getPreparableReporters(daemonConf);
        assertEquals(1, reporters.size());
        assertTrue(reporters.get(0) instanceof JmxPreparableReporter);

        List<String> reporterPlugins = Arrays.asList("org.apache.storm.daemon.metrics.reporters.ConsolePreparableReporter",
            "org.apache.storm.daemon.metrics.reporters.CsvPreparableReporter");

        daemonConf.put(DaemonConfig.STORM_DAEMON_METRICS_REPORTER_PLUGINS, reporterPlugins);
        reporters = MetricsUtils.getPreparableReporters(daemonConf);
        assertEquals(2, reporters.size());
        assertTrue(reporters.get(0) instanceof ConsolePreparableReporter);
        assertTrue(reporters.get(1) instanceof CsvPreparableReporter);
    }

    @Test
    public void getCsvLogDir() {
        Map<String, Object> daemonConf = new HashMap<>();

        String currentPath = new File("").getAbsolutePath();
        daemonConf.put(Config.STORM_LOCAL_DIR, currentPath);
        File dir = new File(currentPath, "csvmetrics");
        assertEquals(dir, MetricsUtils.getCsvLogDir(daemonConf));

        daemonConf.put(DaemonConfig.STORM_DAEMON_METRICS_REPORTER_CSV_LOG_DIR, "./");
        assertEquals(new File("./"), MetricsUtils.getCsvLogDir(daemonConf));
    }

    @Test
    public void getMetricsRateUnit() {
        Map<String, Object> daemonConf = new HashMap<>();

        assertNull(MetricsUtils.getMetricsRateUnit(daemonConf));

        daemonConf.put(Config.STORM_DAEMON_METRICS_REPORTER_PLUGIN_RATE_UNIT, "SECONDS");
        assertEquals(TimeUnit.SECONDS, MetricsUtils.getMetricsRateUnit(daemonConf));

        daemonConf.put(Config.STORM_DAEMON_METRICS_REPORTER_PLUGIN_RATE_UNIT, "MINUTES");
        assertEquals(TimeUnit.MINUTES, MetricsUtils.getMetricsRateUnit(daemonConf));
    }

    @Test
    public void getMetricsDurationUnit() {
        Map<String, Object> daemonConf = new HashMap<>();

        assertNull(MetricsUtils.getMetricsDurationUnit(daemonConf));

        daemonConf.put(Config.STORM_DAEMON_METRICS_REPORTER_PLUGIN_DURATION_UNIT, "SECONDS");
        assertEquals(TimeUnit.SECONDS, MetricsUtils.getMetricsDurationUnit(daemonConf));

        daemonConf.put(Config.STORM_DAEMON_METRICS_REPORTER_PLUGIN_DURATION_UNIT, "MINUTES");
        assertEquals(TimeUnit.MINUTES, MetricsUtils.getMetricsDurationUnit(daemonConf));
    }

    @Test
    public void getMetricsReporterLocale() {
        Map<String, Object> daemonConf = new HashMap<>();

        assertNull(MetricsUtils.getMetricsReporterLocale(daemonConf));

        daemonConf.put(Config.STORM_DAEMON_METRICS_REPORTER_PLUGIN_LOCALE, "en-US");
        assertEquals(Locale.US, MetricsUtils.getMetricsReporterLocale(daemonConf));
    }
}