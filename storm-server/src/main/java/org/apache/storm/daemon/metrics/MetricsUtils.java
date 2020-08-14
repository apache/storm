/*
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

package org.apache.storm.daemon.metrics;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.daemon.metrics.reporters.JmxPreparableReporter;
import org.apache.storm.daemon.metrics.reporters.PreparableReporter;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsUtils {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsUtils.class);

    public static List<PreparableReporter> getPreparableReporters(Map<String, Object> daemonConf) {
        List<String> clazzes = (List<String>) daemonConf.get(DaemonConfig.STORM_DAEMON_METRICS_REPORTER_PLUGINS);
        List<PreparableReporter> reporterList = new ArrayList<>();

        if (clazzes != null) {
            for (String clazz : clazzes) {
                reporterList.add(getPreparableReporter(clazz));
            }
        }
        if (reporterList.isEmpty()) {
            reporterList.add(new JmxPreparableReporter());
        }
        return reporterList;
    }

    private static PreparableReporter getPreparableReporter(String clazz) {
        PreparableReporter reporter = null;
        LOG.info("Using statistics reporter plugin:" + clazz);
        if (clazz != null) {
            reporter = (PreparableReporter) ReflectionUtils.newInstance(clazz);
        }
        return reporter;
    }

    public static File getCsvLogDir(Map<String, Object> daemonConf) {
        String csvMetricsLogDirectory =
            ObjectReader.getString(daemonConf.get(DaemonConfig.STORM_DAEMON_METRICS_REPORTER_CSV_LOG_DIR), null);
        if (csvMetricsLogDirectory == null) {
            csvMetricsLogDirectory = ConfigUtils.absoluteStormLocalDir(daemonConf);
            csvMetricsLogDirectory = csvMetricsLogDirectory + ConfigUtils.FILE_SEPARATOR + "csvmetrics";
        }
        File csvMetricsDir = new File(csvMetricsLogDirectory);
        validateCreateOutputDir(csvMetricsDir);
        return csvMetricsDir;
    }

    private static void validateCreateOutputDir(File dir) {
        if (!dir.exists()) {
            dir.mkdirs();
        }
        if (!dir.canWrite()) {
            throw new IllegalStateException(dir.getName() + " does not have write permissions.");
        }
        if (!dir.isDirectory()) {
            throw new IllegalStateException(dir.getName() + " is not a directory.");
        }
    }

    public static TimeUnit getMetricsRateUnit(Map<String, Object> daemonConf) {
        return getTimeUnitForConfig(daemonConf, Config.STORM_DAEMON_METRICS_REPORTER_PLUGIN_RATE_UNIT);
    }

    public static TimeUnit getMetricsDurationUnit(Map<String, Object> daemonConf) {
        return getTimeUnitForConfig(daemonConf, Config.STORM_DAEMON_METRICS_REPORTER_PLUGIN_DURATION_UNIT);
    }

    public static Locale getMetricsReporterLocale(Map<String, Object> daemonConf) {
        String languageTag = ObjectReader.getString(daemonConf.get(Config.STORM_DAEMON_METRICS_REPORTER_PLUGIN_LOCALE), null);
        if (languageTag != null) {
            return Locale.forLanguageTag(languageTag);
        }
        return null;
    }

    private static TimeUnit getTimeUnitForConfig(Map<String, Object> daemonConf, String configName) {
        String timeUnitString = ObjectReader.getString(daemonConf.get(configName), null);
        if (timeUnitString != null) {
            return TimeUnit.valueOf(timeUnitString);
        }
        return null;
    }
}
