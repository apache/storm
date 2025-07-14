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

package org.apache.storm.daemon.metrics.reporters;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.storm.DaemonConfig;
import org.apache.storm.daemon.metrics.MetricsUtils;
import org.apache.storm.utils.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsolePreparableReporter implements PreparableReporter {
    private static final Logger LOG = LoggerFactory.getLogger(ConsolePreparableReporter.class);
    ConsoleReporter reporter = null;
    Integer reportingIntervalSecs = null;

    @Override
    public void prepare(MetricRegistry metricsRegistry, Map<String, Object> daemonConf) {
        LOG.debug("Preparing...");
        ConsoleReporter.Builder builder = ConsoleReporter.forRegistry(metricsRegistry);

        builder.outputTo(System.out);
        Locale locale = MetricsUtils.getMetricsReporterLocale(daemonConf);
        if (locale != null) {
            builder.formattedFor(locale);
        }

        TimeUnit rateUnit = MetricsUtils.getMetricsRateUnit(daemonConf);
        if (rateUnit != null) {
            builder.convertRatesTo(rateUnit);
        }

        TimeUnit durationUnit = MetricsUtils.getMetricsDurationUnit(daemonConf);
        if (durationUnit != null) {
            builder.convertDurationsTo(durationUnit);
        }
        reporter = builder.build();
        reportingIntervalSecs = ObjectReader.getInt(daemonConf.get(DaemonConfig.STORM_DAEMON_METRICS_REPORTER_INTERVAL_SECS), 10);
    }

    @Override
    public void start() {
        if (reporter != null) {
            LOG.debug("Starting...");
            reporter.start(reportingIntervalSecs, TimeUnit.SECONDS);
        } else {
            throw new IllegalStateException("Attempt to start without preparing " + getClass().getSimpleName());
        }
    }

    @Override
    public void stop() {
        if (reporter != null) {
            LOG.debug("Stopping...");
            reporter.report();
            reporter.stop();
        } else {
            throw new IllegalStateException("Attempt to stop without preparing " + getClass().getSimpleName());
        }
    }
}
