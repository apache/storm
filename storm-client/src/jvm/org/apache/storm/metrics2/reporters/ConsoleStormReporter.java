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

package org.apache.storm.metrics2.reporters;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.storm.Config;
import org.apache.storm.daemon.metrics.ClientMetricsUtils;
import org.apache.storm.metrics2.DimensionalReporter;
import org.apache.storm.metrics2.MetricRegistryProvider;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.apache.storm.metrics2.filters.StormMetricsFilter;
import org.apache.storm.utils.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsoleStormReporter extends ScheduledStormReporter implements DimensionalReporter.DimensionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ConsoleStormReporter.class);

    @Override
    public void prepare(MetricRegistry registry, Map<String, Object> topoConf, Map<String, Object> reporterConf) {
        init(registry, null, reporterConf);
    }

    @Override
    public void prepare(MetricRegistryProvider metricRegistryProvider, Map<String, Object> topoConf,
                        Map<String, Object> reporterConf) {
        init(metricRegistryProvider.getRegistry(), metricRegistryProvider, reporterConf);
    }

    private void init(MetricRegistry registry, MetricRegistryProvider metricRegistryProvider, Map<String, Object> reporterConf) {
        LOG.debug("Preparing ConsoleReporter");
        ConsoleReporter.Builder builder = ConsoleReporter.forRegistry(registry);

        builder.outputTo(System.out);
        Locale locale = ClientMetricsUtils.getMetricsReporterLocale(reporterConf);
        if (locale != null) {
            builder.formattedFor(locale);
        }

        TimeUnit rateUnit = ClientMetricsUtils.getMetricsRateUnit(reporterConf);
        if (rateUnit != null) {
            builder.convertRatesTo(rateUnit);
        }

        TimeUnit durationUnit = ClientMetricsUtils.getMetricsDurationUnit(reporterConf);
        if (durationUnit != null) {
            builder.convertDurationsTo(durationUnit);
        }

        StormMetricsFilter filter = getMetricsFilter(reporterConf);
        if (filter != null) {
            builder.filter(filter);
        }

        //defaults to 10
        reportingPeriod = getReportPeriod(reporterConf);

        //defaults to seconds
        reportingPeriodUnit = getReportPeriodUnit(reporterConf);

        ScheduledReporter consoleReporter = builder.build();

        boolean reportDimensions = isReportDimensionsEnabled(reporterConf);
        if (reportDimensions) {
            if (metricRegistryProvider == null) {
                throw new RuntimeException("MetricRegistryProvider is required to enable reporting dimensions");
            }
            if (rateUnit == null) {
                rateUnit = TimeUnit.SECONDS;
            }
            if (durationUnit == null) {
                durationUnit = TimeUnit.MILLISECONDS;
            }
            DimensionalReporter dimensionalReporter = new DimensionalReporter(metricRegistryProvider, consoleReporter, this,
                    "ConsoleDimensionalReporter",
                    filter, rateUnit, durationUnit, null, true);
            reporter = dimensionalReporter;
        } else {
            reporter = consoleReporter;
        }
    }

    // We're unable to extend ConsoleReporter to handle dimensions, so we'll report dimensions here
    @Override
    public void setDimensions(Map<String, String> dimensions) {
        System.out.println("Using dimensions: ");
        for (Map.Entry<String, String> entry : dimensions.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }
}