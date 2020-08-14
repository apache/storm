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

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import java.io.File;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.storm.daemon.metrics.ClientMetricsUtils;
import org.apache.storm.metrics2.filters.StormMetricsFilter;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvStormReporter extends ScheduledStormReporter {
    public static final String CSV_LOG_DIR = "csv.log.dir";
    private static final Logger LOG = LoggerFactory.getLogger(CsvStormReporter.class);

    private static File getCsvLogDir(Map stormConf, Map reporterConf) {
        String csvMetricsLogDirectory = ObjectReader.getString(reporterConf.get(CSV_LOG_DIR), null);
        if (csvMetricsLogDirectory == null) {
            csvMetricsLogDirectory = ConfigUtils.absoluteStormLocalDir(stormConf);
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

    @Override
    public void prepare(MetricRegistry metricsRegistry, Map<String, Object> topoConf, Map<String, Object> reporterConf) {
        LOG.debug("Preparing...");
        CsvReporter.Builder builder = CsvReporter.forRegistry(metricsRegistry);

        Locale locale = ClientMetricsUtils.getMetricsReporterLocale(reporterConf);
        if (locale != null) {
            builder.formatFor(locale);
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

        File csvMetricsDir = getCsvLogDir(topoConf, reporterConf);
        reporter = builder.build(csvMetricsDir);
    }
}