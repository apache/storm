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
package org.apache.storm.metrics2.reporters;

import com.codahale.metrics.ScheduledReporter;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class SheduledStormReporter<T extends ScheduledReporter> implements StormReporter{
    private static final Logger LOG = LoggerFactory.getLogger(SheduledStormReporter.class);
    protected ScheduledReporter reporter;
    long reportingPeriod;
    TimeUnit reportingPeriodUnit;

    @Override
    public void start() {
        if (reporter != null) {
            LOG.debug("Starting...");
            reporter.start(reportingPeriod, reportingPeriodUnit);
        } else {
            throw new IllegalStateException("Attempt to start without preparing " + getClass().getSimpleName());
        }
    }

    @Override
    public void stop() {
        if (reporter != null) {
            LOG.debug("Stopping...");
            reporter.stop();
        } else {
            throw new IllegalStateException("Attempt to stop without preparing " + getClass().getSimpleName());
        }
    }


    static TimeUnit getReportPeriodUnit(Map<String, Object> reporterConf) {
        TimeUnit unit = getTimeUnitForConfig(reporterConf, REPORT_PERIOD_UNITS);
        return unit == null ? TimeUnit.SECONDS : unit;
    }

    private static TimeUnit getTimeUnitForConfig(Map reporterConf, String configName) {
        String rateUnitString = Utils.getString(reporterConf.get(configName), null);
        if (rateUnitString != null) {
            return TimeUnit.valueOf(rateUnitString);
        }
        return null;
    }

    static long getReportPeriod(Map reporterConf) {
        return Utils.getInt(reporterConf.get(REPORT_PERIOD), 10).longValue();
    }
}
