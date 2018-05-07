/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.metrics2.reporters;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import org.apache.storm.Config;
import org.apache.storm.daemon.metrics.MetricsUtils;
import org.apache.storm.metrics2.filters.StormMetricsFilter;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class JmxStormReporter implements StormReporter {
    private final static Logger LOG = LoggerFactory.getLogger(JmxStormReporter.class);
    public static final String JMX_DOMAIN = "jmx.domain";
    JmxReporter reporter = null;

    @Override
    public void prepare(MetricRegistry metricsRegistry, Map<String, Object> stormConf, Map<String, Object> reporterConf) {
        LOG.info("Preparing...");
        JmxReporter.Builder builder = JmxReporter.forRegistry(metricsRegistry);

        TimeUnit durationUnit = MetricsUtils.getMetricsDurationUnit(reporterConf);
        if (durationUnit != null) {
            builder.convertDurationsTo(durationUnit);
        }

        TimeUnit rateUnit = MetricsUtils.getMetricsRateUnit(reporterConf);
        if (rateUnit != null) {
            builder.convertRatesTo(rateUnit);
        }

        String domain = getMetricsJMXDomain(reporterConf);
        if (domain != null) {
            builder.inDomain(domain);
        }

        StormMetricsFilter filter = ScheduledStormReporter.getMetricsFilter(reporterConf);
        if(filter != null){
            builder.filter(filter);
        }
        // other builder functions not exposed:
        //  * createsObjectNamesWith(ObjectNameFactory onFactory) 
        //  * registerWith (MBeanServer)
        //  * specificDurationUnits (Map<String,TimeUnit> specificDurationUnits)
        //  * specificRateUnits(Map<String,TimeUnit> specificRateUnits)

        reporter = builder.build();
    }

    public static String getMetricsJMXDomain(Map reporterConf) {
        return Utils.getString(reporterConf.get(Config.STORM_DAEMON_METRICS_REPORTER_PLUGIN_DOMAIN), null);
    }

    @Override
    public void start() {
        if (reporter != null) {
            LOG.debug("Starting...");
            reporter.start();
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
}