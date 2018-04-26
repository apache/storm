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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ganglia.GangliaReporter;
import info.ganglia.gmetric4j.gmetric.GMetric;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.storm.daemon.metrics.ClientMetricsUtils;
import org.apache.storm.metrics2.filters.StormMetricsFilter;
import org.apache.storm.utils.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GangliaStormReporter extends ScheduledStormReporter {
    public static final String GANGLIA_HOST = "ganglia.host";
    public static final String GANGLIA_PORT = "ganglia.port";
    public static final String GANGLIA_PREFIXED_WITH = "ganglia.prefixed.with";
    public static final String GANGLIA_DMAX = "ganglia.dmax";
    public static final String GANGLIA_TMAX = "ganglia.tmax";
    public static final String GANGLIA_UDP_ADDRESSING_MODE = "ganglia.udp.addressing.mode";
    public static final String GANGLIA_RATE_UNIT = "ganglia.rate.unit";
    public static final String GANGLIA_DURATION_UNIT = "ganglia.duration.unit";
    public static final String GANGLIA_TTL = "ganglia.ttl";
    public static final String GANGLIA_UDP_GROUP = "ganglia.udp.group";
    private static final Logger LOG = LoggerFactory.getLogger(GangliaStormReporter.class);

    public static String getMetricsTargetUdpGroup(Map reporterConf) {
        return ObjectReader.getString(reporterConf.get(GANGLIA_UDP_GROUP), null);
    }

    public static String getMetricsTargetUdpAddressingMode(Map reporterConf) {
        return ObjectReader.getString(reporterConf.get(GANGLIA_UDP_ADDRESSING_MODE), null);
    }

    public static Integer getMetricsTargetTtl(Map reporterConf) {
        return ObjectReader.getInt(reporterConf.get(GANGLIA_TTL), null);
    }

    public static Integer getGangliaDMax(Map reporterConf) {
        return ObjectReader.getInt(reporterConf.get(GANGLIA_DMAX), null);
    }

    public static Integer getGangliaTMax(Map reporterConf) {
        return ObjectReader.getInt(reporterConf.get(GANGLIA_TMAX), null);
    }

    private static Integer getMetricsTargetPort(Map reporterConf) {
        return ObjectReader.getInt(reporterConf.get(GANGLIA_PORT), null);
    }

    private static String getMetricsPrefixedWith(Map reporterConf) {
        return ObjectReader.getString(reporterConf.get(GANGLIA_PREFIXED_WITH), null);
    }

    @Override
    public void prepare(MetricRegistry metricsRegistry, Map stormConf, Map reporterConf) {
        LOG.debug("Preparing...");
        GangliaReporter.Builder builder = GangliaReporter.forRegistry(metricsRegistry);

        TimeUnit durationUnit = ClientMetricsUtils.getMetricsDurationUnit(reporterConf);
        if (durationUnit != null) {
            builder.convertDurationsTo(durationUnit);
        }

        TimeUnit rateUnit = ClientMetricsUtils.getMetricsRateUnit(reporterConf);
        if (rateUnit != null) {
            builder.convertRatesTo(rateUnit);
        }

        StormMetricsFilter filter = getMetricsFilter(reporterConf);
        if (filter != null) {
            builder.filter(filter);
        }
        String prefix = getMetricsPrefixedWith(reporterConf);
        if (prefix != null) {
            builder.prefixedWith(prefix);
        }

        Integer dmax = getGangliaDMax(reporterConf);
        if (prefix != null) {
            builder.withDMax(dmax);
        }

        Integer tmax = getGangliaTMax(reporterConf);
        if (prefix != null) {
            builder.withTMax(tmax);
        }

        //defaults to 10
        reportingPeriod = getReportPeriod(reporterConf);

        //defaults to seconds
        reportingPeriodUnit = getReportPeriodUnit(reporterConf);

        String group = getMetricsTargetUdpGroup(reporterConf);
        Integer port = getMetricsTargetPort(reporterConf);
        String udpAddressingMode = getMetricsTargetUdpAddressingMode(reporterConf);
        Integer ttl = getMetricsTargetTtl(reporterConf);

        GMetric.UDPAddressingMode mode = udpAddressingMode.equalsIgnoreCase("multicast")
            ? GMetric.UDPAddressingMode.MULTICAST : GMetric.UDPAddressingMode.UNICAST;

        try {
            GMetric sender = new GMetric(group, port, mode, ttl);
            reporter = builder.build(sender);
        } catch (IOException ioe) {
            LOG.error("Exception in GangliaReporter config", ioe);
        }
    }

}