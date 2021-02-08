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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class ClientMetricsUtilsTest {

    @Test
    public void getMetricsRateUnit() {
        Map<String, Object> reporterConf = new HashMap<>();

        assertNull(ClientMetricsUtils.getMetricsRateUnit(reporterConf));

        reporterConf.put("rate.unit", "SECONDS");
        assertEquals(TimeUnit.SECONDS, ClientMetricsUtils.getMetricsRateUnit(reporterConf));

        reporterConf.put("rate.unit", "MINUTES");
        assertEquals(TimeUnit.MINUTES, ClientMetricsUtils.getMetricsRateUnit(reporterConf));
    }

    @Test
    public void getMetricsDurationUnit() {
        Map<String, Object> reporterConf = new HashMap<>();

        assertNull(ClientMetricsUtils.getMetricsDurationUnit(reporterConf));

        reporterConf.put("duration.unit", "SECONDS");
        assertEquals(TimeUnit.SECONDS, ClientMetricsUtils.getMetricsDurationUnit(reporterConf));

        reporterConf.put("duration.unit", "MINUTES");
        assertEquals(TimeUnit.MINUTES, ClientMetricsUtils.getMetricsDurationUnit(reporterConf));
    }

    @Test
    public void getMetricsReporterLocale() {
        Map<String, Object> reporterConf = new HashMap<>();

        assertNull(ClientMetricsUtils.getMetricsReporterLocale(reporterConf));

        reporterConf.put("locale", "en-US");
        assertEquals(Locale.US, ClientMetricsUtils.getMetricsReporterLocale(reporterConf));
    }

    @Test
    public void getTimeUnitForConfig() {
        Map<String, Object> reporterConf = new HashMap<>();
        String dummyKey = "dummy.unit";

        assertNull(ClientMetricsUtils.getTimeUnitForConfig(reporterConf, dummyKey));

        reporterConf.put(dummyKey, "SECONDS");
        assertEquals(TimeUnit.SECONDS, ClientMetricsUtils.getTimeUnitForConfig(reporterConf, dummyKey));

        reporterConf.put(dummyKey, "MINUTES");
        assertEquals(TimeUnit.MINUTES, ClientMetricsUtils.getTimeUnitForConfig(reporterConf, dummyKey));
    }
}