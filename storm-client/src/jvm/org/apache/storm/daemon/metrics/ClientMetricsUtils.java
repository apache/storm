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

package org.apache.storm.daemon.metrics;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.storm.Config;
import org.apache.storm.utils.ObjectReader;

public class ClientMetricsUtils {

    private static final String RATE_UNIT = "rate.unit";
    private static final String DURATION_UNIT = "duration.unit";
    // Use the specified IETF BCP 47 language tag string for a Locale
    private static final String LOCALE = "locale";

    public static TimeUnit getMetricsRateUnit(Map<String, Object> reporterConf) {
        return getTimeUnitForConfig(reporterConf, RATE_UNIT);
    }

    public static TimeUnit getMetricsDurationUnit(Map<String, Object> reporterConf) {
        return getTimeUnitForConfig(reporterConf, DURATION_UNIT);
    }

    public static Locale getMetricsReporterLocale(Map<String, Object> reporterConf) {
        String languageTag = ObjectReader.getString(reporterConf.get(LOCALE), null);
        if (languageTag != null) {
            return Locale.forLanguageTag(languageTag);
        }
        return null;
    }

    public static TimeUnit getTimeUnitForConfig(Map<String, Object> reporterConf, String configName) {
        String timeUnitString = ObjectReader.getString(reporterConf.get(configName), null);
        if (timeUnitString != null) {
            return TimeUnit.valueOf(timeUnitString);
        }
        return null;
    }
}
