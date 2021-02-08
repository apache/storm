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

package org.apache.storm.metrics2.reporters;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;
import java.util.Map;
import org.apache.storm.metrics2.MetricRegistryProvider;


public interface StormReporter extends Reporter {
    String REPORT_PERIOD = "report.period";
    String REPORT_PERIOD_UNITS = "report.period.units";
    String REPORT_DIMENSIONS_ENABLED = "report.dimensions.enabled";

    @Deprecated
    void prepare(MetricRegistry metricsRegistry, Map<String, Object> topoConf, Map<String, Object> reporterConf);

    default void prepare(MetricRegistryProvider metricRegistryProvider, Map<String, Object> topoConf,
                         Map<String, Object> reporterConf) {
        prepare(metricRegistryProvider.getRegistry(), topoConf, reporterConf);
    }

    void start();

    void stop();
}