/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.metricstore;

import java.util.Map;
import org.apache.storm.metric.StormMetricsRegistry;

@SuppressWarnings("unused")
public class NoOpMetricStore implements MetricStore {

    @Override
    public void prepare(Map<String, Object> config, StormMetricsRegistry metricsRegistry) {}

    @Override
    public void insert(Metric metric) { }

    @Override
    public boolean populateValue(Metric metric) {
        return true;
    }

    @Override
    public void close() { }

    @Override
    public void scan(FilterOptions filter, ScanCallback scanCallback) { }
}




