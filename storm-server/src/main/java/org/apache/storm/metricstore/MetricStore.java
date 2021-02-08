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

public interface MetricStore extends AutoCloseable {

    /**
     * Create metric store instance using the configurations provided via the config map.
     *
     * @param config Storm config map
     * @param metricsRegistry The Nimbus daemon metrics registry
     * @throws MetricException on preparation error
     */
    void prepare(Map<String, Object> config, StormMetricsRegistry metricsRegistry) throws MetricException;

    /**
     * Stores a metric in the store.
     *
     * @param metric  Metric to store
     * @throws MetricException on error
     */
    void insert(Metric metric) throws MetricException;

    /**
     * Fill out the numeric values for a metric.
     *
     * @param metric  Metric to populate
     * @return true if the metric was populated, false otherwise
     * @throws MetricException on error
     */
    boolean populateValue(Metric metric) throws MetricException;

    /**
     * Close the metric store.
     */
    @Override
    void close();

    /**
     *  Scans all metrics in the store and returns the ones matching the specified filtering options.
     *
     * @param filter   options to filter by
     * @param scanCallback  callback for each Metric found
     * @throws MetricException  on error
     */
    void scan(FilterOptions filter, ScanCallback scanCallback) throws MetricException;

    /**
     *  Interface used to callback metrics results from a scan.
     */
    interface ScanCallback {
        void cb(Metric metric);
    }
}




