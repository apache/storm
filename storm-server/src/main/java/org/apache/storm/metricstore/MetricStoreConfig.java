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
import org.apache.storm.DaemonConfig;
import org.apache.storm.metric.StormMetricsRegistry;


public class MetricStoreConfig {

    /**
     * Configures metrics store (running on Nimbus) to use the class specified in the conf.
     * @param conf Storm config map
     * @param metricsRegistry The Nimbus daemon metrics registry
     * @return MetricStore prepared store
     * @throws MetricException  on misconfiguration
     */
    public static MetricStore configure(Map<String, Object> conf, StormMetricsRegistry metricsRegistry) throws MetricException {

        try {
            String storeClass = (String) conf.get(DaemonConfig.STORM_METRIC_STORE_CLASS);
            MetricStore store = (MetricStore) (Class.forName(storeClass)).newInstance();
            store.prepare(conf, metricsRegistry);
            return store;
        } catch (Exception e) {
            throw new MetricException("Failed to create metric store", e);
        }
    }

    /**
     * Configures metric processor (running on supervisor) to use the class specified in the conf.
     * @param conf  the supervisor config
     * @return WorkerMetricsProcessor prepared processor
     * @throws MetricException  on misconfiguration
     */
    public static WorkerMetricsProcessor configureMetricProcessor(Map conf) throws MetricException {

        try {
            String processorClass = (String) conf.get(DaemonConfig.STORM_METRIC_PROCESSOR_CLASS);
            WorkerMetricsProcessor processor = (WorkerMetricsProcessor) (Class.forName(processorClass)).newInstance();
            processor.prepare(conf);
            return processor;
        } catch (Exception e) {
            throw new MetricException("Failed to create metric processor", e);
        }
    }
}

