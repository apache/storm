/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.storm.metricstore;

import com.codahale.metrics.Meter;

import java.util.Map;

public class MetricStoreConfig {
    public static final String CONF_KEY_METRIC_STORE_CLASS = "storm.metricstore.class";

    /**
     * Configures metrics store to use the class specified in the conf.
     * @param conf Storm config map
     * @param meter  meter to use for tracking failures
     * @return MetricStore prepared store
     * @throws MetricException  on misconfiguration
     */
    public static MetricStore configure(Map conf, Meter meter) throws MetricException {

        validateConfig(conf);

        String storeClass = conf.get(CONF_KEY_METRIC_STORE_CLASS).toString();
        try {
            MetricStore store = (MetricStore) (Class.forName(storeClass)).newInstance();
            store.prepare(conf, meter);
            return store;
        } catch (Exception e) {
            throw new MetricException("Failed to create metric store", e);
        }
    }

    /**
     * Validates top level configurations of the metrics conf map.
     *
     * @param conf Storm config map
     * @throws MetricException  if config is missing parameters
     */
    private static void validateConfig(Map conf) throws MetricException {
        if (!(conf.containsKey(CONF_KEY_METRIC_STORE_CLASS))) {
            throw new MetricException("Not a valid metrics configuration - Missing " + CONF_KEY_METRIC_STORE_CLASS);
        }
    }

}

