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

import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.IStatefulObject;
import org.apache.storm.metric.api.StateMetric;
import org.apache.storm.task.TopologyContext;

public class BuiltinMetricsUtil {
    public static void registerIconnectionServerMetric(Object server, Map<String, Object> topoConf, TopologyContext context) {
        if (server instanceof IStatefulObject) {
            registerMetric("__recv-iconnection", new StateMetric((IStatefulObject) server), topoConf, context);
        }
    }

    public static void registerMetric(String name, IMetric metric, Map<String, Object> topoConf, TopologyContext context) {
        int bucketSize = ((Number) topoConf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS)).intValue();
        context.registerMetric(name, metric, bucketSize);
    }
}
