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
import org.apache.storm.generated.WorkerMetrics;

public interface WorkerMetricsProcessor {

    /**
     * Process insertion of worker metrics.  The implementation should be thread-safe.
     * @param conf the supervisor config
     * @param metrics  the metrics to process
     * @throws MetricException  on error
     */
    void processWorkerMetrics(Map<String, Object> conf, WorkerMetrics metrics) throws MetricException;

    /**
     * Prepares the metric processor.
     * @param config Storm config map
     * @throws MetricException  on error
     */
    void prepare(Map<String, Object> config) throws MetricException;
}
