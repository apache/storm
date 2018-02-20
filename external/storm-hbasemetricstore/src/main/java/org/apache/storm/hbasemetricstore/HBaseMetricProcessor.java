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

package org.apache.storm.hbasemetricstore;

import java.util.Map;
import org.apache.storm.generated.WorkerMetricPoint;
import org.apache.storm.generated.WorkerMetrics;
import org.apache.storm.metricstore.AggLevel;
import org.apache.storm.metricstore.Metric;
import org.apache.storm.metricstore.MetricException;
import org.apache.storm.metricstore.WorkerMetricsProcessor;

/**
 * WorkerMetricsProcessor implementation that allows the supervisor to directly insert metrics into HBase instead
 * of forwarding metrics to Nimbus for processing.
 */
public class HBaseMetricProcessor extends HBaseStore implements WorkerMetricsProcessor {

    @Override
    public void processWorkerMetrics(Map<String, Object> conf, WorkerMetrics metrics) throws MetricException {

        for (WorkerMetricPoint fields : metrics.get_metricList().get_metrics()) {
            Metric metric = new Metric(fields.get_metricName(), fields.get_timestamp(), metrics.get_topologyId(),
                    fields.get_metricValue(), fields.get_componentId(), fields.get_executorId(),
                    metrics.get_hostname(), fields.get_streamId(), metrics.get_port(), AggLevel.AGG_LEVEL_NONE);
            this.insert(metric);
        }
    }
}
