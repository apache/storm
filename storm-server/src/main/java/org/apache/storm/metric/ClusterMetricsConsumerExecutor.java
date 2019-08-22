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

package org.apache.storm.metric;

import java.util.Collection;
import org.apache.storm.metric.api.DataPoint;
import org.apache.storm.metric.api.IClusterMetricsConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterMetricsConsumerExecutor {
    public static final Logger LOG = LoggerFactory.getLogger(ClusterMetricsConsumerExecutor.class);
    private static final String ERROR_MESSAGE_PREPARATION_CLUSTER_METRICS_CONSUMER_FAILED =
            "Preparation of Cluster Metrics Consumer failed. "
                    + "Please check your configuration and/or corresponding systems and relaunch Nimbus. "
                    + "Skipping handle metrics.";

    private IClusterMetricsConsumer metricsConsumer;
    private String consumerClassName;
    private Object registrationArgument;

    public ClusterMetricsConsumerExecutor(String consumerClassName, Object registrationArgument) {
        this.consumerClassName = consumerClassName;
        this.registrationArgument = registrationArgument;
    }

    public void prepare() {
        try {
            metricsConsumer = (IClusterMetricsConsumer) Class.forName(consumerClassName).newInstance();
            metricsConsumer.prepare(registrationArgument);
        } catch (Exception e) {
            LOG.error("Could not instantiate or prepare Cluster Metrics Consumer with fully qualified name "
                    + consumerClassName,
                    e);

            if (metricsConsumer != null) {
                metricsConsumer.cleanup();
            }
            metricsConsumer = null;
        }
    }

    public void handleDataPoints(final IClusterMetricsConsumer.ClusterInfo clusterInfo, final Collection<DataPoint> dataPoints) {
        if (metricsConsumer == null) {
            LOG.error(ERROR_MESSAGE_PREPARATION_CLUSTER_METRICS_CONSUMER_FAILED);
            return;
        }

        try {
            metricsConsumer.handleDataPoints(clusterInfo, dataPoints);
        } catch (Throwable e) {
            LOG.error("Error while handling cluster data points, consumer class: " + consumerClassName, e);
        }
    }

    public void handleDataPoints(final IClusterMetricsConsumer.SupervisorInfo supervisorInfo, final Collection<DataPoint> dataPoints) {
        if (metricsConsumer == null) {
            LOG.error(ERROR_MESSAGE_PREPARATION_CLUSTER_METRICS_CONSUMER_FAILED);
            return;
        }

        try {
            metricsConsumer.handleDataPoints(supervisorInfo, dataPoints);
        } catch (Throwable e) {
            LOG.error("Error while handling cluster data points, consumer class: " + consumerClassName, e);
        }
    }

    public void cleanup() {
        if (metricsConsumer != null) {
            metricsConsumer.cleanup();
        }
    }
}
