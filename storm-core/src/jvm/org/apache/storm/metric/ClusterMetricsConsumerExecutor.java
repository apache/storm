/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.metric;

import org.apache.storm.Config;
import org.apache.storm.metric.api.DataPoint;
import org.apache.storm.metric.api.IClusterMetricsConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class ClusterMetricsConsumerExecutor {
    public static final Logger LOG = LoggerFactory.getLogger(ClusterMetricsConsumerExecutor.class);

    private IClusterMetricsConsumer metricsConsumer;
    private String consumerClassName;
    private Object registrationArgument;

    public ClusterMetricsConsumerExecutor(String consumerClassName, Object registrationArgument) {
        this.consumerClassName = consumerClassName;
        this.registrationArgument = registrationArgument;
    }

    public void prepare() {
        try {
            metricsConsumer = (IClusterMetricsConsumer)Class.forName(consumerClassName).newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Could not instantiate a class listed in config under section " +
                    Config.STORM_CLUSTER_METRICS_CONSUMER_REGISTER + " with fully qualified name " + consumerClassName, e);
        }

        metricsConsumer.prepare(registrationArgument);
    }

    public void handleDataPoints(final IClusterMetricsConsumer.ClusterInfo clusterInfo, final Collection<DataPoint> dataPoints) {
        try {
            metricsConsumer.handleDataPoints(clusterInfo, dataPoints);
        } catch (Throwable e) {
            LOG.error("Error while handling cluster data points, consumer class: " + consumerClassName, e);
        }
    }

    public void handleDataPoints(final IClusterMetricsConsumer.SupervisorInfo supervisorInfo, final Collection<DataPoint> dataPoints) {
        try {
            metricsConsumer.handleDataPoints(supervisorInfo, dataPoints);
        } catch (Throwable e) {
            LOG.error("Error while handling cluster data points, consumer class: " + consumerClassName, e);
        }
    }

    public void cleanup() {
        metricsConsumer.cleanup();
    }
}