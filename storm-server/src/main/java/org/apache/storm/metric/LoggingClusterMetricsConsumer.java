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

/**
 * Listens for cluster related metrics, dumps them to log.
 *
 * <p>To use, edit the storm.yaml config file:
 * ```yaml
 *   storm.cluster.metrics.register:
 *     - class: "org.apache.storm.metrics.LoggingClusterMetricsConsumer"
 * ```
 */
public class LoggingClusterMetricsConsumer implements IClusterMetricsConsumer {
    public static final Logger LOG = LoggerFactory.getLogger(LoggingClusterMetricsConsumer.class);

    private static String padding = "                       ";

    @Override
    public void prepare(Object registrationArgument) {
    }

    @Override
    public void handleDataPoints(ClusterInfo clusterInfo, Collection<DataPoint> dataPoints) {
        StringBuilder sb = new StringBuilder();
        String header = String.format("%d\t%15s\t%40s\t",
                                      clusterInfo.getTimestamp(), "<cluster>", "<cluster>");
        sb.append(header);
        logDataPoints(dataPoints, sb, header);
    }

    @Override
    public void handleDataPoints(SupervisorInfo supervisorInfo, Collection<DataPoint> dataPoints) {
        StringBuilder sb = new StringBuilder();
        String header = String.format("%d\t%15s\t%40s\t",
                                      supervisorInfo.getTimestamp(),
                                      supervisorInfo.getSrcSupervisorHost(),
                                      supervisorInfo.getSrcSupervisorId());
        sb.append(header);
        for (DataPoint p : dataPoints) {
            sb.delete(header.length(), sb.length());
            sb.append(p.getName())
                    .append(padding).delete(header.length() + 23, sb.length()).append("\t")
                    .append(p.getValue());
            LOG.info(sb.toString());
        }
    }

    @Override
    public void cleanup() {
    }

    private void logDataPoints(Collection<DataPoint> dataPoints, StringBuilder sb, String header) {
        for (DataPoint p : dataPoints) {
            sb.delete(header.length(), sb.length());
            sb.append(p.getName())
                    .append(padding)
                    .delete(header.length() + 23, sb.length())
                    .append("\t")
                    .append(p.getValue());
            LOG.info(sb.toString());
        }
    }
}
