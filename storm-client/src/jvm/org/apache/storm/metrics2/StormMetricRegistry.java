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

package org.apache.storm.metrics2;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.metrics2.reporters.StormReporter;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.utils.ReflectionUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormMetricRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(StormMetricRegistry.class);
    
    private final MetricRegistry registry = new MetricRegistry();
    private final List<StormReporter> reporters = new ArrayList<>();
    private String hostName = null;

    public <T> SimpleGauge<T> gauge(
        T initialValue, String name, String topologyId, String componentId, Integer taskId, Integer port) {
        String metricName = metricName(name, topologyId, componentId, taskId, port);
        return (SimpleGauge<T>)registry.gauge(metricName, () -> new SimpleGauge<>(initialValue));
    }

    public JcMetrics jcMetrics(String name, String topologyId, String componentId, Integer taskId, Integer port) {
        return new JcMetrics(
            gauge(0L, name + "-capacity", topologyId, componentId, taskId, port),
            gauge(0L, name + "-population", topologyId, componentId, taskId, port)
        );
    }

    public Meter meter(String name, WorkerTopologyContext context, String componentId, Integer taskId, String streamId) {
        String metricName = metricName(name, context.getStormId(), componentId, streamId, taskId, context.getThisWorkerPort());
        return registry.meter(metricName);
    }

    public Counter counter(String name, WorkerTopologyContext context, String componentId, Integer taskId, String streamId) {
        String metricName = metricName(name, context.getStormId(), componentId, streamId, taskId, context.getThisWorkerPort());
        return registry.counter(metricName);
    }

    public Counter counter(String name, String topologyId, String componentId, Integer taskId, Integer workerPort, String streamId) {
        String metricName = metricName(name, topologyId, componentId, streamId, taskId, workerPort);
        return registry.counter(metricName);
    }
    
    public MetricRegistry registry() {
        return registry;
    }

    public void start(Map<String, Object> stormConfig, DaemonType type) {
        try {
            hostName = dotToUnderScore(Utils.localHostname());
        } catch (UnknownHostException e) {
            LOG.warn("Unable to determine hostname while starting the metrics system. Hostname will be reported"
                     + " as 'localhost'.");
        }

        LOG.info("Starting metrics reporters...");
        List<Map<String, Object>> reporterList = (List<Map<String, Object>>) stormConfig.get(Config.STORM_METRICS_REPORTERS);
        if (reporterList != null && reporterList.size() > 0) {
            for (Map<String, Object> reporterConfig : reporterList) {
                // only start those requested
                List<String> daemons = (List<String>) reporterConfig.get("daemons");
                for (String daemon : daemons) {
                    if (DaemonType.valueOf(daemon.toUpperCase()) == type) {
                        startReporter(stormConfig, reporterConfig);
                    }
                }
            }
        }
    }

    private void startReporter(Map<String, Object> stormConfig, Map<String, Object> reporterConfig) {
        String clazz = (String) reporterConfig.get("class");
        LOG.info("Attempting to instantiate reporter class: {}", clazz);
        StormReporter reporter = ReflectionUtils.newInstance(clazz);
        if (reporter != null) {
            reporter.prepare(registry, stormConfig, reporterConfig);
            reporter.start();
            reporters.add(reporter);
        }

    }

    public void stop() {
        for (StormReporter sr : reporters) {
            sr.stop();
        }
    }

    private String metricName(String name, String stormId, String componentId, String streamId, Integer taskId, Integer workerPort) {
        StringBuilder sb = new StringBuilder("storm.worker.");
        sb.append(stormId);
        sb.append(".");
        sb.append(hostName);
        sb.append(".");
        sb.append(dotToUnderScore(componentId));
        sb.append(".");
        sb.append(dotToUnderScore(streamId));
        sb.append(".");
        sb.append(taskId);
        sb.append(".");
        sb.append(workerPort);
        sb.append("-");
        sb.append(name);
        return sb.toString();
    }

    private String metricName(String name, String stormId, String componentId, Integer taskId, Integer workerPort) {
        StringBuilder sb = new StringBuilder("storm.worker.");
        sb.append(stormId);
        sb.append(".");
        sb.append(hostName);
        sb.append(".");
        sb.append(dotToUnderScore(componentId));
        sb.append(".");
        sb.append(taskId);
        sb.append(".");
        sb.append(workerPort);
        sb.append("-");
        sb.append(name);
        return sb.toString();
    }

    public String metricName(String name, TopologyContext context) {
        StringBuilder sb = new StringBuilder("storm.topology.");
        sb.append(context.getStormId());
        sb.append(".");
        sb.append(hostName);
        sb.append(".");
        sb.append(dotToUnderScore(context.getThisComponentId()));
        sb.append(".");
        sb.append(context.getThisTaskId());
        sb.append(".");
        sb.append(context.getThisWorkerPort());
        sb.append("-");
        sb.append(name);
        return sb.toString();
    }

    private String dotToUnderScore(String str) {
        return str.replace('.', '_');
    }
}
