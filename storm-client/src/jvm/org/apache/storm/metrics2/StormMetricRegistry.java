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
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
    private final ConcurrentMap<Integer, Map<String, Gauge>> taskIdGauges = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, Map<String, Meter>> taskIdMeters = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, Map<String, Counter>> taskIdCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, Map<String, Timer>> taskIdTimers = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, Map<String, Histogram>> taskIdHistograms = new ConcurrentHashMap<>();
    private String hostName = null;

    public <T> SimpleGauge<T> gauge(
        T initialValue, String name, String topologyId, String componentId, Integer taskId, Integer port) {
        String metricName = metricName(name, topologyId, componentId, taskId, port);
        Gauge gauge = registry.gauge(metricName, () -> new SimpleGauge<>(initialValue));
        saveMetricTaskIdMapping(taskId, metricName, gauge, taskIdGauges);
        return (SimpleGauge<T>) gauge;
    }

    public <T> Gauge<T> gauge(String name, Gauge<T> gauge, TopologyContext context) {
        String metricName = metricName(name, context);
        gauge = registry.register(metricName, gauge);
        saveMetricTaskIdMapping(context.getThisTaskId(), metricName, gauge, taskIdGauges);
        return gauge;
    }

    public JcMetrics jcMetrics(String name, String topologyId, String componentId, Integer taskId, Integer port) {
        SimpleGauge<Long> capacityGauge = gauge(0L, name + "-capacity", topologyId, componentId, taskId, port);
        SimpleGauge<Long> populationGauge = gauge(0L, name + "-population", topologyId, componentId, taskId, port);
        return new JcMetrics(capacityGauge, populationGauge);
    }

    public Meter meter(String name, WorkerTopologyContext context, String componentId, Integer taskId, String streamId) {
        String metricName = metricName(name, context.getStormId(), componentId, streamId, taskId, context.getThisWorkerPort());
        Meter meter = registry.meter(metricName);
        saveMetricTaskIdMapping(taskId, metricName, meter, taskIdMeters);
        return meter;
    }

    public Meter meter(String name, TopologyContext context) {
        String metricName = metricName(name, context);
        Meter meter = registry.meter(metricName);
        saveMetricTaskIdMapping(context.getThisTaskId(), metricName, meter, taskIdMeters);
        return meter;
    }

    public Counter counter(String name, WorkerTopologyContext context, String componentId, Integer taskId, String streamId) {
        String metricName = metricName(name, context.getStormId(), componentId, streamId, taskId, context.getThisWorkerPort());
        Counter counter = registry.counter(metricName);
        saveMetricTaskIdMapping(taskId, metricName, counter, taskIdCounters);
        return counter;
    }

    public Counter counter(String name, String topologyId, String componentId, Integer taskId, Integer workerPort, String streamId) {
        String metricName = metricName(name, topologyId, componentId, streamId, taskId, workerPort);
        Counter counter = registry.counter(metricName);
        saveMetricTaskIdMapping(taskId, metricName, counter, taskIdCounters);
        return counter;
    }

    public Counter counter(String name, TopologyContext context) {
        String metricName = metricName(name, context);
        Counter counter = registry.counter(metricName);
        saveMetricTaskIdMapping(context.getThisTaskId(), metricName, counter, taskIdCounters);
        return counter;
    }

    public void metricSet(String prefix, MetricSet set, TopologyContext context) {
        String baseName = metricName(prefix, context);
        // Instead of registering the metrics as a set, register them individually.
        // This allows fetching the individual metrics by type (getTaskGauges())
        // to work as expected.
        for (Map.Entry<String, Metric> entry : set.getMetrics().entrySet()) {
            String metricName = baseName + "." + entry.getKey();
            Metric metric = registry.register(metricName, entry.getValue());
            if (metric instanceof Gauge) {
                saveMetricTaskIdMapping(context.getThisTaskId(), metricName, (Gauge) metric, taskIdGauges);
            } else if (metric instanceof Meter) {
                saveMetricTaskIdMapping(context.getThisTaskId(), metricName, (Meter) metric, taskIdMeters);
            } else if (metric instanceof Counter) {
                saveMetricTaskIdMapping(context.getThisTaskId(), metricName, (Counter) metric, taskIdCounters);
            } else if (metric instanceof Timer) {
                saveMetricTaskIdMapping(context.getThisTaskId(), metricName, (Timer) metric, taskIdTimers);
            } else if (metric instanceof Histogram) {
                saveMetricTaskIdMapping(context.getThisTaskId(), metricName, (Histogram) metric, taskIdHistograms);
            } else {
                LOG.error("Unable to save taskId mapping for metric {} named {}", metric, metricName);
            }
        }
    }

    public Timer timer(String name, TopologyContext context) {
        String metricName = metricName(name, context);
        Timer timer = registry.timer(metricName);
        saveMetricTaskIdMapping(context.getThisTaskId(), metricName, timer, taskIdTimers);
        return timer;
    }

    public Histogram histogram(String name, TopologyContext context) {
        String metricName = metricName(name, context);
        Histogram histogram = registry.histogram(metricName);
        saveMetricTaskIdMapping(context.getThisTaskId(), metricName, histogram, taskIdHistograms);
        return histogram;
    }

    private static <T extends Metric> void saveMetricTaskIdMapping(Integer taskId, String name, T metric, Map<Integer,
            Map<String, T>> taskIdMetrics) {
        Map<String, T> metrics = taskIdMetrics.computeIfAbsent(taskId, (tid) -> new HashMap<>());
        metrics.put(name, metric);
    }

    private <T extends Metric> Map<String, T> getMetricNameMap(int taskId, Map<Integer, Map<String, T>> taskIdMetrics) {
        Map<String, T> ret = new HashMap<>();
        Map<String, T> taskMetrics = taskIdMetrics.getOrDefault(taskId, Collections.emptyMap());
        ret.putAll(taskMetrics);
        return ret;
    }

    public Map<String, Gauge> getTaskGauges(int taskId) {
        return getMetricNameMap(taskId, taskIdGauges);
    }

    public Map<String, Counter> getTaskCounters(int taskId) {
        return getMetricNameMap(taskId, taskIdCounters);
    }

    public Map<String, Histogram> getTaskHistograms(int taskId) {
        return getMetricNameMap(taskId, taskIdHistograms);
    }

    public Map<String, Meter> getTaskMeters(int taskId) {
        return getMetricNameMap(taskId, taskIdMeters);
    }

    public Map<String, Timer> getTaskTimers(int taskId) {
        return getMetricNameMap(taskId, taskIdTimers);
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
