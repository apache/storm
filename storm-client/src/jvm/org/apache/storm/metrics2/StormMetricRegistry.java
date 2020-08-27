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
    private static final String WORKER_METRIC_PREFIX = "storm.worker.";
    private static final String TOPOLOGY_METRIC_PREFIX = "storm.topology.";
    
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
        MetricNames metricNames = workerMetricName(name, topologyId, componentId, taskId, port);
        Gauge gauge = registry.gauge(metricNames.getLongName(), () -> new SimpleGauge<>(initialValue));
        saveMetricTaskIdMapping(taskId, metricNames, gauge, taskIdGauges);
        return (SimpleGauge<T>) gauge;
    }

    public <T> Gauge<T> gauge(String name, Gauge<T> gauge, TopologyContext context) {
        MetricNames metricNames = topologyMetricName(name, context);
        gauge = registry.register(metricNames.getLongName(), gauge);
        saveMetricTaskIdMapping(context.getThisTaskId(), metricNames, gauge, taskIdGauges);
        return gauge;
    }

    public <T> Gauge<T> gauge(String name, Gauge<T> gauge, String topologyId, String componentId, Integer taskId, Integer port) {
        MetricNames metricNames = workerMetricName(name, topologyId, componentId, taskId, port);
        gauge = registry.register(metricNames.getLongName(), gauge);
        saveMetricTaskIdMapping(taskId, metricNames, gauge, taskIdGauges);
        return gauge;
    }

    public <T> Gauge<T> gauge(String name, Gauge<T> gauge, String topologyId, String componentId,
                              String streamId, Integer taskId, Integer port) {
        MetricNames metricNames = workerMetricName(name, topologyId, componentId, streamId, taskId, port);
        gauge = registry.register(metricNames.getLongName(), gauge);
        saveMetricTaskIdMapping(taskId, metricNames, gauge, taskIdGauges);
        return gauge;
    }

    public Meter meter(String name, WorkerTopologyContext context, String componentId, Integer taskId, String streamId) {
        MetricNames metricNames = workerMetricName(name, context.getStormId(), componentId, streamId, taskId, context.getThisWorkerPort());
        Meter meter = registry.meter(metricNames.getLongName());
        saveMetricTaskIdMapping(taskId, metricNames, meter, taskIdMeters);
        return meter;
    }

    public Meter meter(String name, WorkerTopologyContext context, String componentId, Integer taskId) {
        MetricNames metricNames = workerMetricName(name, context.getStormId(), componentId, taskId, context.getThisWorkerPort());
        Meter meter = registry.meter(metricNames.getLongName());
        saveMetricTaskIdMapping(taskId, metricNames, meter, taskIdMeters);
        return meter;
    }

    public Meter meter(String name, TopologyContext context) {
        MetricNames metricNames = topologyMetricName(name, context);
        Meter meter = registry.meter(metricNames.getLongName());
        saveMetricTaskIdMapping(context.getThisTaskId(), metricNames, meter, taskIdMeters);
        return meter;
    }

    public Counter counter(String name, WorkerTopologyContext context, String componentId, Integer taskId, String streamId) {
        MetricNames metricNames = workerMetricName(name, context.getStormId(), componentId, streamId, taskId, context.getThisWorkerPort());
        Counter counter = registry.counter(metricNames.getLongName());
        saveMetricTaskIdMapping(taskId, metricNames, counter, taskIdCounters);
        return counter;
    }

    public Counter counter(String name, String topologyId, String componentId, Integer taskId, Integer workerPort, String streamId) {
        MetricNames metricNames = workerMetricName(name, topologyId, componentId, streamId, taskId, workerPort);
        Counter counter = registry.counter(metricNames.getLongName());
        saveMetricTaskIdMapping(taskId, metricNames, counter, taskIdCounters);
        return counter;
    }

    public Counter counter(String name, TopologyContext context) {
        MetricNames metricNames = topologyMetricName(name, context);
        Counter counter = registry.counter(metricNames.getLongName());
        saveMetricTaskIdMapping(context.getThisTaskId(), metricNames, counter, taskIdCounters);
        return counter;
    }

    public void metricSet(String prefix, MetricSet set, TopologyContext context) {
        // Instead of registering the metrics as a set, register them individually.
        // This allows fetching the individual metrics by type (getTaskGauges())
        // to work as expected.
        for (Map.Entry<String, Metric> entry : set.getMetrics().entrySet()) {
            MetricNames metricNames = topologyMetricName(prefix + "." + entry.getKey(), context);
            Metric metric = registry.register(metricNames.getLongName(), entry.getValue());
            if (metric instanceof Gauge) {
                saveMetricTaskIdMapping(context.getThisTaskId(), metricNames, (Gauge) metric, taskIdGauges);
            } else if (metric instanceof Meter) {
                saveMetricTaskIdMapping(context.getThisTaskId(), metricNames, (Meter) metric, taskIdMeters);
            } else if (metric instanceof Counter) {
                saveMetricTaskIdMapping(context.getThisTaskId(), metricNames, (Counter) metric, taskIdCounters);
            } else if (metric instanceof Timer) {
                saveMetricTaskIdMapping(context.getThisTaskId(), metricNames, (Timer) metric, taskIdTimers);
            } else if (metric instanceof Histogram) {
                saveMetricTaskIdMapping(context.getThisTaskId(), metricNames, (Histogram) metric, taskIdHistograms);
            } else {
                LOG.error("Unable to save taskId mapping for metric {} named {}", metric, metricNames.getLongName());
            }
        }
    }

    public Timer timer(String name, TopologyContext context) {
        MetricNames metricNames = topologyMetricName(name, context);
        Timer timer = registry.timer(metricNames.getLongName());
        saveMetricTaskIdMapping(context.getThisTaskId(), metricNames, timer, taskIdTimers);
        return timer;
    }

    public Histogram histogram(String name, TopologyContext context) {
        MetricNames metricNames = topologyMetricName(name, context);
        Histogram histogram = registry.histogram(metricNames.getLongName());
        saveMetricTaskIdMapping(context.getThisTaskId(), metricNames, histogram, taskIdHistograms);
        return histogram;
    }

    private static <T extends Metric> void saveMetricTaskIdMapping(Integer taskId, MetricNames names, T metric, Map<Integer,
            Map<String, T>> taskIdMetrics) {
        Map<String, T> metrics = taskIdMetrics.computeIfAbsent(taskId, (tid) -> new HashMap<>());
        if (metrics.get(names.getV2TickName()) != null) {
            LOG.warn("Adding duplicate short metric for {} with long name {}, only the last metric "
                    + "will be reported during the V2 metrics tick.", names.getV2TickName(), names.longName);
        }
        metrics.put(names.getV2TickName(), metric);
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

    public void start(Map<String, Object> topoConf) {
        try {
            hostName = dotToUnderScore(Utils.localHostname());
        } catch (UnknownHostException e) {
            LOG.warn("Unable to determine hostname while starting the metrics system. Hostname will be reported"
                     + " as 'localhost'.");
        }

        LOG.info("Starting metrics reporters...");
        List<Map<String, Object>> reporterList = (List<Map<String, Object>>) topoConf.get(Config.TOPOLOGY_METRICS_REPORTERS);
        
        if (reporterList != null && reporterList.size() > 0) {
            for (Map<String, Object> reporterConfig : reporterList) {
                startReporter(topoConf, reporterConfig);
            }
        }
    }

    private void startReporter(Map<String, Object> topoConf, Map<String, Object> reporterConfig) {
        String clazz = (String) reporterConfig.get("class");
        LOG.info("Attempting to instantiate reporter class: {}", clazz);
        StormReporter reporter = ReflectionUtils.newInstance(clazz);
        if (reporter != null) {
            reporter.prepare(registry, topoConf, reporterConfig);
            reporter.start();
            reporters.add(reporter);
        }

    }

    public void stop() {
        for (StormReporter sr : reporters) {
            sr.stop();
        }
    }

    private MetricNames workerMetricName(String name, String stormId, String componentId, String streamId,
                                         Integer taskId, Integer workerPort) {
        StringBuilder sb = new StringBuilder(WORKER_METRIC_PREFIX);
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
        String longName = sb.toString();
        MetricNames names = new MetricNames(longName, name);
        return names;
    }

    private MetricNames workerMetricName(String name, String stormId, String componentId, Integer taskId, Integer workerPort) {
        StringBuilder sb = new StringBuilder(WORKER_METRIC_PREFIX);
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
        String longName = sb.toString();
        MetricNames names = new MetricNames(longName, name);
        return names;
    }

    private MetricNames topologyMetricName(String name, TopologyContext context) {
        StringBuilder sb = new StringBuilder(TOPOLOGY_METRIC_PREFIX);
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
        String longName = sb.toString();
        MetricNames names = new MetricNames(longName, name);
        return names;
    }

    private String dotToUnderScore(String str) {
        return str.replace('.', '_');
    }

    private static class MetricNames {
        private String longName;
        private String shortName;

        MetricNames(String longName, String shortName) {
            this.longName = longName;
            this.shortName = shortName;
        }

        /**
         * Returns the full metric name to be used for registering with the metrics registry.
         * @return The full metric name.
         */
        String getLongName() {
            return longName;
        }

        /**
         * Returns the short metric name to be used for reporting during the V2 metrics tick.
         * @return The V2 metrics tick name.
         */
        String getV2TickName() {
            return shortName;
        }
    }
}
