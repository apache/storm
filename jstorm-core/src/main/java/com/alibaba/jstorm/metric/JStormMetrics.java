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
package com.alibaba.jstorm.metric;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.MetricInfo;

import com.alibaba.jstorm.common.metric.Counter;
import com.alibaba.jstorm.common.metric.Gauge;
import com.alibaba.jstorm.common.metric.Histogram;
import com.alibaba.jstorm.common.metric.Meter;
import com.alibaba.jstorm.common.metric.MetricRegistry;
import com.alibaba.jstorm.common.metric.Timer;
import com.alibaba.jstorm.common.metric.window.Metric;
import com.alibaba.jstorm.utils.JStormUtils;

public class JStormMetrics implements Serializable {
    private static final Logger LOG = LoggerFactory
            .getLogger(JStormMetrics.class);
    private static final long serialVersionUID = 2046603514943797241L;

    /**
     * Metrics in this object will be uploaded to nimbus
     */
    static MetricRegistry workerMetrics = new MetricRegistry();
    static Map<Integer, MetricRegistry> taskMetrics =
            new ConcurrentHashMap<Integer, MetricRegistry>();
    /**
     * Metrics in this object will be just be output to log, won't be uploaded
     * to nimbus
     */
    static MetricRegistry skipMetrics = new MetricRegistry();

    protected static MetricInfo exposeWorkerMetrics;
    protected static Map<String, MetricInfo> exposeNettyMetrics;
    protected static Map<Integer, MetricInfo> exposeTaskMetrics;

    static {
        registerWorkerGauge(new com.codahale.metrics.Gauge<Double>() {

            @Override
            public Double getValue() {
                // TODO Auto-generated method stub
                return JStormUtils.getCpuUsage();
            }

        }, MetricDef.CPU_USED_RATIO);

        registerWorkerGauge(new com.codahale.metrics.Gauge<Double>() {

            @Override
            public Double getValue() {
                // TODO Auto-generated method stub
                return JStormUtils.getMemUsage();
            }

        }, MetricDef.MEMORY_USED);
    }

    public static MetricRegistry registerTask(int taskId) {
        MetricRegistry ret = taskMetrics.get(taskId);
        if (ret == null) {
            ret = new MetricRegistry();
            taskMetrics.put(taskId, ret);
            LOG.info("Register task MetricRegistry " + taskId);
        }

        return ret;
    }

    public static void unregisterTask(int taskId) {
    	taskMetrics.remove(taskId);
    }

    // the Metric should be one of metrics of task
    // if register this metric through this function,
    // the web UI would do sum operation for the metric
    // the metric will display in component/topology level in web UI
    public static void registerSumMetric(String name) {
        MetricDef.MERGE_SUM_TAG.add(name);
    }

    public static void unregisterSumMetric(String name) {
        MetricDef.MERGE_SUM_TAG.remove(name);
    }

    // the Metric should be one of metrics of task
    // if register this metric through this function,
    // the web UI would do sum operation for the metric
    // the metric will display in component/topology level in web UI
    public static void registerAvgMetric(String name) {
        MetricDef.MERGE_AVG_TAG.add(name);
    }

    public static void unregisterAvgMetric(String name) {
        MetricDef.MERGE_AVG_TAG.remove(name);
    }

    public static <T extends Metric> T registerWorkerMetric(T metric,
            String name, String... args) throws IllegalArgumentException {
        String registerName = MetricRegistry.name(name, args);

        return workerMetrics.register(registerName, metric);
    }

    public static void unregisterWorkerMetric(String name, String... args) {
        String registerName = MetricRegistry.name(name, args);

        workerMetrics.remove(registerName);
    }

    public static <T extends Metric> T registerTaskMetric(T metric, int taskId,
            String name, String... args) throws IllegalArgumentException {
        MetricRegistry metrics = taskMetrics.get(taskId);
        if (metrics == null) {
            throw new IllegalArgumentException("Invalid taskId " + taskId);
        }

        String registerName = MetricRegistry.name(name, args);

        return metrics.register(registerName, metric);
    }

    public static void unregisterTaskMetric(int taskId, String name,
            String... args) throws IllegalArgumentException {
        String registerName = MetricRegistry.name(name, args);
        MetricRegistry metrics = taskMetrics.get(taskId);
        if (metrics == null) {
            throw new IllegalArgumentException("Invalid taskId");
        }
        metrics.remove(registerName);
    }

    public static Gauge<Double> registerWorkerGauge(
            com.codahale.metrics.Gauge<Double> rawGauge, String name,
            String... args) {
        Gauge<Double> ret = new Gauge<Double>(rawGauge);
        registerWorkerMetric(ret, name, args);
        return ret;
    }

    public static Gauge<Double> registerTaskGauge(
            com.codahale.metrics.Gauge<Double> rawGauge, int taskId,
            String name, String... args) {
        Gauge<Double> ret = new Gauge<Double>(rawGauge);
        registerTaskMetric(ret, taskId, name, args);
        return ret;
    }

    public static Counter<Double> registerWorkerCounter(String name,
            String... args) throws IllegalArgumentException {
        Counter<Double> ret =
                (Counter<Double>) Builder.mkInstance(Builder.COUNTER);
        registerWorkerMetric(ret, name, args);
        return ret;
    }

    public static Counter<Double> registerTaskCounter(int taskId, String name,
            String... args) {
        Counter<Double> ret =
                (Counter<Double>) Builder.mkInstance(Builder.COUNTER);
        registerTaskMetric(ret, taskId, name, args);
        return ret;
    }

    public static Meter registerWorkerMeter(String name, String... args)
            throws IllegalArgumentException {
        Meter ret = (Meter) Builder.mkInstance(Builder.METER);
        registerWorkerMetric(ret, name, args);
        return ret;
    }

    public static Meter registerTaskMeter(int taskId, String name,
            String... args) {
        Meter ret = (Meter) Builder.mkInstance(Builder.METER);
        registerTaskMetric(ret, taskId, name, args);
        return ret;
    }

    public static Histogram registerWorkerHistogram(String name, String... args)
            throws IllegalArgumentException {
        Histogram ret = (Histogram) Builder.mkInstance(Builder.HISTOGRAM);
        registerWorkerMetric(ret, name, args);
        return ret;
    }

    public static Histogram registerTaskHistogram(int taskId, String name,
            String... args) {
        Histogram ret = (Histogram) Builder.mkInstance(Builder.HISTOGRAM);
        registerTaskMetric(ret, taskId, name, args);
        return ret;
    }

    public static Timer registerWorkerTimer(String name, String... args)
            throws IllegalArgumentException {
        Timer ret = (Timer) Builder.mkInstance(Builder.TIMER);
        registerWorkerMetric(ret, name, args);
        return ret;
    }

    public static Timer registerTaskTimer(int taskId, String name,
            String... args) {
        Timer ret = (Timer) Builder.mkInstance(Builder.TIMER);
        registerTaskMetric(ret, taskId, name, args);
        return ret;
    }

    public static class Builder {
        public static final int COUNTER = 1;
        public static final int METER = 2;
        public static final int HISTOGRAM = 3;
        public static final int TIMER = 4;

        public static Metric mkInstance(int type) {
            if (type == COUNTER) {
                return new Counter<Double>(Double.valueOf(0));
            } else if (type == METER) {
                return new Meter();
            } else if (type == HISTOGRAM) {
                return new Histogram();
            } else if (type == TIMER) {
                return new Timer();
            } else {
                throw new IllegalArgumentException();
            }
        }
    }

    public static MetricInfo getExposeWorkerMetrics() {
        return exposeWorkerMetrics;
    }

    public static void setExposeWorkerMetrics(MetricInfo exposeWorkerMetrics) {
        JStormMetrics.exposeWorkerMetrics = exposeWorkerMetrics;
    }

    public static Map<Integer, MetricInfo> getExposeTaskMetrics() {
        return exposeTaskMetrics;
    }

    public static void setExposeTaskMetrics(
            Map<Integer, MetricInfo> exposeTaskMetrics) {
        JStormMetrics.exposeTaskMetrics = exposeTaskMetrics;
    }

    public static Map<String, MetricInfo> getExposeNettyMetrics() {
        return exposeNettyMetrics;
    }

    public static void setExposeNettyMetrics(Map<String, MetricInfo> exposeNettyMetrics) {
        JStormMetrics.exposeNettyMetrics = exposeNettyMetrics;
    }

    

    
}
