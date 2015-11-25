package com.alibaba.jstorm.metric;

import backtype.storm.task.TopologyContext;
import com.alibaba.jstorm.common.metric.*;
import com.codahale.metrics.Gauge;

/**
 * metric client for end users to add custom metrics.
 *
 * @author Cody (weiyue.wy@alibaba-inc.com)
 * @since 2.0.5
 */
@SuppressWarnings("unused")
public class MetricClient {
    private static final String GROUP_UDF = "udf";

    private final String topologyId;
    private final String componentId;
    private final int taskId;

    public MetricClient(TopologyContext context) {
        taskId = context.getThisTaskId();
        this.topologyId = context.getTopologyId();
        this.componentId = context.getThisComponentId();
    }

    public AsmGauge registerGauge(String name, Gauge<Double> gauge) {
        return registerGauge(name, GROUP_UDF, gauge);
    }

    public AsmGauge registerGauge(String name, String group, Gauge<Double> gauge) {
        String userMetricName = getMetricName(name, group, MetricType.GAUGE);
        AsmGauge asmGauge = new AsmGauge(gauge);
        JStormMetrics.registerTaskMetric(userMetricName, asmGauge);
        return asmGauge;
    }

    public AsmCounter registerCounter(String name) {
        return registerCounter(name, GROUP_UDF);
    }

    public AsmCounter registerCounter(String name, String group) {
        String userMetricName = getMetricName(name, group, MetricType.COUNTER);
        AsmCounter counter = new AsmCounter();
        JStormMetrics.registerTaskMetric(userMetricName, counter);
        return counter;
    }

    public AsmMeter registerMeter(String name) {
        return registerMeter(name, GROUP_UDF);
    }

    public AsmMeter registerMeter(String name, String group) {
        String userMetricName = getMetricName(name, group, MetricType.METER);
        return (AsmMeter) JStormMetrics.registerTaskMetric(userMetricName, new AsmMeter());
    }

    public AsmTimer registerTimer(String name) {
        return registerTimer(name, GROUP_UDF);
    }

    public AsmTimer registerTimer(String name, String group) {
        String userMetricName = getMetricName(name, group, MetricType.TIMER);
        return (AsmTimer) JStormMetrics.registerTaskMetric(userMetricName, new AsmTimer());
    }

    public AsmHistogram registerHistogram(String name) {
        return registerHistogram(name, GROUP_UDF);
    }

    public AsmHistogram registerHistogram(String name, String group) {
        String userMetricName = getMetricName(name, group, MetricType.HISTOGRAM);
        return (AsmHistogram) JStormMetrics.registerTaskMetric(userMetricName, new AsmHistogram());
    }

    public void unregister(String name, MetricType type) {
        unregister(name, GROUP_UDF, type);
    }

    public void unregister(String name, String group, MetricType type) {
        String userMetricName = getMetricName(name, group, type);
        JStormMetrics.unregisterTaskMetric(userMetricName);
    }

    private String getMetricName(String name, MetricType type) {
        return getMetricName(name, GROUP_UDF, type);
    }

    private String getMetricName(String name, String group, MetricType type) {
        return MetricUtils.taskMetricName(topologyId, componentId, taskId, group, name, type);
    }
}
