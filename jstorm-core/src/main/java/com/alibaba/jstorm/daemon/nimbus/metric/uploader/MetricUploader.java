package com.alibaba.jstorm.daemon.nimbus.metric.uploader;

import backtype.storm.generated.TopologyMetric;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.TopologyMetricsRunnable;

import java.util.Map;

public interface MetricUploader {
    /**
     * Set NimbusData to MetricUploader
     */
    void init(NimbusData nimbusData) throws Exception;

    void cleanup();

    /**
     * register metrics to external metric plugin
     */
    boolean registerMetrics(String clusterName, String topologyId,
                            Map<String, Long> metrics) throws Exception;

    String METRIC_TYPE = "metric.type";
    String METRIC_TYPE_TOPLOGY = "TP";
    String METRIC_TYPE_TASK = "TASK";
    String METRIC_TYPE_ALL = "ALL";
    String METRIC_TIME = "metric.timestamp";

    /**
     * upload topologyMetric to external metric plugin (such as database plugin)
     *
     * @return true means success, false means failure
     */
    boolean upload(String clusterName, String topologyId, TopologyMetric tpMetric, Map<String, Object> metricContext);

    /**
     * upload metrics with given key and metric context. the implementation can retrieve metric data from rocks db
     * in the handler thread, which is kind of lazy-init, making it more GC-friendly
     */
    boolean upload(String clusterName, String topologyId, Object key, Map<String, Object> metricContext);

    /**
     * Send an event to underlying handler
     */
    boolean sendEvent(String clusterName, TopologyMetricsRunnable.Event event);
}
