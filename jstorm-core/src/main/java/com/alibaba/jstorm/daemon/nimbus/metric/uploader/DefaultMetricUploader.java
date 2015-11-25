package com.alibaba.jstorm.daemon.nimbus.metric.uploader;

import backtype.storm.generated.TopologyMetric;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.TopologyMetricsRunnable;
import com.alibaba.jstorm.daemon.nimbus.TopologyMetricsRunnable.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DefaultMetricUploader implements MetricUploader {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    protected NimbusData nimbusData;
    protected TopologyMetricsRunnable metricsRunnable;

    public DefaultMetricUploader() {
    }

    @Override
    public void init(NimbusData nimbusData) throws Exception {
        this.nimbusData = nimbusData;
        this.metricsRunnable = nimbusData.getMetricRunnable();
    }

    @Override
    public void cleanup() {
    }

    @Override
    public boolean registerMetrics(String clusterName, String topologyId,
                                   Map<String, Long> metrics) {
        if (metrics.size() > 0) {
            logger.info("register metrics, topology:{}, total:{}", topologyId, metrics.size());
        }
        return true;
    }

    @Override
    public boolean upload(String clusterName, String topologyId, TopologyMetric tpMetric, Map<String, Object> metricContext) {
        if (tpMetric == null) {
            logger.info("No metric of {}", topologyId);
            return true;
        }

        int totalSize = tpMetric.get_topologyMetric().get_metrics_size() +
                tpMetric.get_componentMetric().get_metrics_size() +
                tpMetric.get_taskMetric().get_metrics_size() +
                tpMetric.get_streamMetric().get_metrics_size() +
                tpMetric.get_workerMetric().get_metrics_size() +
                tpMetric.get_nettyMetric().get_metrics_size();

        logger.info("send metrics, cluster:{}, topology:{}, metric size:{}, metricContext:{}",
                clusterName, topologyId, totalSize, metricContext);

        return true;
    }

    @Override
    public boolean upload(String clusterName, String topologyId, Object key, Map<String, Object> metricContext) {
        metricsRunnable.markUploaded((Integer) key);
        return true;
    }


    @Override
    public boolean sendEvent(String clusterName, Event event) {
        logger.info("Successfully sendEvent {} of {}", event, clusterName);
        return true;
    }
}