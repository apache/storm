package org.apache.storm.hbasemetricstore;

import java.util.Map;
import org.apache.storm.generated.WorkerMetricPoint;
import org.apache.storm.generated.WorkerMetrics;
import org.apache.storm.metricstore.AggLevel;
import org.apache.storm.metricstore.Metric;
import org.apache.storm.metricstore.MetricException;
import org.apache.storm.metricstore.WorkerMetricsProcessor;

/**
 * WorkerMetricsProcessor implementation that allows the supervisor to directly insert metrics into HBase instead
 * of forwarding metrics to Nimbus for processing.
 */
public class HBaseMetricProcessor extends HBaseStore implements WorkerMetricsProcessor {

    @Override
    public void processWorkerMetrics(Map<String, Object> conf, WorkerMetrics metrics) throws MetricException {

        for (WorkerMetricPoint fields : metrics.get_metricList().get_metrics()) {
            Metric metric = new Metric(fields.get_metricName(), fields.get_timestamp(), metrics.get_topologyId(),
                    fields.get_metricValue(), fields.get_componentId(), fields.get_executorId(),
                    metrics.get_hostname(), fields.get_streamId(), metrics.get_port(), AggLevel.AGG_LEVEL_NONE);
            this.insert(metric);
        }
    }
}
