package com.alibaba.jstorm.metric;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.health.HealthCheck.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by wuchong on 15/9/17.
 */
public class JStormHealthReporter extends RunnableCallback {
    private static final Logger LOG = LoggerFactory.getLogger(JStormHealthReporter.class);
    private static final int THREAD_CYCLE = 60;   //report every minute
    private WorkerData workerData;

    public JStormHealthReporter(WorkerData workerData) {
        this.workerData = workerData;
    }

    @Override
    public void run() {
        StormClusterState clusterState = workerData.getZkCluster();
        String topologyId = workerData.getTopologyId();

        Map<Integer, HealthCheckRegistry> taskHealthCheckMap = JStormHealthCheck.getTaskhealthcheckmap();
        int cnt = 0;
        for (Map.Entry<Integer, HealthCheckRegistry> entry : taskHealthCheckMap.entrySet()) {
            Integer taskId = entry.getKey();
            Map<String, Result> results = entry.getValue().runHealthChecks();

            for (Map.Entry<String, Result> result : results.entrySet()) {
                if (!result.getValue().isHealthy()) {
                    try {
                        clusterState.report_task_error(topologyId, taskId, result.getValue().getMessage(), null);
                        cnt++;
                    } catch (Exception e) {
                        LOG.error("Failed to update health data in ZK for topo-{} task-{}.", topologyId, taskId, e);
                    }
                }
            }
        }
        LOG.info("Successfully updated {} health data to ZK for topology:{}", cnt, topologyId);
    }

    @Override
    public Object getResult() {
        return THREAD_CYCLE;
    }

    @Override
    public String getThreadName() {
        return "HealthReporterThread";
    }
}
