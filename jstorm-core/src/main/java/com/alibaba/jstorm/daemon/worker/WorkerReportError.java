package com.alibaba.jstorm.daemon.worker;

import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.utils.TimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Set;

/**
 * Created by xiaojian.fxj on 2015/8/12.
 */
public class WorkerReportError {
    private static Logger LOG = LoggerFactory.getLogger(WorkerReportError.class);
    private StormClusterState zkCluster;
    private String hostName;

    public WorkerReportError(StormClusterState _storm_cluster_state,
                             String _hostName) {
        this.zkCluster = _storm_cluster_state;
        this.hostName = _hostName;
    }
    public void report(String topology_id, Integer worker_port,
                       Set<Integer> tasks, String error) {
        // Report worker's error to zk
        try {
            Date now = new Date();
            String nowStr = TimeFormat.getSecond(now);
            String errorInfo = error + "on " + this.hostName + ":" + worker_port + "," + nowStr;
            for (Integer task : tasks){
                zkCluster.report_task_error(topology_id, task, errorInfo, null);
            }
        } catch (Exception e) {
            LOG.error("Failed update "+worker_port+ "errors to ZK" + "\n", e);
        }
    }
}
