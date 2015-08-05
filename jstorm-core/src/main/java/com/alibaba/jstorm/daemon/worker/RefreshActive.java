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
package com.alibaba.jstorm.daemon.worker;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.cluster.StormBase;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.daemon.nimbus.StatusType;
import com.alibaba.jstorm.task.TaskShutdownDameon;
import com.alibaba.jstorm.utils.JStormUtils;

/**
 * Timely check whether topology is active or not and whether the metrics
 * monitor is enable or disable from ZK
 * 
 * @author yannian/Longda
 * 
 */
public class RefreshActive extends RunnableCallback {
    private static Logger LOG = LoggerFactory.getLogger(RefreshActive.class);

    private WorkerData workerData;

    private AtomicBoolean shutdown;
    private AtomicBoolean monitorEnable;
    private Map<Object, Object> conf;
    private StormClusterState zkCluster;
    private String topologyId;
    private Integer frequence;

    // private Object lock = new Object();

    @SuppressWarnings("rawtypes")
    public RefreshActive(WorkerData workerData) {
        this.workerData = workerData;

        this.shutdown = workerData.getShutdown();
        this.monitorEnable = workerData.getMonitorEnable();
        this.conf = workerData.getStormConf();
        this.zkCluster = workerData.getZkCluster();
        this.topologyId = workerData.getTopologyId();
        this.frequence =
                JStormUtils.parseInt(conf.get(Config.TASK_REFRESH_POLL_SECS),
                        10);
    }

    @Override
    public void run() {

        try {
            StatusType newTopologyStatus = StatusType.activate;
            // /ZK-DIR/topology
            StormBase base = zkCluster.storm_base(topologyId, this);
            if (base == null) {
                // @@@ normally the topology has been removed
                LOG.warn("Failed to get StromBase from ZK of " + topologyId);
                newTopologyStatus = StatusType.killed;
            } else {

                newTopologyStatus = base.getStatus().getStatusType();
            }

            // Process the topology status change
            StatusType oldTopologyStatus = workerData.getTopologyStatus();

            if (newTopologyStatus.equals(oldTopologyStatus)) {
                return;
            }

            LOG.info("Old TopologyStatus:" + oldTopologyStatus
                    + ", new TopologyStatus:" + newTopologyStatus);

            List<TaskShutdownDameon> tasks = workerData.getShutdownTasks();
            if (tasks == null) {
                LOG.info("Tasks aren't ready or begin to shutdown");
                return;
            }

            if (newTopologyStatus.equals(StatusType.active)) {
                for (TaskShutdownDameon task : tasks) {
                    task.active();
                }
            } else if (newTopologyStatus.equals(StatusType.rebalancing)) {
                // TODO
                // But this may be updated in the future.
                for (TaskShutdownDameon task : tasks) {
                    task.deactive();
                }
            } else {
                for (TaskShutdownDameon task : tasks) {
                    task.deactive();
                }
            }
            workerData.setTopologyStatus(newTopologyStatus);

            boolean newMonitorEnable = base.isEnableMonitor();
            boolean oldMonitorEnable = monitorEnable.get();
            if (newMonitorEnable != oldMonitorEnable) {
                LOG.info("Change MonitorEnable from " + oldMonitorEnable
                        + " to " + newMonitorEnable);
                monitorEnable.set(newMonitorEnable);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error("Failed to get topology from ZK ", e);
            return;
        }

    }

    @Override
    public Object getResult() {
        return frequence;
    }
}
