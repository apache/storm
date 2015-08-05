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
package com.alibaba.jstorm.schedule;

import java.util.Date;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.NimbusUtils;
import com.alibaba.jstorm.daemon.nimbus.StatusType;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.utils.TimeFormat;

/**
 * 
 * Scan all task's heartbeat, if task isn't alive, DO
 * NimbusUtils.transition(monitor)
 * 
 * @author Longda
 * 
 */
public class MonitorRunnable implements Runnable {
    private static Logger LOG = LoggerFactory.getLogger(MonitorRunnable.class);

    private NimbusData data;

    public MonitorRunnable(NimbusData data) {
        this.data = data;
    }

    /**
     * @@@ Todo when one topology is being reassigned, the topology should be
     *     skip check
     * @param data
     */
    @Override
    public void run() {
        StormClusterState clusterState = data.getStormClusterState();

        try {
            // Attetion, need first check Assignments
            List<String> active_topologys = clusterState.assignments(null);

            if (active_topologys == null) {
                LOG.info("Failed to get active topologies");
                return;
            }

            for (String topologyid : active_topologys) {
                if (clusterState.storm_base(topologyid, null) == null) {
                    continue;
                }

                LOG.debug("Check tasks " + topologyid);

                // Attention, here don't check /ZK-dir/taskbeats/topologyid to
                // get task ids
                Set<Integer> taskIds = clusterState.task_ids(topologyid);
                if (taskIds == null) {
                    LOG.info("Failed to get task ids of " + topologyid);
                    continue;
                }

                boolean needReassign = false;
                for (Integer task : taskIds) {
                    boolean isTaskDead =
                            NimbusUtils.isTaskDead(data, topologyid, task);
                    if (isTaskDead == true) {
                        LOG.info("Found " + topologyid + ",taskid:" + task
                                + " is dead");

                        ResourceWorkerSlot resource = null;
                        Assignment assignment =
                                clusterState.assignment_info(topologyid, null);
                        if (assignment != null)
                            resource = assignment.getWorkerByTaskId(task);
                        if (resource != null) {
                            Date now = new Date();
                            String nowStr = TimeFormat.getSecond(now);
                            String errorInfo =
                                    "Task-" + task + " is dead on "
                                            + resource.getHostname() + ":"
                                            + resource.getPort() + ", "
                                            + nowStr;
                            LOG.info(errorInfo);
                            clusterState.report_task_error(topologyid, task,
                                    errorInfo);
                        }
                        needReassign = true;
                        break;
                    }
                }
                if (needReassign == true) {
                    NimbusUtils.transition(data, topologyid, false,
                            StatusType.monitor);
                }
            }

        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error(e.getMessage(), e);
        }
    }

}
