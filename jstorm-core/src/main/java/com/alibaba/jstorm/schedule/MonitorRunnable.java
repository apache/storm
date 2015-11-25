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

import backtype.storm.Config;
import backtype.storm.generated.TaskHeartbeat;
import backtype.storm.generated.TopologyTaskHbInfo;

import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.NimbusUtils;
import com.alibaba.jstorm.daemon.nimbus.StatusType;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.jstorm.daemon.nimbus.TopologyMetricsRunnable.TaskDeadEvent;

import java.util.*;

/**
 * Scan all task's heartbeat, if task isn't alive, DO NimbusUtils.transition(monitor)
 * 
 * @author Longda
 */
public class MonitorRunnable implements Runnable {
    private static Logger LOG = LoggerFactory.getLogger(MonitorRunnable.class);

    private NimbusData data;

    public MonitorRunnable(NimbusData data) {
        this.data = data;
    }

    /**
     * @@@ Todo when one topology is being reassigned, the topology should skip check
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
                Assignment assignment = clusterState.assignment_info(topologyid, null);

                Set<Integer> deadTasks = new HashSet<Integer>();
                boolean needReassign = false;
                for (Integer task : taskIds) {
                    boolean isTaskDead = NimbusUtils.isTaskDead(data, topologyid, task);
                    if (isTaskDead == true) {
                        deadTasks.add(task);
                        needReassign = true;
                    }
                }


                TopologyTaskHbInfo topologyHbInfo = data.getTasksHeartbeat().get(topologyid);
                if (needReassign == true) {
                    if (topologyHbInfo != null) {
                        int topologyMasterId = topologyHbInfo.get_topologyMasterId();
                        if (deadTasks.contains(topologyMasterId)) {
                            deadTasks.clear();
                            if (assignment != null) {
                                ResourceWorkerSlot resource = assignment.getWorkerByTaskId(topologyMasterId);
                                if (resource != null)
                                    deadTasks.addAll(resource.getTasks());
                                else
                                    deadTasks.add(topologyMasterId);
                            }
                        } else {
                            Map<Integer, TaskHeartbeat> taskHbs = topologyHbInfo.get_taskHbs();
                            int launchTime = JStormUtils.parseInt(data.getConf().get(Config.NIMBUS_TASK_LAUNCH_SECS));
                            if (taskHbs == null || taskHbs.get(topologyMasterId) == null || taskHbs.get(topologyMasterId).get_uptime() < launchTime) {
                                /*try {
                                    clusterState.topology_heartbeat(topologyid, topologyHbInfo);
                                } catch (Exception e) {
                                    LOG.error("Failed to update task heartbeat info to ZK for " + topologyid, e);
                                }*/
                                return;
                            }
                        }
                        Map<Integer, ResourceWorkerSlot> deadTaskWorkers = new HashMap<>();
                        for (Integer task : deadTasks) {
                            LOG.info("Found " + topologyid + ",taskid:" + task + " is dead");

                            ResourceWorkerSlot resource = null;
                            if (assignment != null)
                                resource = assignment.getWorkerByTaskId(task);
                            if (resource != null) {
                                deadTaskWorkers.put(task, resource);
                                Date now = new Date();
                                String nowStr = TimeFormat.getSecond(now);
                                String errorInfo = "Task-" + task + " is dead on " + resource.getHostname() + ":" + resource.getPort() + ", " + nowStr;
                                LOG.info(errorInfo);
                                clusterState.report_task_error(topologyid, task, errorInfo, null);
                            }
                        }

                        if (deadTaskWorkers.size() > 0) {
                            // notify jstorm monitor
                            TaskDeadEvent event = new TaskDeadEvent();
                            event.clusterName = data.getClusterName();
                            event.topologyId = topologyid;
                            event.deadTasks = deadTaskWorkers;
                            event.timestamp = System.currentTimeMillis();
                            data.getMetricRunnable().pushEvent(event);
                        }
                    }
                    NimbusUtils.transition(data, topologyid, false, StatusType.monitor);
                }
                
                if (topologyHbInfo != null) {
                    try {
                        clusterState.topology_heartbeat(topologyid, topologyHbInfo);
                    } catch (Exception e) {
                        LOG.error("Failed to update task heartbeat info to ZK for " + topologyid, e);
                    }
                }
            }

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

}
