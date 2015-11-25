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
package com.alibaba.jstorm.task.heartbeat;

import backtype.storm.generated.TaskHeartbeat;
import backtype.storm.generated.TopologyTaskHbInfo;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.NimbusClient;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.task.UptimeComputer;
import com.alibaba.jstorm.utils.TimeUtils;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Update the task heartbeat information of topology to Nimbus
 * 
 * @author Basti Liu
 * 
 */
public class TaskHeartbeatUpdater{
    private static final Logger LOG = LoggerFactory
            .getLogger(TaskHeartbeatUpdater.class);

    private int MAX_NUM_TASK_HB_SEND;

    private String topologyId;
    private int taskId;
    
    private Map conf;
    private NimbusClient client;

    private Map<Integer, TaskHeartbeat> taskHbMap;
    private TopologyTaskHbInfo taskHbs;

    private StormClusterState zkCluster;
    
    public TaskHeartbeatUpdater(Map conf, String topologyId, int taskId, StormClusterState zkCluster) {
        this.topologyId = topologyId;
        this.taskId = taskId;

        this.conf = conf;
        this.client = NimbusClient.getConfiguredClient(conf);

        this.zkCluster = zkCluster;
        
        try {
            TopologyTaskHbInfo taskHbInfo = zkCluster.topology_heartbeat(topologyId);
            if (taskHbInfo != null) {
                LOG.info("Found task heartbeat info left in zk for " + topologyId + ": " + taskHbInfo.toString());
                this.taskHbs = taskHbInfo;
                this.taskHbMap = taskHbInfo.get_taskHbs();
                if (this.taskHbMap == null) {
                    this.taskHbMap = new ConcurrentHashMap<Integer, TaskHeartbeat>();
                    taskHbs.set_taskHbs(this.taskHbMap);
                }
                this.taskHbs.set_topologyId(topologyId);
                this.taskHbs.set_topologyMasterId(this.taskId);
            } else {
                LOG.info("There is not any previous task heartbeat info left in zk for " + topologyId);
                this.taskHbMap = new ConcurrentHashMap<Integer, TaskHeartbeat>();
                this.taskHbs = new TopologyTaskHbInfo(this.topologyId, this.taskId);
                this.taskHbs.set_taskHbs(taskHbMap);
            }
        } catch (Exception e) {
            LOG.warn("Failed to get topology heartbeat from zk", e);
        }
        this.MAX_NUM_TASK_HB_SEND = ConfigExtension.getTopologyTaskHbSendNumber(conf);
    }

    public void process(Tuple input) {
        int sourceTask = input.getSourceTask();
        int uptime = (Integer) input.getValue(0);
        
        // Update the heartbeat for source task
        TaskHeartbeat taskHb = taskHbMap.get(sourceTask);
        if (taskHb == null) {
            taskHb = new TaskHeartbeat(TimeUtils.current_time_secs(), uptime);
            taskHbMap.put(sourceTask, taskHb);
        } else {
            taskHb.set_time(TimeUtils.current_time_secs());
            taskHb.set_uptime(uptime);
        }

        // Send heartbeat info of all tasks to nimbus
        if (sourceTask == taskId) {
            // Send heartbeat info of MAX_NUM_TASK_HB_SEND tasks each time
            TopologyTaskHbInfo tmpTaskHbInfo = new TopologyTaskHbInfo(topologyId, taskId);
            Map<Integer, TaskHeartbeat> tmpTaskHbMap = new ConcurrentHashMap<Integer, TaskHeartbeat>();
            tmpTaskHbInfo.set_taskHbs(tmpTaskHbMap);

            int sendCount = 0;
            for (Entry<Integer, TaskHeartbeat> entry : taskHbMap.entrySet()) {
                tmpTaskHbMap.put(entry.getKey(), entry.getValue());
                sendCount++;

                if (sendCount >= MAX_NUM_TASK_HB_SEND) {
                    setTaskHeatbeat(tmpTaskHbInfo);
                    tmpTaskHbMap.clear();
                    sendCount = 0;
                }
            }
            if (tmpTaskHbMap.size() > 0) {
                setTaskHeatbeat(tmpTaskHbInfo);
            }
        }
    }
    
    private void setTaskHeatbeat(TopologyTaskHbInfo topologyTaskHbInfo) {
        try {
            if (topologyTaskHbInfo == null) {
                return;
            }
            if (topologyTaskHbInfo.get_taskHbs() == null) {
                return;
            }

            client.getClient().updateTaskHeartbeat(topologyTaskHbInfo);

            String info = "";
            for (Entry<Integer, TaskHeartbeat> entry : topologyTaskHbInfo.get_taskHbs().entrySet()) {
                info += " " + entry.getKey() + "-" + entry.getValue().get_time(); 
            }
            LOG.info("Update task heartbeat:" + info);
        } catch (TException e) {
            LOG.error("Failed to update task heartbeat info", e);
            if (client != null) {
                client.close();
                client = NimbusClient.getConfiguredClient(conf);
            }
        }
    }
}
