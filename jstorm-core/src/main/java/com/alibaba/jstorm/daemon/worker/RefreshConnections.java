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

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.scheduler.WorkerSlot;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.Assignment.AssignmentType;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.task.Task;
import com.alibaba.jstorm.task.TaskShutdownDameon;
import com.alibaba.jstorm.task.heartbeat.TaskHeartbeat;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;

/**
 * 
 * Update current worker and other workers' zeroMQ connection.
 * 
 * When worker shutdown/create, need update these connection
 * 
 * @author yannian/Longda
 * 
 */
public class RefreshConnections extends RunnableCallback {
    private static Logger LOG = LoggerFactory
            .getLogger(RefreshConnections.class);

    private WorkerData workerData;

    private AtomicBoolean shutdown;

    @SuppressWarnings("rawtypes")
    private Map conf;

    private StormClusterState zkCluster;

    private String topologyId;

    private Set<Integer> outboundTasks;

    private ConcurrentHashMap<WorkerSlot, IConnection> nodeportSocket;

    private IContext context;

    private ConcurrentHashMap<Integer, WorkerSlot> taskNodeport;

    private Integer frequence;

    private String supervisorId;

    private int taskTimeoutSecs;

    // private ReentrantReadWriteLock endpoint_socket_lock;

    @SuppressWarnings("rawtypes")
    public RefreshConnections(WorkerData workerData) {

        this.workerData = workerData;

        this.shutdown = workerData.getShutdown();
        this.conf = workerData.getStormConf();
        this.zkCluster = workerData.getZkCluster();
        this.topologyId = workerData.getTopologyId();
        this.outboundTasks = workerData.getOutboundTasks();
        this.nodeportSocket = workerData.getNodeportSocket();
        this.context = workerData.getContext();
        this.taskNodeport = workerData.getTaskNodeport();
        this.supervisorId = workerData.getSupervisorId();

        // this.endpoint_socket_lock = endpoint_socket_lock;
        frequence =
                JStormUtils
                        .parseInt(conf.get(Config.TASK_REFRESH_POLL_SECS), 5);

        taskTimeoutSecs =
                JStormUtils.parseInt(
                        conf.get(Config.TASK_HEARTBEAT_FREQUENCY_SECS), 10);
        taskTimeoutSecs = taskTimeoutSecs * 3;
    }

    @Override
    public void run() {

        try {
            //
            // @@@ does lock need?
            // endpoint_socket_lock.writeLock().lock();
            //

            synchronized (this) {
                Assignment assignment =
                        zkCluster.assignment_info(topologyId, this);
                if (assignment == null) {
                    String errMsg = "Failed to get Assignment of " + topologyId;
                    LOG.error(errMsg);
                    // throw new RuntimeException(errMsg);
                    return;
                }

                // Compare the assignment timestamp of
                // "jstorm_home/data/supervisor/topo-id/timestamp"
                // with one in workerData to check if the topology code is
                // updated. If so, the outbound
                // task map should be updated accordingly.
                try {
                    Long localAssignmentTS =
                            StormConfig.read_supervisor_topology_timestamp(
                                    conf, topologyId);
                    if (localAssignmentTS.longValue() > workerData
                            .getAssignmentTs().longValue()) {
                        try {
                            if (assignment.getAssignmentType() == AssignmentType.Config) {
                                LOG.info("Get config reload request for " + topologyId);
                                // If config was updated, notify all tasks
                                List<TaskShutdownDameon> taskShutdowns = workerData.getShutdownTasks();
                                Map newConf = StormConfig.read_supervisor_topology_conf(conf, topologyId);
                                workerData.getStormConf().putAll(newConf);
                                for (TaskShutdownDameon taskSD : taskShutdowns) {
                                    taskSD.updateConf(newConf);
                                }
                                workerData.setAssignmentType(AssignmentType.Config);
                            } else {
                                Set<Integer> addedTasks = getAddedTasks(assignment);
                                Set<Integer> removedTasks =
                                        getRemovedTasks(assignment);
                                
                                workerData.updateWorkerData(assignment);
                                
                                if (removedTasks.size() > 0)
                                    shutdownTasks(removedTasks);
                                if (addedTasks.size() > 0)
                                    createTasks(addedTasks);
                                
                                Set<Integer> tmpOutboundTasks =
                                        Worker.worker_output_tasks(workerData);
                                if (outboundTasks.equals(tmpOutboundTasks) == false) {
                                    for (int taskId : tmpOutboundTasks) {
                                        if (outboundTasks.contains(taskId) == false)
                                            workerData
                                                    .addOutboundTaskStatusIfAbsent(taskId);
                                    }
                                    for (int taskId : workerData
                                            .getOutboundTaskStatus().keySet()) {
                                        if (tmpOutboundTasks.contains(taskId) == false) {
                                            workerData
                                                    .removeOutboundTaskStatus(taskId);
                                        }
                                    }
                                    workerData.setOutboundTasks(tmpOutboundTasks);
                                    outboundTasks = tmpOutboundTasks;
                                }
                                workerData.setAssignmentType(AssignmentType.Assign);
                            }

                            // If everything is OK, update the assignment TS.
                            // Then the tasks is
                            // going to update the related data.
                            workerData.setAssignmentTs(localAssignmentTS);
                        } catch (Exception e) {
                            LOG.warn("Failed to update worker data", e);
                        }
                    }

                } catch (FileNotFoundException e) {
                    LOG.warn(
                            "Failed to read supervisor topology timeStamp for "
                                    + topologyId + " port="
                                    + workerData.getPort(), e);
                }

                Set<ResourceWorkerSlot> workers = assignment.getWorkers();
                if (workers == null) {
                    String errMsg =
                            "Failed to get taskToResource of " + topologyId;
                    LOG.error(errMsg);
                    return;
                }
                workerData.getWorkerToResource().addAll(workers);

                Map<Integer, WorkerSlot> my_assignment =
                        new HashMap<Integer, WorkerSlot>();

                Map<String, String> node = assignment.getNodeHost();

                // only reserve outboundTasks
                Set<WorkerSlot> need_connections = new HashSet<WorkerSlot>();

                Set<Integer> localTasks = new HashSet<Integer>();

                if (workers != null && outboundTasks != null) {
                    for (ResourceWorkerSlot worker : workers) {
                        if (supervisorId.equals(worker.getNodeId())
                                && worker.getPort() == workerData.getPort())
                            localTasks.addAll(worker.getTasks());
                        for (Integer id : worker.getTasks()) {
                            if (outboundTasks.contains(id)) {
                                my_assignment.put(id, worker);
                                need_connections.add(worker);
                            }
                        }
                    }
                }
                taskNodeport.putAll(my_assignment);
                workerData.setLocalTasks(localTasks);

                // get which connection need to be remove or add
                Set<WorkerSlot> current_connections = nodeportSocket.keySet();
                Set<WorkerSlot> new_connections = new HashSet<WorkerSlot>();
                Set<WorkerSlot> remove_connections = new HashSet<WorkerSlot>();

                for (WorkerSlot node_port : need_connections) {
                    if (!current_connections.contains(node_port)) {
                        new_connections.add(node_port);
                    }
                }

                for (WorkerSlot node_port : current_connections) {
                    if (!need_connections.contains(node_port)) {
                        remove_connections.add(node_port);
                    }
                }

                // create new connection
                for (WorkerSlot nodePort : new_connections) {

                    String host = node.get(nodePort.getNodeId());

                    int port = nodePort.getPort();

                    IConnection conn = context.connect(topologyId, host, port);

                    nodeportSocket.put(nodePort, conn);

                    LOG.info("Add connection to " + nodePort);
                }

                // close useless connection
                for (WorkerSlot node_port : remove_connections) {
                    LOG.info("Remove connection to " + node_port);
                    nodeportSocket.remove(node_port).close();
                }

                // Update the status of all outbound tasks
                for (Integer taskId : outboundTasks) {
                    boolean isActive = false;
                    int currentTime = TimeUtils.current_time_secs();
                    TaskHeartbeat tHB =
                            zkCluster.task_heartbeat(topologyId, taskId);
                    if (tHB != null) {
                        int taskReportTime = tHB.getTimeSecs();
                        if ((currentTime - taskReportTime) < taskTimeoutSecs)
                            isActive = true;
                    }
                    workerData.updateOutboundTaskStatus(taskId, isActive);
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to refresh worker Connection", e);
            throw new RuntimeException(e);
        }

        // finally {
        // endpoint_socket_lock.writeLock().unlock();
        // }

    }

    @Override
    public Object getResult() {
        return frequence;
    }

    private Set<Integer> getAddedTasks(Assignment assignment) {
        Set<Integer> ret = new HashSet<Integer>();
        try {
            Set<Integer> taskIds =
                    assignment.getCurrentWorkerTasks(
                            workerData.getSupervisorId(), workerData.getPort());
            for (Integer taskId : taskIds) {
                if (!(workerData.getTaskids().contains(taskId)))
                    ret.add(taskId);
            }
        } catch (Exception e) {
            LOG.warn("Failed to get added task list for"
                    + workerData.getTopologyId());
            ;
        }
        return ret;
    }

    private Set<Integer> getRemovedTasks(Assignment assignment) {
        Set<Integer> ret = new HashSet<Integer>();
        try {
            Set<Integer> taskIds =
                    assignment.getCurrentWorkerTasks(
                            workerData.getSupervisorId(), workerData.getPort());
            for (Integer taskId : workerData.getTaskids()) {
                if (!(taskIds.contains(taskId)))
                    ret.add(taskId);
            }
        } catch (Exception e) {
            LOG.warn("Failed to get removed task list for"
                    + workerData.getTopologyId());
            ;
        }
        return ret;
    }

    private void createTasks(Set<Integer> tasks) {
        for (Integer taskId : tasks) {
            try {
                TaskShutdownDameon shutdown = Task.mk_task(workerData, taskId);
                workerData.addShutdownTask(shutdown);
            } catch (Exception e) {
                LOG.error("Failed to create task-" + taskId, e);
            }
        }
    }

    private void shutdownTasks(Set<Integer> tasks) {
        for (Integer taskId : tasks) {
            try {
                List<TaskShutdownDameon> shutdowns =
                        workerData.getShutdownDaemonbyTaskIds(tasks);
                for (TaskShutdownDameon shutdown : shutdowns) {
                    shutdown.shutdown();
                }
            } catch (Exception e) {
                LOG.error("Failed to shutdown task-" + taskId, e);
            }
        }
    }

}
