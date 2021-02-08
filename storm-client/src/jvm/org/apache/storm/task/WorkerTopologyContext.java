/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.task;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;

public class WorkerTopologyContext extends GeneralTopologyContext {
    public static final String SHARED_EXECUTOR = "executor";
    Map<String, Object> userResources;
    Map<String, Object> defaultResources;
    private Integer workerPort;
    private List<Integer> workerTasks;
    private String codeDir;
    private String pidDir;
    private AtomicReference<Map<Integer, NodeInfo>> taskToNodePort;
    private String assignmentId;
    private final AtomicReference<Map<String, String>> nodeToHost;

    public WorkerTopologyContext(
        StormTopology topology,
        Map<String, Object> topoConf,
        Map<Integer, String> taskToComponent,
        Map<String, List<Integer>> componentToSortedTasks,
        Map<String, Map<String, Fields>> componentToStreamToFields,
        String stormId,
        String codeDir,
        String pidDir,
        Integer workerPort,
        List<Integer> workerTasks,
        Map<String, Object> defaultResources,
        Map<String, Object> userResources,
        AtomicReference<Map<Integer, NodeInfo>> taskToNodePort,
        String assignmentId,
        AtomicReference<Map<String, String>> nodeToHost
    ) {
        super(topology, topoConf, taskToComponent, componentToSortedTasks, componentToStreamToFields, stormId);
        this.codeDir = codeDir;
        this.defaultResources = defaultResources;
        this.userResources = userResources;
        try {
            if (pidDir != null) {
                this.pidDir = new File(pidDir).getCanonicalPath();
            } else {
                this.pidDir = null;
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not get canonical path for " + this.pidDir, e);
        }
        this.workerPort = workerPort;
        this.workerTasks = workerTasks;
        this.taskToNodePort = taskToNodePort;
        this.assignmentId = assignmentId;
        this.nodeToHost = nodeToHost;

    }

    public WorkerTopologyContext(
        StormTopology topology,
        Map<String, Object> topoConf,
        Map<Integer, String> taskToComponent,
        Map<String, List<Integer>> componentToSortedTasks,
        Map<String, Map<String, Fields>> componentToStreamToFields,
        String stormId,
        String codeDir,
        String pidDir,
        Integer workerPort,
        List<Integer> workerTasks,
        Map<String, Object> defaultResources,
        Map<String, Object> userResources) {
        this(topology, topoConf, taskToComponent, componentToSortedTasks, componentToStreamToFields, stormId,
             codeDir, pidDir, workerPort, workerTasks, defaultResources, userResources, null, null, null);
    }

    /**
     * Gets all the task ids that are running in this worker process (including the task for this task).
     */
    public List<Integer> getThisWorkerTasks() {
        return workerTasks;
    }

    public Integer getThisWorkerPort() {
        return workerPort;
    }

    public String getAssignmentId() {
        return assignmentId;
    }

    /**
     * Get a map from task Id to NodePort.
     *
     * @return a map from task To NodePort
     */
    public AtomicReference<Map<Integer, NodeInfo>> getTaskToNodePort() {
        return taskToNodePort;
    }

    /**
     * Get a map from nodeId to hostname.
     * @return a map from nodeId to hostname
     */
    public AtomicReference<Map<String, String>> getNodeToHost() {
        return nodeToHost;
    }

    /**
     * Gets the location of the external resources for this worker on the local filesystem. These external resources typically include bolts
     * implemented in other languages, such as Ruby or Python.
     */
    public String getCodeDir() {
        return codeDir;
    }

    /**
     * If this task spawns any subprocesses, those subprocesses must immediately write their PID to this directory on the local filesystem
     * to ensure that Storm properly destroys that process when the worker is shutdown.
     */
    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    public String getPIDDir() {
        return pidDir;
    }

    public Object getResource(String name) {
        return userResources.get(name);
    }

    public ExecutorService getSharedExecutor() {
        return (ExecutorService) defaultResources.get(SHARED_EXECUTOR);
    }
}
