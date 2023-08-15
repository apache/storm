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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;

public class WorkerUserContext extends WorkerTopologyContext {

    public WorkerUserContext(
        StormTopology topology,
        Map<String, Object> topoConf, Map<Integer,
        String> taskToComponent,
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
        AtomicReference<Map<String, String>> nodeToHost) {
        super(topology, topoConf, taskToComponent, componentToSortedTasks, componentToStreamToFields, stormId, codeDir, pidDir, workerPort,
                workerTasks, defaultResources, userResources, taskToNodePort, assignmentId, nodeToHost);
    }

    /**
     * Sets the worker-level data for the given name. This data can then be read by all components running on the same worker,
     * i.e. tasks (spouts, bolts), task hooks and worker hooks.
     *
     * @param name  name of the worker-level data to be set
     * @param data  worker-level data
     */
    public void setResource(String name, Object data) {
        userResources.put(name, data);
    }
}
