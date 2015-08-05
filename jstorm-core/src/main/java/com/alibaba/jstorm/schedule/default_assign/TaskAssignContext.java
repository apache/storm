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
package com.alibaba.jstorm.schedule.default_assign;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class TaskAssignContext {

    private final Map<String, List<ResourceWorkerSlot>> supervisorToWorker;

    private final Map<String, Set<String>> relationship;

    // Map<worker, Map<component name, assigned task num in this worker>
    private final Map<ResourceWorkerSlot, Map<String, Integer>> workerToComponentNum =
            new HashMap<ResourceWorkerSlot, Map<String, Integer>>();

    // Map<available worker, assigned task num in this worker>
    private final Map<ResourceWorkerSlot, Integer> workerToTaskNum =
            new HashMap<ResourceWorkerSlot, Integer>();

    private final Map<String, ResourceWorkerSlot> HostPortToWorkerMap =
            new HashMap<String, ResourceWorkerSlot>();

    public TaskAssignContext(
            Map<String, List<ResourceWorkerSlot>> supervisorToWorker,
            Map<String, Set<String>> relationship) {
        this.supervisorToWorker = supervisorToWorker;
        this.relationship = relationship;

        for (Entry<String, List<ResourceWorkerSlot>> entry : supervisorToWorker
                .entrySet()) {
            for (ResourceWorkerSlot worker : entry.getValue()) {
                workerToTaskNum.put(worker, 0);
                HostPortToWorkerMap.put(worker.getHostPort(), worker);
            }
        }
    }

    public Map<ResourceWorkerSlot, Integer> getWorkerToTaskNum() {
        return workerToTaskNum;
    }

    public Map<String, List<ResourceWorkerSlot>> getSupervisorToWorker() {
        return supervisorToWorker;
    }

    public Map<ResourceWorkerSlot, Map<String, Integer>> getWorkerToComponentNum() {
        return workerToComponentNum;
    }

    public Map<String, Set<String>> getRelationship() {
        return relationship;
    }

    public int getComponentNumOnSupervisor(String supervisor, String name) {
        List<ResourceWorkerSlot> workers = supervisorToWorker.get(supervisor);
        if (workers == null)
            return 0;
        int result = 0;
        for (ResourceWorkerSlot worker : workers) {
            result = result + this.getComponentNumOnWorker(worker, name);
        }
        return result;
    }

    public int getComponentNumOnWorker(ResourceWorkerSlot worker, String name) {
        int result = 0;
        Map<String, Integer> componentNum = workerToComponentNum.get(worker);
        if (componentNum != null && componentNum.get(name) != null)
            result = componentNum.get(name);
        return result;
    }

    public int getTaskNumOnSupervisor(String supervisor) {
        List<ResourceWorkerSlot> workers = supervisorToWorker.get(supervisor);
        if (workers == null)
            return 0;
        int result = 0;
        for (ResourceWorkerSlot worker : workers) {
            result = result + this.getTaskNumOnWorker(worker);
        }
        return result;
    }

    public int getTaskNumOnWorker(ResourceWorkerSlot worker) {
        return worker.getTasks() == null ? 0 : worker.getTasks().size();
    }

    public int getInputComponentNumOnSupervisor(String supervisor, String name) {
        int result = 0;
        List<ResourceWorkerSlot> workers = supervisorToWorker.get(supervisor);
        if (workers == null)
            return 0;
        for (ResourceWorkerSlot worker : workers)
            result = result + this.getInputComponentNumOnWorker(worker, name);
        return result;
    }

    public int getInputComponentNumOnWorker(ResourceWorkerSlot worker,
            String name) {
        int result = 0;
        for (String component : relationship.get(name))
            result = result + this.getComponentNumOnWorker(worker, component);
        return result;
    }

    public Map<String, ResourceWorkerSlot> getHostPortToWorkerMap() {
        return HostPortToWorkerMap;
    }

    public ResourceWorkerSlot getWorker(ResourceWorkerSlot worker) {
        return HostPortToWorkerMap.get(worker.getHostPort());
    }
}
