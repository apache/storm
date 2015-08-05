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

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;

/**
 * Assignment of one Toplogy, stored in /ZK-DIR/assignments/{topologyid}
 * nodeHost {supervisorid: hostname} -- assigned supervisor Map
 * taskStartTimeSecs: {taskid, taskStartSeconds} masterCodeDir: topology source
 * code's dir in Nimbus taskToResource: {taskid, ResourceAssignment}
 * 
 * @author Lixin/Longda
 */
public class Assignment implements Serializable {
    public enum AssignmentType {
        Assign, Config
    }

    private static final long serialVersionUID = 6087667851333314069L;

    private final String masterCodeDir;
    /**
     * @@@ nodeHost store <supervisorId, hostname>, this will waste some zk
     *     storage
     */
    private final Map<String, String> nodeHost;
    private final Map<Integer, Integer> taskStartTimeSecs;
    private final Set<ResourceWorkerSlot> workers;

    private long timeStamp;
    
    private AssignmentType type;

    public Assignment() {
        masterCodeDir = null;
        this.nodeHost = new HashMap<String, String>();
        this.taskStartTimeSecs = new HashMap<Integer, Integer>();
        this.workers = new HashSet<ResourceWorkerSlot>();
        this.timeStamp = System.currentTimeMillis();
        this.type = AssignmentType.Assign;
    }
    
    public Assignment(String masterCodeDir, Set<ResourceWorkerSlot> workers,
            Map<String, String> nodeHost,
            Map<Integer, Integer> taskStartTimeSecs) {
        this.workers = workers;
        this.nodeHost = nodeHost;
        this.taskStartTimeSecs = taskStartTimeSecs;
        this.masterCodeDir = masterCodeDir;
        this.timeStamp = System.currentTimeMillis();
        this.type = AssignmentType.Assign;
    }

    public void setAssignmentType(AssignmentType type) {
        this.type = type;
    }
    
    public AssignmentType getAssignmentType() {
        return type;
    }
    
    public Map<String, String> getNodeHost() {
        return nodeHost;
    }

    public Map<Integer, Integer> getTaskStartTimeSecs() {
        return taskStartTimeSecs;
    }

    public String getMasterCodeDir() {
        return masterCodeDir;
    }

    public Set<ResourceWorkerSlot> getWorkers() {
        return workers;
    }

    /**
     * find workers for every supervisorId (node)
     * 
     * @param supervisorId
     * @return Map<Integer, WorkerSlot>
     */
    public Map<Integer, ResourceWorkerSlot> getTaskToNodePortbyNode(
            String supervisorId) {

        Map<Integer, ResourceWorkerSlot> result =
                new HashMap<Integer, ResourceWorkerSlot>();
        for (ResourceWorkerSlot worker : workers) {
            if (worker.getNodeId().equals(supervisorId)) {
                result.put(worker.getPort(), worker);
            }
        }
        return result;
    }

    public Set<Integer> getCurrentSuperviosrTasks(String supervisorId) {
        Set<Integer> Tasks = new HashSet<Integer>();

        for (ResourceWorkerSlot worker : workers) {
            if (worker.getNodeId().equals(supervisorId))
                Tasks.addAll(worker.getTasks());
        }

        return Tasks;
    }

    public Set<Integer> getCurrentSuperviosrWorkers(String supervisorId) {
        Set<Integer> workerSet = new HashSet<Integer>();

        for (ResourceWorkerSlot worker : workers) {
            if (worker.getNodeId().equals(supervisorId))
                workerSet.add(worker.getPort());
        }

        return workerSet;
    }

    public Set<Integer> getCurrentWorkerTasks(String supervisorId, int port) {

        for (ResourceWorkerSlot worker : workers) {
            if (worker.getNodeId().equals(supervisorId)
                    && worker.getPort() == port)
                return worker.getTasks();
        }

        return new HashSet<Integer>();
    }

    public ResourceWorkerSlot getWorkerByTaskId(Integer taskId) {
        for (ResourceWorkerSlot worker : workers) {
            if (worker.getTasks().contains(taskId))
                return worker;
        }
        return null;
    }

    public long getTimeStamp() {
        return this.timeStamp;
    }

    public void updateTimeStamp() {
        timeStamp = System.currentTimeMillis();
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result =
                prime
                        * result
                        + ((masterCodeDir == null) ? 0 : masterCodeDir
                                .hashCode());
        result =
                prime * result + ((nodeHost == null) ? 0 : nodeHost.hashCode());
        result =
                prime
                        * result
                        + ((taskStartTimeSecs == null) ? 0 : taskStartTimeSecs
                                .hashCode());
        result = prime * result + ((workers == null) ? 0 : workers.hashCode());
        result = prime * result + (int) (timeStamp & 0xFFFFFFFF);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Assignment other = (Assignment) obj;
        if (masterCodeDir == null) {
            if (other.masterCodeDir != null)
                return false;
        } else if (!masterCodeDir.equals(other.masterCodeDir))
            return false;
        if (nodeHost == null) {
            if (other.nodeHost != null)
                return false;
        } else if (!nodeHost.equals(other.nodeHost))
            return false;
        if (taskStartTimeSecs == null) {
            if (other.taskStartTimeSecs != null)
                return false;
        } else if (!taskStartTimeSecs.equals(other.taskStartTimeSecs))
            return false;
        if (workers == null) {
            if (other.workers != null)
                return false;
        } else if (!workers.equals(other.workers))
            return false;
        if (timeStamp != other.timeStamp)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this,
                ToStringStyle.SHORT_PREFIX_STYLE);
    }

}
