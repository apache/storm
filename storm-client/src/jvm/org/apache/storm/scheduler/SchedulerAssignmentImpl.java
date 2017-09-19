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
package org.apache.storm.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.generated.WorkerResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulerAssignmentImpl implements SchedulerAssignment {
    private static final Logger LOG = LoggerFactory.getLogger(SchedulerAssignmentImpl.class);

    /**
     * topology-id this assignment is for.
     */
    private final String topologyId;

    /**
     * assignment detail, a mapping from executor to <code>WorkerSlot</code>
     */
    private final Map<ExecutorDetails, WorkerSlot> executorToSlot = new HashMap<>();
    private final Map<WorkerSlot, WorkerResources> resources = new HashMap<>();
    private final Map<String, Double> nodeIdToTotalSharedOffHeap = new HashMap<>();
    //Used to cache the slotToExecutors mapping.
    private Map<WorkerSlot, Collection<ExecutorDetails>> slotToExecutorsCache = null;

    public SchedulerAssignmentImpl(String topologyId, Map<ExecutorDetails, WorkerSlot> executorToSlot,
            Map<WorkerSlot, WorkerResources> resources, Map<String, Double> nodeIdToTotalSharedOffHeap) {
        this.topologyId = topologyId;       
        if (executorToSlot != null) {
            if (executorToSlot.entrySet().stream().anyMatch((entry) -> entry.getKey() == null || entry.getValue() == null)) {
                throw new RuntimeException("Cannot create a scheduling with a null in it " + executorToSlot);
            }
            this.executorToSlot.putAll(executorToSlot);
        }
        if (resources != null) {
            if (resources.entrySet().stream().anyMatch((entry) -> entry.getKey() == null || entry.getValue() == null)) {
                throw new RuntimeException("Cannot create resources with a null in it " + resources);
            }
            this.resources.putAll(resources);
        }
        if (nodeIdToTotalSharedOffHeap != null) {
            if (nodeIdToTotalSharedOffHeap.entrySet().stream().anyMatch((entry) -> entry.getKey() == null || entry.getValue() == null)) {
                throw new RuntimeException("Cannot create off heap with a null in it " + nodeIdToTotalSharedOffHeap);
            }
            this.nodeIdToTotalSharedOffHeap.putAll(nodeIdToTotalSharedOffHeap);
        }
    }

    public SchedulerAssignmentImpl(String topologyId) {
        this(topologyId, null, null, null);
    }

    public SchedulerAssignmentImpl(SchedulerAssignment assignment) {
        this(assignment.getTopologyId(), assignment.getExecutorToSlot(), 
                assignment.getScheduledResources(), assignment.getNodeIdToTotalSharedOffHeapMemory());
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " topo: " + topologyId + " execToSlots: " + executorToSlot;
    }

    public boolean equalsIgnoreResources(Object other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof SchedulerAssignmentImpl)) {
            return false;
        }
        SchedulerAssignmentImpl o = (SchedulerAssignmentImpl) other;
        
        return this.topologyId.equals(o.topologyId) &&
                this.executorToSlot.equals(o.executorToSlot);
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((topologyId == null) ? 0 : topologyId.hashCode());
        result = prime * result + ((executorToSlot == null) ? 0 : executorToSlot.hashCode());
        return result;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!equalsIgnoreResources(other)) {
            return false;
        }
        SchedulerAssignmentImpl o = (SchedulerAssignmentImpl) other;

        return this.resources.equals(o.resources) &&
            this.nodeIdToTotalSharedOffHeap.equals(o.nodeIdToTotalSharedOffHeap);
    }
    
    @Override
    public Set<WorkerSlot> getSlots() {
        return new HashSet<>(executorToSlot.values());
    }    

    @Deprecated
    public void assign(WorkerSlot slot, Collection<ExecutorDetails> executors) {
        assign(slot, executors, null);
    }

    /**
     * Assign the slot to executors.
     */
    public void assign(WorkerSlot slot, Collection<ExecutorDetails> executors, WorkerResources slotResources) {
        assert(slot != null);
        for (ExecutorDetails executor : executors) {
            this.executorToSlot.put(executor, slot);
        }
        if (slotResources != null) {
            resources.put(slot, slotResources);
        } else {
            resources.remove(slot);
        }
        //Clear the cache scheduling changed
        slotToExecutorsCache = null;
    }

    /**
     * Release the slot occupied by this assignment.
     */
    public void unassignBySlot(WorkerSlot slot) {
        //Clear the cache scheduling is going to change
        slotToExecutorsCache = null;
        List<ExecutorDetails> executors = new ArrayList<>();
        for (ExecutorDetails executor : executorToSlot.keySet()) {
            WorkerSlot ws = executorToSlot.get(executor);
            if (ws.equals(slot)) {
                executors.add(executor);
            }
        }

        // remove
        for (ExecutorDetails executor : executors) {
            executorToSlot.remove(executor);
        }

        resources.remove(slot);

        String node = slot.getNodeId();
        boolean isFound = false;
        for (WorkerSlot ws: executorToSlot.values()) {
            if (node.equals(ws.getNodeId())) {
                isFound = true;
                break;
            }
        }
        if (!isFound) {
            nodeIdToTotalSharedOffHeap.remove(node);
        }
    }

    /**
     * @param slot
     * @return true if slot is occupied by this assignment
     */
    public boolean isSlotOccupied(WorkerSlot slot) {
        return this.executorToSlot.containsValue(slot);
    }

    public boolean isExecutorAssigned(ExecutorDetails executor) {
        return this.executorToSlot.containsKey(executor);
    }
    
    public String getTopologyId() {
        return this.topologyId;
    }

    public Map<ExecutorDetails, WorkerSlot> getExecutorToSlot() {
        return this.executorToSlot;
    }

    /**
     * @return the executors covered by this assignments
     */
    public Set<ExecutorDetails> getExecutors() {
        return this.executorToSlot.keySet();
    }

    public Map<WorkerSlot, Collection<ExecutorDetails>> getSlotToExecutors() {
        Map<WorkerSlot, Collection<ExecutorDetails>> ret = slotToExecutorsCache;
        if (ret != null) {
            return ret;
        }
        ret = new HashMap<>();
        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : executorToSlot.entrySet()) {
            ExecutorDetails exec = entry.getKey();
            WorkerSlot ws = entry.getValue();
            if (!ret.containsKey(ws)) {
                ret.put(ws, new LinkedList<ExecutorDetails>());
            }
            ret.get(ws).add(exec);
        }
        slotToExecutorsCache = ret;
        return ret;
    }

    @Override
    public Map<WorkerSlot, WorkerResources> getScheduledResources() {
        return resources;
    }

    public void setTotalSharedOffHeapMemory(String node, double value) {
        nodeIdToTotalSharedOffHeap.put(node, value);
    }
    
    @Override
    public Map<String, Double> getNodeIdToTotalSharedOffHeapMemory() {
        return nodeIdToTotalSharedOffHeap;
    }
}
