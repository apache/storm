/*
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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.storm.generated.WorkerResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulerAssignmentImpl implements SchedulerAssignment {
    private static final Logger LOG = LoggerFactory.getLogger(SchedulerAssignmentImpl.class);
    private static Function<WorkerSlot, Collection<ExecutorDetails>> MAKE_LIST = (k) -> new LinkedList<>();

    /**
     * topology-id this assignment is for.
     */
    private final String topologyId;

    /**
     * assignment detail, a mapping from executor to <code>WorkerSlot</code>.
     */
    private final Map<ExecutorDetails, WorkerSlot> executorToSlot = new HashMap<>();
    private final Map<WorkerSlot, WorkerResources> resources = new HashMap<>();
    private final Map<String, Double> nodeIdToTotalSharedOffHeapNode = new HashMap<>();
    private final Map<WorkerSlot, Collection<ExecutorDetails>> slotToExecutors = new HashMap<>();

    /**
     * Create a new assignment.
     *
     * @param topologyId                 the id of the topology the assignment is for.
     * @param executorToSlot             the executor to slot mapping for the assignment.  Can be null and set through other methods later.
     * @param resources                  the resources for the current assignments.  Can be null and set through other methods later.
     * @param nodeIdToTotalSharedOffHeap the shared memory for this assignment can be null and set through other methods later.
     */
    public SchedulerAssignmentImpl(String topologyId, Map<ExecutorDetails, WorkerSlot> executorToSlot,
                                   Map<WorkerSlot, WorkerResources> resources, Map<String, Double> nodeIdToTotalSharedOffHeap) {
        this.topologyId = topologyId;
        if (executorToSlot != null) {
            if (executorToSlot.entrySet().stream().anyMatch((entry) -> entry.getKey() == null || entry.getValue() == null)) {
                throw new RuntimeException("Cannot create a scheduling with a null in it " + executorToSlot);
            }
            this.executorToSlot.putAll(executorToSlot);
            for (Map.Entry<ExecutorDetails, WorkerSlot> entry : executorToSlot.entrySet()) {
                slotToExecutors.computeIfAbsent(entry.getValue(), MAKE_LIST).add(entry.getKey());
            }
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
            this.nodeIdToTotalSharedOffHeapNode.putAll(nodeIdToTotalSharedOffHeap);
        }
    }

    public SchedulerAssignmentImpl(String topologyId) {
        this(topologyId, null, null, null);
    }

    public SchedulerAssignmentImpl(SchedulerAssignment assignment) {
        this(assignment.getTopologyId(), assignment.getExecutorToSlot(),
             assignment.getScheduledResources(), assignment.getNodeIdToTotalSharedOffHeapNodeMemory());
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " topo: " + topologyId + " execToSlots: " + executorToSlot;
    }

    /**
     * Like the equals command, but ignores the resources.
     *
     * @param other the object to check for equality against.
     * @return true if they are equal, ignoring resources, else false.
     */
    public boolean equalsIgnoreResources(Object other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof SchedulerAssignmentImpl)) {
            return false;
        }
        SchedulerAssignmentImpl o = (SchedulerAssignmentImpl) other;

        return topologyId.equals(o.topologyId)
               && executorToSlot.equals(o.executorToSlot);
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

        return resources.equals(o.resources)
               && nodeIdToTotalSharedOffHeapNode.equals(o.nodeIdToTotalSharedOffHeapNode);
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
        assert slot != null;
        for (ExecutorDetails executor : executors) {
            this.executorToSlot.put(executor, slot);
        }
        slotToExecutors.computeIfAbsent(slot, MAKE_LIST)
            .addAll(executors);
        if (slotResources != null) {
            resources.put(slot, slotResources);
        } else {
            resources.remove(slot);
        }
    }

    /**
     * Release the slot occupied by this assignment.
     */
    public void unassignBySlot(WorkerSlot slot) {
        Collection<ExecutorDetails> executors = slotToExecutors.remove(slot);

        // remove
        if (executors != null) {
            for (ExecutorDetails executor : executors) {
                executorToSlot.remove(executor);
            }
        }

        resources.remove(slot);

        String node = slot.getNodeId();
        boolean isFound = false;
        for (WorkerSlot ws : executorToSlot.values()) {
            if (node.equals(ws.getNodeId())) {
                isFound = true;
                break;
            }
        }
        if (!isFound) {
            nodeIdToTotalSharedOffHeapNode.remove(node);
        }
    }

    @Override
    public boolean isSlotOccupied(WorkerSlot slot) {
        return this.slotToExecutors.containsKey(slot);
    }

    @Override
    public boolean isExecutorAssigned(ExecutorDetails executor) {
        return this.executorToSlot.containsKey(executor);
    }

    @Override
    public String getTopologyId() {
        return this.topologyId;
    }

    @Override
    public Map<ExecutorDetails, WorkerSlot> getExecutorToSlot() {
        return this.executorToSlot;
    }

    @Override
    public Set<ExecutorDetails> getExecutors() {
        return this.executorToSlot.keySet();
    }

    @Override
    public Map<WorkerSlot, Collection<ExecutorDetails>> getSlotToExecutors() {
        return slotToExecutors;
    }

    @Override
    public Map<WorkerSlot, WorkerResources> getScheduledResources() {
        return resources;
    }

    public void setTotalSharedOffHeapNodeMemory(String node, double value) {
        nodeIdToTotalSharedOffHeapNode.put(node, value);
    }

    @Override
    public Map<String, Double> getNodeIdToTotalSharedOffHeapNodeMemory() {
        return nodeIdToTotalSharedOffHeapNode;
    }
}
