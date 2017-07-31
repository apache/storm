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

package org.apache.storm.scheduler.resource;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Represents a single node in the cluster. */
public class RAS_Node {
    private static final Logger LOG = LoggerFactory.getLogger(RAS_Node.class);

    //A map consisting of all workers on the node.
    //The key of the map is the worker id and the value is the corresponding workerslot object
    private Map<String, WorkerSlot> slots = new HashMap<>();

    // A map describing which topologies are using which slots on this node.  The format of the map is the following:
    // {TopologyId -> {WorkerId -> {Executors}}}
    private Map<String, Map<String, Collection<ExecutorDetails>>> topIdToUsedSlots = new HashMap<>();

    private final double totalMemory;
    private final double totalCpu;
    private final String nodeId;
    private String hostname;
    private boolean isAlive;
    private SupervisorDetails sup;
    private final Cluster cluster;
    private final Set<WorkerSlot> originallyFreeSlots;

    public RAS_Node(
        String nodeId,
        SupervisorDetails sup,
        Cluster cluster,
        Map<String, WorkerSlot> workerIdToWorker,
        Map<String, Map<String, Collection<ExecutorDetails>>> assignmentMap) {
        //Node ID and supervisor ID are the same.
        this.nodeId = nodeId;
        if (sup == null) {
            isAlive = false;
        } else {
            isAlive = !cluster.isBlackListed(this.nodeId);
        }

        this.cluster = cluster;

        // initialize slots for this node
        if (workerIdToWorker != null) {
            slots = workerIdToWorker;
        }

        //initialize assignment map
        if (assignmentMap != null) {
            topIdToUsedSlots = assignmentMap;
        }

        //check if node is alive
        if (isAlive && sup != null) {
            hostname = sup.getHost();
            this.sup = sup;
        }

        totalMemory = isAlive ? getTotalMemoryResources() : 0.0;
        totalCpu = isAlive ? getTotalCpuResources() : 0.0;
        HashSet<String> freeById = new HashSet<>(slots.keySet());
        if (assignmentMap != null) {
            for (Map<String, Collection<ExecutorDetails>> assignment : assignmentMap.values()) {
                freeById.removeAll(assignment.keySet());
            }
        }
        originallyFreeSlots = new HashSet<>();
        for (WorkerSlot slot : slots.values()) {
            if (freeById.contains(slot.getId())) {
                originallyFreeSlots.add(slot);
            }
        }
    }

    public String getId() {
        return nodeId;
    }

    public String getHostname() {
        return hostname;
    }

    private Collection<WorkerSlot> workerIdsToWorkers(Collection<String> workerIds) {
        Collection<WorkerSlot> ret = new LinkedList<WorkerSlot>();
        for (String workerId : workerIds) {
            ret.add(slots.get(workerId));
        }
        return ret;
    }

    public Collection<String> getFreeSlotsId() {
        if (!isAlive) {
            return new HashSet<String>();
        }
        Collection<String> usedSlotsId = getUsedSlotsId();
        Set<String> ret = new HashSet<>();
        ret.addAll(slots.keySet());
        ret.removeAll(usedSlotsId);
        return ret;
    }

    public Collection<WorkerSlot> getSlotsAvailbleTo(TopologyDetails td) {
        //Try to reuse a slot if possible....
        HashSet<WorkerSlot> ret = new HashSet<>();
        Map<String, Collection<ExecutorDetails>> assigned = topIdToUsedSlots.get(td.getId());
        if (assigned != null) {
            ret.addAll(workerIdsToWorkers(assigned.keySet()));
        }
        ret.addAll(getFreeSlots());
        ret.retainAll(
            originallyFreeSlots); //RAS does not let you move things or modify existing assignments
        return ret;
    }

    public Collection<WorkerSlot> getFreeSlots() {
        return workerIdsToWorkers(getFreeSlotsId());
    }

    private Collection<String> getUsedSlotsId() {
        Collection<String> ret = new LinkedList<String>();
        for (Map<String, Collection<ExecutorDetails>> entry : topIdToUsedSlots.values()) {
            ret.addAll(entry.keySet());
        }
        return ret;
    }

    public Collection<WorkerSlot> getUsedSlots() {
        return workerIdsToWorkers(getUsedSlotsId());
    }

    public Collection<WorkerSlot> getUsedSlots(String topId) {
        Collection<WorkerSlot> ret = null;
        if (topIdToUsedSlots.get(topId) != null) {
            ret = workerIdsToWorkers(topIdToUsedSlots.get(topId).keySet());
        }
        return ret;
    }

    public boolean isAlive() {
        return isAlive;
    }

    /** Get a collection of the topology ids currently running on this node. */
    public Collection<String> getRunningTopologies() {
        return topIdToUsedSlots.keySet();
    }

    public boolean isTotallyFree() {
        return getUsedSlots().isEmpty();
    }

    public int totalSlotsFree() {
        return getFreeSlots().size();
    }

    public int totalSlotsUsed() {
        return getUsedSlots().size();
    }

    public int totalSlotsUsed(String topId) {
        return getUsedSlots(topId).size();
    }

    public int totalSlots() {
        return slots.size();
    }

    /** Free all slots on this node. This will update the Cluster too. */
    public void freeAllSlots() {
        if (!isAlive) {
            LOG.warn("Freeing all slots on a dead node {} ", nodeId);
        }
        cluster.freeSlots(slots.values());
        //clearing assignments
        topIdToUsedSlots.clear();
    }

    /**
     * frees a single executor.
     *
     * @param exec is the executor to free
     * @param topo the topology the executor is a part of
     */
    public void freeSingleExecutor(ExecutorDetails exec, TopologyDetails topo) {
        Map<String, Collection<ExecutorDetails>> usedSlots = topIdToUsedSlots.get(topo.getId());
        if (usedSlots == null) {
            throw new IllegalArgumentException("Topology " + topo + " is not assigned");
        }
        WorkerSlot ws = null;
        Set<ExecutorDetails> updatedAssignment = new HashSet<>();
        for (Entry<String, Collection<ExecutorDetails>> entry : usedSlots.entrySet()) {
            if (entry.getValue().contains(exec)) {
                ws = slots.get(entry.getKey());
                updatedAssignment.addAll(entry.getValue());
                updatedAssignment.remove(exec);
                break;
            }
        }

        if (ws == null) {
            throw new IllegalArgumentException(
                "Executor " + exec + " is not assinged on this node to " + topo);
        }
        free(ws);
        if (!updatedAssignment.isEmpty()) {
            assign(ws, topo, updatedAssignment);
        }
    }

    /**
     * Frees a single slot in this node.
     *
     * @param ws the slot to free
     */
    public void free(WorkerSlot ws) {
        LOG.debug("freeing WorkerSlot {} on node {}", ws, hostname);
        if (!slots.containsKey(ws.getId())) {
            throw new IllegalArgumentException(
                "Tried to free a slot " + ws + " that was not" + " part of this node " + nodeId);
        }

        TopologyDetails topo = findTopologyUsingWorker(ws);
        if (topo == null) {
            throw new IllegalArgumentException("Tried to free a slot " + ws + " that was already free!");
        }

        //free slot
        cluster.freeSlot(ws);
        //cleanup internal assignments
        topIdToUsedSlots.get(topo.getId()).remove(ws.getId());
    }

    /**
     * Find a which topology is running on a worker slot.
     *
     * @return the topology using the worker slot. If worker slot is free then return null
     */
    private TopologyDetails findTopologyUsingWorker(WorkerSlot ws) {
        for (Entry<String, Map<String, Collection<ExecutorDetails>>> entry :
            topIdToUsedSlots.entrySet()) {
            String topoId = entry.getKey();
            Set<String> workerIds = entry.getValue().keySet();
            for (String workerId : workerIds) {
                if (ws.getId().equals(workerId)) {
                    return cluster.getTopologies().getById(topoId);
                }
            }
        }
        return null;
    }

    /**
     * Assigns a worker to a node.
     *
     * @param target the worker slot to assign the executors
     * @param td the topology the executors are from
     * @param executors executors to assign to the specified worker slot
     */
    public void assign(WorkerSlot target, TopologyDetails td, Collection<ExecutorDetails> executors) {
        if (!isAlive) {
            throw new IllegalStateException("Trying to adding to a dead node " + nodeId);
        }
        Collection<WorkerSlot> freeSlots = getFreeSlots();
        if (freeSlots.isEmpty()) {
            throw new IllegalStateException("Trying to assign to a full node " + nodeId);
        }
        if (executors.size() == 0) {
            LOG.warn("Trying to assign nothing from " + td.getId() + " to " + nodeId + " (Ignored)");
        }
        if (target == null) {
            target = getFreeSlots().iterator().next();
        }
        if (!freeSlots.contains(target)) {
            throw new IllegalStateException(
                "Trying to assign already used slot " + target.getPort() + " on node " + nodeId);
        }
        LOG.debug("target slot: {}", target);

        cluster.assign(target, td.getId(), executors);

        //assigning internally
        if (!topIdToUsedSlots.containsKey(td.getId())) {
            topIdToUsedSlots.put(td.getId(), new HashMap<String, Collection<ExecutorDetails>>());
        }

        if (!topIdToUsedSlots.get(td.getId()).containsKey(target.getId())) {
            topIdToUsedSlots.get(td.getId()).put(target.getId(), new LinkedList<ExecutorDetails>());
        }
        topIdToUsedSlots.get(td.getId()).get(target.getId()).addAll(executors);
    }

    public void assignSingleExecutor(WorkerSlot ws, ExecutorDetails exec, TopologyDetails td) {
        if (!isAlive) {
            throw new IllegalStateException("Trying to adding to a dead node " + nodeId);
        }
        Collection<WorkerSlot> freeSlots = getFreeSlots();
        Set<ExecutorDetails> toAssign = new HashSet<>();
        toAssign.add(exec);
        if (!freeSlots.contains(ws)) {
            Map<String, Collection<ExecutorDetails>> usedSlots = topIdToUsedSlots.get(td.getId());
            if (usedSlots == null) {
                throw new IllegalArgumentException(
                    "Slot " + ws + " is not availble to schedue " + exec + " on");
            }
            Collection<ExecutorDetails> alreadyHere = usedSlots.get(ws.getId());
            if (alreadyHere == null) {
                throw new IllegalArgumentException(
                    "Slot " + ws + " is not availble to schedue " + exec + " on");
            }
            toAssign.addAll(alreadyHere);
            free(ws);
        }
        assign(ws, td, toAssign);
    }

    /**
     * Would scheduling exec in ws fit with the current resource constraints.
     *
     * @param ws the slot to possibly put exec in
     * @param exec the executor to possibly place in ws
     * @param td the topology exec is a part of
     * @return true if it would fit else false
     */
    public boolean wouldFit(WorkerSlot ws, ExecutorDetails exec, TopologyDetails td) {
        if (!nodeId.equals(ws.getNodeId())) {
            throw new IllegalStateException("Slot " + ws + " is not a part of this node " + nodeId);
        }
        return isAlive
            && cluster.wouldFit(
            ws,
            exec,
            td,
            td.getTopologyWorkerMaxHeapSize(),
            getAvailableMemoryResources(),
            getAvailableCpuResources());
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof RAS_Node) {
            return nodeId.equals(((RAS_Node) other).nodeId);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return nodeId.hashCode();
    }

    @Override
    public String toString() {
        return "{Node: "
            + ((sup == null) ? "null (possibly down)" : sup.getHost())
            + ", Avail [ Mem: "
            + getAvailableMemoryResources()
            + ", CPU: "
            + getAvailableCpuResources()
            + ", Slots: "
            + this.getFreeSlots()
            + "] Total [ Mem: "
            + ((sup == null) ? "N/A" : this.getTotalMemoryResources())
            + ", CPU: "
            + ((sup == null) ? "N/A" : this.getTotalCpuResources())
            + ", Slots: "
            + this.slots.values()
            + " ]}";
    }

    public static int countFreeSlotsAlive(Collection<RAS_Node> nodes) {
        int total = 0;
        for (RAS_Node n : nodes) {
            if (n.isAlive()) {
                total += n.totalSlotsFree();
            }
        }
        return total;
    }

    public static int countTotalSlotsAlive(Collection<RAS_Node> nodes) {
        int total = 0;
        for (RAS_Node n : nodes) {
            if (n.isAlive()) {
                total += n.totalSlots();
            }
        }
        return total;
    }

    /**
     * Gets the available memory resources for this node.
     *
     * @return the available memory for this node
     */
    public Double getAvailableMemoryResources() {
        double used = cluster.getScheduledMemoryForNode(nodeId);
        return totalMemory - used;
    }

    /**
     * Gets the total memory resources for this node.
     *
     * @return the total memory for this node
     */
    public Double getTotalMemoryResources() {
        if (sup != null) {
            return sup.getTotalMemory();
        } else {
            return 0.0;
        }
    }

    /**
     * Gets the available cpu resources for this node.
     *
     * @return the available cpu for this node
     */
    public double getAvailableCpuResources() {
        return totalCpu - cluster.getScheduledCpuForNode(nodeId);
    }

    /**
     * Gets the total cpu resources for this node.
     *
     * @return the total cpu for this node
     */
    public Double getTotalCpuResources() {
        if (sup != null) {
            return sup.getTotalCPU();
        } else {
            return 0.0;
        }
    }
}
