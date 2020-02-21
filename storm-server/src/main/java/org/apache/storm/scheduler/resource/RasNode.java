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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.storm.Config;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceOffer;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a single node in the cluster.
 */
public class RasNode implements Comparable<RasNode> {
    private static final Logger LOG = LoggerFactory.getLogger(RasNode.class);
    private final String nodeId;
    private final Cluster cluster;
    private final Set<WorkerSlot> originallyFreeSlots;
    //A map consisting of all workers on the node.
    //The key of the map is the worker id and the value is the corresponding workerslot object
    private Map<String, WorkerSlot> slots = new HashMap<>();
    // A map describing which topologies are using which slots on this node.  The format of the map is the following:
    // {TopologyId -> {WorkerId -> {Executors}}}
    private Map<String, Map<String, Collection<ExecutorDetails>>> topIdToUsedSlots = new HashMap<>();
    private String hostname;
    private boolean isAlive;
    private SupervisorDetails sup;
    private boolean loggedUnderageUsage = false;

    /**
     * Create a new node.
     * @param nodeId the id of the node.
     * @param sup the supervisor this is for.
     * @param cluster the cluster this is a part of.
     * @param workerIdToWorker the mapping of slots already assigned to this node.
     * @param assignmentMap the mapping of executors already assigned to this node.
     */
    public RasNode(
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
        Collection<WorkerSlot> ret = new LinkedList<>();
        for (String workerId : workerIds) {
            ret.add(slots.get(workerId));
        }
        return ret;
    }

    /**
     * Get the IDs of all free slots on this node.
     * @return the ids of the free slots.
     */
    public Collection<String> getFreeSlotsId() {
        if (!isAlive) {
            return new HashSet<>();
        }
        Set<String> ret = new HashSet<>(slots.keySet());
        ret.removeAll(getUsedSlotsId());
        return ret;
    }

    public Collection<WorkerSlot> getSlotsAvailableToScheduleOn() {
        return originallyFreeSlots;
    }

    public Collection<WorkerSlot> getFreeSlots() {
        return workerIdsToWorkers(getFreeSlotsId());
    }

    private Collection<String> getUsedSlotsId() {
        Collection<String> ret = new LinkedList<>();
        for (Map<String, Collection<ExecutorDetails>> entry : topIdToUsedSlots.values()) {
            ret.addAll(entry.keySet());
        }
        return ret;
    }

    public Collection<WorkerSlot> getUsedSlots() {
        return workerIdsToWorkers(getUsedSlotsId());
    }

    /**
     * Get slots used by the given topology.
     * @param topId the id of the topology to get.
     * @return the slots currently assigned to that topology on this node.
     */
    public Collection<WorkerSlot> getUsedSlots(String topId) {
        if (topIdToUsedSlots.get(topId) != null) {
            return workerIdsToWorkers(topIdToUsedSlots.get(topId).keySet());
        } else {
            return Collections.emptySet();
        }
    }

    public boolean isAlive() {
        return isAlive;
    }

    /**
     * Get a collection of the topology ids currently running on this node.
     */
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

    /**
     * Free all slots on this node. This will update the Cluster too.
     */
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
     * @param target    the worker slot to assign the executors
     * @param td        the topology the executors are from
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
        topIdToUsedSlots.computeIfAbsent(td.getId(), (tid) -> new HashMap<>())
            .computeIfAbsent(target.getId(), (tid) -> new LinkedList<>())
            .addAll(executors);
    }

    /**
     * Assign a single executor to a slot, even if other things are in the slot.
     * @param ws the slot to assign it to.
     * @param exec the executor to assign.
     * @param td the topology for the executor.
     */
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
     * @param ws   the slot to possibly put exec in
     * @param exec the executor to possibly place in ws
     * @param td   the topology exec is a part of
     * @return true if it would fit else false
     */
    public boolean wouldFit(WorkerSlot ws, ExecutorDetails exec, TopologyDetails td) {
        assert nodeId.equals(ws.getNodeId()) : "Slot " + ws + " is not a part of this node " + nodeId;
        if (!isAlive || !cluster.wouldFit(
                ws,
                exec,
                td,
                getTotalAvailableResources(),
                td.getTopologyWorkerMaxHeapSize())) {
            return false;
        }

        boolean oneExecutorPerWorker = (Boolean) td.getConf().get(Config.TOPOLOGY_RAS_ONE_EXECUTOR_PER_WORKER);
        boolean oneComponentPerWorker = (Boolean) td.getConf().get(Config.TOPOLOGY_RAS_ONE_COMPONENT_PER_WORKER);

        if (oneExecutorPerWorker) {
            return !getUsedSlots(td.getId()).contains(ws);
        }

        if (oneComponentPerWorker) {
            Set<String> components = new HashSet<>();
            Map<String, Collection<ExecutorDetails>> topologyExecutors = topIdToUsedSlots.get(td.getId());
            if (topologyExecutors != null) {
                Collection<ExecutorDetails> slotExecs = topologyExecutors.get(ws.getId());
                if (slotExecs != null) {
                    // components from WorkerSlot
                    for (ExecutorDetails slotExec : slotExecs) {
                        components.add(td.getComponentFromExecutor(slotExec));
                    }
                    // component from exec
                    components.add(td.getComponentFromExecutor(exec));
                }
            }
            return components.size() <= 1;
        }

        return true;
    }

    /**
     * Is there any possibility that exec could ever fit on this node.
     * @param exec the executor to schedule
     * @param td the topology the executor is a part of
     * @return true if there is the possibility it might fit, no guarantee that it will, or false if there is no
     *     way it would ever fit.
     */
    public boolean couldEverFit(ExecutorDetails exec, TopologyDetails td) {
        if (!isAlive) {
            return false;
        }
        NormalizedResourceOffer avail = getTotalAvailableResources();
        NormalizedResourceRequest requestedResources = td.getTotalResources(exec);
        return avail.couldFit(cluster.getMinWorkerCpu(), requestedResources);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof RasNode) {
            return nodeId.equals(((RasNode) other).nodeId);
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

    /**
     * Gets the available memory resources for this node.
     *
     * @return the available memory for this node
     */
    public double getAvailableMemoryResources() {
        return getTotalAvailableResources().getTotalMemoryMb();
    }

    /**
     * Gets total resources for this node.
     */
    public NormalizedResourceOffer getTotalResources() {
        if (sup != null) {
            return sup.getTotalResources();
        } else {
            return new NormalizedResourceOffer();
        }
    }

    /**
     * Gets all available resources for this node.
     *
     * @return All of the available resources.
     */
    public NormalizedResourceOffer getTotalAvailableResources() {
        if (sup != null) {
            NormalizedResourceOffer availableResources = new NormalizedResourceOffer(sup.getTotalResources());
            if (availableResources.remove(cluster.getAllScheduledResourcesForNode(sup.getId()), cluster.getResourceMetrics())) {
                if (!loggedUnderageUsage) {
                    LOG.error("Resources on {} became negative and was clamped to 0 {}.", hostname, availableResources);
                    loggedUnderageUsage = true;
                }
            }
            return availableResources;
        } else {
            return new NormalizedResourceOffer();
        }
    }

    /**
     * Gets the total memory resources for this node.
     *
     * @return the total memory for this node
     */
    public double getTotalMemoryResources() {
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
        return getTotalAvailableResources().getTotalCpu();
    }

    /**
     * Gets the total cpu resources for this node.
     *
     * @return the total cpu for this node
     */
    public double getTotalCpuResources() {
        if (sup != null) {
            return sup.getTotalCpu();
        } else {
            return 0.0;
        }
    }

    @Override
    public int compareTo(RasNode o) {
        return this.nodeId.compareTo(o.nodeId);
    }
}
