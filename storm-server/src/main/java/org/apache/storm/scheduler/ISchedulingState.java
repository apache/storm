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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.daemon.nimbus.TopologyResources;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.networktopography.DNSToSwitchMapping;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceOffer;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceRequest;

/**
 * An interface that provides access to the current scheduling state.
 * The scheduling state is not guaranteed to be thread safe.
 */
public interface ISchedulingState {

    /**
     * Get all of the topologies.
     *
     * @return all of the topologies that are a part of the cluster.
     */
    Topologies getTopologies();

    /**
     * Get all of the topologies that need scheduling.
     *
     * @return all of the topologies that are not fully scheduled.
     */
    List<TopologyDetails> needsSchedulingTopologies();

    /**
     * Does the topology need scheduling.
     *
     * <p>A topology needs scheduling if one of the following conditions holds:
     *
     * <ul>
     * <li>Although the topology is assigned slots, but is squeezed. i.e. the topology is assigned
     * less slots than desired.
     * <li>There are unassigned executors in this topology
     * </ul>
     */
    boolean needsScheduling(TopologyDetails topology);

    /**
     * Like {@link #needsScheduling(TopologyDetails)} but does not take into account the number of workers requested. This is because the
     * number of workers is ignored in RAS
     *
     * @param topology the topology to check
     * @return true if the topology needs scheduling else false.
     */
    boolean needsSchedulingRas(TopologyDetails topology);

    /**
     * Get all of the hosts that are blacklisted.
     *
     * @return all of the hosts that are blacklisted
     */
    Set<String> getBlacklistedHosts();

    /**
     * Check is a given supervisor is on a blacklisted host.
     *
     * @param supervisorId the id of the supervisor
     * @return true if it is else false
     */
    boolean isBlackListed(String supervisorId);

    /**
     * Check if a given host is blacklisted.
     *
     * @param host the name of the host
     * @return true if it is else false.
     */
    boolean isBlacklistedHost(String host);

    /**
     * Map a supervisor to a given host.
     *
     * @param supervisorId the id of the supervisor
     * @return the actual host name the supervisor is on
     */
    String getHost(String supervisorId);

    /**
     * get the unassigned executors of the topology.
     *
     * @param topology the topology to check
     * @return the unassigned executors of the topology.
     */
    Collection<ExecutorDetails> getUnassignedExecutors(TopologyDetails topology);

    /**
     * Get the executor to component name map for executors that need to be scheduled.
     *
     * @param topology the topology this is for
     * @return a executor -> component-id map which needs scheduling in this topology.
     */
    Map<ExecutorDetails, String> getNeedsSchedulingExecutorToComponents(TopologyDetails topology);

    /**
     * Get the component name to executor list for executors that need to be scheduled.
     *
     * @param topology the topology this is for
     * @return a component-id -> executors map which needs scheduling in this topology.
     */
    Map<String, List<ExecutorDetails>> getNeedsSchedulingComponentToExecutors(
        TopologyDetails topology);

    /**
     * Get all the used ports of this supervisor.
     */
    Set<Integer> getUsedPorts(SupervisorDetails supervisor);

    /**
     * Return the available ports of this supervisor.
     */
    Set<Integer> getAvailablePorts(SupervisorDetails supervisor);

    /**
     * Get the ports that are not blacklisted.
     *
     * @param supervisor the supervisor
     * @return the ports that are not blacklisted
     */
    Set<Integer> getAssignablePorts(SupervisorDetails supervisor);

    /**
     * Return all the available slots on this supervisor.
     */
    List<WorkerSlot> getAvailableSlots(SupervisorDetails supervisor);

    /**
     * Get all the available worker slots in the cluster.
     */
    List<WorkerSlot> getAvailableSlots();

    /**
     * Get all the available worker slots in the cluster, that are not blacklisted.
     * @param blacklistedSupervisorIds list of supervisor ids that should also be considered blacklisted.
     */
    List<WorkerSlot> getNonBlacklistedAvailableSlots(List<String> blacklistedSupervisorIds);

    /**
     * Return all non-blacklisted slots on this supervisor.
     *
     * @param supervisor the supervisor
     * @return the non-blacklisted slots
     */
    List<WorkerSlot> getAssignableSlots(SupervisorDetails supervisor);

    /**
     * Get all non-blacklisted slots in the cluster.
     */
    List<WorkerSlot> getAssignableSlots();

    /**
     * Get all currently occupied slots.
     */
    Collection<WorkerSlot> getUsedSlots();

    /**
     * Check if a slot is occupied or not.
     *
     * @param slot the slot be to checked.
     * @return true if the specified slot is occupied.
     */
    boolean isSlotOccupied(WorkerSlot slot);

    /**
     * Get the number of workers assigned to a topology.
     *
     * @param topology the topology this is for
     * @return the number of workers assigned to this topology.
     */
    int getAssignedNumWorkers(TopologyDetails topology);

    /**
     * Get the resources on the supervisor that are available to be scheduled.
     * @param sd the supervisor.
     * @return the resources available to be scheduled.
     */
    NormalizedResourceOffer getAvailableResources(SupervisorDetails sd);

    /**
     * Would scheduling exec on ws fit? With a heap <= maxHeap total memory added <= memoryAvailable and cpu added <= cpuAvailable.
     *
     * @param ws                 the slot to put it in
     * @param exec               the executor to investigate
     * @param td                 the topology detains for this executor
     * @param resourcesAvailable all the available resources
     * @param maxHeap            the maximum heap size for ws
     * @return true it fits else false
     */
    boolean wouldFit(
        WorkerSlot ws,
        ExecutorDetails exec,
        TopologyDetails td,
        NormalizedResourceOffer resourcesAvailable,
        double maxHeap);

    /**
     * get the current assignment for the topology.
     */
    SchedulerAssignment getAssignmentById(String topologyId);

    /**
     * get slots used by a topology.
     */
    Collection<WorkerSlot> getUsedSlotsByTopologyId(String topologyId);

    /**
     * Get a specific supervisor with the <code>nodeId</code>.
     */
    SupervisorDetails getSupervisorById(String nodeId);

    /**
     * Get all the supervisors on the specified <code>host</code>.
     *
     * @param host hostname of the supervisor
     * @return the <code>SupervisorDetails</code> object.
     */
    List<SupervisorDetails> getSupervisorsByHost(String host);

    /**
     * Get all the assignments.
     */
    Map<String, SchedulerAssignment> getAssignments();

    /**
     * Get all the supervisors.
     */
    Map<String, SupervisorDetails> getSupervisors();

    /**
     * Get all scheduled resources for node.
     */
    NormalizedResourceRequest getAllScheduledResourcesForNode(String nodeId);

    /**
     * Get the resources in the cluster that are available for scheduling.
     * @param blacklistedSupervisorIds other ids that are tentatively blacklisted.
     */
    NormalizedResourceOffer getNonBlacklistedClusterAvailableResources(Collection<String> blacklistedSupervisorIds);

    /**
     * Get the total amount of CPU resources in cluster.
     */
    double getClusterTotalCpuResource();

    /**
     * Get the total amount of memory resources in cluster.
     */
    double getClusterTotalMemoryResource();

    /**
     * Get the total amount of generic resources (excluding CPU and memory) in cluster.
     */
    Map<String, Double> getClusterTotalGenericResources();

    /**
     * Get the network topography (rackId -> nodes in the rack).
     */
    Map<String, List<String>> getNetworkTopography();

    /**
     * Get host -> rack map - the inverse of networkTopography.
     */
    default Map<String, String> getHostToRack() {
        Map<String, String> ret = new HashMap<>();
        Map<String, List<String>> networkTopography = getNetworkTopography();
        if (networkTopography != null) {
            networkTopography.forEach((rack, hosts) -> hosts.forEach(host -> ret.put(host, rack)));
        }
        return ret;
    }

    /**
     * Get all topology scheduler statuses.
     */
    Map<String, String> getStatusMap();

    /**
     * Get the amount of resources used by topologies. Used for displaying resource information on the UI.
     *
     * @return a map that contains multiple topologies and the resources the topology requested and assigned. Key: topology id Value: an
     *     array that describes the resources the topology requested and assigned in the following format: {requestedMemOnHeap,
     *     requestedMemOffHeap, requestedCpu, assignedMemOnHeap, assignedMemOffHeap, assignedCpu}
     */
    Map<String, TopologyResources> getTopologyResourcesMap();

    /**
     * Get the amount of used and free resources on a supervisor. Used for displaying resource information on the UI
     *
     * @return a map where the key is the supervisor id and the value is a map that represents resource usage for a supervisor in the
     *     following format: {totalMem, totalCpu, usedMem, usedCpu}
     */
    Map<String, SupervisorResources> getSupervisorsResourcesMap();

    /**
     * Gets the reference to the full topology->worker resource map.
     *
     * @return map of topology -> map of worker slot ->resources for that worker
     */
    Map<String, Map<WorkerSlot, WorkerResources>> getWorkerResourcesMap();

    /**
     * Get the resources for a given slot.
     *
     * @param ws the slot
     * @return the resources currently assigned
     */
    WorkerResources getWorkerResources(WorkerSlot ws);

    /**
     * Get the total memory currently scheduled on a node.
     *
     * @param nodeId the id of the node
     * @return the total memory currently scheduled on the node
     */
    double getScheduledMemoryForNode(String nodeId);

    /**
     * Get the total cpu currently scheduled on a node.
     *
     * @param nodeId the id of the node
     * @return the total cpu currently scheduled on the node
     */
    double getScheduledCpuForNode(String nodeId);

    /**
     * Get the nimbus configuration.
     */
    Map<String, Object> getConf();

    /**
     * Determine the list of racks on which topologyIds have been assigned. Note that the returned set
     * may contain {@link DNSToSwitchMapping#DEFAULT_RACK} if {@link #getHostToRack()} is null or
     * does not contain the assigned host.
     *
     * @param topologyIds for which assignments are examined.
     * @return set of racks on which assignments have been made.
     */
    default Set<String> getAssignedRacks(String... topologyIds) {
        Set<String> ret = new HashSet<>();
        Map<String, String> networkTopographyInverted = getHostToRack();
        for (String topologyId: topologyIds) {
            SchedulerAssignment assignment = getAssignmentById(topologyId);
            if (assignment == null) {
                continue;
            }
            for (WorkerSlot slot : assignment.getSlots()) {
                String nodeId = slot.getNodeId();
                SupervisorDetails supervisorDetails = getSupervisorById(nodeId);
                String hostId = supervisorDetails.getHost();
                String rackId = networkTopographyInverted.getOrDefault(hostId, DNSToSwitchMapping.DEFAULT_RACK);
                ret.add(rackId);
            }
        }
        return ret;
    }
}
