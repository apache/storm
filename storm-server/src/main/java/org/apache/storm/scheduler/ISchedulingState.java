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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.daemon.nimbus.TopologyResources;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.scheduler.Cluster.SupervisorResources;

/** An interface that provides access to the current scheduling state. */
public interface ISchedulingState {

    /**
     * Get all of the topologies.
     * @return all of the topologies that are a part of the cluster.
     */
    Topologies getTopologies();

    /**
     * Get all of the topologies that need scheduling.
     * @return all of the topologies that are not fully scheduled.
     */
    List<TopologyDetails> needsSchedulingTopologies();

    /**
     * Does the topology need scheduling?
     *
     * <p>A topology needs scheduling if one of the following conditions holds:
     *
     * <ul>
     *   <li>Although the topology is assigned slots, but is squeezed. i.e. the topology is assigned
     *       less slots than desired.
     *   <li>There are unassigned executors in this topology
     * </ul>
     */
    boolean needsScheduling(TopologyDetails topology);

    /**
     * Like {@link #needsScheduling(TopologyDetails)} but does not take into account the number of
     * workers requested. This is because the number of workers is ignored in RAS
     *
     * @param topology the topology to check
     * @return true if the topology needs scheduling else false.
     */
    boolean needsSchedulingRas(TopologyDetails topology);

    /**
     * Get all of the hosts that are blacklisted.
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
     * @param topology the topology this is for
     * @return a executor -> component-id map which needs scheduling in this topology.
     */
    Map<ExecutorDetails, String> getNeedsSchedulingExecutorToComponents(TopologyDetails topology);

    /**
     * @param topology the topology this is for
     * @return a component-id -> executors map which needs scheduling in this topology.
     */
    Map<String, List<ExecutorDetails>> getNeedsSchedulingComponentToExecutors(
        TopologyDetails topology);

    /** Get all the used ports of this supervisor. */
    Set<Integer> getUsedPorts(SupervisorDetails supervisor);

    /** Return the available ports of this supervisor. */
    Set<Integer> getAvailablePorts(SupervisorDetails supervisor);

    /**
     * Get the ports that are not blacklisted.
     *
     * @param supervisor the supervisor
     * @return the ports that are not blacklisted
     */
    Set<Integer> getAssignablePorts(SupervisorDetails supervisor);

    /** Return all the available slots on this supervisor. */
    List<WorkerSlot> getAvailableSlots(SupervisorDetails supervisor);

    /** Get all the available worker slots in the cluster. */
    List<WorkerSlot> getAvailableSlots();

    /**
     * Return all non-blacklisted slots on this supervisor.
     *
     * @param supervisor the supervisor
     * @return the non-blacklisted slots
     */
    List<WorkerSlot> getAssignableSlots(SupervisorDetails supervisor);

    /** Get all non-blacklisted slots in the cluster. */
    List<WorkerSlot> getAssignableSlots();

    /** Get all currently occupied slots. */
    Collection<WorkerSlot> getUsedSlots();

    /**
     * Check if a slot is occupied or not.
     * @param slot the slot be to checked.
     * @return true if the specified slot is occupied.
     */
    boolean isSlotOccupied(WorkerSlot slot);

    /**
     * Get the number of workers assigned to a topology.
     * @param topology the topology this is for
     * @return the number of workers assigned to this topology.
     */
    int getAssignedNumWorkers(TopologyDetails topology);

    /**
     * Would scheduling exec on ws fit? With a heap <= maxHeap total memory added <= memoryAvailable
     * and cpu added <= cpuAvailable.
     *
     * @param ws the slot to put it in
     * @param exec the executor to investigate
     * @param td the topology detains for this executor
     * @param maxHeap the maximum heap size for ws
     * @param memoryAvailable the amount of memory available
     * @param cpuAvailable the amount of CPU available
     * @return true it fits else false
     */
    boolean wouldFit(
        WorkerSlot ws,
        ExecutorDetails exec,
        TopologyDetails td,
        double maxHeap,
        double memoryAvailable,
        double cpuAvailable);

    /** get the current assignment for the topology. */
    SchedulerAssignment getAssignmentById(String topologyId);

    /** get slots used by a topology. */
    Collection<WorkerSlot> getUsedSlotsByTopologyId(String topologyId);

    /** Get a specific supervisor with the <code>nodeId</code>. */
    SupervisorDetails getSupervisorById(String nodeId);

    /**
     * Get all the supervisors on the specified <code>host</code>.
     *
     * @param host hostname of the supervisor
     * @return the <code>SupervisorDetails</code> object.
     */
    List<SupervisorDetails> getSupervisorsByHost(String host);

    /** Get all the assignments. */
    Map<String, SchedulerAssignment> getAssignments();

    /** Get all the supervisors. */
    Map<String, SupervisorDetails> getSupervisors();

    /** Get the total amount of CPU resources in cluster. */
    double getClusterTotalCpuResource();

    /** Get the total amount of memory resources in cluster. */
    double getClusterTotalMemoryResource();

    /** Get the network topography (rackId -> nodes in the rack). */
    Map<String, List<String>> getNetworkTopography();

    /** Get all topology scheduler statuses. */
    Map<String, String> getStatusMap();

    /**
     * Get the amount of resources used by topologies. Used for displaying resource information on the
     * UI.
     *
     * @return a map that contains multiple topologies and the resources the topology requested and
     *     assigned. Key: topology id Value: an array that describes the resources the topology
     *     requested and assigned in the following format: {requestedMemOnHeap, requestedMemOffHeap,
     *     requestedCpu, assignedMemOnHeap, assignedMemOffHeap, assignedCpu}
     */
    Map<String, TopologyResources> getTopologyResourcesMap();

    /**
     * Get the amount of used and free resources on a supervisor. Used for displaying resource
     * information on the UI
     *
     * @return a map where the key is the supervisor id and the value is a map that represents
     *     resource usage for a supervisor in the following format: {totalMem, totalCpu, usedMem,
     *     usedCpu}
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

    /** Get the nimbus configuration. */
    Map<String, Object> getConf();
}
