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

import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.storm.Config;
import org.apache.storm.daemon.nimbus.TopologyResources;
import org.apache.storm.generated.SharedMemory;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.networktopography.DNSToSwitchMapping;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ReflectionUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cluster implements ISchedulingState {
    private static final Logger LOG = LoggerFactory.getLogger(Cluster.class);

    public static class SupervisorResources {
        private final double totalMem;
        private final double totalCpu;
        private final double usedMem;
        private final double usedCpu;

        /**
         * Constructor for a Supervisor's resources.
         *
         * @param totalMem the total mem on the supervisor
         * @param totalCpu the total CPU on the supervisor
         * @param usedMem the used mem on the supervisor
         * @param usedCpu the used CPU on the supervisor
         */
        public SupervisorResources(double totalMem, double totalCpu, double usedMem, double usedCpu) {
            this.totalMem = totalMem;
            this.totalCpu = totalCpu;
            this.usedMem = usedMem;
            this.usedCpu = usedCpu;
        }

        public double getUsedMem() {
            return usedMem;
        }

        public double getUsedCpu() {
            return usedCpu;
        }

        public double getTotalMem() {
            return totalMem;
        }

        public double getTotalCpu() {
            return totalCpu;
        }

        private SupervisorResources add(WorkerResources wr) {
            return new SupervisorResources(
                totalMem,
                totalCpu,
                usedMem + wr.get_mem_off_heap() + wr.get_mem_on_heap(),
                usedCpu + wr.get_cpu());
        }

        public SupervisorResources addMem(Double value) {
            return new SupervisorResources(totalMem, totalCpu, usedMem + value, usedCpu);
        }
    }

    /**
     * key: supervisor id, value: supervisor details.
     */
    private final Map<String, SupervisorDetails> supervisors = new HashMap<>();

    /**
     * key: rack, value: nodes in that rack.
     */
    private final Map<String, List<String>> networkTopography = new HashMap<>();

    /**
     * key: topologyId, value: topology's current assignments.
     */
    private final Map<String, SchedulerAssignmentImpl> assignments = new HashMap<>();

    /**
     * key topologyId, Value: scheduler's status.
     */
    private final Map<String, String> status = new HashMap<>();

    /**
     * A map from hostname to supervisor ids.
     */
    private final Map<String, List<String>> hostToId = new HashMap<>();

    private final Map<String, Object> conf;

    private Set<String> blackListedHosts = new HashSet<>();
    private INimbus inimbus;
    private final Topologies topologies;
    private final Map<String, Double> scheduledCPUCache = new HashMap<>();
    private final Map<String, Double> scheduledMemoryCache = new HashMap<>();

    public Cluster(
        INimbus nimbus,
        Map<String, SupervisorDetails> supervisors,
        Map<String, ? extends SchedulerAssignment> map,
        Topologies topologies,
        Map<String, Object> conf) {
        this(nimbus, supervisors, map, topologies, conf, null, null, null);
    }

    /**
     * Copy constructor.
     */
    public Cluster(Cluster src) {
        this(
            src.inimbus,
            src.supervisors,
            src.assignments,
            src.topologies,
            new HashMap<>(src.conf),
            src.status,
            src.blackListedHosts,
            src.networkTopography);
    }

    /**
     * Testing Constructor that takes an existing cluster and replaces the topologies in it.
     *
     * @param src the original cluster
     * @param topologies the new topolgoies to use
     */
    @VisibleForTesting
    public Cluster(Cluster src, Topologies topologies) {
        this(
            src.inimbus,
            src.supervisors,
            src.assignments,
            topologies,
            new HashMap<>(src.conf),
            src.status,
            src.blackListedHosts,
            src.networkTopography);
    }

    private Cluster(
        INimbus nimbus,
        Map<String, SupervisorDetails> supervisors,
        Map<String, ? extends SchedulerAssignment> assignments,
        Topologies topologies,
        Map<String, Object> conf,
        Map<String, String> status,
        Set<String> blackListedHosts,
        Map<String, List<String>> networkTopography) {
        this.inimbus = nimbus;
        this.supervisors.putAll(supervisors);

        for (Map.Entry<String, SupervisorDetails> entry : supervisors.entrySet()) {
            String nodeId = entry.getKey();
            SupervisorDetails supervisor = entry.getValue();
            String host = supervisor.getHost();
            List<String> ids = hostToId.get(host);
            if (ids == null) {
                ids = new ArrayList<>();
                hostToId.put(host, ids);
            }
            ids.add(nodeId);
        }
        this.conf = conf;
        this.topologies = topologies;

        ArrayList<String> supervisorHostNames = new ArrayList<String>();
        for (SupervisorDetails s : supervisors.values()) {
            supervisorHostNames.add(s.getHost());
        }

        //Initialize the network topography
        if (networkTopography == null || networkTopography.isEmpty()) {
            //Initialize the network topography
            String clazz = (String) conf.get(Config.STORM_NETWORK_TOPOGRAPHY_PLUGIN);
            if (clazz != null && !clazz.isEmpty()) {
                DNSToSwitchMapping topographyMapper =
                    (DNSToSwitchMapping) ReflectionUtils.newInstance(clazz);

                Map<String, String> resolvedSuperVisors = topographyMapper.resolve(supervisorHostNames);
                for (Map.Entry<String, String> entry : resolvedSuperVisors.entrySet()) {
                    String hostName = entry.getKey();
                    String rack = entry.getValue();
                    List<String> nodesForRack = this.networkTopography.get(rack);
                    if (nodesForRack == null) {
                        nodesForRack = new ArrayList<String>();
                        this.networkTopography.put(rack, nodesForRack);
                    }
                    nodesForRack.add(hostName);
                }
            }
        } else {
            this.networkTopography.putAll(networkTopography);
        }

        if (status != null) {
            this.status.putAll(status);
        }

        if (blackListedHosts != null) {
            this.blackListedHosts.addAll(blackListedHosts);
        }

        setAssignments(assignments, true);
    }

    /**
     * Check if the given topology is allowed for modification right now. If not throw an
     * IllegalArgumentException else go on.
     *
     * @param topologyId the id of the topology to check
     */
    protected void assertValidTopologyForModification(String topologyId) {
        //NOOP
    }

    /**
     * Set the list of hosts that are blacklisted.
     *
     * @param hosts the new hosts that are blacklisted.
     */
    public void setBlacklistedHosts(Set<String> hosts) {
        if (hosts == blackListedHosts) {
            //NOOP
            return;
        }
        blackListedHosts.clear();
        blackListedHosts.addAll(hosts);
    }

    @Override
    public Topologies getTopologies() {
        return topologies;
    }

    @Override
    public Set<String> getBlacklistedHosts() {
        return blackListedHosts;
    }

    public void blacklistHost(String host) {
        blackListedHosts.add(host);
    }

    @Override
    public boolean isBlackListed(String supervisorId) {
        return blackListedHosts.contains(getHost(supervisorId));
    }

    @Override
    public boolean isBlacklistedHost(String host) {
        return blackListedHosts.contains(host);
    }

    @Override
    public String getHost(String supervisorId) {
        return inimbus.getHostName(supervisors, supervisorId);
    }

    @Override
    public List<TopologyDetails> needsSchedulingTopologies() {
        List<TopologyDetails> ret = new ArrayList<TopologyDetails>();
        for (TopologyDetails topology : getTopologies()) {
            if (needsScheduling(topology)) {
                ret.add(topology);
            }
        }

        return ret;
    }

    @Override
    public boolean needsScheduling(TopologyDetails topology) {
        int desiredNumWorkers = topology.getNumWorkers();
        int assignedNumWorkers = this.getAssignedNumWorkers(topology);
        return desiredNumWorkers > assignedNumWorkers || getUnassignedExecutors(topology).size() > 0;
    }

    @Override
    public boolean needsSchedulingRas(TopologyDetails topology) {
        return getUnassignedExecutors(topology).size() > 0;
    }

    @Override
    public Map<ExecutorDetails, String> getNeedsSchedulingExecutorToComponents(
        TopologyDetails topology) {
        Collection<ExecutorDetails> allExecutors = new HashSet<>(topology.getExecutors());

        SchedulerAssignment assignment = assignments.get(topology.getId());
        if (assignment != null) {
            allExecutors.removeAll(assignment.getExecutors());
        }
        return topology.selectExecutorToComponent(allExecutors);
    }

    @Override
    public Map<String, List<ExecutorDetails>> getNeedsSchedulingComponentToExecutors(
        TopologyDetails topology) {
        Map<ExecutorDetails, String> executorToComponents =
            getNeedsSchedulingExecutorToComponents(topology);
        Map<String, List<ExecutorDetails>> componentToExecutors = new HashMap<>();
        for (Map.Entry<ExecutorDetails, String> entry : executorToComponents.entrySet()) {
            ExecutorDetails executor = entry.getKey();
            String component = entry.getValue();
            if (!componentToExecutors.containsKey(component)) {
                componentToExecutors.put(component, new ArrayList<>());
            }

            componentToExecutors.get(component).add(executor);
        }

        return componentToExecutors;
    }

    @Override
    public Set<Integer> getUsedPorts(SupervisorDetails supervisor) {
        Set<Integer> usedPorts = new HashSet<>();

        for (SchedulerAssignment assignment : assignments.values()) {
            for (WorkerSlot slot : assignment.getExecutorToSlot().values()) {
                if (slot.getNodeId().equals(supervisor.getId())) {
                    usedPorts.add(slot.getPort());
                }
            }
        }

        return usedPorts;
    }

    @Override
    public Set<Integer> getAvailablePorts(SupervisorDetails supervisor) {
        Set<Integer> usedPorts = this.getUsedPorts(supervisor);

        Set<Integer> ret = new HashSet<>();
        ret.addAll(getAssignablePorts(supervisor));
        ret.removeAll(usedPorts);

        return ret;
    }

    @Override
    public Set<Integer> getAssignablePorts(SupervisorDetails supervisor) {
        if (isBlackListed(supervisor.id)) {
            return Collections.emptySet();
        }
        return supervisor.allPorts;
    }

    @Override
    public List<WorkerSlot> getAvailableSlots() {
        List<WorkerSlot> slots = new ArrayList<WorkerSlot>();
        for (SupervisorDetails supervisor : this.supervisors.values()) {
            slots.addAll(this.getAvailableSlots(supervisor));
        }

        return slots;
    }

    @Override
    public List<WorkerSlot> getAvailableSlots(SupervisorDetails supervisor) {
        Set<Integer> ports = this.getAvailablePorts(supervisor);
        List<WorkerSlot> slots = new ArrayList<WorkerSlot>(ports.size());

        for (Integer port : ports) {
            slots.add(new WorkerSlot(supervisor.getId(), port));
        }

        return slots;
    }

    @Override
    public List<WorkerSlot> getAssignableSlots(SupervisorDetails supervisor) {
        Set<Integer> ports = this.getAssignablePorts(supervisor);
        List<WorkerSlot> slots = new ArrayList<WorkerSlot>(ports.size());

        for (Integer port : ports) {
            slots.add(new WorkerSlot(supervisor.getId(), port));
        }

        return slots;
    }

    @Override
    public List<WorkerSlot> getAssignableSlots() {
        List<WorkerSlot> slots = new ArrayList<WorkerSlot>();
        for (SupervisorDetails supervisor : this.supervisors.values()) {
            slots.addAll(this.getAssignableSlots(supervisor));
        }

        return slots;
    }

    @Override
    public Collection<ExecutorDetails> getUnassignedExecutors(TopologyDetails topology) {
        if (topology == null) {
            return new ArrayList<ExecutorDetails>(0);
        }

        Collection<ExecutorDetails> ret = new HashSet<>(topology.getExecutors());

        SchedulerAssignment assignment = getAssignmentById(topology.getId());
        if (assignment != null) {
            Set<ExecutorDetails> assignedExecutors = assignment.getExecutors();
            ret.removeAll(assignedExecutors);
        }

        return ret;
    }

    @Override
    public int getAssignedNumWorkers(TopologyDetails topology) {
        SchedulerAssignment assignment =
            topology != null ? this.getAssignmentById(topology.getId()) : null;
        if (assignment == null) {
            return 0;
        }

        Set<WorkerSlot> slots = new HashSet<WorkerSlot>();
        slots.addAll(assignment.getExecutorToSlot().values());
        return slots.size();
    }

    private WorkerResources calculateWorkerResources(
        TopologyDetails td, Collection<ExecutorDetails> executors) {
        double onHeapMem = 0.0;
        double offHeapMem = 0.0;
        double cpu = 0.0;
        double sharedOn = 0.0;
        double sharedOff = 0.0;
        for (ExecutorDetails exec : executors) {
            Double onHeapMemForExec = td.getOnHeapMemoryRequirement(exec);
            if (onHeapMemForExec != null) {
                onHeapMem += onHeapMemForExec;
            }
            Double offHeapMemForExec = td.getOffHeapMemoryRequirement(exec);
            if (offHeapMemForExec != null) {
                offHeapMem += offHeapMemForExec;
            }
            Double cpuForExec = td.getTotalCpuReqTask(exec);
            if (cpuForExec != null) {
                cpu += cpuForExec;
            }
        }

        for (SharedMemory shared : td.getSharedMemoryRequests(executors)) {
            onHeapMem += shared.get_on_heap();
            sharedOn += shared.get_on_heap();
            offHeapMem += shared.get_off_heap_worker();
            sharedOff += shared.get_off_heap_worker();
        }

        WorkerResources ret = new WorkerResources();
        ret.set_cpu(cpu);
        ret.set_mem_on_heap(onHeapMem);
        ret.set_mem_off_heap(offHeapMem);
        ret.set_shared_mem_on_heap(sharedOn);
        ret.set_shared_mem_off_heap(sharedOff);
        return ret;
    }

    @Override
    public boolean wouldFit(
        WorkerSlot ws,
        ExecutorDetails exec,
        TopologyDetails td,
        double maxHeap,
        double memoryAvailable,
        double cpuAvailable) {
        //NOTE this is called lots and lots by schedulers, so anything we can do to make it faster is going to help a lot.
        //CPU is simplest because it does not have odd interactions.
        double cpuNeeded = td.getTotalCpuReqTask(exec);
        if (cpuNeeded > cpuAvailable) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Could not schedule {}:{} on {} not enough CPU {} > {}",
                    td.getName(),
                    exec,
                    ws,
                    cpuNeeded,
                    cpuAvailable);
            }
            //Not enough CPU no need to try any more
            return false;
        }

        //Lets see if we can make the Memory one fast too, at least in the failure case.
        //The totalMemReq is not really that accurate because it does not include shared memory, but if it does not fit we know
        // Even with shared it will not work
        double minMemNeeded = td.getTotalMemReqTask(exec);
        if (minMemNeeded > memoryAvailable) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Could not schedule {}:{} on {} not enough Mem {} > {}", td.getName(), exec, ws, minMemNeeded, memoryAvailable);
            }
            //Not enough minimum MEM no need to try any more
            return false;
        }

        double currentTotal = 0.0;
        double afterTotal = 0.0;
        double afterOnHeap = 0.0;
        Set<ExecutorDetails> wouldBeAssigned = new HashSet<>();
        wouldBeAssigned.add(exec);
        SchedulerAssignmentImpl assignment = assignments.get(td.getId());
        if (assignment != null) {
            Collection<ExecutorDetails> currentlyAssigned = assignment.getSlotToExecutors().get(ws);
            if (currentlyAssigned != null) {
                wouldBeAssigned.addAll(currentlyAssigned);
                WorkerResources wrCurrent = calculateWorkerResources(td, currentlyAssigned);
                currentTotal = wrCurrent.get_mem_off_heap() + wrCurrent.get_mem_on_heap();
            }
            WorkerResources wrAfter = calculateWorkerResources(td, wouldBeAssigned);
            afterTotal = wrAfter.get_mem_off_heap() + wrAfter.get_mem_on_heap();
            afterOnHeap = wrAfter.get_mem_on_heap();

            currentTotal += calculateSharedOffHeapMemory(ws.getNodeId(), assignment);
            afterTotal += calculateSharedOffHeapMemory(ws.getNodeId(), assignment, exec);
        }

        double memoryAdded = afterTotal - currentTotal;
        if (memoryAdded > memoryAvailable) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Could not schedule {}:{} on {} not enough Mem {} > {}",
                    td.getName(),
                    exec,
                    ws,
                    memoryAdded,
                    memoryAvailable);
            }
            return false;
        }
        if (afterOnHeap > maxHeap) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Could not schedule {}:{} on {} HEAP would be too large {} > {}",
                    td.getName(),
                    exec,
                    ws,
                    afterOnHeap,
                    maxHeap);
            }
            return false;
        }
        return true;
    }

    /**
     * Assign the slot to the executors for this topology.
     *
     * @throws RuntimeException if the specified slot is already occupied.
     */
    public void assign(WorkerSlot slot, String topologyId, Collection<ExecutorDetails> executors) {
        assertValidTopologyForModification(topologyId);
        if (isSlotOccupied(slot)) {
            throw new RuntimeException(
                "slot: [" + slot.getNodeId() + ", " + slot.getPort() + "] is already occupied.");
        }

        TopologyDetails td = topologies.getById(topologyId);
        if (td == null) {
            throw new IllegalArgumentException(
                "Trying to schedule for topo "
                    + topologyId
                    + " but that is not a known topology "
                    + topologies.getAllIds());
        }
        WorkerResources resources = calculateWorkerResources(td, executors);
        SchedulerAssignmentImpl assignment = assignments.get(topologyId);
        if (assignment == null) {
            assignment = new SchedulerAssignmentImpl(topologyId);
            assignments.put(topologyId, assignment);
        } else {
            for (ExecutorDetails executor : executors) {
                if (assignment.isExecutorAssigned(executor)) {
                    throw new RuntimeException(
                        "Attempting to assign executor: "
                            + executor
                            + " of topology: "
                            + topologyId
                            + " to workerslot: "
                            + slot
                            + ". The executor is already assigned to workerslot: "
                            + assignment.getExecutorToSlot().get(executor)
                            + ". The executor must unassigned before it can be assigned to another slot!");
                }
            }
        }

        assignment.assign(slot, executors, resources);
        String nodeId = slot.getNodeId();
        assignment.setTotalSharedOffHeapMemory(
            nodeId, calculateSharedOffHeapMemory(nodeId, assignment));
        scheduledCPUCache.remove(nodeId);
        scheduledMemoryCache.remove(nodeId);
    }

    /**
     * Assign everything for the given topology.
     *
     * @param assignment the new assignment to make
     */
    public void assign(SchedulerAssignment assignment, boolean ignoreSingleExceptions) {
        String id = assignment.getTopologyId();
        assertValidTopologyForModification(id);
        Map<WorkerSlot, Collection<ExecutorDetails>> slotToExecs = assignment.getSlotToExecutors();
        for (Entry<WorkerSlot, Collection<ExecutorDetails>> entry : slotToExecs.entrySet()) {
            try {
                assign(entry.getKey(), id, entry.getValue());
            } catch (RuntimeException e) {
                if (!ignoreSingleExceptions) {
                    throw e;
                }
            }
        }
    }

    /**
     * Calculate the amount of shared off heap memory on a given nodes with the given assignment.
     *
     * @param nodeId the id of the node
     * @param assignment the current assignment
     * @return the amount of shared off heap memory for that node in MB
     */
    private double calculateSharedOffHeapMemory(String nodeId, SchedulerAssignmentImpl assignment) {
        return calculateSharedOffHeapMemory(nodeId, assignment, null);
    }

    private double calculateSharedOffHeapMemory(
        String nodeId, SchedulerAssignmentImpl assignment, ExecutorDetails extra) {
        TopologyDetails td = topologies.getById(assignment.getTopologyId());
        Set<ExecutorDetails> executorsOnNode = new HashSet<>();
        for (Entry<WorkerSlot, Collection<ExecutorDetails>> entry :
            assignment.getSlotToExecutors().entrySet()) {
            if (nodeId.equals(entry.getKey().getNodeId())) {
                executorsOnNode.addAll(entry.getValue());
            }
        }
        if (extra != null) {
            executorsOnNode.add(extra);
        }
        double memorySharedWithinNode = 0.0;
        //Now check for overlap on the node
        for (SharedMemory shared : td.getSharedMemoryRequests(executorsOnNode)) {
            memorySharedWithinNode += shared.get_off_heap_node();
        }
        return memorySharedWithinNode;
    }

    /**
     * Free the specified slot.
     *
     * @param slot the slot to free
     */
    public void freeSlot(WorkerSlot slot) {
        // remove the slot from the existing assignments
        for (SchedulerAssignmentImpl assignment : assignments.values()) {
            if (assignment.isSlotOccupied(slot)) {
                assertValidTopologyForModification(assignment.getTopologyId());
                assignment.unassignBySlot(slot);

                String nodeId = slot.getNodeId();
                assignment.setTotalSharedOffHeapMemory(
                    nodeId, calculateSharedOffHeapMemory(nodeId, assignment));
                scheduledCPUCache.remove(nodeId);
                scheduledMemoryCache.remove(nodeId);
            }
        }
    }

    /**
     * free the slots.
     *
     * @param slots multiple slots to free
     */
    public void freeSlots(Collection<WorkerSlot> slots) {
        if (slots != null) {
            for (WorkerSlot slot : slots) {
                freeSlot(slot);
            }
        }
    }

    @Override
    public boolean isSlotOccupied(WorkerSlot slot) {
        for (SchedulerAssignment assignment : assignments.values()) {
            if (assignment.isSlotOccupied(slot)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public SchedulerAssignment getAssignmentById(String topologyId) {
        if (assignments.containsKey(topologyId)) {
            return assignments.get(topologyId);
        }

        return null;
    }

    @Override
    public Collection<WorkerSlot> getUsedSlotsByTopologyId(String topologyId) {
        SchedulerAssignmentImpl assignment = assignments.get(topologyId);
        if (assignment == null) {
            return Collections.emptySet();
        }
        return assignment.getSlots();
    }

    @Override
    public SupervisorDetails getSupervisorById(String nodeId) {
        return supervisors.get(nodeId);
    }

    @Override
    public Collection<WorkerSlot> getUsedSlots() {
        Set<WorkerSlot> ret = new HashSet<>();
        for (SchedulerAssignmentImpl s : assignments.values()) {
            ret.addAll(s.getExecutorToSlot().values());
        }
        return ret;
    }

    @Override
    public List<SupervisorDetails> getSupervisorsByHost(String host) {
        List<String> nodeIds = this.hostToId.get(host);
        List<SupervisorDetails> ret = new ArrayList<SupervisorDetails>();

        if (nodeIds != null) {
            for (String nodeId : nodeIds) {
                ret.add(this.getSupervisorById(nodeId));
            }
        }

        return ret;
    }

    @Override
    public Map<String, SchedulerAssignment> getAssignments() {
        return new HashMap<String, SchedulerAssignment>(assignments);
    }

    /**
     * Set assignments for cluster.
     */
    public void setAssignments(
        Map<String, ? extends SchedulerAssignment> newAssignments, boolean ignoreSingleExceptions) {
        if (newAssignments == assignments) {
            //NOOP
            return;
        }
        for (SchedulerAssignment assignment : newAssignments.values()) {
            assertValidTopologyForModification(assignment.getTopologyId());
        }
        for (SchedulerAssignment assignment : assignments.values()) {
            assertValidTopologyForModification(assignment.getTopologyId());
        }
        assignments.clear();
        for (SchedulerAssignment assignment : newAssignments.values()) {
            assign(assignment, ignoreSingleExceptions);
        }
    }

    @Override
    public Map<String, SupervisorDetails> getSupervisors() {
        return this.supervisors;
    }

    @Override
    public double getClusterTotalCpuResource() {
        double sum = 0.0;
        for (SupervisorDetails sup : supervisors.values()) {
            sum += sup.getTotalCPU();
        }
        return sum;
    }

    @Override
    public double getClusterTotalMemoryResource() {
        double sum = 0.0;
        for (SupervisorDetails sup : supervisors.values()) {
            sum += sup.getTotalMemory();
        }
        return sum;
    }

    @Override
    public Map<String, List<String>> getNetworkTopography() {
        return networkTopography;
    }

    @VisibleForTesting
    public void setNetworkTopography(Map<String, List<String>> networkTopography) {
        this.networkTopography.clear();
        this.networkTopography.putAll(networkTopography);
    }

    /**
     * Get heap memory usage for a worker's main process and logwriter process.
     * @param topConf - the topology config
     * @return the assigned memory (in MB)
     */
    public static Double getAssignedMemoryForSlot(final Map<String, Object> topConf) {
        Double totalWorkerMemory = 0.0;
        final Integer topologyWorkerDefaultMemoryAllocation = 768;

        List<String> topologyWorkerGcChildopts = ConfigUtils.getValueAsList(
            Config.TOPOLOGY_WORKER_GC_CHILDOPTS, topConf);
        List<String> workerGcChildopts = ConfigUtils.getValueAsList(
            Config.WORKER_GC_CHILDOPTS, topConf);
        Double memGcChildopts = null;
        memGcChildopts = Utils.parseJvmHeapMemByChildOpts(
            topologyWorkerGcChildopts, null);
        if (memGcChildopts == null) {
            memGcChildopts = Utils.parseJvmHeapMemByChildOpts(
                workerGcChildopts, null);
        }

        List<String> topologyWorkerChildopts = ConfigUtils.getValueAsList(
            Config.TOPOLOGY_WORKER_CHILDOPTS, topConf);
        Double memTopologyWorkerChildopts = Utils.parseJvmHeapMemByChildOpts(
            topologyWorkerChildopts, null);

        List<String> workerChildopts = ConfigUtils.getValueAsList(
            Config.WORKER_CHILDOPTS, topConf);
        Double memWorkerChildopts = Utils.parseJvmHeapMemByChildOpts(
            workerChildopts, null);

        if (memGcChildopts != null) {
            totalWorkerMemory += memGcChildopts;
        } else if (memTopologyWorkerChildopts != null) {
            totalWorkerMemory += memTopologyWorkerChildopts;
        } else if (memWorkerChildopts != null) {
            totalWorkerMemory += memWorkerChildopts;
        } else {
            Object workerHeapMemoryMb = topConf.get(
                Config.WORKER_HEAP_MEMORY_MB);
            totalWorkerMemory += ObjectReader.getInt(
                workerHeapMemoryMb, topologyWorkerDefaultMemoryAllocation);
        }

        List<String> topoWorkerLwChildopts = ConfigUtils.getValueAsList(
            Config.TOPOLOGY_WORKER_LOGWRITER_CHILDOPTS, topConf);
        if (topoWorkerLwChildopts != null) {
            totalWorkerMemory += Utils.parseJvmHeapMemByChildOpts(
                topoWorkerLwChildopts, 0.0);
        }
        return totalWorkerMemory;
    }

    /**
     * set scheduler status for a topology.
     */
    public void setStatus(String topologyId, String statusMessage) {
        assertValidTopologyForModification(topologyId);
        LOG.info("STATUS - {} {}", topologyId, statusMessage);
        status.put(topologyId, statusMessage);
    }

    public void setStatusIfAbsent(String topologyId, String statusMessage) {
        assertValidTopologyForModification(topologyId);
        status.putIfAbsent(topologyId, statusMessage);
    }

    @Override
    public Map<String, String> getStatusMap() {
        return status;
    }

    public String getStatus(String topoId) {
        return status.get(topoId);
    }

    /**
     * set scheduler status map.
     */
    public void setStatusMap(Map<String, String> statusMap) {
        if (statusMap == this.status) {
            return; //This is a NOOP
        }
        for (String topologyId : statusMap.keySet()) {
            assertValidTopologyForModification(topologyId);
        }
        for (String topologyId : status.keySet()) {
            assertValidTopologyForModification(topologyId);
        }
        this.status.clear();
        this.status.putAll(statusMap);
    }

    @Override
    public Map<String, TopologyResources> getTopologyResourcesMap() {
        Map<String, TopologyResources> ret = new HashMap<>(assignments.size());
        for (TopologyDetails td : topologies.getTopologies()) {
            String topoId = td.getId();
            SchedulerAssignmentImpl assignment = assignments.get(topoId);
            ret.put(topoId, new TopologyResources(td, assignment));
        }
        return ret;
    }

    @Override
    public Map<String, SupervisorResources> getSupervisorsResourcesMap() {
        Map<String, SupervisorResources> ret = new HashMap<>();
        for (SupervisorDetails sd : supervisors.values()) {
            ret.put(sd.getId(), new SupervisorResources(sd.getTotalMemory(), sd.getTotalMemory(), 0, 0));
        }
        for (SchedulerAssignmentImpl assignment : assignments.values()) {
            for (Entry<WorkerSlot, WorkerResources> entry :
                assignment.getScheduledResources().entrySet()) {
                String id = entry.getKey().getNodeId();
                SupervisorResources sr = ret.get(id);
                if (sr == null) {
                    sr = new SupervisorResources(0, 0, 0, 0);
                }
                sr = sr.add(entry.getValue());
                ret.put(id, sr);
            }
            Map<String, Double> nodeIdToSharedOffHeap = assignment.getNodeIdToTotalSharedOffHeapMemory();
            if (nodeIdToSharedOffHeap != null) {
                for (Entry<String, Double> entry : nodeIdToSharedOffHeap.entrySet()) {
                    String id = entry.getKey();
                    SupervisorResources sr = ret.get(id);
                    if (sr == null) {
                        sr = new SupervisorResources(0, 0, 0, 0);
                    }
                    sr = sr.addMem(entry.getValue());
                    ret.put(id, sr);
                }
            }
        }
        return ret;
    }

    @Override
    public Map<String, Map<WorkerSlot, WorkerResources>> getWorkerResourcesMap() {
        HashMap<String, Map<WorkerSlot, WorkerResources>> ret = new HashMap<>();
        for (Entry<String, SchedulerAssignmentImpl> entry : assignments.entrySet()) {
            ret.put(entry.getKey(), entry.getValue().getScheduledResources());
        }
        return ret;
    }

    @Override
    public WorkerResources getWorkerResources(WorkerSlot ws) {
        WorkerResources ret = null;
        for (SchedulerAssignmentImpl assignment : assignments.values()) {
            ret = assignment.getScheduledResources().get(ws);
            if (ret != null) {
                break;
            }
        }
        return ret;
    }

    @Override
    public double getScheduledMemoryForNode(String nodeId) {
        Double ret = scheduledMemoryCache.get(nodeId);
        if (ret != null) {
            return ret;
        }
        double totalMemory = 0.0;
        for (SchedulerAssignmentImpl assignment : assignments.values()) {
            for (Entry<WorkerSlot, WorkerResources> entry :
                assignment.getScheduledResources().entrySet()) {
                if (nodeId.equals(entry.getKey().getNodeId())) {
                    WorkerResources resources = entry.getValue();
                    totalMemory += resources.get_mem_off_heap() + resources.get_mem_on_heap();
                }
            }
            Double sharedOffHeap = assignment.getNodeIdToTotalSharedOffHeapMemory().get(nodeId);
            if (sharedOffHeap != null) {
                totalMemory += sharedOffHeap;
            }
        }
        scheduledMemoryCache.put(nodeId, totalMemory);
        return totalMemory;
    }

    @Override
    public double getScheduledCpuForNode(String nodeId) {
        Double ret = scheduledCPUCache.get(nodeId);
        if (ret != null) {
            return ret;
        }
        double totalCpu = 0.0;
        for (SchedulerAssignmentImpl assignment : assignments.values()) {
            for (Entry<WorkerSlot, WorkerResources> entry :
                assignment.getScheduledResources().entrySet()) {
                if (nodeId.equals(entry.getKey().getNodeId())) {
                    WorkerResources resources = entry.getValue();
                    totalCpu += resources.get_cpu();
                }
            }
        }
        scheduledCPUCache.put(nodeId, totalCpu);
        return totalCpu;
    }

    public INimbus getINimbus() {
        return this.inimbus;
    }

    @Override
    public Map<String, Object> getConf() {
        return this.conf;
    }

    /**
     * Unassign everything for the given topology id.
     *
     * @param topoId the is of the topology to unassign
     */
    public void unassign(String topoId) {
        assertValidTopologyForModification(topoId);
        freeSlots(getUsedSlotsByTopologyId(topoId));
    }

    /**
     * Update the assignments and status from the other cluster.
     *
     * @param other the cluster to get the assignments and status from
     */
    public void updateFrom(Cluster other) {
        for (SchedulerAssignment assignment : other.getAssignments().values()) {
            assertValidTopologyForModification(assignment.getTopologyId());
        }
        setAssignments(other.getAssignments(), false);
        setStatusMap(other.getStatusMap());
    }
}
