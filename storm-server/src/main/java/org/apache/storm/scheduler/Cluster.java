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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.daemon.nimbus.TopologyResources;
import org.apache.storm.generated.SharedMemory;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.networktopography.DNSToSwitchMapping;
import org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceOffer;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceRequest;
import org.apache.storm.scheduler.resource.normalization.NormalizedResources;
import org.apache.storm.scheduler.resource.normalization.ResourceMetrics;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ReflectionUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The current state of the storm cluster.  Cluster is not currently thread safe.
 */
public class Cluster implements ISchedulingState {
    private static final Logger LOG = LoggerFactory.getLogger(Cluster.class);
    private static final Function<String, Set<WorkerSlot>> MAKE_SET = (x) -> new HashSet<>();
    private static final Function<String, Map<WorkerSlot, NormalizedResourceRequest>> MAKE_MAP = (x) -> new HashMap<>();

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
    private final Topologies topologies;
    private final Map<String, Map<WorkerSlot, NormalizedResourceRequest>> nodeToScheduledResourcesCache;
    private final Map<String, Set<WorkerSlot>> nodeToUsedSlotsCache;
    private final Map<String, NormalizedResourceRequest> totalResourcesPerNodeCache = new HashMap<>();
    private final ResourceMetrics resourceMetrics;
    private SchedulerAssignmentImpl assignment;
    private Set<String> blackListedHosts = new HashSet<>();
    private INimbus inimbus;

    public Cluster(
        INimbus nimbus,
        ResourceMetrics resourceMetrics,
        Map<String, SupervisorDetails> supervisors,
        Map<String, ? extends SchedulerAssignment> map,
        Topologies topologies,
        Map<String, Object> conf) {
        this(nimbus, resourceMetrics, supervisors, map, topologies, conf, null, null, null);
    }

    /**
     * Copy constructor.
     */
    public Cluster(Cluster src) {
        this(
            src.inimbus,
            src.resourceMetrics,
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
     * @param src        the original cluster
     * @param topologies the new topolgoies to use
     */
    @VisibleForTesting
    public Cluster(Cluster src, Topologies topologies) {
        this(
            src.inimbus,
            src.resourceMetrics,
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
        ResourceMetrics resourceMetrics,
        Map<String, SupervisorDetails> supervisors,
        Map<String, ? extends SchedulerAssignment> assignments,
        Topologies topologies,
        Map<String, Object> conf,
        Map<String, String> status,
        Set<String> blackListedHosts,
        Map<String, List<String>> networkTopography) {
        this.inimbus = nimbus;
        this.resourceMetrics = resourceMetrics;
        this.supervisors.putAll(supervisors);
        this.nodeToScheduledResourcesCache = new HashMap<>(this.supervisors.size());
        this.nodeToUsedSlotsCache = new HashMap<>(this.supervisors.size());

        for (Map.Entry<String, SupervisorDetails> entry : supervisors.entrySet()) {
            String nodeId = entry.getKey();
            SupervisorDetails supervisor = entry.getValue();
            String host = supervisor.getHost();
            List<String> ids = hostToId.computeIfAbsent(host, k -> new ArrayList<>());
            ids.add(nodeId);
        }
        this.conf = conf;
        this.topologies = topologies;

        ArrayList<String> supervisorHostNames = new ArrayList<>();
        for (SupervisorDetails s : supervisors.values()) {
            supervisorHostNames.add(s.getHost());
        }

        //Initialize the network topography
        if (networkTopography == null || networkTopography.isEmpty()) {
            //Initialize the network topography
            String clazz = (String) conf.get(Config.STORM_NETWORK_TOPOGRAPHY_PLUGIN);
            if (clazz == null || clazz.isEmpty()) {
                clazz = DefaultRackDNSToSwitchMapping.class.getName();
            }
            DNSToSwitchMapping topographyMapper = ReflectionUtils.newInstance(clazz);

            Map<String, String> resolvedSuperVisors = topographyMapper.resolve(supervisorHostNames);
            for (Map.Entry<String, String> entry : resolvedSuperVisors.entrySet()) {
                String hostName = entry.getKey();
                String rack = entry.getValue();
                List<String> nodesForRack = this.networkTopography.computeIfAbsent(rack, k -> new ArrayList<>());
                nodesForRack.add(hostName);
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
     * Get heap memory usage for a worker's main process and logwriter process.
     *
     * @param topConf - the topology config
     * @return the assigned memory (in MB)
     */
    public static double getAssignedMemoryForSlot(final Map<String, Object> topConf) {
        double totalWorkerMemory = 0.0;
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
     * Check if the given topology is allowed for modification right now. If not throw an IllegalArgumentException else go on.
     *
     * @param topologyId the id of the topology to check
     */
    protected void assertValidTopologyForModification(String topologyId) {
        //NOOP
    }

    @Override
    public Topologies getTopologies() {
        return topologies;
    }

    @Override
    public Set<String> getBlacklistedHosts() {
        return blackListedHosts;
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
        List<TopologyDetails> ret = new ArrayList<>();
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
        return nodeToUsedSlotsCache.computeIfAbsent(supervisor.getId(), MAKE_SET)
            .stream()
            .map(WorkerSlot::getPort)
            .collect(Collectors.toSet());
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
        if (isBlackListed(supervisor.getId())) {
            return Collections.emptySet();
        }
        return supervisor.getAllPorts();
    }

    @Override
    public List<WorkerSlot> getNonBlacklistedAvailableSlots(List<String> blacklistedSupervisorIds) {
        List<WorkerSlot> slots = new ArrayList<>();
        for (SupervisorDetails supervisor : this.supervisors.values()) {
            if (!isBlackListed(supervisor.getId()) && !blacklistedSupervisorIds.contains(supervisor.getId())) {
                slots.addAll(getAvailableSlots(supervisor));
            }
        }

        return slots;
    }

    @Override
    public List<WorkerSlot> getAvailableSlots() {
        List<WorkerSlot> slots = new ArrayList<>();
        for (SupervisorDetails supervisor : this.supervisors.values()) {
            slots.addAll(this.getAvailableSlots(supervisor));
        }

        return slots;
    }

    @Override
    public List<WorkerSlot> getAvailableSlots(SupervisorDetails supervisor) {
        Set<Integer> ports = this.getAvailablePorts(supervisor);
        List<WorkerSlot> slots = new ArrayList<>(ports.size());

        for (Integer port : ports) {
            slots.add(new WorkerSlot(supervisor.getId(), port));
        }

        return slots;
    }

    @Override
    public List<WorkerSlot> getAssignableSlots(SupervisorDetails supervisor) {
        Set<Integer> ports = this.getAssignablePorts(supervisor);
        List<WorkerSlot> slots = new ArrayList<>(ports.size());

        for (Integer port : ports) {
            slots.add(new WorkerSlot(supervisor.getId(), port));
        }

        return slots;
    }

    @Override
    public List<WorkerSlot> getAssignableSlots() {
        List<WorkerSlot> slots = new ArrayList<>();
        for (SupervisorDetails supervisor : this.supervisors.values()) {
            slots.addAll(this.getAssignableSlots(supervisor));
        }

        return slots;
    }

    @Override
    public Collection<ExecutorDetails> getUnassignedExecutors(TopologyDetails topology) {
        if (topology == null) {
            return new ArrayList<>(0);
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

        Set<WorkerSlot> slots = new HashSet<>();
        slots.addAll(assignment.getExecutorToSlot().values());
        return slots.size();
    }

    @Override
    public NormalizedResourceOffer getAvailableResources(SupervisorDetails sd) {
        NormalizedResourceOffer ret = new NormalizedResourceOffer(sd.getTotalResources());
        for (SchedulerAssignment assignment: assignments.values()) {
            for (Entry<WorkerSlot, WorkerResources> entry: assignment.getScheduledResources().entrySet()) {
                if (sd.getId().equals(entry.getKey().getNodeId())) {
                   ret.remove(entry.getValue(), getResourceMetrics());
                }
            }
        }
        return ret;
    }

    private void addResource(Map<String, Double> resourceMap, String resourceName, Double valueToBeAdded) {
        if (!resourceMap.containsKey(resourceName)) {
            resourceMap.put(resourceName, 0.0);
        }
        Double currentPresent = resourceMap.get(resourceName);
        resourceMap.put(resourceName, currentPresent + valueToBeAdded);
    }

    private WorkerResources calculateWorkerResources(
        TopologyDetails td, Collection<ExecutorDetails> executors) {
        NormalizedResourceRequest totalResources = new NormalizedResourceRequest();
        Map<String, Double> sharedTotalResources = new HashMap<>();
        for (ExecutorDetails exec : executors) {
            NormalizedResourceRequest allResources = td.getTotalResources(exec);
            if (allResources == null) {
                continue;
            }
            totalResources.add(allResources);
        }
        for (SharedMemory shared : td.getSharedMemoryRequests(executors)) {
            totalResources.addOffHeap(shared.get_off_heap_worker());
            totalResources.addOnHeap(shared.get_off_heap_worker());

            addResource(
                sharedTotalResources,
                Constants.COMMON_OFFHEAP_MEMORY_RESOURCE_NAME, shared.get_off_heap_worker()
            );
            addResource(
                sharedTotalResources,
                Constants.COMMON_ONHEAP_MEMORY_RESOURCE_NAME, shared.get_on_heap()
            );
        }
        sharedTotalResources = NormalizedResources.RESOURCE_NAME_NORMALIZER.normalizedResourceMap(sharedTotalResources);
        WorkerResources ret = new WorkerResources();
        ret.set_resources(totalResources.toNormalizedMap());
        ret.set_shared_resources(sharedTotalResources);

        ret.set_cpu(totalResources.getTotalCpu());
        ret.set_mem_off_heap(totalResources.getOffHeapMemoryMb());
        ret.set_mem_on_heap(totalResources.getOnHeapMemoryMb());
        ret.set_shared_mem_off_heap(
            sharedTotalResources.getOrDefault(
                Constants.COMMON_OFFHEAP_MEMORY_RESOURCE_NAME, 0.0)
        );
        ret.set_shared_mem_on_heap(
            sharedTotalResources.getOrDefault(
                Constants.COMMON_ONHEAP_MEMORY_RESOURCE_NAME, 0.0)
        );
        return ret;
    }

    @Override
    public boolean wouldFit(
        WorkerSlot ws,
        ExecutorDetails exec,
        TopologyDetails td,
        NormalizedResourceOffer resourcesAvailable,
        double maxHeap) {

        NormalizedResourceRequest requestedResources = td.getTotalResources(exec);
        if (!resourcesAvailable.couldHoldIgnoringSharedMemory(requestedResources)) {
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
        double memoryAvailable = resourcesAvailable.getTotalMemoryMb();

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
        double sharedOffHeapMemory = calculateSharedOffHeapMemory(nodeId, assignment);
        assignment.setTotalSharedOffHeapMemory(nodeId, sharedOffHeapMemory);
        updateCachesForWorkerSlot(slot, resources, sharedOffHeapMemory);
        totalResourcesPerNodeCache.remove(slot.getNodeId());
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
     * @param nodeId     the id of the node
     * @param assignment the current assignment
     * @return the amount of shared off heap memory for that node in MB
     */
    private double calculateSharedOffHeapMemory(String nodeId, SchedulerAssignmentImpl assignment) {
        return calculateSharedOffHeapMemory(nodeId, assignment, null);
    }

    private double calculateSharedOffHeapMemory(
        String nodeId, SchedulerAssignmentImpl assignment, ExecutorDetails extra) {
        double memorySharedWithinNode = 0.0;
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
                nodeToScheduledResourcesCache.computeIfAbsent(nodeId, MAKE_MAP).put(slot, new NormalizedResourceRequest());
                nodeToUsedSlotsCache.computeIfAbsent(nodeId, MAKE_SET).remove(slot);
            }
        }
        //Invalidate the cache as something on the node changed
        totalResourcesPerNodeCache.remove(slot.getNodeId());
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
        return nodeToUsedSlotsCache.computeIfAbsent(slot.getNodeId(), MAKE_SET).contains(slot);
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
        return nodeToUsedSlotsCache.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
    }

    @Override
    public List<SupervisorDetails> getSupervisorsByHost(String host) {
        List<String> nodeIds = this.hostToId.get(host);
        List<SupervisorDetails> ret = new ArrayList<>();

        if (nodeIds != null) {
            for (String nodeId : nodeIds) {
                ret.add(this.getSupervisorById(nodeId));
            }
        }

        return ret;
    }

    @Override
    public Map<String, SchedulerAssignment> getAssignments() {
        return new HashMap<>(assignments);
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
        totalResourcesPerNodeCache.clear();
        nodeToScheduledResourcesCache.values().forEach(Map::clear);
        nodeToUsedSlotsCache.values().forEach(Set::clear);
        for (SchedulerAssignment assignment : newAssignments.values()) {
            assign(assignment, ignoreSingleExceptions);
        }
    }

    @Override
    public Map<String, SupervisorDetails> getSupervisors() {
        return this.supervisors;
    }

    @Override
    public NormalizedResourceOffer getNonBlacklistedClusterAvailableResources(Collection<String> blacklistedSupervisorIds) {
        NormalizedResourceOffer available = new NormalizedResourceOffer();
        for (SupervisorDetails sup : supervisors.values()) {
            if (!isBlackListed(sup.getId()) && !blacklistedSupervisorIds.contains(sup.getId())) {
                available.add(sup.getTotalResources());
                available.remove(getAllScheduledResourcesForNode(sup.getId()), getResourceMetrics());
            }
        }
        return available;
    }

    @Override
    public double getClusterTotalCpuResource() {
        double sum = 0.0;
        for (SupervisorDetails sup : supervisors.values()) {
            sum += sup.getTotalCpu();
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
     * set scheduler status for a topology.
     */
    public void setStatus(TopologyDetails td, String statusMessage) {
        setStatus(td.getId(), statusMessage);
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

    public String getStatus(String topoId) {
        return status.get(topoId);
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
            ret.put(sd.getId(), new SupervisorResources(sd.getTotalMemory(), sd.getTotalCpu(), 0, 0));
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

    /**
     * This medhod updates ScheduledResources and UsedSlots cache for given workerSlot.
     */
    private void updateCachesForWorkerSlot(WorkerSlot workerSlot, WorkerResources workerResources, Double sharedoffHeapMemory) {
        String nodeId = workerSlot.getNodeId();
        NormalizedResourceRequest normalizedResourceRequest = new NormalizedResourceRequest();
        normalizedResourceRequest.add(workerResources);
        normalizedResourceRequest.addOffHeap(sharedoffHeapMemory);
        nodeToScheduledResourcesCache.computeIfAbsent(nodeId, MAKE_MAP).put(workerSlot, normalizedResourceRequest);
        nodeToUsedSlotsCache.computeIfAbsent(nodeId, MAKE_SET).add(workerSlot);
    }

    public ResourceMetrics getResourceMetrics() {
        return resourceMetrics;
    }

    @Override
    public NormalizedResourceRequest getAllScheduledResourcesForNode(String nodeId) {
        return totalResourcesPerNodeCache.computeIfAbsent(nodeId, (nid) -> {
            NormalizedResourceRequest totalScheduledResources = new NormalizedResourceRequest();
            for (NormalizedResourceRequest req : nodeToScheduledResourcesCache.computeIfAbsent(nodeId, MAKE_MAP).values()) {
                totalScheduledResources.add(req);
            }
            return totalScheduledResources;
        });
    }

    @Override
    public double getScheduledMemoryForNode(String nodeId) {
        return getAllScheduledResourcesForNode(nodeId).getTotalMemoryMb();
    }

    @Override
    public double getScheduledCpuForNode(String nodeId) {
        return getAllScheduledResourcesForNode(nodeId).getTotalCpu();
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
