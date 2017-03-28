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

package org.apache.storm.scheduler.resource.strategies.scheduling;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.resource.RAS_Node;
import org.apache.storm.scheduler.resource.RAS_Nodes;
import org.apache.storm.scheduler.resource.ResourceUtils;
import org.apache.storm.scheduler.resource.SchedulingResult;
import org.apache.storm.scheduler.resource.SchedulingState;
import org.apache.storm.scheduler.resource.SchedulingStatus;
import org.apache.commons.collections.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.Component;

public class DefaultResourceAwareStrategy implements IStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultResourceAwareStrategy.class);
    private Cluster _cluster;
    private Topologies _topologies;
    private Map<String, List<String>> _clusterInfo;
    private RAS_Nodes _nodes;

    private TreeSet<ObjectResources> _sortedRacks = null;
    private Map<String, TreeSet<ObjectResources>> _rackIdToSortedNodes = new HashMap<String, TreeSet<ObjectResources>>();

    public void prepare (SchedulingState schedulingState) {
        _cluster = schedulingState.cluster;
        _topologies = schedulingState.topologies;
        _nodes = schedulingState.nodes;
        _clusterInfo = schedulingState.cluster.getNetworkTopography();
        LOG.debug(this.getClusterInfo());
    }

    public SchedulingResult schedule(TopologyDetails td) {
        if (_nodes.getNodes().size() <= 0) {
            LOG.warn("No available nodes to schedule tasks on!");
            return SchedulingResult.failure(SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES, "No available nodes to schedule tasks on!");
        }
        Collection<ExecutorDetails> unassignedExecutors = new HashSet<ExecutorDetails>(_cluster.getUnassignedExecutors(td));
        Map<WorkerSlot, Collection<ExecutorDetails>> schedulerAssignmentMap = new HashMap<>();
        LOG.debug("ExecutorsNeedScheduling: {}", unassignedExecutors);
        Collection<ExecutorDetails> scheduledTasks = new ArrayList<>();
        List<Component> spouts = this.getSpouts(td);

        if (spouts.size() == 0) {
            LOG.error("Cannot find a Spout!");
            return SchedulingResult.failure(SchedulingStatus.FAIL_INVALID_TOPOLOGY, "Cannot find a Spout!");
        }

        //order executors to be scheduled
        List<ExecutorDetails> orderedExecutors = orderExecutors(td, unassignedExecutors);

        Collection<ExecutorDetails> executorsNotScheduled = new HashSet<>(unassignedExecutors);

        for (ExecutorDetails exec : orderedExecutors) {
            LOG.debug("\n\nAttempting to schedule: {} of component {}[ REQ {} ]",
                    exec, td.getExecutorToComponent().get(exec),
                    td.getTaskResourceReqList(exec));
            scheduleExecutor(exec, td, schedulerAssignmentMap, scheduledTasks);
        }

        executorsNotScheduled.removeAll(scheduledTasks);
        LOG.debug("/* Scheduling left over task (most likely sys tasks) */");
        // schedule left over system tasks
        for (ExecutorDetails exec : executorsNotScheduled) {
            scheduleExecutor(exec, td, schedulerAssignmentMap, scheduledTasks);
        }

        SchedulingResult result;
        executorsNotScheduled.removeAll(scheduledTasks);
        if (executorsNotScheduled.size() > 0) {
            LOG.error("Not all executors successfully scheduled: {}",
                    executorsNotScheduled);
            schedulerAssignmentMap = null;
            result = SchedulingResult.failure(SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES,
                    (td.getExecutors().size() - unassignedExecutors.size()) + "/" + td.getExecutors().size() + " executors scheduled");
        } else {
            LOG.debug("All resources successfully scheduled!");
            result = SchedulingResult.successWithMsg(schedulerAssignmentMap, "Fully Scheduled by DefaultResourceAwareStrategy");
        }
        if (schedulerAssignmentMap == null) {
            LOG.error("Topology {} not successfully scheduled!", td.getId());
        }
        return result;
    }

    /**
     * Schedule executor exec from topology td
     *
     * @param exec the executor to schedule
     * @param td the topology executor exec is a part of
     * @param schedulerAssignmentMap the assignments already calculated
     * @param scheduledTasks executors that have been scheduled
     */
    private void scheduleExecutor(ExecutorDetails exec, TopologyDetails td, Map<WorkerSlot,
            Collection<ExecutorDetails>> schedulerAssignmentMap, Collection<ExecutorDetails> scheduledTasks) {
        WorkerSlot targetSlot = this.findWorkerForExec(exec, td, schedulerAssignmentMap);
        if (targetSlot != null) {
            RAS_Node targetNode = this.idToNode(targetSlot.getNodeId());
            if (!schedulerAssignmentMap.containsKey(targetSlot)) {
                schedulerAssignmentMap.put(targetSlot, new LinkedList<ExecutorDetails>());
            }

            schedulerAssignmentMap.get(targetSlot).add(exec);
            targetNode.consumeResourcesforTask(exec, td);
            scheduledTasks.add(exec);
            LOG.debug("TASK {} assigned to Node: {} avail [ mem: {} cpu: {} ] total [ mem: {} cpu: {} ] on slot: {} on Rack: {}", exec,
                    targetNode.getHostname(), targetNode.getAvailableMemoryResources(),
                    targetNode.getAvailableCpuResources(), targetNode.getTotalMemoryResources(),
                    targetNode.getTotalCpuResources(), targetSlot, nodeToRack(targetNode));
        } else {
            LOG.error("Not Enough Resources to schedule Task {}", exec);
        }
    }

    /**
     * Find a worker to schedule executor exec on
     *
     * @param exec the executor to schedule
     * @param td the topology that the executor is a part of
     * @param scheduleAssignmentMap already calculated assignments
     * @return a worker to assign exec on.  Returns null if a worker cannot be successfully found in cluster
     */
    private WorkerSlot findWorkerForExec(ExecutorDetails exec, TopologyDetails td, Map<WorkerSlot, Collection<ExecutorDetails>> scheduleAssignmentMap) {
        WorkerSlot ws = null;

        // iterate through an ordered list of all racks available to make sure we cannot schedule the first executor in any rack before we "give up"
        // the list is ordered in decreasing order of effective resources. With the rack in the front of the list having the most effective resources.
        if (_sortedRacks == null) {
            _sortedRacks = sortRacks(td.getId(), scheduleAssignmentMap);
        }

        for (ObjectResources rack : _sortedRacks) {
            ws = this.getBestWorker(exec, td, rack.id, scheduleAssignmentMap);
            if (ws != null) {
                LOG.debug("best rack: {}", rack.id);
                break;
            }
        }
        return ws;
    }

    /**
     * Get the best worker to assign executor exec on a rack
     *
     * @param exec the executor to schedule
     * @param td the topology that the executor is a part of
     * @param rackId the rack id of the rack to find a worker on
     * @param scheduleAssignmentMap already calculated assignments
     * @return a worker to assign executor exec to. Returns null if a worker cannot be successfully found on rack with rackId
     */
    private WorkerSlot getBestWorker(ExecutorDetails exec, TopologyDetails td, String rackId, Map<WorkerSlot, Collection<ExecutorDetails>> scheduleAssignmentMap) {

        if (!_rackIdToSortedNodes.containsKey(rackId)) {
            _rackIdToSortedNodes.put(rackId, sortNodes(this.getAvailableNodesFromRack(rackId), rackId, td.getId(), scheduleAssignmentMap));
        }

        TreeSet<ObjectResources> sortedNodes = _rackIdToSortedNodes.get(rackId);

        double taskMem = td.getTotalMemReqTask(exec);
        double taskCPU = td.getTotalCpuReqTask(exec);
        for (ObjectResources nodeResources : sortedNodes) {
            RAS_Node n = _nodes.getNodeById(nodeResources.id);
            if (n.getAvailableCpuResources() >= taskCPU && n.getAvailableMemoryResources() >= taskMem && n.getFreeSlots().size() > 0) {
                for (WorkerSlot ws : n.getFreeSlots()) {
                    if (checkWorkerConstraints(exec, ws, td, scheduleAssignmentMap)) {
                        return ws;
                    }
                }
            }
        }
        return null;
    }

    /**
     * interface for calculating the number of existing executors scheduled on a object (rack or node)
     */
    private interface ExistingScheduleFunc {
        int getNumExistingSchedule(String objectId);
    }

    /**
     * a class to contain individual object resources as well as cumulative stats
     */
    static class AllResources {
        List<ObjectResources> objectResources = new LinkedList<ObjectResources>();
        double availMemResourcesOverall = 0.0;
        double totalMemResourcesOverall = 0.0;
        double availCpuResourcesOverall = 0.0;
        double totalCpuResourcesOverall = 0.0;
        String identifier;

        public AllResources(String identifier) {
            this.identifier = identifier;
        }
    }

    /**
     * class to keep track of resources on a rack or node
     */
     static class ObjectResources {
        String id;
        double availMem = 0.0;
        double totalMem = 0.0;
        double availCpu = 0.0;
        double totalCpu = 0.0;
        double effectiveResources = 0.0;

        public ObjectResources(String id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return this.id;
        }
    }

    /**
     * Sorted Nodes
     *
     * @param availNodes            a list of all the nodes we want to sort
     * @param rackId                the rack id availNodes are a part of
     * @param topoId                the topology that we are trying to schedule
     * @param scheduleAssignmentMap calculated assignments so far
     * @return a sorted list of nodes
     * <p>
     * Nodes are sorted by two criteria. 1) the number executors of the topology that needs to be scheduled is already on the node in descending order.
     * The reasoning to sort based on criterion 1 is so we schedule the rest of a topology on the same node as the existing executors of the topology.
     * 2) the subordinate/subservient resource availability percentage of a node in descending order
     * We calculate the resource availability percentage by dividing the resource availability on the node by the resource availability of the entire rack
     * By doing this calculation, nodes that have exhausted or little of one of the resources mentioned above will be ranked after nodes that have more balanced resource availability.
     * So we will be less likely to pick a node that have a lot of one resource but a low amount of another.
     */
    private TreeSet<ObjectResources> sortNodes(List<RAS_Node> availNodes, String rackId, final String topoId, final Map<WorkerSlot, Collection<ExecutorDetails>> scheduleAssignmentMap) {
        AllResources allResources = new AllResources("RACK");
        List<ObjectResources> nodes = allResources.objectResources;
        final Map<String, String> nodeIdToRackId = new HashMap<String, String>();

        for (RAS_Node ras_node : availNodes) {
            String nodeId = ras_node.getId();
            ObjectResources node = new ObjectResources(nodeId);

            double availMem = ras_node.getAvailableMemoryResources();
            double availCpu = ras_node.getAvailableCpuResources();
            int freeSlots = ras_node.totalSlotsFree();
            double totalMem = ras_node.getTotalMemoryResources();
            double totalCpu = ras_node.getTotalCpuResources();
            int totalSlots = ras_node.totalSlots();

            node.availMem = availMem;
            node.totalMem = totalMem;
            node.availCpu = availCpu;
            node.totalCpu = totalCpu;
            nodes.add(node);

            allResources.availMemResourcesOverall += availMem;
            allResources.availCpuResourcesOverall += availCpu;

            allResources.totalMemResourcesOverall += totalMem;
            allResources.totalCpuResourcesOverall += totalCpu;
        }


        LOG.debug("Rack {}: Overall Avail [ CPU {} MEM {} ] Total [ CPU {} MEM {} ]",
                rackId, allResources.availCpuResourcesOverall, allResources.availMemResourcesOverall, allResources.totalCpuResourcesOverall, allResources.totalMemResourcesOverall);

        return sortObjectResources(allResources, new ExistingScheduleFunc() {
            @Override
            public int getNumExistingSchedule(String objectId) {

                //Get execs already assigned in rack
                Collection<ExecutorDetails> execs = new LinkedList<ExecutorDetails>();
                if (_cluster.getAssignmentById(topoId) != null) {
                    for (Map.Entry<ExecutorDetails, WorkerSlot> entry : _cluster.getAssignmentById(topoId).getExecutorToSlot().entrySet()) {
                        WorkerSlot workerSlot = entry.getValue();
                        ExecutorDetails exec = entry.getKey();
                        if (workerSlot.getNodeId().equals(objectId)) {
                            execs.add(exec);
                        }
                    }
                }
                // get execs already scheduled in the current scheduling
                for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> entry : scheduleAssignmentMap.entrySet()) {

                    WorkerSlot workerSlot = entry.getKey();
                    if (workerSlot.getNodeId().equals(objectId)) {
                        execs.addAll(entry.getValue());
                    }
                }
                return execs.size();
            }
        });
    }

    /**
     * Sort racks
     *
     * @param topoId                topology id
     * @param scheduleAssignmentMap calculated assignments so far
     * @return a sorted list of racks
     * Racks are sorted by two criteria. 1) the number executors of the topology that needs to be scheduled is already on the rack in descending order.
     * The reasoning to sort based on  criterion 1 is so we schedule the rest of a topology on the same rack as the existing executors of the topology.
     * 2) the subordinate/subservient resource availability percentage of a rack in descending order
     * We calculate the resource availability percentage by dividing the resource availability on the rack by the resource availability of the entire cluster
     * By doing this calculation, racks that have exhausted or little of one of the resources mentioned above will be ranked after racks that have more balanced resource availability.
     * So we will be less likely to pick a rack that have a lot of one resource but a low amount of another.
     */
    TreeSet<ObjectResources> sortRacks(final String topoId, final Map<WorkerSlot, Collection<ExecutorDetails>> scheduleAssignmentMap) {
        AllResources allResources = new AllResources("Cluster");
        List<ObjectResources> racks = allResources.objectResources;

        final Map<String, String> nodeIdToRackId = new HashMap<String, String>();

        for (Map.Entry<String, List<String>> entry : _clusterInfo.entrySet()) {
            String rackId = entry.getKey();
            List<String> nodeIds = entry.getValue();
            ObjectResources rack = new ObjectResources(rackId);
            racks.add(rack);
            for (String nodeId : nodeIds) {
                RAS_Node node = _nodes.getNodeById(this.NodeHostnameToId(nodeId));
                double availMem = node.getAvailableMemoryResources();
                double availCpu = node.getAvailableCpuResources();
                double totalMem = node.getTotalMemoryResources();
                double totalCpu = node.getTotalCpuResources();

                rack.availMem += availMem;
                rack.totalMem += totalMem;
                rack.availCpu += availCpu;
                rack.totalCpu += totalCpu;
                nodeIdToRackId.put(nodeId, rack.id);

                allResources.availMemResourcesOverall += availMem;
                allResources.availCpuResourcesOverall += availCpu;

                allResources.totalMemResourcesOverall += totalMem;
                allResources.totalCpuResourcesOverall += totalCpu;
            }
        }
        LOG.debug("Cluster Overall Avail [ CPU {} MEM {} ] Total [ CPU {} MEM {} ]",
                allResources.availCpuResourcesOverall, allResources.availMemResourcesOverall, allResources.totalCpuResourcesOverall, allResources.totalMemResourcesOverall);

        return sortObjectResources(allResources, new ExistingScheduleFunc() {
            @Override
            public int getNumExistingSchedule(String objectId) {

                String rackId = objectId;
                //Get execs already assigned in rack
                Collection<ExecutorDetails> execs = new LinkedList<ExecutorDetails>();
                if (_cluster.getAssignmentById(topoId) != null) {
                    for (Map.Entry<ExecutorDetails, WorkerSlot> entry : _cluster.getAssignmentById(topoId).getExecutorToSlot().entrySet()) {
                        String nodeId = entry.getValue().getNodeId();
                        String hostname = idToNode(nodeId).getHostname();
                        ExecutorDetails exec = entry.getKey();
                        if (nodeIdToRackId.get(hostname) != null && nodeIdToRackId.get(hostname).equals(rackId)) {
                            execs.add(exec);
                        }
                    }
                }
                // get execs already scheduled in the current scheduling
                for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> entry : scheduleAssignmentMap.entrySet()) {
                    WorkerSlot workerSlot = entry.getKey();
                    String nodeId = workerSlot.getNodeId();
                    String hostname = idToNode(nodeId).getHostname();
                    if (nodeIdToRackId.get(hostname).equals(rackId)) {
                        execs.addAll(entry.getValue());
                    }
                }
                return execs.size();
            }
        });
    }

    /**
     * Sort objects by the following two criteria. 1) the number executors of the topology that needs to be scheduled is already on the object (node or rack) in descending order.
     * The reasoning to sort based on criterion 1 is so we schedule the rest of a topology on the same object (node or rack) as the existing executors of the topology.
     * 2) the subordinate/subservient resource availability percentage of a rack in descending order
     * We calculate the resource availability percentage by dividing the resource availability of the object (node or rack) by the resource availability of the entire rack or cluster depending on if object
     * references a node or a rack.
     * By doing this calculation, objects (node or rack) that have exhausted or little of one of the resources mentioned above will be ranked after racks that have more balanced resource availability.
     * So we will be less likely to pick a rack that have a lot of one resource but a low amount of another.
     *
     * @param allResources         contains all individual ObjectResources as well as cumulative stats
     * @param existingScheduleFunc a function to get existing executors already scheduled on this object
     * @return a sorted list of ObjectResources
     */
    private TreeSet<ObjectResources> sortObjectResources(final AllResources allResources, final ExistingScheduleFunc existingScheduleFunc) {

        for (ObjectResources objectResources : allResources.objectResources) {
            StringBuilder sb = new StringBuilder();
            if (allResources.availCpuResourcesOverall <= 0.0  || allResources.availMemResourcesOverall <= 0.0) {
                objectResources.effectiveResources = 0.0;
            } else {
                List<Double> values = new LinkedList<Double>();

                //add cpu
                double cpuPercent = (objectResources.availCpu / allResources.availCpuResourcesOverall) * 100.0;
                values.add(cpuPercent);
                sb.append(String.format("CPU %f(%f%%) ", objectResources.availCpu, cpuPercent));

                //add memory
                double memoryPercent = (objectResources.availMem / allResources.availMemResourcesOverall) * 100.0;
                values.add(memoryPercent);
                sb.append(String.format("MEM %f(%f%%) ", objectResources.availMem, memoryPercent));

                objectResources.effectiveResources = Collections.min(values);
            }
            LOG.debug("{}: Avail [ {} ] Total [ CPU {} MEM {}] effective resources: {}",
                    objectResources.id, sb.toString(),
                    objectResources.totalCpu, objectResources.totalMem, objectResources.effectiveResources);
        }

        TreeSet<ObjectResources> sortedObjectResources = new TreeSet<ObjectResources>(new Comparator<ObjectResources>() {
            @Override
            public int compare(ObjectResources o1, ObjectResources o2) {

                int execsScheduled1 = existingScheduleFunc.getNumExistingSchedule(o1.id);
                int execsScheduled2 = existingScheduleFunc.getNumExistingSchedule(o2.id);
                if (execsScheduled1 > execsScheduled2) {
                    return -1;
                } else if (execsScheduled1 < execsScheduled2) {
                    return 1;
                } else {
                    if (o1.effectiveResources > o2.effectiveResources) {
                        return -1;
                    } else if (o1.effectiveResources < o2.effectiveResources) {
                        return 1;
                    } else {
                        List<Double> o1_values = new LinkedList<Double>();
                        List<Double> o2_values = new LinkedList<Double>();
                        o1_values.add((o1.availCpu / allResources.availCpuResourcesOverall) * 100.0);
                        o2_values.add((o2.availCpu / allResources.availCpuResourcesOverall) * 100.0);

                        o1_values.add((o1.availMem / allResources.availMemResourcesOverall) * 100.0);
                        o2_values.add((o2.availMem / allResources.availMemResourcesOverall) * 100.0);

                        double o1_avg = ResourceUtils.avg(o1_values);
                        double o2_avg = ResourceUtils.avg(o2_values);

                        if (o1_avg > o2_avg) {
                            return -1;
                        } else if (o1_avg < o2_avg) {
                            return 1;
                        } else {
                            return o1.id.compareTo(o2.id);
                        }
                    }
                }
            }
        });
        sortedObjectResources.addAll(allResources.objectResources);
        LOG.debug("Sorted Object Resources: {}", sortedObjectResources);
        return sortedObjectResources;
    }

    /**
     * Get the rack on which a node is a part of
     *
     * @param node the node to find out which rack its on
     * @return the rack id
     */
    private String nodeToRack(RAS_Node node) {
        for (Map.Entry<String, List<String>> entry : _clusterInfo
                .entrySet()) {
            if (entry.getValue().contains(node.getHostname())) {
                return entry.getKey();
            }
        }
        LOG.error("Node: {} not found in any racks", node.getHostname());
        return null;
    }

    /**
     * get a list nodes from a rack
     *
     * @param rackId the rack id of the rack to get nodes from
     * @return a list of nodes
     */
    private List<RAS_Node> getAvailableNodesFromRack(String rackId) {
        List<RAS_Node> retList = new ArrayList<>();
        for (String node_id : _clusterInfo.get(rackId)) {
            retList.add(_nodes.getNodeById(this
                    .NodeHostnameToId(node_id)));
        }
        return retList;
    }

    /**
     * sort components by the number of in and out connections that need to be made
     *
     * @param componentMap The components that need to be sorted
     * @return a sorted set of components
     */
    private Set<Component> sortComponents(final Map<String, Component> componentMap) {
        Set<Component> sortedComponents = new TreeSet<Component>(new Comparator<Component>() {
            @Override
            public int compare(Component o1, Component o2) {
                int connections1 = 0;
                int connections2 = 0;

                for (String childId : (List<String>) ListUtils.union(o1.children, o1.parents)) {
                    connections1 += (componentMap.get(childId).execs.size() * o1.execs.size());
                }

                for (String childId : (List<String>) ListUtils.union(o2.children, o2.parents)) {
                    connections2 += (componentMap.get(childId).execs.size() * o2.execs.size());
                }

                if (connections1 > connections1) {
                    return -1;
                } else if (connections1 < connections2) {
                    return 1;
                } else {
                    return o1.id.compareTo(o2.id);
                }
            }
        });
        sortedComponents.addAll(componentMap.values());
        return sortedComponents;
    }

    /**
     * Sort a component's neighbors by the number of connections it needs to make with this component
     *
     * @param thisComp     the component that we need to sort its neighbors
     * @param componentMap all the components to sort
     * @return a sorted set of components
     */
    private Set<Component> sortNeighbors(final Component thisComp, final Map<String, Component> componentMap) {
        Set<Component> sortedComponents = new TreeSet<Component>(new Comparator<Component>() {
            @Override
            public int compare(Component o1, Component o2) {
                int connections1 = o1.execs.size() * thisComp.execs.size();
                int connections2 = o2.execs.size() * thisComp.execs.size();
                if (connections1 > connections2) {
                    return -1;
                } else if (connections1 < connections2) {
                    return 1;
                } else {
                    return o1.id.compareTo(o2.id);
                }
            }
        });
        sortedComponents.addAll(componentMap.values());
        return sortedComponents;
    }

    /**
     * Order executors based on how many in and out connections it will potentially need to make.
     * First order components by the number of in and out connections it will have.  Then iterate through the sorted list of components.
     * For each component sort the neighbors of that component by how many connections it will have to make with that component.
     * Add an executor from this component and then from each neighboring component in sorted order.  Do this until there is nothing left to schedule
     *
     * @param td                  The topology the executors belong to
     * @param unassignedExecutors a collection of unassigned executors that need to be unassigned. Should only try to assign executors from this list
     * @return a list of executors in sorted order
     */
    private List<ExecutorDetails> orderExecutors(TopologyDetails td, Collection<ExecutorDetails> unassignedExecutors) {
        Map<String, Component> componentMap = td.getComponents();
        List<ExecutorDetails> execsScheduled = new LinkedList<>();

        Map<String, Queue<ExecutorDetails>> compToExecsToSchedule = new HashMap<>();
        for (Component component : componentMap.values()) {
            compToExecsToSchedule.put(component.id, new LinkedList<ExecutorDetails>());
            for (ExecutorDetails exec : component.execs) {
                if (unassignedExecutors.contains(exec)) {
                    compToExecsToSchedule.get(component.id).add(exec);
                }
            }
        }

        Set<Component> sortedComponents = sortComponents(componentMap);
        sortedComponents.addAll(componentMap.values());

        for (Component currComp : sortedComponents) {
            Map<String, Component> neighbors = new HashMap<String, Component>();
            for (String compId : (List<String>) ListUtils.union(currComp.children, currComp.parents)) {
                neighbors.put(compId, componentMap.get(compId));
            }
            Set<Component> sortedNeighbors = sortNeighbors(currComp, neighbors);
            Queue<ExecutorDetails> currCompExesToSched = compToExecsToSchedule.get(currComp.id);

            boolean flag = false;
            do {
                flag = false;
                if (!currCompExesToSched.isEmpty()) {
                    execsScheduled.add(currCompExesToSched.poll());
                    flag = true;
                }

                for (Component neighborComp : sortedNeighbors) {
                    Queue<ExecutorDetails> neighborCompExesToSched = compToExecsToSchedule.get(neighborComp.id);
                    if (!neighborCompExesToSched.isEmpty()) {
                        execsScheduled.add(neighborCompExesToSched.poll());
                        flag = true;
                    }
                }
            } while (flag);
        }
        return execsScheduled;
    }

    /**
     * Get a list of all the spouts in the topology
     *
     * @param td topology to get spouts from
     * @return a list of spouts
     */
    private List<Component> getSpouts(TopologyDetails td) {
        List<Component> spouts = new ArrayList<>();

        for (Component c : td.getComponents().values()) {
            if (c.type == Component.ComponentType.SPOUT) {
                spouts.add(c);
            }
        }
        return spouts;
    }

    /**
     * Get the remaining amount memory that can be assigned to a worker given the set worker max heap size
     *
     * @param ws                    the worker to get the remaining amount of memory that can be assigned to it
     * @param td                    the topology that has executors running on the worker
     * @param scheduleAssignmentMap the schedulings calculated so far
     * @return The remaining amount of memory
     */
    private Double getWorkerScheduledMemoryAvailable(WorkerSlot ws, TopologyDetails td, Map<WorkerSlot, Collection<ExecutorDetails>> scheduleAssignmentMap) {
        Double memScheduleUsed = this.getWorkerScheduledMemoryUse(ws, td, scheduleAssignmentMap);
        return td.getTopologyWorkerMaxHeapSize() - memScheduleUsed;
    }

    /**
     * Get the amount of memory already assigned to a worker
     *
     * @param ws                    the worker to get the amount of memory assigned to a worker
     * @param td                    the topology that has executors running on the worker
     * @param scheduleAssignmentMap the schedulings calculated so far
     * @return the amount of memory
     */
    private Double getWorkerScheduledMemoryUse(WorkerSlot ws, TopologyDetails td, Map<WorkerSlot, Collection<ExecutorDetails>> scheduleAssignmentMap) {
        Double totalMem = 0.0;
        Collection<ExecutorDetails> execs = scheduleAssignmentMap.get(ws);
        if (execs != null) {
            for (ExecutorDetails exec : execs) {
                totalMem += td.getTotalMemReqTask(exec);
            }
        }
        return totalMem;
    }

    /**
     * Checks whether we can schedule an Executor exec on the worker slot ws
     * Only considers memory currently.  May include CPU in the future
     *
     * @param exec                  the executor to check whether it can be asssigned to worker ws
     * @param ws                    the worker to check whether executor exec can be assigned to it
     * @param td                    the topology that the exec is from
     * @param scheduleAssignmentMap the schedulings calculated so far
     * @return a boolean: True denoting the exec can be scheduled on ws and false if it cannot
     */
    private boolean checkWorkerConstraints(ExecutorDetails exec, WorkerSlot ws, TopologyDetails td, Map<WorkerSlot, Collection<ExecutorDetails>> scheduleAssignmentMap) {
        boolean retVal = false;
        if (this.getWorkerScheduledMemoryAvailable(ws, td, scheduleAssignmentMap) >= td.getTotalMemReqTask(exec)) {
            retVal = true;
        }
        return retVal;
    }

    /**
     * Get the amount of resources available and total for each node
     *
     * @return a String with cluster resource info for debug
     */
    private String getClusterInfo() {
        String retVal = "Cluster info:\n";
        for (Map.Entry<String, List<String>> clusterEntry : _clusterInfo.entrySet()) {
            String clusterId = clusterEntry.getKey();
            retVal += "Rack: " + clusterId + "\n";
            for (String nodeHostname : clusterEntry.getValue()) {
                RAS_Node node = this.idToNode(this.NodeHostnameToId(nodeHostname));
                retVal += "-> Node: " + node.getHostname() + " " + node.getId() + "\n";
                retVal += "--> Avail Resources: {Mem " + node.getAvailableMemoryResources() + ", CPU " + node.getAvailableCpuResources() + " Slots: " + node.totalSlotsFree() + "}\n";
                retVal += "--> Total Resources: {Mem " + node.getTotalMemoryResources() + ", CPU " + node.getTotalCpuResources() + " Slots: " + node.totalSlots() + "}\n";
            }
        }
        return retVal;
    }

    /**
     * hostname to Id
     *
     * @param hostname the hostname to convert to node id
     * @return the id of a node
     */
    public String NodeHostnameToId(String hostname) {
        for (RAS_Node n : _nodes.getNodes()) {
            if (n.getHostname() == null) {
                continue;
            }
            if (n.getHostname().equals(hostname)) {
                return n.getId();
            }
        }
        LOG.error("Cannot find Node with hostname {}", hostname);
        return null;
    }

    /**
     * Find RAS_Node for specified node id
     *
     * @param id the node/supervisor id to lookup
     * @return a RAS_Node object
     */
    public RAS_Node idToNode(String id) {
        RAS_Node ret = _nodes.getNodeById(id);
        if (ret == null) {
            LOG.error("Cannot find Node with Id: {}", id);
        }
        return ret;
    }
}
