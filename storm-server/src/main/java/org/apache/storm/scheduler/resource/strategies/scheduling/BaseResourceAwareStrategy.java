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

package org.apache.storm.scheduler.resource.strategies.scheduling;

import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import org.apache.storm.generated.ComponentType;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.Component;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.RAS_Node;
import org.apache.storm.scheduler.resource.RAS_Nodes;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceOffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseResourceAwareStrategy implements IStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(BaseResourceAwareStrategy.class);
    protected Cluster cluster;
    private Map<String, List<String>> networkTopography;
    protected RAS_Nodes nodes;

    @VisibleForTesting
    void prepare(Cluster cluster) {
        this.cluster = cluster;
        nodes = new RAS_Nodes(cluster);
        networkTopography = cluster.getNetworkTopography();
        logClusterInfo();
    }

    @Override
    public void prepare(Map<String, Object> config) {
        //NOOP
    }

    /**
     * Schedule executor exec from topology td.
     *
     * @param exec the executor to schedule
     * @param td the topology executor exec is a part of
     * @param scheduledTasks executors that have been scheduled
     */
    protected void scheduleExecutor(
            ExecutorDetails exec, TopologyDetails td, Collection<ExecutorDetails> scheduledTasks, List<ObjectResources> sortedNodes) {
        WorkerSlot targetSlot = findWorkerForExec(exec, td, sortedNodes);
        if (targetSlot != null) {
            RAS_Node targetNode = idToNode(targetSlot.getNodeId());
            targetNode.assignSingleExecutor(targetSlot, exec, td);
            scheduledTasks.add(exec);
            LOG.debug(
                    "TASK {} assigned to Node: {} avail [ mem: {} cpu: {} ] total [ mem: {} cpu: {} ] on "
                            + "slot: {} on Rack: {}",
                    exec,
                    targetNode.getHostname(),
                    targetNode.getAvailableMemoryResources(),
                    targetNode.getAvailableCpuResources(),
                    targetNode.getTotalMemoryResources(),
                    targetNode.getTotalCpuResources(),
                    targetSlot,
                    nodeToRack(targetNode));
        } else {
            LOG.error("Not Enough Resources to schedule Task {}", exec);
        }
    }

    protected abstract TreeSet<ObjectResources> sortObjectResources(
            final AllResources allResources, ExecutorDetails exec, TopologyDetails topologyDetails,
            final ExistingScheduleFunc existingScheduleFunc
    );

    /**
     * Find a worker to schedule executor exec on.
     *
     * @param exec the executor to schedule
     * @param td the topology that the executor is a part of
     * @return a worker to assign exec on. Returns null if a worker cannot be successfully found in cluster
     */
    protected WorkerSlot findWorkerForExec(ExecutorDetails exec, TopologyDetails td, List<ObjectResources> sortedNodes) {
        for (ObjectResources nodeResources : sortedNodes) {
            RAS_Node node = nodes.getNodeById(nodeResources.id);
            for (WorkerSlot ws : node.getSlotsAvailbleTo(td)) {
                if (node.wouldFit(ws, exec, td)) {
                    return ws;
                }
            }
        }
        return null;
    }

    /**
     * interface for calculating the number of existing executors scheduled on a object (rack or
     * node).
     */
    protected interface ExistingScheduleFunc {
        int getNumExistingSchedule(String objectId);
    }

    /**
     * a class to contain individual object resources as well as cumulative stats.
     */
    static class AllResources {
        List<ObjectResources> objectResources = new LinkedList<>();
        NormalizedResourceOffer availableResourcesOverall = new NormalizedResourceOffer();
        NormalizedResourceOffer totalResourcesOverall = new NormalizedResourceOffer();
        String identifier;

        public AllResources(String identifier) {
            this.identifier = identifier;
        }

        public AllResources(AllResources other) {
            this (null,
                new NormalizedResourceOffer(other.availableResourcesOverall),
                new NormalizedResourceOffer(other.totalResourcesOverall),
                other.identifier);
            List<ObjectResources> objectResourcesList = new ArrayList<>();
            for (ObjectResources objectResource : other.objectResources) {
                objectResourcesList.add(new ObjectResources(objectResource));
            }
            this.objectResources = objectResourcesList;
        }

        public AllResources(List<ObjectResources> objectResources, NormalizedResourceOffer availableResourcesOverall,
                            NormalizedResourceOffer totalResourcesOverall, String identifier) {
            this.objectResources = objectResources;
            this.availableResourcesOverall = availableResourcesOverall;
            this.totalResourcesOverall = totalResourcesOverall;
            this.identifier = identifier;
        }
    }

    /**
     * class to keep track of resources on a rack or node.
     */
    static class ObjectResources {
        public final String id;
        public NormalizedResourceOffer availableResources = new NormalizedResourceOffer();
        public NormalizedResourceOffer totalResources = new NormalizedResourceOffer();
        public double effectiveResources = 0.0;

        public ObjectResources(String id) {
            this.id = id;
        }

        public ObjectResources(ObjectResources other) {
            this(other.id, other.availableResources, other.totalResources, other.effectiveResources);
        }

        public ObjectResources(String id, NormalizedResourceOffer availableResources, NormalizedResourceOffer totalResources,
                               double effectiveResources) {
            this.id = id;
            this.availableResources = availableResources;
            this.totalResources = totalResources;
            this.effectiveResources = effectiveResources;
        }

        @Override
        public String toString() {
            return this.id;
        }
    }

    /**
     * Nodes are sorted by two criteria.
     *
     * <p>1) the number executors of the topology that needs to be scheduled is already on the node in
     * descending order. The reasoning to sort based on criterion 1 is so we schedule the rest of a
     * topology on the same node as the existing executors of the topology.
     *
     * <p>2) the subordinate/subservient resource availability percentage of a node in descending
     * order We calculate the resource availability percentage by dividing the resource availability
     * that have exhausted or little of one of the resources mentioned above will be ranked after
     * on the node by the resource availability of the entire rack By doing this calculation, nodes
     * nodes that have more balanced resource availability. So we will be less likely to pick a node
     * that have a lot of one resource but a low amount of another.
     *
     * @param availNodes a list of all the nodes we want to sort
     * @param rackId the rack id availNodes are a part of
     * @return a sorted list of nodes.
     */
    protected TreeSet<ObjectResources> sortNodes(
            List<RAS_Node> availNodes, ExecutorDetails exec, TopologyDetails topologyDetails, String rackId) {
        AllResources allResources = new AllResources("RACK");
        List<ObjectResources> nodes = allResources.objectResources;

        for (RAS_Node rasNode : availNodes) {
            String nodeId = rasNode.getId();
            ObjectResources node = new ObjectResources(nodeId);

            node.availableResources = rasNode.getTotalAvailableResources();
            node.totalResources = rasNode.getTotalResources();

            nodes.add(node);
            allResources.availableResourcesOverall.add(node.availableResources);
            allResources.totalResourcesOverall.add(node.totalResources);

        }

        LOG.debug(
            "Rack {}: Overall Avail [ {} ] Total [ {} ]",
            rackId,
            allResources.availableResourcesOverall,
            allResources.totalResourcesOverall);

        String topoId = topologyDetails.getId();
        return sortObjectResources(
            allResources,
            exec,
            topologyDetails,
            new ExistingScheduleFunc() {
                @Override
                public int getNumExistingSchedule(String objectId) {

                    //Get execs already assigned in rack
                    Collection<ExecutorDetails> execs = new LinkedList<>();
                    if (cluster.getAssignmentById(topoId) != null) {
                        for (Map.Entry<ExecutorDetails, WorkerSlot> entry :
                            cluster.getAssignmentById(topoId).getExecutorToSlot().entrySet()) {
                            WorkerSlot workerSlot = entry.getValue();
                            ExecutorDetails exec = entry.getKey();
                            if (workerSlot.getNodeId().equals(objectId)) {
                                execs.add(exec);
                            }
                        }
                    }
                    return execs.size();
                }
            });
    }

    protected List<ObjectResources> sortAllNodes(TopologyDetails td, ExecutorDetails exec,
                                                 List<String> favoredNodes, List<String> unFavoredNodes) {
        TreeSet<ObjectResources> sortedRacks = sortRacks(exec, td);
        ArrayList<ObjectResources> totallySortedNodes = new ArrayList<>();
        for (ObjectResources rack : sortedRacks) {
            final String rackId = rack.id;
            TreeSet<ObjectResources> sortedNodes = sortNodes(
                    getAvailableNodesFromRack(rackId), exec, td, rackId);
            totallySortedNodes.addAll(sortedNodes);
        }
        //Now do some post processing to add make some nodes preferred over others.
        if (favoredNodes != null || unFavoredNodes != null) {
            HashMap<String, Integer> hostOrder = new HashMap<>();
            if (favoredNodes != null) {
                int size = favoredNodes.size();
                for (int i = 0; i < size; i++) {
                    //First in the list is the most desired so gets the Lowest possible value
                    hostOrder.put(favoredNodes.get(i), -(size - i));
                }
            }
            if (unFavoredNodes != null) {
                int size = unFavoredNodes.size();
                for (int i = 0; i < size; i++) {
                    //First in the list is the least desired so gets the highest value
                    hostOrder.put(unFavoredNodes.get(i), size - i);
                }
            }
            //java guarantees a stable sort so we can just return 0 for values we don't want to move.
            Collections.sort(totallySortedNodes, (o1, o2) -> {
                RAS_Node n1 = this.nodes.getNodeById(o1.id);
                String host1 = n1.getHostname();
                int h1Value = hostOrder.getOrDefault(host1, 0);

                RAS_Node n2 = this.nodes.getNodeById(o2.id);
                String host2 = n2.getHostname();
                int h2Value = hostOrder.getOrDefault(host2, 0);

                return Integer.compare(h1Value, h2Value);
            });
        }
        return totallySortedNodes;
    }

    /**
     * Racks are sorted by two criteria.
     *
     * <p>1) the number executors of the topology that needs to be scheduled is already on the rack in descending order.
     * The reasoning to sort based on criterion 1 is so we schedule the rest of a topology on the same rack as the
     * existing executors of the topology.
     *
     * <p>2) the subordinate/subservient resource availability percentage of a rack in descending order We calculate
     * the resource availability percentage by dividing the resource availability on the rack by the resource
     * availability of the  entire cluster By doing this calculation, racks that have exhausted or little of one of
     * the resources mentioned above will be ranked after racks that have more balanced resource availability. So we
     * will be less likely to pick a rack that have a lot of one resource but a low amount of another.
     *
     * @return a sorted list of racks
     */
    @VisibleForTesting
    TreeSet<ObjectResources> sortRacks(ExecutorDetails exec, TopologyDetails topologyDetails) {
        AllResources allResources = new AllResources("Cluster");
        List<ObjectResources> racks = allResources.objectResources;

        final Map<String, String> nodeIdToRackId = new HashMap<String, String>();

        for (Map.Entry<String, List<String>> entry : networkTopography.entrySet()) {
            String rackId = entry.getKey();
            List<String> nodeIds = entry.getValue();
            ObjectResources rack = new ObjectResources(rackId);
            racks.add(rack);
            for (String nodeId : nodeIds) {
                RAS_Node node = nodes.getNodeById(nodeHostnameToId(nodeId));
                rack.availableResources.add(node.getTotalAvailableResources());
                rack.totalResources.add(node.getTotalAvailableResources());

                nodeIdToRackId.put(nodeId, rack.id);

                allResources.totalResourcesOverall.add(rack.totalResources);
                allResources.availableResourcesOverall.add(rack.availableResources);

            }
        }
        LOG.debug(
            "Cluster Overall Avail [ {} ] Total [ {} ]",
            allResources.availableResourcesOverall,
            allResources.totalResourcesOverall);

        String topoId = topologyDetails.getId();
        return sortObjectResources(
            allResources,
            exec,
            topologyDetails,
            (objectId) -> {
                String rackId = objectId;
                //Get execs already assigned in rack
                Collection<ExecutorDetails> execs = new LinkedList<>();
                if (cluster.getAssignmentById(topoId) != null) {
                    for (Map.Entry<ExecutorDetails, WorkerSlot> entry :
                        cluster.getAssignmentById(topoId).getExecutorToSlot().entrySet()) {
                        String nodeId = entry.getValue().getNodeId();
                        String hostname = idToNode(nodeId).getHostname();
                        ExecutorDetails exec1 = entry.getKey();
                        if (nodeIdToRackId.get(hostname) != null
                            && nodeIdToRackId.get(hostname).equals(rackId)) {
                            execs.add(exec1);
                        }
                    }
                }
                return execs.size();
            });
    }


    /**
     * Get the rack on which a node is a part of.
     *
     * @param node the node to find out which rack its on
     * @return the rack id
     */
    protected String nodeToRack(RAS_Node node) {
        for (Map.Entry<String, List<String>> entry : networkTopography.entrySet()) {
            if (entry.getValue().contains(node.getHostname())) {
                return entry.getKey();
            }
        }
        LOG.error("Node: {} not found in any racks", node.getHostname());
        return null;
    }

    /**
     * get a list nodes from a rack.
     *
     * @param rackId the rack id of the rack to get nodes from
     * @return a list of nodes
     */
    protected List<RAS_Node> getAvailableNodesFromRack(String rackId) {
        List<RAS_Node> retList = new ArrayList<>();
        for (String nodeId : networkTopography.get(rackId)) {
            retList.add(nodes.getNodeById(this.nodeHostnameToId(nodeId)));
        }
        return retList;
    }

    /**
     * Order executors in favor of shuffleGrouping.
     * @param td The topology the executors belong to
     * @param unassignedExecutors a collection of unassigned executors that need to be unassigned. Should only try to
     *     assign executors from this list
     * @return a list of executors in sorted order
     */
    protected List<ExecutorDetails> orderExecutors(
        TopologyDetails td, Collection<ExecutorDetails> unassignedExecutors) {
        Map<String, Component> componentMap = td.getComponents();
        List<ExecutorDetails> execsScheduled = new LinkedList<>();

        Map<String, Queue<ExecutorDetails>> compToExecsToSchedule = new HashMap<>();
        for (Map.Entry<String, Component> componentEntry: componentMap.entrySet()) {
            Component component = componentEntry.getValue();
            compToExecsToSchedule.put(component.getId(), new LinkedList<>());
            for (ExecutorDetails exec : component.getExecs()) {
                if (unassignedExecutors.contains(exec)) {
                    compToExecsToSchedule.get(component.getId()).add(exec);
                    LOG.info("{} has unscheduled executor {}", component.getId(), exec);
                }
            }
        }

        List<Component> sortedComponents = topologicalSortComponents(componentMap);

        for (Component currComp: sortedComponents) {
            int numExecs = compToExecsToSchedule.get(currComp.getId()).size();
            for (int i = 0; i < numExecs; i++) {
                execsScheduled.addAll(takeExecutors(currComp, numExecs - i, componentMap, compToExecsToSchedule));
            }
        }

        LOG.info("The ordering result is {}", execsScheduled);

        return execsScheduled;
    }

    private List<ExecutorDetails> takeExecutors(Component currComp, int numExecs,
                                                final Map<String, Component> componentMap,
                                                final Map<String, Queue<ExecutorDetails>> compToExecsToSchedule) {
        List<ExecutorDetails> execsScheduled = new ArrayList<>();
        Queue<ExecutorDetails> currQueue = compToExecsToSchedule.get((currComp.getId()));
        Set<String> sortedChildren = getSortedChildren(currComp, componentMap);

        execsScheduled.add(currQueue.poll());

        for (String childId: sortedChildren) {
            Component childComponent = componentMap.get(childId);
            Queue<ExecutorDetails> childQueue = compToExecsToSchedule.get(childId);
            int childNumExecs = childQueue.size();
            if (childNumExecs == 0) {
                continue;
            }
            int numExecsToTake = 1;
            if (isShuffleFromParentToChild(currComp, childComponent)) {
                // if it's shuffle grouping, truncate
                numExecsToTake = Math.max(1, childNumExecs / numExecs);
            } // otherwise, one-by-one

            for (int i = 0; i < numExecsToTake; i++) {
                execsScheduled.addAll(takeExecutors(childComponent, childNumExecs, componentMap, compToExecsToSchedule));
            }
        }

        return execsScheduled;
    }

    private Set<String> getSortedChildren(Component component, final Map<String, Component> componentMap) {
        Set<String> children = component.getChildren();
        Set<String> sortedChildren =
                new TreeSet<String>((o1, o2) -> {
                    Component child1 = componentMap.get(o1);
                    Component child2 = componentMap.get(o2);
                    boolean child1IsShuffle = isShuffleFromParentToChild(component, child1);
                    boolean child2IsShuffle = isShuffleFromParentToChild(component, child2);

                    if (child1IsShuffle && child2IsShuffle) {
                        return o1.compareTo(o2);
                    } else if (child1IsShuffle) {
                        return 1;
                    } else {
                        return -1;
                    }
                });
        sortedChildren.addAll(children);
        return sortedChildren;
    }

    private boolean isShuffleFromParentToChild(Component parent, Component child) {
        for (Map.Entry<GlobalStreamId, Grouping> inputEntry: child.getInput().entrySet()) {
            GlobalStreamId globalStreamId = inputEntry.getKey();
            Grouping grouping = inputEntry.getValue();
            if (globalStreamId.get_componentId().equals(parent.getId())
                    && (inputEntry.getValue().is_set_local_or_shuffle() || grouping.is_set_shuffle())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Sort components topologically.
     * @param componentMap The map of component Id to Component Object.
     * @return The sorted components
     */
    private List<Component> topologicalSortComponents(final Map<String, Component> componentMap) {
        List<Component> sortedComponents = new ArrayList<>();

        boolean[] visited = new boolean[componentMap.size()];
        int[] inDegree = new int[componentMap.size()];
        List<String> componentIds = new ArrayList<>(componentMap.keySet());
        Map<String, Integer> compIdToIndex = new HashMap<>();
        for (int i = 0; i < componentIds.size(); i++) {
            compIdToIndex.put(componentIds.get(i), i);
        }

        //initialize the in-degree array
        for (int i = 0; i < inDegree.length; i++) {
            String compId = componentIds.get(i);
            Component comp = componentMap.get(compId);
            for (String childId: comp.getChildren()) {
                inDegree[compIdToIndex.get(childId)] += 1;
            }
        }

        //sorting components topologically
        for (int t = 0; t < inDegree.length; t++) {
            for (int i = 0; i < inDegree.length; i++) {
                if (inDegree[i] == 0 && !visited[i]) {
                    String compId = componentIds.get(i);
                    Component comp = componentMap.get(compId);
                    sortedComponents.add(comp);
                    visited[i] = true;
                    for (String childId : comp.getChildren()) {
                        inDegree[compIdToIndex.get(childId)]--;
                    }
                    break;
                }
            }
        }

        return sortedComponents;
    }

    /**
     * Get a list of all the spouts in the topology.
     *
     * @param td topology to get spouts from
     * @return a list of spouts
     */
    protected List<Component> getSpouts(TopologyDetails td) {
        List<Component> spouts = new ArrayList<>();

        for (Component c : td.getComponents().values()) {
            if (c.getType() == ComponentType.SPOUT) {
                spouts.add(c);
            }
        }
        return spouts;
    }

    /**
     * Get the amount of resources available and total for each node.
     *
     * @return a String with cluster resource info for debug
     */
    private void logClusterInfo() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Cluster:");
            for (Map.Entry<String, List<String>> clusterEntry : networkTopography.entrySet()) {
                String rackId = clusterEntry.getKey();
                LOG.debug("Rack: {}", rackId);
                for (String nodeHostname : clusterEntry.getValue()) {
                    RAS_Node node = idToNode(this.nodeHostnameToId(nodeHostname));
                    LOG.debug("-> Node: {} {}", node.getHostname(), node.getId());
                    LOG.debug(
                        "--> Avail Resources: {Mem {}, CPU {} Slots: {}}",
                        node.getAvailableMemoryResources(),
                        node.getAvailableCpuResources(),
                        node.totalSlotsFree());
                    LOG.debug(
                        "--> Total Resources: {Mem {}, CPU {} Slots: {}}",
                        node.getTotalMemoryResources(),
                        node.getTotalCpuResources(),
                        node.totalSlots());
                }
            }
        }
    }

    /**
     * hostname to Id.
     *
     * @param hostname the hostname to convert to node id
     * @return the id of a node
     */
    public String nodeHostnameToId(String hostname) {
        for (RAS_Node n : nodes.getNodes()) {
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
     * Find RAS_Node for specified node id.
     *
     * @param id the node/supervisor id to lookup
     * @return a RAS_Node object
     */
    public RAS_Node idToNode(String id) {
        RAS_Node ret = nodes.getNodeById(id);
        if (ret == null) {
            LOG.error("Cannot find Node with Id: {}", id);
        }
        return ret;
    }
}
