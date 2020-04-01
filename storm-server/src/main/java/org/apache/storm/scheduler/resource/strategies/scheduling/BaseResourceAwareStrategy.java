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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.storm.generated.ComponentType;
import org.apache.storm.networktopography.DNSToSwitchMapping;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.Component;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.RasNode;
import org.apache.storm.scheduler.resource.RasNodes;
import org.apache.storm.scheduler.resource.SchedulingResult;
import org.apache.storm.scheduler.resource.SchedulingStatus;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceOffer;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceRequest;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.storm.shade.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseResourceAwareStrategy implements IStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(BaseResourceAwareStrategy.class);
    protected Cluster cluster;
    // Rack id to list of host names in that rack
    private Map<String, List<String>> networkTopography;
    private final Map<String, String> superIdToRack = new HashMap<>();
    private final Map<String, String> superIdToHostname = new HashMap<>();
    private final Map<String, List<RasNode>> hostnameToNodes = new HashMap<>();
    private final Map<String, List<RasNode>> rackIdToNodes = new HashMap<>();
    protected RasNodes nodes;

    @VisibleForTesting
    void prepare(Cluster cluster) {
        this.cluster = cluster;
        nodes = new RasNodes(cluster);
        networkTopography = cluster.getNetworkTopography();
        Map<String, String> hostToRack = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : networkTopography.entrySet()) {
            String rackId = entry.getKey();
            for (String hostName: entry.getValue()) {
                hostToRack.put(hostName, rackId);
            }
        }
        for (RasNode node: nodes.getNodes()) {
            String superId = node.getId();
            String hostName = node.getHostname();
            String rackId = hostToRack.getOrDefault(hostName, DNSToSwitchMapping.DEFAULT_RACK);
            superIdToHostname.put(superId, hostName);
            superIdToRack.put(superId, rackId);
            hostnameToNodes.computeIfAbsent(hostName, (hn) -> new ArrayList<>()).add(node);
            rackIdToNodes.computeIfAbsent(rackId, (hn) -> new ArrayList<>()).add(node);
        }
        logClusterInfo();
    }

    @Override
    public void prepare(Map<String, Object> config) {
        //NOOP
    }

    protected SchedulingResult mkNotEnoughResources(TopologyDetails td) {
        return  SchedulingResult.failure(
            SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES,
            td.getExecutors().size() + " executors not scheduled");
    }

    /**
     * Schedule executor exec from topology td.
     *
     * @param exec           the executor to schedule
     * @param td             the topology executor exec is a part of
     * @param scheduledTasks executors that have been scheduled
     * @return true if scheduled successfully, else false.
     */
    protected boolean scheduleExecutor(
            ExecutorDetails exec, TopologyDetails td, Collection<ExecutorDetails> scheduledTasks, Iterable<String> sortedNodes) {
        WorkerSlot targetSlot = findWorkerForExec(exec, td, sortedNodes);
        if (targetSlot != null) {
            RasNode targetNode = idToNode(targetSlot.getNodeId());
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
            return true;
        } else {
            String comp = td.getExecutorToComponent().get(exec);
            NormalizedResourceRequest requestedResources = td.getTotalResources(exec);
            LOG.warn("Not Enough Resources to schedule Task {} - {} {}", exec, comp, requestedResources);
            return false;
        }
    }

    protected abstract TreeSet<ObjectResources> sortObjectResources(
        AllResources allResources, ExecutorDetails exec, TopologyDetails topologyDetails,
        ExistingScheduleFunc existingScheduleFunc
    );

    /**
     * Find a worker to schedule executor exec on.
     *
     * @param exec the executor to schedule
     * @param td   the topology that the executor is a part of
     * @return a worker to assign exec on. Returns null if a worker cannot be successfully found in cluster
     */
    protected WorkerSlot findWorkerForExec(ExecutorDetails exec, TopologyDetails td, Iterable<String> sortedNodes) {
        for (String id : sortedNodes) {
            RasNode node = nodes.getNodeById(id);
            if (node.couldEverFit(exec, td)) {
                for (WorkerSlot ws : node.getSlotsAvailableToScheduleOn()) {
                    if (node.wouldFit(ws, exec, td)) {
                        return ws;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Nodes are sorted by two criteria.
     *
     * <p>1) the number executors of the topology that needs to be scheduled is already on the node in
     * descending order. The reasoning to sort based on criterion 1 is so we schedule the rest of a topology on the same node as the
     * existing executors of the topology.
     *
     * <p>2) the subordinate/subservient resource availability percentage of a node in descending
     * order We calculate the resource availability percentage by dividing the resource availability that have exhausted or little of one of
     * the resources mentioned above will be ranked after on the node by the resource availability of the entire rack By doing this
     * calculation, nodes nodes that have more balanced resource availability. So we will be less likely to pick a node that have a lot of
     * one resource but a low amount of another.
     *
     * @param availNodes a list of all the nodes we want to sort
     * @param rackId     the rack id availNodes are a part of
     * @return a sorted list of nodes.
     */
    protected TreeSet<ObjectResources> sortNodes(
            List<RasNode> availNodes, ExecutorDetails exec, TopologyDetails topologyDetails, String rackId,
            Map<String, AtomicInteger> scheduledCount) {
        AllResources allRackResources = new AllResources("RACK");
        List<ObjectResources> nodes = allRackResources.objectResources;

        for (RasNode rasNode : availNodes) {
            String superId = rasNode.getId();
            ObjectResources node = new ObjectResources(superId);

            node.availableResources = rasNode.getTotalAvailableResources();
            node.totalResources = rasNode.getTotalResources();

            nodes.add(node);
            allRackResources.availableResourcesOverall.add(node.availableResources);
            allRackResources.totalResourcesOverall.add(node.totalResources);

        }

        LOG.debug(
            "Rack {}: Overall Avail [ {} ] Total [ {} ]",
            rackId,
            allRackResources.availableResourcesOverall,
            allRackResources.totalResourcesOverall);

        return sortObjectResources(
            allRackResources,
            exec,
            topologyDetails,
            (superId) -> {
                AtomicInteger count = scheduledCount.get(superId);
                if (count == null) {
                    return 0;
                }
                return count.get();
            });
    }

    protected List<String> makeHostToNodeIds(List<String> hosts) {
        if (hosts == null) {
            return Collections.emptyList();
        }
        List<String> ret = new ArrayList<>(hosts.size());
        for (String host: hosts) {
            List<RasNode> nodes = hostnameToNodes.get(host);
            if (nodes != null) {
                for (RasNode node : nodes) {
                    ret.add(node.getId());
                }
            }
        }
        return ret;
    }

    private static class LazyNodeSortingIterator implements Iterator<String> {
        private final LazyNodeSorting parent;
        private final Iterator<ObjectResources> rackIterator;
        private Iterator<ObjectResources> nodeIterator;
        private String nextValueFromNode = null;
        private final Iterator<String> pre;
        private final Iterator<String> post;
        private final Set<String> skip;

        LazyNodeSortingIterator(LazyNodeSorting parent,
                                       TreeSet<ObjectResources> sortedRacks) {
            this.parent = parent;
            rackIterator = sortedRacks.iterator();
            pre = parent.favoredNodeIds.iterator();
            post = Stream.concat(parent.unFavoredNodeIds.stream(), parent.greyListedSupervisorIds.stream())
                            .collect(Collectors.toList())
                            .iterator();
            skip = parent.skippedNodeIds;
        }

        private Iterator<ObjectResources> getNodeIterator() {
            if (nodeIterator != null && nodeIterator.hasNext()) {
                return nodeIterator;
            }
            //need to get the next node iterator
            if (rackIterator.hasNext()) {
                ObjectResources rack = rackIterator.next();
                final String rackId = rack.id;
                nodeIterator = parent.getSortedNodesFor(rackId).iterator();
                return nodeIterator;
            }

            return null;
        }

        @Override
        public boolean hasNext() {
            if (pre.hasNext()) {
                return true;
            }
            if (nextValueFromNode != null) {
                return true;
            }
            while (true) {
                //For the node we don't know if we have another one unless we look at the contents
                Iterator<ObjectResources> nodeIterator = getNodeIterator();
                if (nodeIterator == null || !nodeIterator.hasNext()) {
                    break;
                }
                String tmp = nodeIterator.next().id;
                if (!skip.contains(tmp)) {
                    nextValueFromNode = tmp;
                    return true;
                }
            }
            if (post.hasNext()) {
                return true;
            }
            return false;
        }

        @Override
        public String next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            if (pre.hasNext()) {
                return pre.next();
            }
            if (nextValueFromNode != null) {
                String tmp = nextValueFromNode;
                nextValueFromNode = null;
                return tmp;
            }
            return post.next();
        }
    }

    private class LazyNodeSorting implements Iterable<String> {
        private final Map<String, AtomicInteger> perNodeScheduledCount = new HashMap<>();
        private final TreeSet<ObjectResources> sortedRacks;
        private final Map<String, TreeSet<ObjectResources>> cachedNodes = new HashMap<>();
        private final ExecutorDetails exec;
        private final TopologyDetails td;
        private final List<String> favoredNodeIds;
        private final List<String> unFavoredNodeIds;
        private final List<String> greyListedSupervisorIds;
        private final Set<String> skippedNodeIds = new HashSet<>();

        LazyNodeSorting(TopologyDetails td, ExecutorDetails exec,
                               List<String> favoredNodeIds, List<String> unFavoredNodeIds) {
            this.favoredNodeIds = favoredNodeIds;
            this.unFavoredNodeIds = unFavoredNodeIds;
            this.greyListedSupervisorIds = cluster.getGreyListedSupervisors();
            this.unFavoredNodeIds.removeAll(favoredNodeIds);
            this.favoredNodeIds.removeAll(greyListedSupervisorIds);
            this.unFavoredNodeIds.removeAll(greyListedSupervisorIds);
            skippedNodeIds.addAll(favoredNodeIds);
            skippedNodeIds.addAll(unFavoredNodeIds);
            skippedNodeIds.addAll(greyListedSupervisorIds);

            this.td = td;
            this.exec = exec;
            String topoId = td.getId();
            SchedulerAssignment assignment = cluster.getAssignmentById(topoId);
            if (assignment != null) {
                for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> entry :
                    assignment.getSlotToExecutors().entrySet()) {
                    String superId = entry.getKey().getNodeId();
                    perNodeScheduledCount.computeIfAbsent(superId, (sid) -> new AtomicInteger(0))
                        .getAndAdd(entry.getValue().size());
                }
            }
            sortedRacks = sortRacks(exec, td);
        }

        private TreeSet<ObjectResources> getSortedNodesFor(String rackId) {
            return cachedNodes.computeIfAbsent(rackId,
                (rid) -> sortNodes(rackIdToNodes.getOrDefault(rid, Collections.emptyList()), exec, td, rid, perNodeScheduledCount));
        }

        @Override
        public Iterator<String> iterator() {
            return new LazyNodeSortingIterator(this, sortedRacks);
        }
    }

    protected Iterable<String> sortAllNodes(TopologyDetails td, ExecutorDetails exec,
                                            List<String> favoredNodeIds, List<String> unFavoredNodeIds) {
        return new LazyNodeSorting(td, exec, favoredNodeIds, unFavoredNodeIds);
    }

    private AllResources createClusterAllResources() {
        AllResources allResources = new AllResources("Cluster");
        List<ObjectResources> racks = allResources.objectResources;

        //This is the first time so initialize the resources.
        for (Map.Entry<String, List<String>> entry : networkTopography.entrySet()) {
            String rackId = entry.getKey();
            List<String> nodeHosts = entry.getValue();
            ObjectResources rack = new ObjectResources(rackId);
            racks.add(rack);
            for (String nodeHost : nodeHosts) {
                for (RasNode node : hostnameToNodes(nodeHost)) {
                    rack.availableResources.add(node.getTotalAvailableResources());
                    rack.totalResources.add(node.getTotalAvailableResources());
                }
            }

            allResources.totalResourcesOverall.add(rack.totalResources);
            allResources.availableResourcesOverall.add(rack.availableResources);
        }

        LOG.debug(
            "Cluster Overall Avail [ {} ] Total [ {} ]",
            allResources.availableResourcesOverall,
            allResources.totalResourcesOverall);
        return allResources;
    }

    private Map<String, AtomicInteger> getScheduledCount(TopologyDetails topologyDetails) {
        String topoId = topologyDetails.getId();
        SchedulerAssignment assignment = cluster.getAssignmentById(topoId);
        Map<String, AtomicInteger> scheduledCount = new HashMap<>();
        if (assignment != null) {
            for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> entry :
                assignment.getSlotToExecutors().entrySet()) {
                String superId = entry.getKey().getNodeId();
                String rackId = superIdToRack.get(superId);
                scheduledCount.computeIfAbsent(rackId, (rid) -> new AtomicInteger(0))
                    .getAndAdd(entry.getValue().size());
            }
        }
        return scheduledCount;
    }

    /**
     * Racks are sorted by two criteria.
     *
     * <p>1) the number executors of the topology that needs to be scheduled is already on the rack in descending order.
     * The reasoning to sort based on criterion 1 is so we schedule the rest of a topology on the same rack as the existing executors of the
     * topology.
     *
     * <p>2) the subordinate/subservient resource availability percentage of a rack in descending order We calculate
     * the resource availability percentage by dividing the resource availability on the rack by the resource availability of the  entire
     * cluster By doing this calculation, racks that have exhausted or little of one of the resources mentioned above will be ranked after
     * racks that have more balanced resource availability. So we will be less likely to pick a rack that have a lot of one resource but a
     * low amount of another.
     *
     * @return a sorted list of racks
     */
    @VisibleForTesting
    TreeSet<ObjectResources> sortRacks(ExecutorDetails exec, TopologyDetails topologyDetails) {

        final AllResources allResources = createClusterAllResources();
        final Map<String, AtomicInteger> scheduledCount = getScheduledCount(topologyDetails);

        return sortObjectResources(
            allResources,
            exec,
            topologyDetails,
            (rackId) -> {
                AtomicInteger count = scheduledCount.get(rackId);
                if (count == null) {
                    return 0;
                }
                return count.get();
            });
    }

    /**
     * Get the rack on which a node is a part of.
     *
     * @param node the node to find out which rack its on
     * @return the rack id
     */
    protected String nodeToRack(RasNode node) {
        return superIdToRack.get(node.getId());
    }

    /**
     * sort components by the number of in and out connections that need to be made, in descending order.
     *
     * @param componentMap The components that need to be sorted
     * @return a sorted set of components
     */
    private Set<Component> sortComponents(final Map<String, Component> componentMap) {
        Set<Component> sortedComponents =
            new TreeSet<>((o1, o2) -> {
                int connections1 = 0;
                int connections2 = 0;

                for (String childId : Sets.union(o1.getChildren(), o1.getParents())) {
                    connections1 +=
                        (componentMap.get(childId).getExecs().size() * o1.getExecs().size());
                }

                for (String childId : Sets.union(o2.getChildren(), o2.getParents())) {
                    connections2 +=
                        (componentMap.get(childId).getExecs().size() * o2.getExecs().size());
                }

                if (connections1 > connections2) {
                    return -1;
                } else if (connections1 < connections2) {
                    return 1;
                } else {
                    return o1.getId().compareTo(o2.getId());
                }
            });
        sortedComponents.addAll(componentMap.values());
        return sortedComponents;
    }

    /**
     * Sort a component's neighbors by the number of connections it needs to make with this component.
     *
     * @param thisComp     the component that we need to sort its neighbors
     * @param componentMap all the components to sort
     * @return a sorted set of components
     */
    private Set<Component> sortNeighbors(
        final Component thisComp, final Map<String, Component> componentMap) {
        Set<Component> sortedComponents =
            new TreeSet<>((o1, o2) -> {
                int connections1 = o1.getExecs().size() * thisComp.getExecs().size();
                int connections2 = o2.getExecs().size() * thisComp.getExecs().size();
                if (connections1 < connections2) {
                    return -1;
                } else if (connections1 > connections2) {
                    return 1;
                } else {
                    return o1.getId().compareTo(o2.getId());
                }
            });
        sortedComponents.addAll(componentMap.values());
        return sortedComponents;
    }

    /**
     * Order executors based on how many in and out connections it will potentially need to make, in descending order. First order
     * components by the number of in and out connections it will have.  Then iterate through the sorted list of components. For each
     * component sort the neighbors of that component by how many connections it will have to make with that component. Add an executor from
     * this component and then from each neighboring component in sorted order. Do this until there is nothing left to schedule.
     *
     * @param td                  The topology the executors belong to
     * @param unassignedExecutors a collection of unassigned executors that need to be assigned. Should only try to assign executors from
     *                            this list
     * @return a list of executors in sorted order
     */
    protected List<ExecutorDetails> orderExecutors(
        TopologyDetails td, Collection<ExecutorDetails> unassignedExecutors) {
        Map<String, Component> componentMap = td.getComponents();
        List<ExecutorDetails> execsScheduled = new LinkedList<>();

        Map<String, Queue<ExecutorDetails>> compToExecsToSchedule = new HashMap<>();
        for (Component component : componentMap.values()) {
            compToExecsToSchedule.put(component.getId(), new LinkedList<>());
            for (ExecutorDetails exec : component.getExecs()) {
                if (unassignedExecutors.contains(exec)) {
                    compToExecsToSchedule.get(component.getId()).add(exec);
                }
            }
        }

        Set<Component> sortedComponents = sortComponents(componentMap);
        sortedComponents.addAll(componentMap.values());

        for (Component currComp : sortedComponents) {
            Map<String, Component> neighbors = new HashMap<>();
            for (String compId : Sets.union(currComp.getChildren(), currComp.getParents())) {
                neighbors.put(compId, componentMap.get(compId));
            }
            Set<Component> sortedNeighbors = sortNeighbors(currComp, neighbors);
            Queue<ExecutorDetails> currCompExesToSched = compToExecsToSchedule.get(currComp.getId());

            boolean flag = false;
            do {
                flag = false;
                if (!currCompExesToSched.isEmpty()) {
                    execsScheduled.add(currCompExesToSched.poll());
                    flag = true;
                }

                for (Component neighborComp : sortedNeighbors) {
                    Queue<ExecutorDetails> neighborCompExesToSched =
                        compToExecsToSchedule.get(neighborComp.getId());
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
     * Log a bunch of stuff for debugging.
     */
    private void logClusterInfo() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Cluster:");
            for (Map.Entry<String, List<String>> clusterEntry : networkTopography.entrySet()) {
                String rackId = clusterEntry.getKey();
                LOG.debug("Rack: {}", rackId);
                for (String nodeHostname : clusterEntry.getValue()) {
                    for (RasNode node : hostnameToNodes(nodeHostname)) {
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
    }

    /**
     * hostname to Ids.
     *
     * @param hostname the hostname.
     * @return the ids n that node.
     */
    public List<RasNode> hostnameToNodes(String hostname) {
        return hostnameToNodes.getOrDefault(hostname, Collections.emptyList());
    }

    /**
     * Find RASNode for specified node id.
     *
     * @param id the node/supervisor id to lookup
     * @return a RASNode object
     */
    public RasNode idToNode(String id) {
        RasNode ret = nodes.getNodeById(id);
        if (ret == null) {
            LOG.error("Cannot find Node with Id: {}", id);
        }
        return ret;
    }

    /**
     * interface for calculating the number of existing executors scheduled on a object (rack or node).
     */
    protected interface ExistingScheduleFunc {
        int getNumExistingSchedule(String objectId);
    }

    /**
     * a class to contain individual object resources as well as cumulative stats.
     */
    protected static class AllResources {
        List<ObjectResources> objectResources = new LinkedList<>();
        final NormalizedResourceOffer availableResourcesOverall;
        final NormalizedResourceOffer totalResourcesOverall;
        String identifier;

        public AllResources(String identifier) {
            this.identifier = identifier;
            this.availableResourcesOverall = new NormalizedResourceOffer();
            this.totalResourcesOverall = new NormalizedResourceOffer();
        }

        public AllResources(AllResources other) {
            this(null,
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
    protected static class ObjectResources {
        public final String id;
        public NormalizedResourceOffer availableResources;
        public NormalizedResourceOffer totalResources;
        public double effectiveResources = 0.0;

        public ObjectResources(String id) {
            this.id = id;
            this.availableResources = new NormalizedResourceOffer();
            this.totalResources = new NormalizedResourceOffer();
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
}
