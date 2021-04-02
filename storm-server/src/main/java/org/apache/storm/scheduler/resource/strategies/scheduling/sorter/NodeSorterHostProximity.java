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

package org.apache.storm.scheduler.resource.strategies.scheduling.sorter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.storm.Config;
import org.apache.storm.networktopography.DNSToSwitchMapping;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.RasNode;
import org.apache.storm.scheduler.resource.RasNodes;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceOffer;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceRequest;
import org.apache.storm.scheduler.resource.strategies.scheduling.BaseResourceAwareStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.ObjectResourcesItem;
import org.apache.storm.scheduler.resource.strategies.scheduling.ObjectResourcesSummary;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeSorterHostProximity implements INodeSorter {
    private static final Logger LOG = LoggerFactory.getLogger(NodeSorterHostProximity.class);

    // instance variables from class instantiation
    protected final BaseResourceAwareStrategy.NodeSortType nodeSortType;

    protected Cluster cluster;
    protected TopologyDetails topologyDetails;

    // Instance variables derived from Cluster.
    private final Map<String, String> superIdToRack = new HashMap<>();
    private final Map<String, List<RasNode>> hostnameToNodes = new HashMap<>();
    private final Map<String, String> nodeIdToHostname = new HashMap<>();
    private final Map<String, Set<String>> rackIdToHosts = new HashMap<>();
    protected List<String> greyListedSupervisorIds;

    // Instance variables from Cluster and TopologyDetails.
    protected List<String> favoredNodeIds;
    protected List<String> unFavoredNodeIds;

    // Updated in prepare method
    ExecutorDetails exec;

    public NodeSorterHostProximity(Cluster cluster, TopologyDetails topologyDetails) {
        this(cluster, topologyDetails, BaseResourceAwareStrategy.NodeSortType.COMMON);
    }

    /**
     * Initialize for the default implementation node sorting.
     *
     * <p>
     *  <li>{@link BaseResourceAwareStrategy.NodeSortType#GENERIC_RAS} sorting implemented in
     *  {@link #sortObjectResourcesGeneric(ObjectResourcesSummary, ExecutorDetails, NodeSorterHostProximity.ExistingScheduleFunc)}</li>
     *  <li>{@link BaseResourceAwareStrategy.NodeSortType#DEFAULT_RAS} sorting implemented in
     *  {@link #sortObjectResourcesDefault(ObjectResourcesSummary, NodeSorterHostProximity.ExistingScheduleFunc)}</li>
     *  <li>{@link BaseResourceAwareStrategy.NodeSortType#COMMON} sorting implemented in
     *  {@link #sortObjectResourcesCommon(ObjectResourcesSummary, ExecutorDetails, NodeSorterHostProximity.ExistingScheduleFunc)}</li>
     * </p>
     *
     * @param cluster for which nodes will be sorted.
     * @param topologyDetails the topology to sort for.
     * @param nodeSortType type of sorting to be applied to object resource collection {@link BaseResourceAwareStrategy.NodeSortType}.
     */
    public NodeSorterHostProximity(Cluster cluster, TopologyDetails topologyDetails, BaseResourceAwareStrategy.NodeSortType nodeSortType) {
        this.cluster = cluster;
        this.topologyDetails = topologyDetails;
        this.nodeSortType = nodeSortType;

        // from Cluster
        greyListedSupervisorIds = cluster.getGreyListedSupervisors();
        Map<String, String> hostToRack = cluster.getHostToRack();
        RasNodes nodes = new RasNodes(cluster);
        for (RasNode node: nodes.getNodes()) {
            String superId = node.getId();
            String hostName = node.getHostname();
            if (!node.isAlive() || hostName == null) {
                continue;
            }
            String rackId = hostToRack.getOrDefault(hostName, DNSToSwitchMapping.DEFAULT_RACK);
            superIdToRack.put(superId, rackId);
            hostnameToNodes.computeIfAbsent(hostName, (hn) -> new ArrayList<>()).add(node);
            nodeIdToHostname.put(superId, hostName);
            rackIdToHosts.computeIfAbsent(rackId, r -> new HashSet<>()).add(hostName);
        }

        // from TopologyDetails
        Map<String, Object> topoConf = topologyDetails.getConf();

        // From Cluster and TopologyDetails - and cleaned-up
        favoredNodeIds = makeHostToNodeIds((List<String>) topoConf.get(Config.TOPOLOGY_SCHEDULER_FAVORED_NODES));
        unFavoredNodeIds = makeHostToNodeIds((List<String>) topoConf.get(Config.TOPOLOGY_SCHEDULER_UNFAVORED_NODES));
        favoredNodeIds.removeAll(greyListedSupervisorIds);
        unFavoredNodeIds.removeAll(greyListedSupervisorIds);
        unFavoredNodeIds.removeAll(favoredNodeIds);
    }

    @VisibleForTesting
    public Map<String, Set<String>> getRackIdToHosts() {
        return rackIdToHosts;
    }

    @Override
    public void prepare(ExecutorDetails exec) {
        this.exec = exec;
    }

    /**
     * Scheduling uses {@link #sortAllNodes()} which eventually
     * calls this method whose behavior can be altered by setting {@link #nodeSortType}.
     *
     * @param resourcesSummary     contains all individual {@link ObjectResourcesItem} as well as cumulative stats
     * @param exec                 executor for which the sorting is done
     * @param existingScheduleFunc a function to get existing executors already scheduled on this object
     * @return an {@link Iterable} of sorted {@link ObjectResourcesItem}
     */
    protected Iterable<ObjectResourcesItem> sortObjectResources(
            ObjectResourcesSummary resourcesSummary, ExecutorDetails exec, ExistingScheduleFunc existingScheduleFunc) {
        switch (nodeSortType) {
            case DEFAULT_RAS:
                return sortObjectResourcesDefault(resourcesSummary, existingScheduleFunc);
            case GENERIC_RAS:
                return sortObjectResourcesGeneric(resourcesSummary, exec, existingScheduleFunc);
            case COMMON:
                return sortObjectResourcesCommon(resourcesSummary, exec, existingScheduleFunc);
            default:
                return null;
        }
    }

    /**
     * Sort objects by the following three criteria.
     *
     * <li>
     *     The number executors of the topology that needs to be scheduled is already on the object (node or rack)
     *     in descending order. The reasoning to sort based on criterion 1 is so we schedule the rest of a topology on
     *     the same object (node or rack) as the existing executors of the topology.
     * </li>
     *
     * <li>
     *     The subordinate/subservient resource availability percentage of a rack in descending order We calculate the
     *     resource availability percentage by dividing the resource availability of the object (node or rack) by the
     *     resource availability of the entire rack or cluster depending on if object references a node or a rack.
     *     How this differs from the DefaultResourceAwareStrategy is that the percentage boosts the node or rack if it is
     *     requested by the executor that the sorting is being done for and pulls it down if it is not.
     *     By doing this calculation, objects (node or rack) that have exhausted or little of one of the resources mentioned
     *     above will be ranked after racks that have more balanced resource availability and nodes or racks that have
     *     resources that are not requested will be ranked below . So we will be less likely to pick a rack that
     *     have a lot of one resource but a low amount of another and have a lot of resources that are not requested by the executor.
     *     This is similar to logic used {@link #sortObjectResourcesGeneric(ObjectResourcesSummary, ExecutorDetails, ExistingScheduleFunc)}.
     * </li>
     *
     * <li>
     *     The tie between two nodes with same resource availability is broken by using the node with lower minimum
     *     percentage used. This comparison was used in {@link #sortObjectResourcesDefault(ObjectResourcesSummary, ExistingScheduleFunc)}
     *     but here it is made subservient to modified resource availbility used in
     *     {@link #sortObjectResourcesGeneric(ObjectResourcesSummary, ExecutorDetails, ExistingScheduleFunc)}.
     *
     * </li>
     *
     * @param allResources         contains all individual ObjectResources as well as cumulative stats
     * @param exec                 executor for which the sorting is done
     * @param existingScheduleFunc a function to get existing executors already scheduled on this object
     * @return an {@link Iterable} of sorted {@link ObjectResourcesItem}
     */
    private Iterable<ObjectResourcesItem> sortObjectResourcesCommon(
            final ObjectResourcesSummary allResources, final ExecutorDetails exec,
            final ExistingScheduleFunc existingScheduleFunc) {
        // Copy and modify allResources
        ObjectResourcesSummary affinityBasedAllResources = new ObjectResourcesSummary(allResources);
        final NormalizedResourceOffer availableResourcesOverall = allResources.getAvailableResourcesOverall();
        final NormalizedResourceRequest requestedResources = (exec != null) ? topologyDetails.getTotalResources(exec) : null;
        affinityBasedAllResources.getObjectResources().forEach(
            x -> {
                if (requestedResources != null) {
                    // negate unrequested resources
                    x.availableResources.updateForRareResourceAffinity(requestedResources);
                }
                x.minResourcePercent = availableResourcesOverall.calculateMinPercentageUsedBy(x.availableResources);
                x.avgResourcePercent = availableResourcesOverall.calculateAveragePercentageUsedBy(x.availableResources);

                LOG.trace("for {}: minResourcePercent={}, avgResourcePercent={}, numExistingSchedule={}",
                        x.id, x.minResourcePercent, x.avgResourcePercent,
                        existingScheduleFunc.getNumExistingSchedule(x.id));
            }
        );

        // Use the following comparator to sort
        Comparator<ObjectResourcesItem> comparator = (o1, o2) -> {
            int execsScheduled1 = existingScheduleFunc.getNumExistingSchedule(o1.id);
            int execsScheduled2 = existingScheduleFunc.getNumExistingSchedule(o2.id);
            if (execsScheduled1 > execsScheduled2) {
                return -1;
            } else if (execsScheduled1 < execsScheduled2) {
                return 1;
            }
            double o1Avg = o1.avgResourcePercent;
            double o2Avg = o2.avgResourcePercent;
            if (o1Avg > o2Avg) {
                return -1;
            } else if (o1Avg < o2Avg) {
                return 1;
            }
            if (o1.minResourcePercent > o2.minResourcePercent) {
                return -1;
            } else if (o1.minResourcePercent < o2.minResourcePercent) {
                return 1;
            }
            return o1.id.compareTo(o2.id);
        };
        TreeSet<ObjectResourcesItem> sortedObjectResources = new TreeSet(comparator);
        sortedObjectResources.addAll(affinityBasedAllResources.getObjectResources());
        LOG.debug("Sorted Object Resources: {}", sortedObjectResources);
        return sortedObjectResources;
    }

    /**
     * Sort objects by the following two criteria.
     *
     * <li>the number executors of the topology that needs to be scheduled is already on the
     * object (node or rack) in descending order. The reasoning to sort based on criterion 1 is so we schedule the rest
     * of a topology on the same object (node or rack) as the existing executors of the topology.</li>
     *
     * <li>the subordinate/subservient resource availability percentage of a rack in descending order We calculate the
     * resource availability percentage by dividing the resource availability of the object (node or rack) by the
     * resource availability of the entire rack or cluster depending on if object references a node or a rack.
     * How this differs from the DefaultResourceAwareStrategy is that the percentage boosts the node or rack if it is
     * requested by the executor that the sorting is being done for and pulls it down if it is not.
     * By doing this calculation, objects (node or rack) that have exhausted or little of one of the resources mentioned
     * above will be ranked after racks that have more balanced resource availability and nodes or racks that have
     * resources that are not requested will be ranked below . So we will be less likely to pick a rack that
     * have a lot of one resource but a low amount of another and have a lot of resources that are not requested by the executor.</li>
     *
     * @param allResources         contains all individual ObjectResources as well as cumulative stats
     * @param exec                 executor for which the sorting is done
     * @param existingScheduleFunc a function to get existing executors already scheduled on this object
     * @return an {@link Iterable} of sorted {@link ObjectResourcesItem}
     */
    @Deprecated
    private Iterable<ObjectResourcesItem> sortObjectResourcesGeneric(
            final ObjectResourcesSummary allResources, ExecutorDetails exec,
            final ExistingScheduleFunc existingScheduleFunc) {
        ObjectResourcesSummary affinityBasedAllResources = new ObjectResourcesSummary(allResources);
        final NormalizedResourceOffer availableResourcesOverall = allResources.getAvailableResourcesOverall();
        final NormalizedResourceRequest requestedResources = (exec != null) ? topologyDetails.getTotalResources(exec) : null;
        affinityBasedAllResources.getObjectResources().forEach(
            x -> {
                if (requestedResources != null) {
                    // negate unrequested resources
                    x.availableResources.updateForRareResourceAffinity(requestedResources);
                }
                x.minResourcePercent = availableResourcesOverall.calculateMinPercentageUsedBy(x.availableResources);
                x.avgResourcePercent = availableResourcesOverall.calculateAveragePercentageUsedBy(x.availableResources);

                LOG.trace("for {}: minResourcePercent={}, avgResourcePercent={}, numExistingSchedule={}",
                    x.id, x.minResourcePercent, x.avgResourcePercent,
                    existingScheduleFunc.getNumExistingSchedule(x.id));
            }
        );

        Comparator<ObjectResourcesItem> comparator = (o1, o2) -> {
            int execsScheduled1 = existingScheduleFunc.getNumExistingSchedule(o1.id);
            int execsScheduled2 = existingScheduleFunc.getNumExistingSchedule(o2.id);
            if (execsScheduled1 > execsScheduled2) {
                return -1;
            } else if (execsScheduled1 < execsScheduled2) {
                return 1;
            }
            double o1Avg = o1.avgResourcePercent;
            double o2Avg = o2.avgResourcePercent;
            if (o1Avg > o2Avg) {
                return -1;
            } else if (o1Avg < o2Avg) {
                return 1;
            }
            return o1.id.compareTo(o2.id);
        };
        TreeSet<ObjectResourcesItem> sortedObjectResources = new TreeSet<>(comparator);
        sortedObjectResources.addAll(affinityBasedAllResources.getObjectResources());
        LOG.debug("Sorted Object Resources: {}", sortedObjectResources);
        return sortedObjectResources;
    }

    /**
     * Sort objects by the following two criteria.
     *
     * <li>the number executors of the topology that needs to be scheduled is already on the
     * object (node or rack) in descending order. The reasoning to sort based on criterion 1 is so we schedule the rest
     * of a topology on the same object (node or rack) as the existing executors of the topology.</li>
     *
     * <li>the subordinate/subservient resource availability percentage of a rack in descending order We calculate the
     * resource availability percentage by dividing the resource availability of the object (node or rack) by the
     * resource availability of the entire rack or cluster depending on if object references a node or a rack.
     * By doing this calculation, objects (node or rack) that have exhausted or little of one of the resources mentioned
     * above will be ranked after racks that have more balanced resource availability. So we will be less likely to pick
     * a rack that have a lot of one resource but a low amount of another.</li>
     *
     * @param allResources         contains all individual ObjectResources as well as cumulative stats
     * @param existingScheduleFunc a function to get existing executors already scheduled on this object
     * @return an {@link Iterable} of sorted {@link ObjectResourcesItem}
     */
    @Deprecated
    private Iterable<ObjectResourcesItem> sortObjectResourcesDefault(
            final ObjectResourcesSummary allResources,
            final ExistingScheduleFunc existingScheduleFunc) {

        final NormalizedResourceOffer availableResourcesOverall = allResources.getAvailableResourcesOverall();
        for (ObjectResourcesItem objectResources : allResources.getObjectResources()) {
            objectResources.minResourcePercent =
                    availableResourcesOverall.calculateMinPercentageUsedBy(objectResources.availableResources);
            objectResources.avgResourcePercent =
                    availableResourcesOverall.calculateAveragePercentageUsedBy(objectResources.availableResources);
            LOG.trace("for {}: minResourcePercent={}, avgResourcePercent={}, numExistingSchedule={}",
                    objectResources.id, objectResources.minResourcePercent, objectResources.avgResourcePercent,
                    existingScheduleFunc.getNumExistingSchedule(objectResources.id));
        }

        Comparator<ObjectResourcesItem> comparator = (o1, o2) -> {
            int execsScheduled1 = existingScheduleFunc.getNumExistingSchedule(o1.id);
            int execsScheduled2 = existingScheduleFunc.getNumExistingSchedule(o2.id);
            if (execsScheduled1 > execsScheduled2) {
                return -1;
            } else if (execsScheduled1 < execsScheduled2) {
                return 1;
            }
            if (o1.minResourcePercent > o2.minResourcePercent) {
                return -1;
            } else if (o1.minResourcePercent < o2.minResourcePercent) {
                return 1;
            }
            double diff = o1.avgResourcePercent - o2.avgResourcePercent;
            if (diff > 0.0) {
                return -1;
            } else if (diff < 0.0) {
                return 1;
            }
            return o1.id.compareTo(o2.id);
        };
        TreeSet<ObjectResourcesItem> sortedObjectResources = new TreeSet<>(comparator);
        sortedObjectResources.addAll(allResources.getObjectResources());
        LOG.debug("Sorted Object Resources: {}", sortedObjectResources);
        return sortedObjectResources;
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
     * @param availHosts a collection of all the hosts we want to sort
     * @param rackId     the rack id availNodes are a part of
     * @return an iterable of sorted hosts.
     */
    private Iterable<ObjectResourcesItem> sortHosts(
            Collection<String> availHosts, ExecutorDetails exec, String rackId,
            Map<String, AtomicInteger> scheduledCount) {
        ObjectResourcesSummary rackResourcesSummary = new ObjectResourcesSummary("RACK");
        availHosts.forEach(h -> {
            ObjectResourcesItem hostItem = new ObjectResourcesItem(h);
            for (RasNode x : hostnameToNodes.get(h)) {
                hostItem.add(new ObjectResourcesItem(x.getId(), x.getTotalAvailableResources(), x.getTotalResources(), 0, 0));
            }
            rackResourcesSummary.addObjectResourcesItem(hostItem);
        });

        LOG.debug(
                "Rack {}: Overall Avail [ {} ] Total [ {} ]",
                rackId,
                rackResourcesSummary.getAvailableResourcesOverall(),
                rackResourcesSummary.getTotalResourcesOverall());

        return sortObjectResources(
            rackResourcesSummary,
            exec,
            (hostId) -> {
                AtomicInteger count = scheduledCount.get(hostId);
                if (count == null) {
                    return 0;
                }
                return count.get();
            });
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
     * @param availRasNodes a list of all the nodes we want to sort
     * @param hostId     the host-id that availNodes are a part of
     * @return an {@link Iterable} of sorted {@link ObjectResourcesItem} for nodes.
     */
    private Iterable<ObjectResourcesItem> sortNodes(
            List<RasNode> availRasNodes, ExecutorDetails exec, String hostId,
            Map<String, AtomicInteger> scheduledCount) {
        ObjectResourcesSummary hostResourcesSummary = new ObjectResourcesSummary("HOST");
        availRasNodes.forEach(x ->
                hostResourcesSummary.addObjectResourcesItem(
                        new ObjectResourcesItem(x.getId(), x.getTotalAvailableResources(), x.getTotalResources(), 0, 0)
                )
        );

        LOG.debug(
            "Host {}: Overall Avail [ {} ] Total [ {} ]",
            hostId,
            hostResourcesSummary.getAvailableResourcesOverall(),
            hostResourcesSummary.getTotalResourcesOverall());

        return sortObjectResources(
            hostResourcesSummary,
            exec,
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

    private class LazyNodeSortingIterator implements Iterator<String> {
        private final LazyNodeSorting parent;
        private final Iterator<ObjectResourcesItem> rackIterator;
        private Iterator<ObjectResourcesItem> hostIterator;
        private Iterator<ObjectResourcesItem> nodeIterator;
        private String nextValueFromNode = null;
        private final Iterator<String> pre;
        private final Iterator<String> post;
        private final Set<String> skip;

        LazyNodeSortingIterator(LazyNodeSorting parent, Iterable<ObjectResourcesItem> sortedRacks) {
            this.parent = parent;
            rackIterator = sortedRacks.iterator();
            pre = favoredNodeIds.iterator();
            post = Stream.concat(unFavoredNodeIds.stream(), greyListedSupervisorIds.stream())
                            .collect(Collectors.toList())
                            .iterator();
            skip = parent.skippedNodeIds;
        }

        private Iterator<ObjectResourcesItem> getNodeIterator() {
            if (nodeIterator != null && nodeIterator.hasNext()) {
                return nodeIterator;
            }
            //need to get the next host/node iterator
            if (hostIterator != null && hostIterator.hasNext()) {
                ObjectResourcesItem host = hostIterator.next();
                final String hostId = host.id;
                nodeIterator = parent.getSortedNodesForHost(hostId).iterator();
                return nodeIterator;
            }
            if (rackIterator.hasNext()) {
                ObjectResourcesItem rack = rackIterator.next();
                final String rackId = rack.id;
                hostIterator = parent.getSortedHostsForRack(rackId).iterator();
                ObjectResourcesItem host = hostIterator.next();
                final String hostId = host.id;
                nodeIterator = parent.getSortedNodesForHost(hostId).iterator();
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
                Iterator<ObjectResourcesItem> nodeIterator = getNodeIterator();
                if (nodeIterator == null || !nodeIterator.hasNext()) {
                    break;
                }
                String tmp = nodeIterator.next().id;
                if (!skip.contains(tmp)) {
                    nextValueFromNode = tmp;
                    return true;
                }
            }
            return post.hasNext();
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
        private final Map<String, AtomicInteger> perHostScheduledCount = new HashMap<>();
        private final Map<String, AtomicInteger> perNodeScheduledCount = new HashMap<>();
        private final Iterable<ObjectResourcesItem> sortedRacks;
        private final Map<String, Iterable<ObjectResourcesItem>> cachedHosts = new HashMap<>();
        private final Map<String, Iterable<ObjectResourcesItem>> cachedNodesByHost = new HashMap<>();
        private final ExecutorDetails exec;
        private final Set<String> skippedNodeIds = new HashSet<>();

        LazyNodeSorting(ExecutorDetails exec) {
            this.exec = exec;
            skippedNodeIds.addAll(favoredNodeIds);
            skippedNodeIds.addAll(unFavoredNodeIds);
            skippedNodeIds.addAll(greyListedSupervisorIds);

            String topoId = topologyDetails.getId();
            SchedulerAssignment assignment = cluster.getAssignmentById(topoId);
            if (assignment != null) {
                for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> entry :
                    assignment.getSlotToExecutors().entrySet()) {
                    String superId = entry.getKey().getNodeId();
                    String hostId = nodeIdToHostname.get(superId);
                    perHostScheduledCount.computeIfAbsent(hostId, id -> new AtomicInteger(0))
                        .getAndAdd(entry.getValue().size());
                    perNodeScheduledCount.computeIfAbsent(superId, id -> new AtomicInteger(0))
                        .getAndAdd(entry.getValue().size());
                }
            }
            sortedRacks = getSortedRacks();
        }

        private Iterable<ObjectResourcesItem> getSortedHostsForRack(String rackId) {
            return cachedHosts.computeIfAbsent(rackId,
                id -> sortHosts(rackIdToHosts.getOrDefault(id, Collections.emptySet()), exec, id, perHostScheduledCount));
        }

        private Iterable<ObjectResourcesItem> getSortedNodesForHost(String hostId) {
            return cachedNodesByHost.computeIfAbsent(hostId,
                id -> sortNodes(hostnameToNodes.getOrDefault(id, Collections.emptyList()), exec, id, perNodeScheduledCount));
        }

        @Override
        public Iterator<String> iterator() {
            return new LazyNodeSortingIterator(this, sortedRacks);
        }
    }

    @Override
    public Iterable<String> sortAllNodes() {
        return new LazyNodeSorting(exec);
    }

    private ObjectResourcesSummary createClusterSummarizedResources() {
        ObjectResourcesSummary clusterResourcesSummary = new ObjectResourcesSummary("Cluster");
        rackIdToHosts.forEach((rackId, hostIds) -> {
            if (hostIds == null || hostIds.isEmpty()) {
                LOG.info("Ignoring Rack {} since it has no hosts", rackId);
            } else {
                ObjectResourcesItem rack = new ObjectResourcesItem(rackId);
                for (String hostId : hostIds) {
                    for (RasNode node : hostnameToNodes(hostId)) {
                        rack.availableResources.add(node.getTotalAvailableResources());
                        rack.totalResources.add(node.getTotalResources());
                    }
                }
                clusterResourcesSummary.addObjectResourcesItem(rack);
            }
        });

        LOG.debug(
            "Cluster Overall Avail [ {} ] Total [ {} ], rackCnt={}, hostCnt={}",
            clusterResourcesSummary.getAvailableResourcesOverall(),
            clusterResourcesSummary.getTotalResourcesOverall(),
            clusterResourcesSummary.getObjectResources().size(),
            rackIdToHosts.values().stream().mapToInt(x -> x.size()).sum());
        return clusterResourcesSummary;
    }

    public Map<String, AtomicInteger> getScheduledExecCntByRackId() {
        SchedulerAssignment assignment = cluster.getAssignmentById(topologyDetails.getId());
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
     * @return an iterable of sorted racks
     */
    public Iterable<ObjectResourcesItem> getSortedRacks() {

        final ObjectResourcesSummary clusterResourcesSummary = createClusterSummarizedResources();
        final Map<String, AtomicInteger> scheduledCount = getScheduledExecCntByRackId();

        return sortObjectResources(
            clusterResourcesSummary,
            exec,
            (rackId) -> {
                AtomicInteger count = scheduledCount.get(rackId);
                if (count == null) {
                    return 0;
                }
                return count.get();
            });
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
     * interface for calculating the number of existing executors scheduled on a object (rack or node).
     */
    public interface ExistingScheduleFunc {
        int getNumExistingSchedule(String objectId);
    }
}
