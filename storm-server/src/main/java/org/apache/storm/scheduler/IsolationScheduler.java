/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.lang.Validate;
import org.apache.storm.DaemonConfig;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// for each isolated topology:
//   compute even distribution of executors -> workers on the number of workers specified for the topology
//   compute distribution of workers to machines
// determine host -> list of [slot, topology id, executors]
// iterate through hosts and: a machine is good if:
//   1. only running workers from one isolated topology
//   2. all workers running on it match one of the distributions of executors for that topology
//   3. matches one of the # of workers
// blacklist the good hosts and remove those workers from the list of need to be assigned workers
// otherwise unassign all other workers for isolated topologies if assigned
public class IsolationScheduler implements IScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(IsolationScheduler.class);

    private Map<String, Number> isoMachines;

    @Override
    public void prepare(Map<String, Object> conf, StormMetricsRegistry metricsRegistry) {
        this.isoMachines = (Map<String, Number>) conf.get(DaemonConfig.ISOLATION_SCHEDULER_MACHINES);
        Validate.notEmpty(isoMachines);
    }

    @Override
    public Map<String, Map<String, Double>> config() {
        return Collections.emptyMap();
    }

    // get host -> all assignable worker slots for non-blacklisted machines (assigned or not assigned)
    // will then have a list of machines that need to be assigned (machine -> [topology, list of list of executors])
    // match each spec to a machine (who has the right number of workers), free everything else on that machine and assign those slots
    // (do one topology at a time)
    // blacklist all machines who had production slots defined
    // log isolated topologies who weren't able to get enough slots / machines
    // run default scheduler on isolated topologies that didn't have enough slots + non-isolated topologies on remaining machines
    // set blacklist to what it was initially
    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        List<TopologyDetails> isoTopologies = isolatedTopologies(topologies.getTopologies());
        Set<String> isoIds = extractTopologyIds(isoTopologies);
        Map<String, Set<Set<ExecutorDetails>>> topologyWorkerSpecs = topologyWorkerSpecs(isoTopologies);
        Map<String, Map<Integer, Integer>> topologyMachineDistributions = topologyMachineDistributions(isoTopologies);
        Map<String, List<AssignmentInfo>> hostAssignments = hostAssignments(cluster);

        for (Map.Entry<String, List<AssignmentInfo>> entry : hostAssignments.entrySet()) {
            List<AssignmentInfo> assignments = entry.getValue();
            String topologyId = assignments.get(0).getTopologyId();
            Map<Integer, Integer> distribution = topologyMachineDistributions.get(topologyId);
            Set<Set<ExecutorDetails>> workerSpecs = topologyWorkerSpecs.get(topologyId);
            int numWorkers = assignments.size();

            if (isoIds.contains(topologyId)
                && checkAssignmentTopology(assignments, topologyId)
                && distribution.containsKey(numWorkers)
                && checkAssignmentWorkerSpecs(assignments, workerSpecs)) {
                decrementDistribution(distribution, numWorkers);
                for (AssignmentInfo ass : assignments) {
                    workerSpecs.remove(ass.getExecutors());
                }
                cluster.blacklistHost(entry.getKey());
            } else {
                for (AssignmentInfo ass : assignments) {
                    if (isoIds.contains(ass.getTopologyId())) {
                        cluster.freeSlot(ass.getWorkerSlot());
                    }
                }
            }
        }

        Map<String, Set<WorkerSlot>> hostUsedSlots = hostToUsedSlots(cluster);
        LinkedList<HostAssignableSlots> hss = hostAssignableSlots(cluster);
        for (Map.Entry<String, Set<Set<ExecutorDetails>>> entry : topologyWorkerSpecs.entrySet()) {
            String topologyId = entry.getKey();
            Set<Set<ExecutorDetails>> executorSet = entry.getValue();
            List<Integer> workerNum = distributionToSortedAmounts(topologyMachineDistributions.get(topologyId));
            for (Integer num : workerNum) {
                HostAssignableSlots hostSlots = hss.peek();
                List<WorkerSlot> slot = hostSlots != null ? hostSlots.getWorkerSlots() : null;

                if (slot != null && slot.size() >= num) {
                    hss.poll();
                    cluster.freeSlots(hostUsedSlots.get(hostSlots.getHostName()));
                    for (WorkerSlot tmpSlot : slot.subList(0, num)) {
                        Set<ExecutorDetails> executor = removeElemFromExecutorsSet(executorSet);
                        cluster.assign(tmpSlot, topologyId, executor);
                    }
                    cluster.blacklistHost(hostSlots.getHostName());
                }
            }
        }

        List<String> failedTopologyIds = extractFailedTopologyIds(topologyWorkerSpecs);
        if (failedTopologyIds.size() > 0) {
            LOG.warn("Unable to isolate topologies " + failedTopologyIds
                     + ". No machine had enough worker slots to run the remaining workers for these topologies. "
                     + "Clearing all other resources and will wait for enough resources for "
                     + "isolated topologies before allocating any other resources.");
            // clear workers off all hosts that are not blacklisted
            Map<String, Set<WorkerSlot>> usedSlots = hostToUsedSlots(cluster);
            Set<Map.Entry<String, Set<WorkerSlot>>> entries = usedSlots.entrySet();
            for (Map.Entry<String, Set<WorkerSlot>> entry : entries) {
                if (!cluster.isBlacklistedHost(entry.getKey())) {
                    cluster.freeSlots(entry.getValue());
                }
            }
        } else {
            // run default scheduler on non-isolated topologies
            Set<String> allocatedTopologies = allocatedTopologies(topologyWorkerSpecs);
            Topologies leftOverTopologies = leftoverTopologies(topologies, allocatedTopologies);
            DefaultScheduler.defaultSchedule(leftOverTopologies, cluster);
        }
        Set<String> origBlacklist = cluster.getBlacklistedHosts();
        cluster.setBlacklistedHosts(origBlacklist);
    }

    private Set<ExecutorDetails> removeElemFromExecutorsSet(Set<Set<ExecutorDetails>> executorsSets) {
        Set<ExecutorDetails> elem = executorsSets.iterator().next();
        executorsSets.remove(elem);
        return elem;
    }

    private List<TopologyDetails> isolatedTopologies(Collection<TopologyDetails> topologies) {
        Set<String> topologyNames = isoMachines.keySet();
        List<TopologyDetails> isoTopologies = new ArrayList<TopologyDetails>();
        for (TopologyDetails topo : topologies) {
            if (topologyNames.contains(topo.getName())) {
                isoTopologies.add(topo);
            }
        }
        return isoTopologies;
    }

    private Set<String> extractTopologyIds(List<TopologyDetails> topologies) {
        Set<String> ids = new HashSet<String>();
        if (topologies != null && topologies.size() > 0) {
            for (TopologyDetails topology : topologies) {
                ids.add(topology.getId());
            }
        }
        return ids;
    }

    private List<String> extractFailedTopologyIds(Map<String, Set<Set<ExecutorDetails>>> isoTopologyWorkerSpecs) {
        List<String> failedTopologyIds = new ArrayList<String>();
        for (Map.Entry<String, Set<Set<ExecutorDetails>>> topoWorkerSpecsEntry : isoTopologyWorkerSpecs.entrySet()) {
            Set<Set<ExecutorDetails>> workerSpecs = topoWorkerSpecsEntry.getValue();
            if (workerSpecs != null && !workerSpecs.isEmpty()) {
                failedTopologyIds.add(topoWorkerSpecsEntry.getKey());
            }
        }
        return failedTopologyIds;
    }

    // map from topology id -> set of sets of executors
    private Map<String, Set<Set<ExecutorDetails>>> topologyWorkerSpecs(List<TopologyDetails> topologies) {
        Map<String, Set<Set<ExecutorDetails>>> workerSpecs = new HashMap<String, Set<Set<ExecutorDetails>>>();
        for (TopologyDetails topology : topologies) {
            workerSpecs.put(topology.getId(), computeWorkerSpecs(topology));
        }
        return workerSpecs;
    }

    private Map<String, List<AssignmentInfo>> hostAssignments(Cluster cluster) {
        Collection<SchedulerAssignment> assignmentValues = cluster.getAssignments().values();
        Map<String, List<AssignmentInfo>> hostAssignments = new HashMap<String, List<AssignmentInfo>>();

        for (SchedulerAssignment sa : assignmentValues) {
            Map<WorkerSlot, List<ExecutorDetails>> slotExecutors = Utils.reverseMap(sa.getExecutorToSlot());
            Set<Map.Entry<WorkerSlot, List<ExecutorDetails>>> entries = slotExecutors.entrySet();
            for (Map.Entry<WorkerSlot, List<ExecutorDetails>> entry : entries) {
                WorkerSlot slot = entry.getKey();
                List<ExecutorDetails> executors = entry.getValue();

                String host = cluster.getHost(slot.getNodeId());
                AssignmentInfo ass = new AssignmentInfo(slot, sa.getTopologyId(), new HashSet<ExecutorDetails>(executors));
                List<AssignmentInfo> executorList = hostAssignments.get(host);
                if (executorList == null) {
                    executorList = new ArrayList<AssignmentInfo>();
                    hostAssignments.put(host, executorList);
                }
                executorList.add(ass);
            }
        }
        return hostAssignments;
    }

    private Set<Set<ExecutorDetails>> computeWorkerSpecs(TopologyDetails topology) {
        Map<String, List<ExecutorDetails>> compExecutors = Utils.reverseMap(topology.getExecutorToComponent());

        List<ExecutorDetails> allExecutors = new ArrayList<ExecutorDetails>();
        Collection<List<ExecutorDetails>> values = compExecutors.values();
        for (List<ExecutorDetails> value : values) {
            allExecutors.addAll(value);
        }

        int numWorkers = topology.getNumWorkers();
        int bucketIndex = 0;
        Map<Integer, Set<ExecutorDetails>> bucketExecutors = new HashMap<Integer, Set<ExecutorDetails>>(numWorkers);
        for (ExecutorDetails executor : allExecutors) {
            Set<ExecutorDetails> executors = bucketExecutors.get(bucketIndex);
            if (executors == null) {
                executors = new HashSet<ExecutorDetails>();
                bucketExecutors.put(bucketIndex, executors);
            }
            executors.add(executor);
            bucketIndex = (bucketIndex + 1) % numWorkers;
        }

        return new HashSet<Set<ExecutorDetails>>(bucketExecutors.values());
    }

    private Map<String, Map<Integer, Integer>> topologyMachineDistributions(List<TopologyDetails> isoTopologies) {
        Map<String, Map<Integer, Integer>> machineDistributions = new HashMap<String, Map<Integer, Integer>>();
        for (TopologyDetails topology : isoTopologies) {
            machineDistributions.put(topology.getId(), machineDistribution(topology));
        }
        return machineDistributions;
    }

    private Map<Integer, Integer> machineDistribution(TopologyDetails topology) {
        int machineNum = isoMachines.get(topology.getName()).intValue();
        int workerNum = topology.getNumWorkers();
        TreeMap<Integer, Integer> distribution = Utils.integerDivided(workerNum, machineNum);

        if (distribution.containsKey(0)) {
            distribution.remove(0);
        }
        return distribution;
    }

    private boolean checkAssignmentTopology(List<AssignmentInfo> assignments, String topologyId) {
        for (AssignmentInfo ass : assignments) {
            if (!topologyId.equals(ass.getTopologyId())) {
                return false;
            }
        }
        return true;
    }

    private boolean checkAssignmentWorkerSpecs(List<AssignmentInfo> assigments, Set<Set<ExecutorDetails>> workerSpecs) {
        for (AssignmentInfo ass : assigments) {
            if (!workerSpecs.contains(ass.getExecutors())) {
                return false;
            }
        }
        return true;
    }

    private void decrementDistribution(Map<Integer, Integer> distribution, int value) {
        Integer distValue = distribution.get(value);
        if (distValue != null) {
            int newValue = distValue - 1;
            if (newValue == 0) {
                distribution.remove(value);
            } else {
                distribution.put(value, newValue);
            }
        }
    }

    private Map<String, Set<WorkerSlot>> hostToUsedSlots(Cluster cluster) {
        Collection<WorkerSlot> usedSlots = cluster.getUsedSlots();
        Map<String, Set<WorkerSlot>> hostUsedSlots = new HashMap<String, Set<WorkerSlot>>();
        for (WorkerSlot slot : usedSlots) {
            String host = cluster.getHost(slot.getNodeId());
            Set<WorkerSlot> slots = hostUsedSlots.get(host);
            if (slots == null) {
                slots = new HashSet<WorkerSlot>();
                hostUsedSlots.put(host, slots);
            }
            slots.add(slot);
        }
        return hostUsedSlots;
    }

    // returns list of list of slots, reverse sorted by number of slots
    private LinkedList<HostAssignableSlots> hostAssignableSlots(Cluster cluster) {
        List<WorkerSlot> assignableSlots = cluster.getAssignableSlots();
        Map<String, List<WorkerSlot>> hostAssignableSlots = new HashMap<String, List<WorkerSlot>>();
        for (WorkerSlot slot : assignableSlots) {
            String host = cluster.getHost(slot.getNodeId());
            List<WorkerSlot> slots = hostAssignableSlots.get(host);
            if (slots == null) {
                slots = new ArrayList<WorkerSlot>();
                hostAssignableSlots.put(host, slots);
            }
            slots.add(slot);
        }
        List<HostAssignableSlots> sortHostAssignSlots = new ArrayList<HostAssignableSlots>();
        for (Map.Entry<String, List<WorkerSlot>> entry : hostAssignableSlots.entrySet()) {
            sortHostAssignSlots.add(new HostAssignableSlots(entry.getKey(), entry.getValue()));
        }
        Collections.sort(sortHostAssignSlots, new Comparator<HostAssignableSlots>() {
            @Override
            public int compare(HostAssignableSlots o1, HostAssignableSlots o2) {
                return o2.getWorkerSlots().size() - o1.getWorkerSlots().size();
            }
        });
        Collections.shuffle(sortHostAssignSlots);

        return new LinkedList<HostAssignableSlots>(sortHostAssignSlots);
    }

    private List<Integer> distributionToSortedAmounts(Map<Integer, Integer> distributions) {
        List<Integer> sorts = new ArrayList<Integer>();
        for (Map.Entry<Integer, Integer> entry : distributions.entrySet()) {
            int workers = entry.getKey();
            int machines = entry.getValue();
            for (int i = 0; i < machines; i++) {
                sorts.add(workers);
            }
        }
        Collections.sort(sorts, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o2.intValue() - o1.intValue();
            }
        });

        return sorts;
    }

    private Set<String> allocatedTopologies(Map<String, Set<Set<ExecutorDetails>>> topologyToWorkerSpecs) {
        Set<String> allocatedTopologies = new HashSet<String>();
        Set<Map.Entry<String, Set<Set<ExecutorDetails>>>> entries = topologyToWorkerSpecs.entrySet();
        for (Map.Entry<String, Set<Set<ExecutorDetails>>> entry : entries) {
            if (entry.getValue().isEmpty()) {
                allocatedTopologies.add(entry.getKey());
            }
        }
        return allocatedTopologies;
    }

    private Topologies leftoverTopologies(Topologies topologies, Set<String> filterIds) {
        Collection<TopologyDetails> topos = topologies.getTopologies();
        Map<String, TopologyDetails> leftoverTopologies = new HashMap<String, TopologyDetails>();
        for (TopologyDetails topo : topos) {
            String id = topo.getId();
            if (!filterIds.contains(id)) {
                leftoverTopologies.put(id, topo);
            }
        }
        return new Topologies(leftoverTopologies);
    }

    class AssignmentInfo {
        private WorkerSlot workerSlot;
        private String topologyId;
        private Set<ExecutorDetails> executors;

        AssignmentInfo(WorkerSlot workerSlot, String topologyId, Set<ExecutorDetails> executors) {
            this.workerSlot = workerSlot;
            this.topologyId = topologyId;
            this.executors = executors;
        }

        public WorkerSlot getWorkerSlot() {
            return workerSlot;
        }

        public String getTopologyId() {
            return topologyId;
        }

        public Set<ExecutorDetails> getExecutors() {
            return executors;
        }

    }

    class HostAssignableSlots {
        private String hostName;
        private List<WorkerSlot> workerSlots;

        HostAssignableSlots(String hostName, List<WorkerSlot> workerSlots) {
            this.hostName = hostName;
            this.workerSlots = workerSlots;
        }

        public String getHostName() {
            return hostName;
        }

        public List<WorkerSlot> getWorkerSlots() {
            return workerSlots;
        }

    }
}
