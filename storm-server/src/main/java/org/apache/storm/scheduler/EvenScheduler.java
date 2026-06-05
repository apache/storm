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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.storm.DaemonConfig;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.storm.shade.com.google.common.collect.Sets;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EvenScheduler implements IScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(EvenScheduler.class);

    @VisibleForTesting
    public static List<WorkerSlot> sortSlots(List<WorkerSlot> availableSlots) {
        //For example, we have a three nodes(supervisor1, supervisor2, supervisor3) cluster:
        //slots before sort:
        //supervisor1:6700, supervisor1:6701,
        //supervisor2:6700, supervisor2:6701, supervisor2:6702,
        //supervisor3:6700, supervisor3:6703, supervisor3:6702, supervisor3:6701
        //slots after sort:
        //supervisor3:6700, supervisor2:6700, supervisor1:6700,
        //supervisor3:6701, supervisor2:6701, supervisor1:6701,
        //supervisor3:6702, supervisor2:6702,
        //supervisor3:6703

        if (availableSlots != null && availableSlots.size() > 0) {
            // group by node
            Map<String, List<WorkerSlot>> slotGroups = new TreeMap<>();
            for (WorkerSlot slot : availableSlots) {
                String node = slot.getNodeId();
                List<WorkerSlot> slots = null;
                if (slotGroups.containsKey(node)) {
                    slots = slotGroups.get(node);
                } else {
                    slots = new ArrayList<WorkerSlot>();
                    slotGroups.put(node, slots);
                }
                slots.add(slot);
            }

            // sort by port: from small to large
            for (List<WorkerSlot> slots : slotGroups.values()) {
                Collections.sort(slots, new Comparator<WorkerSlot>() {
                    @Override
                    public int compare(WorkerSlot o1, WorkerSlot o2) {
                        return o1.getPort() - o2.getPort();
                    }
                });
            }

            // sort by available slots size: from large to small
            List<List<WorkerSlot>> list = new ArrayList<List<WorkerSlot>>(slotGroups.values());
            Collections.sort(list, new Comparator<List<WorkerSlot>>() {
                @Override
                public int compare(List<WorkerSlot> o1, List<WorkerSlot> o2) {
                    return o2.size() - o1.size();
                }
            });

            return ServerUtils.interleaveAll(list);
        }

        return null;
    }

    public static Map<WorkerSlot, List<ExecutorDetails>> getAliveAssignedWorkerSlotExecutors(Cluster cluster, String topologyId) {
        SchedulerAssignment existingAssignment = cluster.getAssignmentById(topologyId);
        Map<ExecutorDetails, WorkerSlot> executorToSlot = null;
        if (existingAssignment != null) {
            executorToSlot = existingAssignment.getExecutorToSlot();
        }

        return Utils.reverseMap(executorToSlot);
    }

    /**
     * Round-robin relocation of currently-assigned workers onto fully-idle supervisors. Each round-robin iteration moves
     * at most one worker per topology, so multiple topologies share the idle slots and a single returning supervisor ends
     * up hosting workers from several topologies — preserving the per-supervisor workload diversity that a fresh
     * cluster has after submission.
     *
     * <p>Per-topology cap in one scheduling round is
     * {@code idleSupervisorCount * floor(numWorkers / nonBlacklistedSupervisorCount)}, further tightened by
     * {@link DaemonConfig#NIMBUS_EVEN_REBALANCE_MAX_FREE_PER_TOPOLOGY} when set to a positive value. Topologies whose
     * computed cap is zero (typically {@code numWorkers < numSupervisors}) are skipped entirely. The trigger remains
     * binary — only fires when at least one supervisor has zero used slots — so a near-balanced cluster sees no
     * movement.
     *
     * <p>Workers are always pulled from the supervisor where this topology has the most workers, and only when that
     * supervisor would still hold at least one worker afterward. Each pulled worker's executors are placed directly
     * onto an idle slot, so the subsequent sortSlots / interleave pass cannot drop them back into the just-vacated
     * slots. Ties between equally loaded source supervisors are resolved by supervisor id, lexicographically.
     *
     * <p>Gated by {@link DaemonConfig#NIMBUS_EVEN_REBALANCE_ON_IDLE_SUPERVISOR_ENABLED}: when disabled (the default) the
     * method returns before scanning any supervisor, so a cluster that has not opted in pays no per-scheduling-round cost.
     */
    @VisibleForTesting
    static void redistributeOntoIdleSupervisors(Topologies topologies, Cluster cluster) {
        if (!ObjectReader.getBoolean(
                cluster.getConf().get(DaemonConfig.NIMBUS_EVEN_REBALANCE_ON_IDLE_SUPERVISOR_ENABLED), false)) {
            return;
        }
        int nonBlacklistedSupervisorCount = 0;
        int idleSupervisorCount = 0;
        Deque<WorkerSlot> idleTargets = new ArrayDeque<>();
        Set<String> idleSupervisorIds = new HashSet<>();
        List<SupervisorDetails> supervisors = new ArrayList<>(cluster.getSupervisors().values());
        supervisors.sort(Comparator.comparing(SupervisorDetails::getId));
        for (SupervisorDetails s : supervisors) {
            if (cluster.isBlackListed(s.getId())) {
                continue;
            }
            if (s.getAllPorts().isEmpty()) {
                continue;
            }
            nonBlacklistedSupervisorCount++;
            if (cluster.isIdleSupervisorAvailableForEvenRebalance(s)) {
                idleSupervisorCount++;
                idleSupervisorIds.add(s.getId());
                List<Integer> ports = new ArrayList<>(s.getAllPorts());
                Collections.sort(ports);
                for (Integer port : ports) {
                    idleTargets.add(new WorkerSlot(s.getId(), port));
                }
            }
        }
        if (idleTargets.isEmpty() || nonBlacklistedSupervisorCount == 0 || idleSupervisorCount == 0) {
            return;
        }

        int maxFree = ObjectReader.getInt(
                cluster.getConf().get(DaemonConfig.NIMBUS_EVEN_REBALANCE_MAX_FREE_PER_TOPOLOGY), 0);

        List<TopologyDetails> orderedTopos = new ArrayList<>();
        Map<String, Integer> remainingBudget = new HashMap<>();
        for (TopologyDetails topo : topologies.getTopologies()) {
            // Equivalent to cluster.hasIdleSupervisorReusableBy(topo) but reuses the idle-supervisor set already
            // computed above instead of re-reading the config and re-scanning every supervisor for each topology.
            if (!topologyCanReuseIdleSupervisor(cluster, topo, idleSupervisorIds)) {
                continue;
            }
            int target = (topo.getNumWorkers() / nonBlacklistedSupervisorCount) * idleSupervisorCount;
            if (target <= 0) {
                continue;
            }
            if (maxFree > 0) {
                target = Math.min(target, maxFree);
            }
            orderedTopos.add(topo);
            remainingBudget.put(topo.getId(), target);
        }
        if (orderedTopos.isEmpty()) {
            return;
        }
        orderedTopos.sort(Comparator.comparing(TopologyDetails::getId));

        int totalRelocated = 0;
        while (!idleTargets.isEmpty()) {
            boolean movedThisIteration = false;
            for (TopologyDetails topo : orderedTopos) {
                if (idleTargets.isEmpty()) {
                    break;
                }
                int remaining = remainingBudget.getOrDefault(topo.getId(), 0);
                if (remaining <= 0) {
                    continue;
                }
                if (relocateOneWorkerOntoIdleSlot(topo, cluster, idleTargets)) {
                    remainingBudget.put(topo.getId(), remaining - 1);
                    totalRelocated++;
                    movedThisIteration = true;
                } else {
                    remainingBudget.put(topo.getId(), 0);
                }
            }
            if (!movedThisIteration) {
                break;
            }
        }
        if (totalRelocated > 0) {
            LOG.info("EvenScheduler: relocated {} worker(s) onto idle supervisor(s) round-robin across {} topologies.",
                    totalRelocated, orderedTopos.size());
        }
    }

    /**
     * Returns true when at least one of the already-identified idle supervisors does not currently host {@code topology}
     * -- i.e. the topology can gain workload diversity by relocating onto it. This mirrors the binary trigger in
     * {@link Cluster#hasIdleSupervisorReusableBy(TopologyDetails)} but operates on the pre-computed {@code idleSupervisorIds}
     * to avoid the redundant per-topology config read and full supervisor rescan.
     */
    private static boolean topologyCanReuseIdleSupervisor(Cluster cluster, TopologyDetails topology,
                                                          Set<String> idleSupervisorIds) {
        Set<String> nodesUsedByTopology = new HashSet<>();
        for (WorkerSlot slot : cluster.getUsedSlotsByTopologyId(topology.getId())) {
            nodesUsedByTopology.add(slot.getNodeId());
        }
        for (String idleSupervisorId : idleSupervisorIds) {
            if (!nodesUsedByTopology.contains(idleSupervisorId)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Pulls a single worker from the supervisor where {@code topology} currently has the most workers and reassigns its
     * executors onto the next idle slot from {@code idleTargets}. Returns false (without consuming an idle target) if
     * the topology has no eligible source supervisor — namely all of its supervisors host at most one of its workers,
     * which would otherwise drain that supervisor to zero and turn it into the next round's idle.
     */
    private static boolean relocateOneWorkerOntoIdleSlot(TopologyDetails topology, Cluster cluster,
                                                         Deque<WorkerSlot> idleTargets) {
        Map<WorkerSlot, List<ExecutorDetails>> slotToExecutors =
                getAliveAssignedWorkerSlotExecutors(cluster, topology.getId());
        Map<String, List<WorkerSlot>> nodeToSlots = new HashMap<>();
        for (WorkerSlot slot : slotToExecutors.keySet()) {
            nodeToSlots.computeIfAbsent(slot.getNodeId(), k -> new ArrayList<>()).add(slot);
        }
        List<Map.Entry<String, List<WorkerSlot>>> candidates = new ArrayList<>(nodeToSlots.entrySet());
        candidates.removeIf(e -> e.getValue().size() < 2);
        candidates.sort(Comparator
                .<Map.Entry<String, List<WorkerSlot>>>comparingInt(e -> e.getValue().size())
                .reversed()
                .thenComparing(Map.Entry::getKey));
        if (candidates.isEmpty()) {
            return false;
        }
        List<WorkerSlot> slots = candidates.get(0).getValue();
        slots.sort(Comparator.comparingInt(WorkerSlot::getPort));
        WorkerSlot victim = slots.get(slots.size() - 1);
        Collection<ExecutorDetails> execs = slotToExecutors.get(victim);
        if (execs == null || execs.isEmpty()) {
            return false;
        }
        if (idleTargets.isEmpty()) {
            return false;
        }
        WorkerSlot target = idleTargets.poll();
        cluster.freeSlot(victim);
        cluster.assign(target, topology.getId(), execs);
        return true;
    }

    private static Map<ExecutorDetails, WorkerSlot> scheduleTopology(TopologyDetails topology, Cluster cluster) {
        List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
        Set<ExecutorDetails> allExecutors = topology.getExecutors();
        Map<WorkerSlot, List<ExecutorDetails>> aliveAssigned = getAliveAssignedWorkerSlotExecutors(cluster, topology.getId());
        int totalSlotsToUse = Math.min(topology.getNumWorkers(), availableSlots.size() + aliveAssigned.size());

        List<WorkerSlot> sortedList = sortSlots(availableSlots);
        if (sortedList == null) {
            LOG.error("No available slots for topology: {}", topology.getName());
            return new HashMap<ExecutorDetails, WorkerSlot>();
        }

        //allow requesting slots number bigger than available slots
        int toIndex = (totalSlotsToUse - aliveAssigned.size())
                      > sortedList.size() ? sortedList.size() : (totalSlotsToUse - aliveAssigned.size());
        List<WorkerSlot> reassignSlots = sortedList.subList(0, toIndex);

        Set<ExecutorDetails> aliveExecutors = new HashSet<ExecutorDetails>();
        for (List<ExecutorDetails> list : aliveAssigned.values()) {
            aliveExecutors.addAll(list);
        }
        Set<ExecutorDetails> reassignExecutors = Sets.difference(allExecutors, aliveExecutors);

        Map<ExecutorDetails, WorkerSlot> reassignment = new HashMap<ExecutorDetails, WorkerSlot>();
        if (reassignSlots.size() == 0) {
            return reassignment;
        }

        List<ExecutorDetails> executors = new ArrayList<ExecutorDetails>(reassignExecutors);
        Collections.sort(executors, new Comparator<ExecutorDetails>() {
            @Override
            public int compare(ExecutorDetails o1, ExecutorDetails o2) {
                return o1.getStartTask() - o2.getStartTask();
            }
        });

        for (int i = 0; i < executors.size(); i++) {
            reassignment.put(executors.get(i), reassignSlots.get(i % reassignSlots.size()));
        }

        if (reassignment.size() != 0) {
            LOG.info("Available slots: {}", availableSlots.toString());
        }
        return reassignment;
    }

    public static void scheduleTopologiesEvenly(Topologies topologies, Cluster cluster) {
        redistributeOntoIdleSupervisors(topologies, cluster);
        for (TopologyDetails topology : cluster.needsSchedulingTopologies()) {
            // needsSchedulingTopologies() returns the cluster's full topology set, but this run is scoped to the
            // topologies passed in: EvenScheduler.schedule passes the full set (so the guard is a no-op), while
            // DefaultScheduler.defaultSchedule calls us once per leftover topology with a single-topology Topologies.
            // redistributeOntoIdleSupervisors above acted only on that passed-in set too. Skip topologies outside it so
            // the leftover path never schedules one the caller excluded -- e.g. a down isolated topology on a reserved host.
            if (topologies.getById(topology.getId()) == null) {
                continue;
            }
            String topologyId = topology.getId();
            Map<ExecutorDetails, WorkerSlot> newAssignment = scheduleTopology(topology, cluster);
            Map<WorkerSlot, List<ExecutorDetails>> nodePortToExecutors = Utils.reverseMap(newAssignment);

            for (Map.Entry<WorkerSlot, List<ExecutorDetails>> entry : nodePortToExecutors.entrySet()) {
                WorkerSlot nodePort = entry.getKey();
                List<ExecutorDetails> executors = entry.getValue();
                cluster.assign(nodePort, topologyId, executors);
            }
        }
    }

    @Override
    public void prepare(Map<String, Object> conf, StormMetricsRegistry metricsRegistry) {
        //noop
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        scheduleTopologiesEvenly(topologies, cluster);
    }

    @Override
    public Map<String, Map<String, Double>> config() {
        return Collections.emptyMap();
    }

}
