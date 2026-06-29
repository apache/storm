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
package org.apache.storm.scheduler;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.blacklist.TestUtilsForBlacklistScheduler;
import org.apache.storm.scheduler.resource.normalization.ResourceMetrics;
import org.apache.storm.topology.TopologyBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the idle-supervisor rebalance behavior added to
 * {@link EvenScheduler#redistributeOntoIdleSupervisors(Topologies, Cluster)} and its per-supervisor eligibility predicate
 * {@link Cluster#isIdleSupervisorAvailableForEvenRebalance(SupervisorDetails)}.
 *
 * <p>Trigger condition is binary: at least one non-blacklisted supervisor with zero used slots must exist. The cluster
 * being "almost balanced" never triggers the new logic, so a near-even distribution is preserved as-is. Each round only
 * frees up to {@code nimbus.even.rebalance.max.free.per.topology} workers and never drains a supervisor down to zero.
 * The tests assert on the observable effect of {@code redistributeOntoIdleSupervisors} (the resulting assignment) rather
 * than on any internal boolean predicate.
 */
public class TestEvenSchedulerIdleSupervisor {

    private static final String TOPO_ID = "topo-1";

    /**
     * supA and supB host the topology; supC is freshly returned and idle. Topology has 2 workers on supA and 1 on supB.
     */
    private Cluster buildClusterWithIdleSupervisor(boolean enableRebalance, int maxFreePerTopology) {
        return buildClusterWithIdleSupervisor(TestUtilsForBlacklistScheduler.genSupervisors(3, 4),
                evenRebalanceConf(enableRebalance, maxFreePerTopology));
    }

    private Cluster buildClusterWithIdleSupervisor(Map<String, SupervisorDetails> supMap, Map<String, Object> conf) {
        // Build a topology and assign 3 workers: two on sup-0 and one on sup-1. sup-2 stays idle.
        TopologyDetails topology = makeTopologyDetails(TOPO_ID, 3);

        WorkerSlot s0p0 = new WorkerSlot("sup-0", 0);
        WorkerSlot s0p1 = new WorkerSlot("sup-0", 1);
        WorkerSlot s1p0 = new WorkerSlot("sup-1", 0);

        List<ExecutorDetails> execs = new LinkedList<>(topology.getExecutors());
        Collections.sort(execs, (a, b) -> Integer.compare(a.getStartTask(), b.getStartTask()));
        // Distribute the executors round-robin onto the three slots so each slot has at least one.
        Map<ExecutorDetails, WorkerSlot> execToSlot = new HashMap<>();
        WorkerSlot[] slotRing = new WorkerSlot[]{s0p0, s0p1, s1p0};
        for (int i = 0; i < execs.size(); i++) {
            execToSlot.put(execs.get(i), slotRing[i % slotRing.length]);
        }
        SchedulerAssignmentImpl assignment = new SchedulerAssignmentImpl(TOPO_ID, execToSlot, null, null);

        Map<String, SchedulerAssignmentImpl> assignments = new HashMap<>();
        assignments.put(TOPO_ID, assignment);

        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put(TOPO_ID, topology);
        Topologies topologies = new Topologies(topoMap);

        return newCluster(supMap, assignments, topologies, conf);
    }

    private Map<String, Object> evenRebalanceConf(boolean enableRebalance, int maxFreePerTopology) {
        Map<String, Object> conf = new HashMap<>();
        conf.put(DaemonConfig.NIMBUS_EVEN_REBALANCE_ON_IDLE_SUPERVISOR_ENABLED, enableRebalance);
        conf.put(DaemonConfig.NIMBUS_EVEN_REBALANCE_MAX_FREE_PER_TOPOLOGY, maxFreePerTopology);
        conf.put(DaemonConfig.NIMBUS_EVEN_REBALANCE_IDLE_SUPERVISOR_MIN_STABLE_ROUNDS, 0);
        conf.put(DaemonConfig.SUPERVISOR_MONITOR_FREQUENCY_SECS, 3);
        return conf;
    }

    private Map<String, SupervisorDetails> genSupervisorsWithUptime(int numSup, int numPorts, long uptimeSecs) {
        Map<String, SupervisorDetails> supMap = new HashMap<>();
        for (int i = 0; i < numSup; i++) {
            SupervisorDetails sup = supervisor("sup-" + i, "host-" + i, numPorts, uptimeSecs);
            supMap.put(sup.getId(), sup);
        }
        return supMap;
    }

    private SupervisorDetails supervisor(String id, String host, int numPorts, long uptimeSecs) {
        List<Number> ports = new LinkedList<>();
        for (int port = 0; port < numPorts; port++) {
            ports.add(port);
        }
        return new SupervisorDetails(id, host, null, ports, null, uptimeSecs);
    }

    private Cluster newCluster(Map<String, SupervisorDetails> supMap,
                               Map<String, SchedulerAssignmentImpl> assignments,
                               Topologies topologies,
                               Map<String, Object> conf) {
        StormMetricsRegistry metricsRegistry = new StormMetricsRegistry();
        ResourceMetrics resourceMetrics = new ResourceMetrics(metricsRegistry);
        return new Cluster(new TestUtilsForBlacklistScheduler.INimbusTest(), resourceMetrics, supMap,
                assignments, topologies, conf);
    }

    private TopologyDetails makeTopologyDetails(String id, int numWorkers, int parallelism) {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_NAME, id);
        conf.put(Config.TOPOLOGY_WORKERS, numWorkers);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout-0", new TestUtilsForBlacklistScheduler.TestSpout(), parallelism);
        builder.setBolt("bolt-0", new TestUtilsForBlacklistScheduler.TestBolt(), parallelism).shuffleGrouping("spout-0");
        StormTopology stormTopology = builder.createTopology();

        Map<ExecutorDetails, String> execsAndComps = TestUtilsForBlacklistScheduler.genExecsAndComps(
                stormTopology, parallelism, parallelism);
        return new TopologyDetails(id, conf, stormTopology, numWorkers, execsAndComps, 0, "user");
    }

    private TopologyDetails makeTopologyDetails(String id, int numWorkers) {
        return makeTopologyDetails(id, numWorkers, 3);
    }

    private TopologyDetails firstTopology(Cluster cluster) {
        return cluster.getTopologies().getById(TOPO_ID);
    }

    private int usedSlotCount(Cluster cluster, String supervisorId) {
        SupervisorDetails s = cluster.getSupervisorById(supervisorId);
        return cluster.getUsedPorts(s).size();
    }

    @Test
    public void disabledByDefault_doesNotTrigger() {
        Cluster cluster = buildClusterWithIdleSupervisor(false, 1);

        EvenScheduler.redistributeOntoIdleSupervisors(cluster.getTopologies(), cluster);

        // Disabled flag must short-circuit the rebalance even when an idle supervisor exists: nothing moves onto sup-2.
        assertEquals(2, usedSlotCount(cluster, "sup-0"));
        assertEquals(1, usedSlotCount(cluster, "sup-1"));
        assertEquals(0, usedSlotCount(cluster, "sup-2"));
        assertFalse(cluster.needsScheduling(firstTopology(cluster)),
                "needsScheduling must remain false when the new behavior is disabled and the topology is fully assigned");
    }

    @Test
    public void enabledWithIdleSupervisor_doesNotChangeGenericNeedsScheduling() {
        Cluster cluster = buildClusterWithIdleSupervisor(true, 1);

        // Enabling the opt-in idle rebalance must not leak into the generic scheduling triggers other schedulers use.
        assertFalse(cluster.needsScheduling(firstTopology(cluster)),
                "needsScheduling is used by schedulers other than EvenScheduler; the idle trigger stays out of that generic path");
        assertFalse(cluster.needsSchedulingRas(firstTopology(cluster)),
                "ResourceAwareScheduler keeps using needsSchedulingRas, so this opt-in EvenScheduler feature is out of RAS scope");

        EvenScheduler.redistributeOntoIdleSupervisors(cluster.getTopologies(), cluster);

        // The feature itself still fires: one worker relocates onto the idle supervisor (the observable trigger)...
        assertEquals(1, usedSlotCount(cluster, "sup-2"));
        assertEquals(3, cluster.getAssignedNumWorkers(firstTopology(cluster)));
        // ...while the generic triggers stay false afterward -- the relocation kept the topology fully assigned.
        assertFalse(cluster.needsScheduling(firstTopology(cluster)));
        assertFalse(cluster.needsSchedulingRas(firstTopology(cluster)));
    }

    @Test
    public void noIdleSupervisor_doesNotTrigger() {
        // Two supervisors, both serving the topology -> no idle supervisor present.
        Map<String, SupervisorDetails> supMap = TestUtilsForBlacklistScheduler.genSupervisors(2, 4);
        TopologyDetails topology = makeTopologyDetails(TOPO_ID, 3);

        Map<String, SchedulerAssignmentImpl> assignments = new HashMap<>();
        assignments.put(TOPO_ID, buildAssignment(topology, new WorkerSlot[]{
                new WorkerSlot("sup-0", 0), new WorkerSlot("sup-0", 1), new WorkerSlot("sup-1", 0),
        }));

        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put(TOPO_ID, topology);
        Topologies topologies = new Topologies(topoMap);

        Cluster cluster = newCluster(supMap, assignments, topologies, evenRebalanceConf(true, 1));

        EvenScheduler.redistributeOntoIdleSupervisors(cluster.getTopologies(), cluster);

        // No supervisor has zero used slots, so the binary trigger never fires: the assignment is left untouched.
        assertEquals(2, usedSlotCount(cluster, "sup-0"));
        assertEquals(1, usedSlotCount(cluster, "sup-1"));
        assertEquals(3, cluster.getAssignedNumWorkers(firstTopology(cluster)));
        assertFalse(cluster.needsScheduling(firstTopology(cluster)));
    }

    @Test
    public void redistributeRelocatesAtMostMaxFreeWorkersPerTopology() {
        Cluster cluster = buildClusterWithIdleSupervisor(true, 1);
        assertEquals(2, usedSlotCount(cluster, "sup-0"));
        assertEquals(1, usedSlotCount(cluster, "sup-1"));
        assertEquals(0, usedSlotCount(cluster, "sup-2"));

        EvenScheduler.redistributeOntoIdleSupervisors(cluster.getTopologies(), cluster);

        // max-free=1 caps the topology to a single relocation; pulled from the most-loaded supervisor (sup-0)
        // and placed directly onto the idle supervisor.
        assertEquals(1, usedSlotCount(cluster, "sup-0"));
        assertEquals(1, usedSlotCount(cluster, "sup-1"));
        assertEquals(1, usedSlotCount(cluster, "sup-2"));
        assertEquals(3, cluster.getAssignedNumWorkers(firstTopology(cluster)));
    }

    @Test
    public void redistributeNeverDrainsSupervisorToZero() {
        Map<String, SupervisorDetails> supMap = TestUtilsForBlacklistScheduler.genSupervisors(3, 4);
        TopologyDetails topology = makeTopologyDetails(TOPO_ID, 2);

        Map<String, SchedulerAssignmentImpl> assignments = new HashMap<>();
        assignments.put(TOPO_ID, buildAssignment(topology, new WorkerSlot[]{
                new WorkerSlot("sup-0", 0), new WorkerSlot("sup-1", 0),
        }));

        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put(TOPO_ID, topology);
        Topologies topologies = new Topologies(topoMap);

        Cluster cluster = newCluster(supMap, assignments, topologies, evenRebalanceConf(true, 5));

        EvenScheduler.redistributeOntoIdleSupervisors(topologies, cluster);

        // floor(2/3)=0 → topology gets a budget of 0 and is skipped entirely. No source supervisor is drained.
        assertEquals(1, usedSlotCount(cluster, "sup-0"));
        assertEquals(1, usedSlotCount(cluster, "sup-1"));
        assertEquals(0, usedSlotCount(cluster, "sup-2"));
    }

    @Test
    public void scheduleTopologiesEvenly_movesOneWorkerToIdleSupervisor() {
        Cluster cluster = buildClusterWithIdleSupervisor(true, 1);

        Topologies topologies = cluster.getTopologies();
        EvenScheduler.scheduleTopologiesEvenly(topologies, cluster);

        // After scheduling: idle supervisor (sup-2) should now host exactly 1 worker.
        assertEquals(1, usedSlotCount(cluster, "sup-2"));
        // Total worker count is preserved (3) and respects the topology's numWorkers.
        int total = usedSlotCount(cluster, "sup-0")
                + usedSlotCount(cluster, "sup-1")
                + usedSlotCount(cluster, "sup-2");
        assertEquals(3, total);
        assertEquals(3, cluster.getAssignedNumWorkers(firstTopology(cluster)),
                "relocation must preserve the topology's declared worker count, not just keep 3 slots occupied");
    }

    /**
     * Single-worker topology + idle supervisors must produce no movement: {@code floor(1 / N) = 0} for any N >= 2, so the
     * drain budget evaluates to zero regardless of how many idle supervisors exist. Without this guard a single-worker
     * topology would ping-pong between supervisors every monitor cycle.
     */
    @Test
    public void singleWorkerTopology_doesNotMoveDespiteIdleSupervisors() {
        String topoId = "topo-single";
        Map<String, SupervisorDetails> supMap = TestUtilsForBlacklistScheduler.genSupervisors(3, 4);
        TopologyDetails topology = makeTopologyDetails(topoId, 1, 1);

        Map<String, SchedulerAssignmentImpl> assignments = new HashMap<>();
        assignments.put(topoId, buildAssignment(topology, new WorkerSlot[]{ new WorkerSlot("sup-0", 0) }));

        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put(topoId, topology);
        Topologies topologies = new Topologies(topoMap);

        Cluster cluster = newCluster(supMap, assignments, topologies, evenRebalanceConf(true, 0));

        EvenScheduler.scheduleTopologiesEvenly(topologies, cluster);

        assertEquals(1, usedSlotCount(cluster, "sup-0"));
        assertEquals(0, usedSlotCount(cluster, "sup-1"));
        assertEquals(0, usedSlotCount(cluster, "sup-2"));
    }

    /**
     * 8-worker topology starts at distribution (4, 4, 0). With max-free unbounded the budget targets
     * floor(numWorkers / numSupervisors) = 2 workers for the idle supervisor, and the round ends at (3, 3, 2)
     * — fully even — without disturbing topologies on the next round (no supervisor is idle anymore).
     */
    @Test
    public void evenDistributionInOneRound_unboundedMaxFree() {
        String topoId = "topo-even";
        Map<String, SupervisorDetails> supMap = TestUtilsForBlacklistScheduler.genSupervisors(3, 4);
        TopologyDetails topology = makeTopologyDetails(topoId, 8, 4);

        Map<String, SchedulerAssignmentImpl> assignments = new HashMap<>();
        assignments.put(topoId, buildAssignment(topology, new WorkerSlot[]{
                new WorkerSlot("sup-0", 0), new WorkerSlot("sup-0", 1),
                new WorkerSlot("sup-0", 2), new WorkerSlot("sup-0", 3),
                new WorkerSlot("sup-1", 0), new WorkerSlot("sup-1", 1),
                new WorkerSlot("sup-1", 2), new WorkerSlot("sup-1", 3),
        }));

        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put(topoId, topology);
        Topologies topologies = new Topologies(topoMap);

        Cluster cluster = newCluster(supMap, assignments, topologies, evenRebalanceConf(true, 0));

        assertEquals(4, usedSlotCount(cluster, "sup-0"));
        assertEquals(4, usedSlotCount(cluster, "sup-1"));
        assertEquals(0, usedSlotCount(cluster, "sup-2"));

        EvenScheduler.scheduleTopologiesEvenly(topologies, cluster);

        // Idle supervisor absorbs exactly floor(8/3) = 2 workers in one round; total worker count is preserved.
        assertEquals(2, usedSlotCount(cluster, "sup-2"));
        assertEquals(8, usedSlotCount(cluster, "sup-0")
                + usedSlotCount(cluster, "sup-1")
                + usedSlotCount(cluster, "sup-2"));
        assertEquals(8, cluster.getAssignedNumWorkers(cluster.getTopologies().getById(topoId)),
                "relocation must preserve the declared worker count of an 8-worker topology");
        // No supervisor is idle anymore, so a second pass relocates nothing -- the trigger will not refire next round.
        EvenScheduler.redistributeOntoIdleSupervisors(cluster.getTopologies(), cluster);
        assertEquals(2, usedSlotCount(cluster, "sup-2"));
        assertEquals(8, usedSlotCount(cluster, "sup-0")
                + usedSlotCount(cluster, "sup-1")
                + usedSlotCount(cluster, "sup-2"));
    }

    /**
     * Two equally-sized topologies share the same returning supervisor round-robin: each contributes one worker, so
     * sup-2 ends up hosting workers from both — restoring per-supervisor workload diversity the way a fresh submission
     * would.
     */
    @Test
    public void multipleTopologies_shareIdleSlotsRoundRobin() {
        Map<String, SupervisorDetails> supMap = TestUtilsForBlacklistScheduler.genSupervisors(3, 4);

        TopologyDetails topoA = makeTopologyDetails("topo-A", 4, 2);
        TopologyDetails topoB = makeTopologyDetails("topo-B", 4, 2);

        Map<String, SchedulerAssignmentImpl> assignments = new HashMap<>();
        assignments.put("topo-A", buildAssignment(topoA, new WorkerSlot[]{
                new WorkerSlot("sup-0", 0), new WorkerSlot("sup-0", 1),
                new WorkerSlot("sup-1", 0), new WorkerSlot("sup-1", 1),
        }));
        assignments.put("topo-B", buildAssignment(topoB, new WorkerSlot[]{
                new WorkerSlot("sup-0", 2), new WorkerSlot("sup-0", 3),
                new WorkerSlot("sup-1", 2), new WorkerSlot("sup-1", 3),
        }));

        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put("topo-A", topoA);
        topoMap.put("topo-B", topoB);
        Topologies topologies = new Topologies(topoMap);

        Cluster cluster = newCluster(supMap, assignments, topologies, evenRebalanceConf(true, 0));

        EvenScheduler.redistributeOntoIdleSupervisors(topologies, cluster);

        // floor(4/3)=1 per topology, two topologies → sup-2 hosts 1 worker from each, in round-robin order.
        // Exact counts (not >= 1) are what actually enforce round-robin fairness: a broken inner loop that let the
        // first topology grab both idle slots would leave topo-B at 0 here.
        assertEquals(2, usedSlotCount(cluster, "sup-2"));
        assertEquals(1, supervisorWorkerCount(cluster, "topo-A", "sup-2"));
        assertEquals(1, supervisorWorkerCount(cluster, "topo-B", "sup-2"));
        // Each topology kept its total worker count; only one host moved.
        assertEquals(4, cluster.getAssignedNumWorkers(topoA));
        assertEquals(4, cluster.getAssignedNumWorkers(topoB));
        // Donor supervisors are never drained to zero (which would make them the next round's idle target).
        assertTrue(usedSlotCount(cluster, "sup-0") > 0);
        assertTrue(usedSlotCount(cluster, "sup-1") > 0);
    }

    /**
     * Flap-guard boundary: with 3 stable rounds at a 3s monitor frequency a returning supervisor must have been up for
     * at least 9 seconds before it is eligible. {@code uptime == requiredUptime} is the first value that moves, making
     * the off-by-one contract explicit: {@code uptimeSecs >= minStableRounds * monitorFrequencySecs}.
     */
    @ParameterizedTest
    @CsvSource({
            "8, false",   // threshold - 1: too young, stays idle
            "9, true",    // exactly at threshold: eligible
            "10, true",   // threshold + 1: eligible
    })
    public void flapGuardHonorsMinStableRoundBoundary(long sup2UptimeSecs, boolean expectMove) {
        Map<String, SupervisorDetails> supMap = genSupervisorsWithUptime(3, 4, 100);
        supMap.put("sup-2", supervisor("sup-2", "host-2", 4, sup2UptimeSecs));

        Map<String, Object> conf = evenRebalanceConf(true, 1);
        conf.put(DaemonConfig.NIMBUS_EVEN_REBALANCE_IDLE_SUPERVISOR_MIN_STABLE_ROUNDS, 3);
        conf.put(DaemonConfig.SUPERVISOR_MONITOR_FREQUENCY_SECS, 3);
        Cluster cluster = buildClusterWithIdleSupervisor(supMap, conf);

        EvenScheduler.scheduleTopologiesEvenly(cluster.getTopologies(), cluster);

        // 3 stable rounds at a 3 second monitor frequency require at least 9 seconds of supervisor uptime before sup-2
        // becomes an eligible target; the placement below is the observable expression of that boundary.
        if (expectMove) {
            assertEquals(1, usedSlotCount(cluster, "sup-2"));
            assertEquals(3, cluster.getAssignedNumWorkers(firstTopology(cluster)));
        } else {
            assertEquals(2, usedSlotCount(cluster, "sup-0"));
            assertEquals(1, usedSlotCount(cluster, "sup-1"));
            assertEquals(0, usedSlotCount(cluster, "sup-2"));
        }
    }

    @Test
    public void donorTieBreaksBySupervisorIdWhenWorkerCountsTie() {
        Map<String, SupervisorDetails> supMap = genSupervisorsWithUptime(3, 4, 100);
        TopologyDetails topology = makeTopologyDetails("topo-tie", 4, 4);

        Map<String, SchedulerAssignmentImpl> assignments = new HashMap<>();
        assignments.put("topo-tie", buildAssignment(topology, new WorkerSlot[]{
                new WorkerSlot("sup-0", 0), new WorkerSlot("sup-0", 1),
                new WorkerSlot("sup-1", 0), new WorkerSlot("sup-1", 1),
        }));

        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put("topo-tie", topology);
        Topologies topologies = new Topologies(topoMap);
        Cluster cluster = newCluster(supMap, assignments, topologies, evenRebalanceConf(true, 1));

        EvenScheduler.redistributeOntoIdleSupervisors(topologies, cluster);

        assertEquals(1, supervisorWorkerCount(cluster, "topo-tie", "sup-0"),
                "sup-0 and sup-1 started with two workers each; lexicographic tie-break chooses sup-0 as donor");
        assertEquals(2, supervisorWorkerCount(cluster, "topo-tie", "sup-1"));
        assertEquals(1, supervisorWorkerCount(cluster, "topo-tie", "sup-2"));
        assertEquals(4, cluster.getAssignedNumWorkers(topology),
                "the tie-break relocation preserves the topology's declared worker count");
    }

    /**
     * Two simultaneously-idle supervisors must let a single topology relocate
     * {@code floor(numWorkers / nonBlacklistedSupervisorCount) * idleSupervisorCount} workers in one round, exercising the
     * {@code * idleSupervisorCount} term of the budget formula. With 4 non-blacklisted supervisors (two busy, two idle)
     * and an 8-worker topology the budget is {@code floor(8 / 4) * 2 = 4}; a regression that dropped the multiplier would
     * compute 2 and relocate only half as many workers. Every other test has exactly one usable idle supervisor, so this
     * fixture is the one that pins the multiplier down.
     */
    @Test
    public void twoIdleSupervisors_budgetScalesWithIdleSupervisorCount() {
        String topoId = "topo-two-idle";
        Map<String, SupervisorDetails> supMap = TestUtilsForBlacklistScheduler.genSupervisors(4, 4);
        TopologyDetails topology = makeTopologyDetails(topoId, 8, 4);

        Map<String, SchedulerAssignmentImpl> assignments = new HashMap<>();
        assignments.put(topoId, buildAssignment(topology, new WorkerSlot[]{
                new WorkerSlot("sup-0", 0), new WorkerSlot("sup-0", 1),
                new WorkerSlot("sup-0", 2), new WorkerSlot("sup-0", 3),
                new WorkerSlot("sup-1", 0), new WorkerSlot("sup-1", 1),
                new WorkerSlot("sup-1", 2), new WorkerSlot("sup-1", 3),
        }));

        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put(topoId, topology);
        Topologies topologies = new Topologies(topoMap);

        Cluster cluster = newCluster(supMap, assignments, topologies, evenRebalanceConf(true, 0));

        // sup-2 and sup-3 both start idle.
        assertEquals(0, usedSlotCount(cluster, "sup-2"));
        assertEquals(0, usedSlotCount(cluster, "sup-3"));

        EvenScheduler.redistributeOntoIdleSupervisors(topologies, cluster);

        // budget = floor(8 / 4 non-blacklisted) * 2 idle = 4 relocations onto the idle supervisors. Asserting the sum
        // (not a single supervisor) keeps this independent of the idle-slot fill order. Dropping the * idleSupervisorCount
        // term would compute budget 2 and move only 2 workers, failing this assertion.
        assertEquals(4, usedSlotCount(cluster, "sup-2") + usedSlotCount(cluster, "sup-3"),
                "budget must scale with the number of simultaneously-idle supervisors: floor(8/4) * 2 = 4");
        assertEquals(4, usedSlotCount(cluster, "sup-0") + usedSlotCount(cluster, "sup-1"));
        assertEquals(8, cluster.getAssignedNumWorkers(cluster.getTopologies().getById(topoId)),
                "relocation preserves the declared worker count");
    }

    /**
     * {@code max.free.per.topology} must be able to bind more tightly than the even-distribution budget. With 3
     * supervisors and a 6-worker topology the even budget is {@code floor(6 / 3) * 1 = 2}, but {@code maxFree = 1} clamps
     * it to a single relocation. This is the only fixture where the {@code Math.min(target, maxFree)} clamp is the
     * strictly binding constraint -- removing the clamp would relocate 2 workers and push sup-2 to 2, failing the
     * assertion below.
     */
    @Test
    public void maxFreePerTopologyClampsBelowEvenBudget() {
        String topoId = "topo-clamp";
        Map<String, SupervisorDetails> supMap = TestUtilsForBlacklistScheduler.genSupervisors(3, 4);
        TopologyDetails topology = makeTopologyDetails(topoId, 6, 3);

        Map<String, SchedulerAssignmentImpl> assignments = new HashMap<>();
        assignments.put(topoId, buildAssignment(topology, new WorkerSlot[]{
                new WorkerSlot("sup-0", 0), new WorkerSlot("sup-0", 1), new WorkerSlot("sup-0", 2),
                new WorkerSlot("sup-1", 0), new WorkerSlot("sup-1", 1), new WorkerSlot("sup-1", 2),
        }));

        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put(topoId, topology);
        Topologies topologies = new Topologies(topoMap);

        Cluster cluster = newCluster(supMap, assignments, topologies, evenRebalanceConf(true, 1));

        EvenScheduler.redistributeOntoIdleSupervisors(topologies, cluster);

        // even budget = floor(6/3)*1 = 2, but maxFree=1 clamps it to a single relocation. Without Math.min(target,
        // maxFree) two workers would move and sup-2 would hold 2.
        assertEquals(1, usedSlotCount(cluster, "sup-2"),
                "max.free.per.topology must clamp the even-distribution budget of 2 down to 1");
        assertEquals(6, cluster.getAssignedNumWorkers(cluster.getTopologies().getById(topoId)));
        assertTrue(usedSlotCount(cluster, "sup-0") > 0);
        assertTrue(usedSlotCount(cluster, "sup-1") > 0);
    }

    @Test
    public void blacklistedIdleSupervisorIsNotReusableTarget() {
        Cluster cluster = buildClusterWithIdleSupervisor(true, 1);
        cluster.blacklistHost("host-2");

        assertFalse(cluster.isIdleSupervisorAvailableForEvenRebalance(cluster.getSupervisorById("sup-2")),
                "IsolationScheduler represents reserved hosts by blacklisting them before delegating to DefaultScheduler");

        EvenScheduler.scheduleTopologiesEvenly(cluster.getTopologies(), cluster);

        // The blacklisted idle supervisor is never a target, so the assignment is left untouched.
        assertEquals(2, usedSlotCount(cluster, "sup-0"));
        assertEquals(1, usedSlotCount(cluster, "sup-1"));
        assertEquals(0, usedSlotCount(cluster, "sup-2"));
        assertEquals(3, cluster.getAssignedNumWorkers(firstTopology(cluster)));
    }

    /**
     * Regression for the apache/storm#8778 follow-up: in the {@link DefaultScheduler} path the per-topology
     * {@code max.free.per.topology} cap must bind once per scheduling round, not once per redistribute call.
     *
     * <p>{@link DefaultScheduler#defaultSchedule(Topologies, Cluster)} runs the idle-supervisor redistribute once over
     * the full topology set, then delegates to {@link EvenScheduler#scheduleTopologiesEvenly(Topologies, Cluster, boolean)}
     * once per under-assigned topology (now passing {@code redistributeOntoIdle=false}) -- the 2-arg overload it used to
     * call ran the redistribute a second time. With two idle supervisors the
     * full-set pass fills one of them (consuming the cap), leaving the second idle for the per-topology pass to fill
     * again, so an under-assigned topology relocated up to {@code 2 * maxFree} workers in a single round.
     *
     * <p>The fixture makes the topology under-assigned by declaring more workers than it has slots (8 declared, 4
     * executors on 4 slots) so {@code needsScheduling} stays true and the delegated call fires, while leaving no
     * executor unassigned -- the ordinary even-scheduling pass is then a no-op and only the redistribute relocations are
     * observable. sup-0 and sup-1 are donors with two workers each; sup-2 and sup-3 start idle.
     */
    @Test
    public void defaultSchedulerAppliesMaxFreeCapOncePerRound() {
        String topoId = "topo-double-cap";
        Map<String, SupervisorDetails> supMap = TestUtilsForBlacklistScheduler.genSupervisors(4, 4);
        // 8 declared workers but only 4 executors: needsScheduling stays true (8 > 4 assigned) with nothing to reassign.
        TopologyDetails topology = makeTopologyDetails(topoId, 8, 2);

        Map<String, SchedulerAssignmentImpl> assignments = new HashMap<>();
        assignments.put(topoId, buildAssignment(topology, new WorkerSlot[]{
                new WorkerSlot("sup-0", 0), new WorkerSlot("sup-0", 1),
                new WorkerSlot("sup-1", 0), new WorkerSlot("sup-1", 1),
        }));

        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put(topoId, topology);
        Topologies topologies = new Topologies(topoMap);

        Cluster cluster = newCluster(supMap, assignments, topologies, evenRebalanceConf(true, 1));

        // sup-2 and sup-3 both start idle; the topology has two workers on each of sup-0 and sup-1.
        assertEquals(0, usedSlotCount(cluster, "sup-2"));
        assertEquals(0, usedSlotCount(cluster, "sup-3"));

        DefaultScheduler.defaultSchedule(new Topologies(topology), cluster);

        // maxFree=1 caps relocation at one existing worker per round. The old double redistribute moved two -- one onto
        // each idle supervisor; with the cap applied once per round only one idle supervisor is filled and the other
        // stays untouched.
        int relocatedOntoIdle = supervisorWorkerCount(cluster, topoId, "sup-2")
                + supervisorWorkerCount(cluster, topoId, "sup-3");
        assertEquals(1, relocatedOntoIdle,
                "max.free.per.topology must cap idle-supervisor relocation at 1 per round, not 2 (apache/storm#8778 follow-up)");
        assertEquals(0, supervisorWorkerCount(cluster, topoId, "sup-3"),
                "the second idle supervisor stays idle once the per-round cap is reached");
        // The relocation moves existing workers only -- the assigned worker count is unchanged and no executor is lost.
        assertEquals(4, cluster.getAssignedNumWorkers(cluster.getTopologies().getById(topoId)),
                "relocation must preserve the topology's four assigned workers");
    }

    @Test
    public void defaultSchedulerIdleRebalanceHonorsLeftoverTopologySubset() {
        Map<String, SupervisorDetails> supMap = genSupervisorsWithUptime(3, 4, 100);
        TopologyDetails isolated = makeTopologyDetails("topo-isolated", 2, 2);
        TopologyDetails regular = makeTopologyDetails("topo-regular", 3, 3);

        Map<String, SchedulerAssignmentImpl> assignments = new HashMap<>();
        assignments.put(isolated.getId(), buildAssignment(isolated, new WorkerSlot[]{
                new WorkerSlot("sup-0", 0), new WorkerSlot("sup-0", 1),
        }));
        assignments.put(regular.getId(), buildAssignment(regular, new WorkerSlot[]{
                new WorkerSlot("sup-1", 0), new WorkerSlot("sup-1", 1), new WorkerSlot("sup-1", 2),
        }));

        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put(isolated.getId(), isolated);
        topoMap.put(regular.getId(), regular);
        Topologies allTopologies = new Topologies(topoMap);
        Cluster cluster = newCluster(supMap, assignments, allTopologies, evenRebalanceConf(true, 0));

        cluster.blacklistHost("host-0");
        DefaultScheduler.defaultSchedule(new Topologies(regular), cluster);

        assertEquals(2, supervisorWorkerCount(cluster, isolated.getId(), "sup-0"),
                "the isolated topology is not in the leftover topology set, so DefaultScheduler must not move it");
        assertEquals(0, supervisorWorkerCount(cluster, isolated.getId(), "sup-2"));
        assertEquals(2, supervisorWorkerCount(cluster, regular.getId(), "sup-1"));
        assertEquals(1, supervisorWorkerCount(cluster, regular.getId(), "sup-2"));
        // Both topologies keep their declared worker counts: the leftover one is relocated, the excluded one untouched.
        assertEquals(3, cluster.getAssignedNumWorkers(regular));
        assertEquals(2, cluster.getAssignedNumWorkers(isolated));
    }

    @Test
    public void isolationSchedulerOnlyRelocatesLeftoverTopologyOntoNonIsolatedIdleSupervisor() {
        Map<String, SupervisorDetails> supMap = genSupervisorsWithUptime(3, 4, 100);
        TopologyDetails isolated = makeTopologyDetails("topo-isolated", 1, 1);
        TopologyDetails regular = makeTopologyDetails("topo-regular", 3, 3);

        Map<String, SchedulerAssignmentImpl> assignments = new HashMap<>();
        assignments.put(isolated.getId(), buildAssignment(isolated, new WorkerSlot[]{
                new WorkerSlot("sup-0", 0),
        }));
        assignments.put(regular.getId(), buildAssignment(regular, new WorkerSlot[]{
                new WorkerSlot("sup-1", 0), new WorkerSlot("sup-1", 1), new WorkerSlot("sup-1", 2),
        }));

        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put(isolated.getId(), isolated);
        topoMap.put(regular.getId(), regular);
        Topologies topologies = new Topologies(topoMap);

        Map<String, Object> conf = evenRebalanceConf(true, 0);
        conf.put(DaemonConfig.ISOLATION_SCHEDULER_MACHINES, Collections.singletonMap(isolated.getName(), 1));
        Cluster cluster = newCluster(supMap, assignments, topologies, conf);

        IsolationScheduler scheduler = new IsolationScheduler();
        scheduler.prepare(conf, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);

        assertEquals(1, supervisorWorkerCount(cluster, isolated.getId(), "sup-0"),
                "the already-isolated topology remains on its isolated host and is not selected as a donor");
        assertEquals(0, supervisorWorkerCount(cluster, isolated.getId(), "sup-2"));
        assertEquals(2, supervisorWorkerCount(cluster, regular.getId(), "sup-1"));
        assertEquals(1, supervisorWorkerCount(cluster, regular.getId(), "sup-2"),
                "only the leftover regular topology is allowed to move onto the non-isolated idle supervisor");
        // The relocated leftover topology and the untouched isolated topology both keep their declared worker counts.
        assertEquals(3, cluster.getAssignedNumWorkers(regular));
        assertEquals(1, cluster.getAssignedNumWorkers(isolated));
    }

    /**
     * IsolationScheduler reserves a host by blacklisting it before delegating the remaining (non-isolated) topologies to
     * {@link DefaultScheduler#defaultSchedule(Topologies, Cluster)}. Here the isolated topology is down -- it has no
     * assigned workers at all -- yet its reserved host (sup-2) must not be treated as an idle relocation target even
     * though it has zero used slots. The leftover regular topology is rebalanced onto the genuinely idle, non-reserved
     * sup-3 and never onto the reserved sup-2.
     */
    @Test
    public void reservedHostForDownIsolatedTopologyIsNotTreatedAsIdle() {
        Map<String, SupervisorDetails> supMap = genSupervisorsWithUptime(4, 4, 100);
        TopologyDetails isolated = makeTopologyDetails("topo-isolated", 1, 1);
        TopologyDetails regular = makeTopologyDetails("topo-regular", 4, 4);

        Map<String, SchedulerAssignmentImpl> assignments = new HashMap<>();
        // The isolated topology is down: it has no assignment at all.
        assignments.put(regular.getId(), buildAssignment(regular, new WorkerSlot[]{
                new WorkerSlot("sup-0", 0), new WorkerSlot("sup-0", 1),
                new WorkerSlot("sup-1", 0), new WorkerSlot("sup-1", 1),
        }));

        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put(isolated.getId(), isolated);
        topoMap.put(regular.getId(), regular);
        Topologies allTopologies = new Topologies(topoMap);
        Cluster cluster = newCluster(supMap, assignments, allTopologies, evenRebalanceConf(true, 0));

        // sup-2 is reserved for the (down) isolated topology -- IsolationScheduler represents this by blacklisting it.
        cluster.blacklistHost("host-2");

        assertFalse(cluster.isIdleSupervisorAvailableForEvenRebalance(cluster.getSupervisorById("sup-2")),
                "a blacklisted reserved host is never an even-rebalance target, even with zero used slots");
        assertTrue(cluster.isIdleSupervisorAvailableForEvenRebalance(cluster.getSupervisorById("sup-3")),
                "the non-reserved idle supervisor is available");

        // IsolationScheduler delegates the leftover (non-isolated) topologies to DefaultScheduler with the reserved
        // host already blacklisted.
        DefaultScheduler.defaultSchedule(new Topologies(regular), cluster);

        assertEquals(0, usedSlotCount(cluster, "sup-2"),
                "the reserved host stays idle: the down isolated topology's machine is not repopulated by rebalance");
        assertEquals(0, supervisorWorkerCount(cluster, regular.getId(), "sup-2"));
        assertEquals(1, supervisorWorkerCount(cluster, regular.getId(), "sup-3"),
                "the leftover regular topology rebalances onto the genuinely idle, non-reserved supervisor");
        assertEquals(1, supervisorWorkerCount(cluster, regular.getId(), "sup-0"));
        assertEquals(2, supervisorWorkerCount(cluster, regular.getId(), "sup-1"));
        assertEquals(4, cluster.getAssignedNumWorkers(regular),
                "the leftover topology keeps all 4 workers; the move never loses executors");
        assertEquals(0, cluster.getUsedSlotsByTopologyId(isolated.getId()).size(),
                "the isolated topology is down and is never scheduled by the leftover path");
    }

    private SchedulerAssignmentImpl buildAssignment(TopologyDetails topology, WorkerSlot[] slots) {
        List<ExecutorDetails> execs = new LinkedList<>(topology.getExecutors());
        Collections.sort(execs, (a, b) -> Integer.compare(a.getStartTask(), b.getStartTask()));
        Map<ExecutorDetails, WorkerSlot> map = new HashMap<>();
        for (int i = 0; i < execs.size(); i++) {
            map.put(execs.get(i), slots[i % slots.length]);
        }
        return new SchedulerAssignmentImpl(topology.getId(), map, null, null);
    }

    private int supervisorWorkerCount(Cluster cluster, String topologyId, String supervisorId) {
        int count = 0;
        for (WorkerSlot slot : cluster.getUsedSlotsByTopologyId(topologyId)) {
            if (slot.getNodeId().equals(supervisorId)) {
                count++;
            }
        }
        return count;
    }
}
