/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.scheduler.resource.strategies.scheduling;

import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SchedulerAssignmentImpl;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.ResourceAwareScheduler;
import org.apache.storm.scheduler.resource.SchedulingResult;
import org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.*;

import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.resource.normalization.ResourceMetrics;

public class TestConstraintSolverStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(TestConstraintSolverStrategy.class);
    private static final int MAX_TRAVERSAL_DEPTH = 2000;
    private static final int NORMAL_BOLT_PARALLEL = 11;
    //Dropping the parallelism of the bolts to 3 instead of 11 so we can find a solution in a reasonable amount of work when backtracking.
    private static final int BACKTRACK_BOLT_PARALLEL = 3;

    public Map<String, Object> makeTestTopoConf() {
        List<List<String>> constraints = new LinkedList<>();
        addContraints("spout-0", "bolt-0", constraints);
        addContraints("bolt-2", "spout-0", constraints);
        addContraints("bolt-1", "bolt-2", constraints);
        addContraints("bolt-1", "bolt-0", constraints);
        addContraints("bolt-1", "spout-0", constraints);
        List<String> spread = new LinkedList<>();
        spread.add("spout-0");

        Map<String, Object> config = Utils.readDefaultConfig();
        config.put(DaemonConfig.RESOURCE_AWARE_SCHEDULER_MAX_STATE_SEARCH, MAX_TRAVERSAL_DEPTH);
        config.put(Config.TOPOLOGY_SPREAD_COMPONENTS, spread);
        config.put(Config.TOPOLOGY_RAS_CONSTRAINTS, constraints);
        config.put(Config.TOPOLOGY_RAS_CONSTRAINT_MAX_STATE_SEARCH, MAX_TRAVERSAL_DEPTH);
        config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 100_000);
        config.put(Config.TOPOLOGY_PRIORITY, 1);
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 10);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 100);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 0.0);

        return config;
    }

    public TopologyDetails makeTopology(Map<String, Object> config, int boltParallel) {
        return genTopology("testTopo", config, 1, 4, 4, boltParallel, 0, 0, "user");
    }

    public Cluster makeCluster(Topologies topologies) {
        return makeCluster(topologies, null);
    }

    public Cluster makeCluster(Topologies topologies, Map<String, SupervisorDetails> supMap) {
        if (supMap == null) {
            supMap = genSupervisors(4, 2, 120, 1200);
        }
        Map<String, Object> config = Utils.readDefaultConfig();
        return new Cluster(new INimbusTest(), new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);
    }

    public void basicUnitTestWithKillAndRecover(ConstraintSolverStrategy cs, int boltParallel) {
        Map<String, Object> config = makeTestTopoConf();
        cs.prepare(config);

        TopologyDetails topo = makeTopology(config, boltParallel);
        Topologies topologies = new Topologies(topo);
        Cluster cluster = makeCluster(topologies);

        LOG.info("Scheduling...");
        SchedulingResult result = cs.schedule(cluster, topo);
        LOG.info("Done scheduling {}...", result);

        Assert.assertTrue("Assert scheduling topology success " + result, result.isSuccess());
        Assert.assertEquals("topo all executors scheduled? " + cluster.getUnassignedExecutors(topo),
            0, cluster.getUnassignedExecutors(topo).size());
        Assert.assertTrue("Valid Scheduling?", ConstraintSolverStrategy.validateSolution(cluster, topo));
        LOG.info("Slots Used {}", cluster.getAssignmentById(topo.getId()).getSlots());
        LOG.info("Assignment {}", cluster.getAssignmentById(topo.getId()).getSlotToExecutors());

        //simulate worker loss
        SchedulerAssignment assignment = cluster.getAssignmentById(topo.getId());

        Set<WorkerSlot> slotsToDelete = new HashSet<>();
        Set<WorkerSlot> slots = assignment.getSlots();
        int i = 0;
        for (WorkerSlot slot: slots) {
            if (i % 2 == 0) {
                slotsToDelete.add(slot);
            }
            i++;
        }

        LOG.info("KILL WORKER(s) {}", slotsToDelete);
        for (WorkerSlot slot: slotsToDelete) {
            cluster.freeSlot(slot);
        }

        cs = new ConstraintSolverStrategy();
        cs.prepare(config);
        LOG.info("Scheduling again...");
        result = cs.schedule(cluster, topo);
        LOG.info("Done scheduling {}...", result);

        Assert.assertTrue("Assert scheduling topology success " + result, result.isSuccess());
        Assert.assertEquals("topo all executors scheduled?", 0, cluster.getUnassignedExecutors(topo).size());
        Assert.assertTrue("Valid Scheduling?", ConstraintSolverStrategy.validateSolution(cluster, topo));
    }

    @Test
    public void testConstraintSolverForceBacktrack() {
        //The best way to force backtracking is to change the heuristic so the components are reversed, so it is hard
        // to find an answer.
        ConstraintSolverStrategy cs = new ConstraintSolverStrategy() {
            @Override
            public <K extends Comparable<K>, V extends Comparable<V>> NavigableMap<K, V> sortByValues(final Map<K, V> map) {
                return super.sortByValues(map).descendingMap();
            }
        };
        basicUnitTestWithKillAndRecover(cs, BACKTRACK_BOLT_PARALLEL);
    }

    @Test
    public void testConstraintSolver() {
        basicUnitTestWithKillAndRecover(new ConstraintSolverStrategy(), NORMAL_BOLT_PARALLEL);
    }

    public void basicFailureTest(String confKey, Object confValue, ConstraintSolverStrategy cs) {
        Map<String, Object> config = makeTestTopoConf();
        config.put(confKey, confValue);
        cs.prepare(config);

        TopologyDetails topo = makeTopology(config, NORMAL_BOLT_PARALLEL);
        Topologies topologies = new Topologies(topo);
        Cluster cluster = makeCluster(topologies);

        LOG.info("Scheduling...");
        SchedulingResult result = cs.schedule(cluster, topo);
        LOG.info("Done scheduling {}...", result);

        Assert.assertTrue("Assert scheduling topology success " + result, !result.isSuccess());
    }

    @Test
    public void testTooManyStateTransitions() {
        basicFailureTest(Config.TOPOLOGY_RAS_CONSTRAINT_MAX_STATE_SEARCH, 10, new ConstraintSolverStrategy());
    }

    @Test
    public void testTimeout() {
        try (Time.SimulatedTime simulating = new Time.SimulatedTime()) {
            ConstraintSolverStrategy cs = new ConstraintSolverStrategy() {
                @Override
                protected SolverResult backtrackSearch(SearcherState state) {
                    //Each time we try to schedule a new component simulate taking 1 second longer
                    Time.advanceTime(1_000);
                    return super.backtrackSearch(state);

                }
            };
            basicFailureTest(Config.TOPOLOGY_RAS_CONSTRAINT_MAX_TIME_SECS, 2, cs);
        }
    }

    /*
     * Test scheduling large number of executors and constraints.
     *
     * Cluster has sufficient resources for scheduling to succeed but can fail due to StackOverflowError.
     */
    @ParameterizedTest
    @ValueSource(ints = {1, 20})
    public void testScheduleLargeExecutorConstraintCount(int parallelismMultiplier) {
        // Add 1 topology with large number of executors and constraints. Too many can cause a java.lang.StackOverflowError
        Config config = createCSSClusterConfig(10, 10, 0, null);
        config.put(Config.TOPOLOGY_RAS_CONSTRAINT_MAX_STATE_SEARCH, 50000);

        List<List<String>> constraints = new LinkedList<>();
        addContraints("spout-0", "spout-0", constraints);
        addContraints("bolt-1", "bolt-1", constraints);
        addContraints("spout-0", "bolt-0", constraints);
        addContraints("bolt-2", "spout-0", constraints);
        addContraints("bolt-1", "bolt-2", constraints);
        addContraints("bolt-1", "bolt-0", constraints);
        addContraints("bolt-1", "spout-0", constraints);

        config.put(Config.TOPOLOGY_RAS_CONSTRAINTS, constraints);
        TopologyDetails topo = genTopology("testTopo-" + parallelismMultiplier, config, 10, 10, 30 * parallelismMultiplier, 30 * parallelismMultiplier, 31414, 0, "user");
        Topologies topologies = new Topologies(topo);

        Map<String, SupervisorDetails> supMap = genSupervisors(30 * parallelismMultiplier, 30, 3500, 35000);
        Cluster cluster = makeCluster(topologies, supMap);

        ResourceAwareScheduler scheduler = new ResourceAwareScheduler();
        scheduler.prepare(config);
        scheduler.schedule(topologies, cluster);

        boolean scheduleSuccess = isStatusSuccess(cluster.getStatus(topo.getId()));

        if (parallelismMultiplier == 1) {
            Assert.assertTrue(scheduleSuccess);
        } else if (parallelismMultiplier == 20) {
            // For default JVM, scheduling currently fails due to StackOverflow.
            // For now just log the results of the test. Change to assert when StackOverflow issue is fixed.
            LOG.info("testScheduleLargeExecutorCount scheduling {} with {}x executor multiplier", scheduleSuccess ? "succeeds" : "fails",
                    parallelismMultiplier);
        }
    }

    @Test
    public void testIntegrationWithRAS() {
        List<List<String>> constraints = new LinkedList<>();
        addContraints("spout-0", "bolt-0", constraints);
        addContraints("bolt-1", "bolt-1", constraints);
        addContraints("bolt-1", "bolt-2", constraints);
        List<String> spread = new LinkedList<>();
        spread.add("spout-0");

        Map<String, Object> config = Utils.readDefaultConfig();
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, ConstraintSolverStrategy.class.getName());
        config.put(Config.TOPOLOGY_SPREAD_COMPONENTS, spread);
        config.put(Config.TOPOLOGY_RAS_CONSTRAINTS, constraints);
        config.put(Config.TOPOLOGY_RAS_CONSTRAINT_MAX_STATE_SEARCH, MAX_TRAVERSAL_DEPTH);
        config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 100_000);
        config.put(Config.TOPOLOGY_PRIORITY, 1);
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 10);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 100);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 0.0);

        TopologyDetails topo = genTopology("testTopo", config, 2, 3, 30, 300, 0, 0, "user");
        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put(topo.getId(), topo);
        Topologies topologies = new Topologies(topoMap);
        Map<String, SupervisorDetails> supMap = genSupervisors(30, 16, 400, 1024 * 4);
        Cluster cluster = makeCluster(topologies, supMap);
        ResourceAwareScheduler rs = new ResourceAwareScheduler();
        rs.prepare(config);
        try {
            rs.schedule(topologies, cluster);

            assertStatusSuccess(cluster, topo.getId());
            Assert.assertEquals("topo all executors scheduled?", 0, cluster.getUnassignedExecutors(topo).size());
        } finally {
            rs.cleanup();
        }

        //simulate worker loss
        Map<ExecutorDetails, WorkerSlot> newExecToSlot = new HashMap<>();
        Map<ExecutorDetails, WorkerSlot> execToSlot = cluster.getAssignmentById(topo.getId()).getExecutorToSlot();
        Iterator<Map.Entry<ExecutorDetails, WorkerSlot>> it =execToSlot.entrySet().iterator();
        for (int i = 0; i<execToSlot.size()/2; i++) {
            ExecutorDetails exec = it.next().getKey();
            WorkerSlot ws = it.next().getValue();
            newExecToSlot.put(exec, ws);
        }
        Map<String, SchedulerAssignment> newAssignments = new HashMap<>();
        newAssignments.put(topo.getId(), new SchedulerAssignmentImpl(topo.getId(), newExecToSlot, null, null));
        cluster.setAssignments(newAssignments, false);
        
        rs.prepare(config);
        try {
            rs.schedule(topologies, cluster);

            assertStatusSuccess(cluster, topo.getId());
            Assert.assertEquals("topo all executors scheduled?", 0, cluster.getUnassignedExecutors(topo).size());
        } finally {
            rs.cleanup();
        }
    }

    public static void addContraints(String comp1, String comp2, List<List<String>> constraints) {
        LinkedList<String> constraintPair = new LinkedList<>();
        constraintPair.add(comp1);
        constraintPair.add(comp2);
        constraints.add(constraintPair);
    }
}
