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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
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
import org.apache.storm.scheduler.resource.strategies.scheduling.sorter.ExecSorterByConstraintSeverity;
import org.apache.storm.scheduler.resource.strategies.scheduling.sorter.IExecSorter;
import org.apache.storm.scheduler.resource.strategies.scheduling.sorter.NodeSorterHostProximity;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.json.simple.JSONValue;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
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

@RunWith(Parameterized.class)
public class TestConstraintSolverStrategy {
    @Parameters
    public static Object[] data() {
        return new Object[] { false, true };
    }

    private static final Logger LOG = LoggerFactory.getLogger(TestConstraintSolverStrategy.class);
    private static final int MAX_TRAVERSAL_DEPTH = 2000;
    private static final int NORMAL_BOLT_PARALLEL = 11;
    //Dropping the parallelism of the bolts to 3 instead of 11 so we can find a solution in a reasonable amount of work when backtracking.
    private static final int BACKTRACK_BOLT_PARALLEL = 3;
    private static final int CO_LOCATION_CNT = 2;

    // class members
    public Boolean consolidatedConfigFlag = Boolean.TRUE;

    public TestConstraintSolverStrategy(boolean consolidatedConfigFlag) {
        this.consolidatedConfigFlag = consolidatedConfigFlag;
        List<Class> classesToDebug = Arrays.asList(TestConstraintSolverStrategy.class,
                BaseResourceAwareStrategy.class, ResourceAwareScheduler.class,
                NodeSorterHostProximity.class, Cluster.class
        );
        Level logLevel = Level.INFO ; // switch to Level.DEBUG for verbose otherwise Level.INFO
        classesToDebug.forEach(x -> Configurator.setLevel(x.getName(), logLevel));
        LOG.info("Running tests with consolidatedConfigFlag={}", consolidatedConfigFlag);
    }

    /**
     * Helper function to add a constraint specifying two components that cannot co-exist.
     * Note that it is redundant to specify the converse.
     *
     * @param comp1 first component name
     * @param comp2 second component name
     * @param constraints the resulting constraint list of lists which is updated
     */
    public static void addConstraints(String comp1, String comp2, List<List<String>> constraints) {
        LinkedList<String> constraintPair = new LinkedList<>();
        constraintPair.add(comp1);
        constraintPair.add(comp2);
        constraints.add(constraintPair);
    }

    /**
     * Make test Topology configuration, but with the newer spread constraints that allow associating a number
     * with the spread. This number represents the maximum co-located component count. Default under the old
     * configuration is assumed to be 1.
     *
     * @param maxCoLocationCnt Maximum co-located component (spout-0), minimum value is 1.
     * @return topology configuration map
     */
    public Map<String, Object> makeTestTopoConf(int maxCoLocationCnt) {
        if (maxCoLocationCnt < 1) {
            maxCoLocationCnt = 1;
        }
        List<List<String>> constraints = new LinkedList<>();
        addConstraints("spout-0", "bolt-0", constraints);
        addConstraints("bolt-2", "spout-0", constraints);
        addConstraints("bolt-1", "bolt-2", constraints);
        addConstraints("bolt-1", "bolt-0", constraints);
        addConstraints("bolt-1", "spout-0", constraints);

        Map<String, Integer> spreads = new HashMap<>();
        spreads.put("spout-0", maxCoLocationCnt);

        Map<String, Object> config = Utils.readDefaultConfig();

        setConstraintConfig(constraints, spreads, config);

        config.put(DaemonConfig.RESOURCE_AWARE_SCHEDULER_MAX_STATE_SEARCH, MAX_TRAVERSAL_DEPTH);
        config.put(Config.TOPOLOGY_RAS_CONSTRAINT_MAX_STATE_SEARCH, MAX_TRAVERSAL_DEPTH);
        config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 100_000);
        config.put(Config.TOPOLOGY_PRIORITY, 1);
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 10);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 100);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 0.0);

        return config;
    }

    /**
     * Set Config.TOPOLOGY_RAS_CONSTRAINTS (when consolidatedConfigFlag is true) or both
     * Config.TOPOLOGY_RAS_CONSTRAINTS/Config.TOPOLOGY_SPREAD_COMPONENTS (when consolidatedConfigFlag is false).
     *
     * When consolidatedConfigFlag is true, use the new more consolidated format to set Config.TOPOLOGY_RAS_CONSTRAINTS.
     * When false, use the old configuration format for Config.TOPOLOGY_RAS_CONSTRAINTS/TOPOLOGY_SPREAD_COMPONENTS.
     *
     * @param constraints List of components, where the first one cannot co-exist with the others in the list
     * @param spreads Map of component and its maxCoLocationCnt
     * @param config Configuration to be updated
     */
    private void setConstraintConfig(List<List<String>> constraints, Map<String, Integer> spreads, Map<String, Object> config) {
        if (consolidatedConfigFlag) {
            // single configuration for each component
            Map<String, Map<String,Object>> modifiedConstraints = new HashMap<>();
            for (List<String> constraint: constraints) {
                if (constraint.size() < 2) {
                    continue;
                }
                String comp = constraint.get(0);
                List<String> others = constraint.subList(1, constraint.size());
                List<Object> incompatibleComponents = (List<Object>) modifiedConstraints.computeIfAbsent(comp, k -> new HashMap<>())
                        .computeIfAbsent(ConstraintSolverConfig.CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS, k -> new ArrayList<>());
                incompatibleComponents.addAll(others);
            }
            for (String comp: spreads.keySet()) {
                modifiedConstraints.computeIfAbsent(comp, k -> new HashMap<>()).put(ConstraintSolverConfig.CONSTRAINT_TYPE_MAX_NODE_CO_LOCATION_CNT, "" + spreads.get(comp));
            }
            config.put(Config.TOPOLOGY_RAS_CONSTRAINTS, modifiedConstraints);
        } else {
            // constraint and MaxCoLocationCnts are separate - no maxCoLocationCnt implied as 1
            config.put(Config.TOPOLOGY_RAS_CONSTRAINTS, constraints);
            for (Map.Entry<String, Integer> e: spreads.entrySet()) {
                if (e.getValue() > 1) {
                    Assert.fail(String.format("Invalid %s=%d for component=%s, expecting 1 for old-style configuration",
                            ConstraintSolverConfig.CONSTRAINT_TYPE_MAX_NODE_CO_LOCATION_CNT,
                            e.getValue(),
                            e.getKey()));
                }
            }
            config.put(Config.TOPOLOGY_SPREAD_COMPONENTS, new ArrayList(spreads.keySet()));
        }
    }

    public Map<String, Object> makeTestTopoConf() {
        return makeTestTopoConf(1);
    }

    public static TopologyDetails makeTopology(Map<String, Object> config, int boltParallel) {
        return genTopology("testTopo", config, 1, 4, 4, boltParallel, 0, 0, "user");
    }

    public static Cluster makeCluster(Topologies topologies) {
        return makeCluster(topologies, null);
    }

    public static Cluster makeCluster(Topologies topologies, Map<String, SupervisorDetails> supMap) {
        if (supMap == null) {
            supMap = genSupervisors(4, 2, 120, 1200);
        }
        Map<String, Object> config = Utils.readDefaultConfig();
        return new Cluster(new INimbusTest(), new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);
    }

    public void basicUnitTestWithKillAndRecover(ConstraintSolverStrategy cs, int boltParallel, int coLocationCnt) {
        Map<String, Object> config = makeTestTopoConf(coLocationCnt);
        cs.prepare(config);

        TopologyDetails topo = makeTopology(config, boltParallel);
        Topologies topologies = new Topologies(topo);
        Cluster cluster = makeCluster(topologies);

        LOG.info("Scheduling...");
        SchedulingResult result = cs.schedule(cluster, topo);
        LOG.info("Done scheduling {}...", result);

        Assert.assertTrue("Assert scheduling topology success " + result, result.isSuccess());
        Assert.assertEquals("Assert no unassigned executors, found unassigned: " + cluster.getUnassignedExecutors(topo),
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

    /**
     * See if constraint configuration can be instantiated with no or partial constraints.
     */
    @Test
    public void testMissingConfig() {
        // no configs
        new ConstraintSolverConfig("test-topoid-1", new HashMap<>(), new HashSet<>());

        // with one or more undefined components with partial constraints
        {
            String s = consolidatedConfigFlag ?
                    String.format(
                            "{ \"comp-1\": "
                                    + "                  { \"%s\": 2, "
                                    + "                    \"%s\": [\"comp-2\", \"comp-3\" ] }, "
                                    + "     \"comp-2\": "
                                    + "                  { \"%s\": [ \"comp-4\" ] }, "
                                    + "     \"comp-3\": "
                                    + "                  { \"%s\": \"comp-5\" }, "
                                    + "     \"comp-6\": "
                                    + "                  { \"%s\": 2 }"
                                    + "}",
                            ConstraintSolverConfig.CONSTRAINT_TYPE_MAX_NODE_CO_LOCATION_CNT,
                            ConstraintSolverConfig.CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS,
                            ConstraintSolverConfig.CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS,
                            ConstraintSolverConfig.CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS,
                            ConstraintSolverConfig.CONSTRAINT_TYPE_MAX_NODE_CO_LOCATION_CNT
                    )
                    :
                    "[ "
                            + "[ \"comp-1\", \"comp-2\" ], "
                            + "[ \"comp-1\", \"comp-3\" ], "
                            + "[ \"comp-2\", \"comp-3\" ], "
                            + "[ \"comp-2\", \"comp-4\" ], "
                            + "[ \"comp-3\", \"comp-5\" ] "
                            + "]"
                    ;

            Object jsonValue = JSONValue.parse(s);
            Map<String, Object> conf = new HashMap<>();
            conf.put(Config.TOPOLOGY_RAS_CONSTRAINTS, jsonValue);
            new ConstraintSolverConfig("test-topoid-2", conf, new HashSet<>());
            new ConstraintSolverConfig("test-topoid-3", conf, new HashSet<>(Arrays.asList("comp-x")));
            new ConstraintSolverConfig("test-topoid-4", conf, new HashSet<>(Arrays.asList("comp-1")));
            new ConstraintSolverConfig("test-topoid-5", conf, new HashSet<>(Arrays.asList("comp-1, comp-x")));
        }
    }

    @Test
    public void testNewConstraintFormat() {
        String s = String.format(
                "{ \"comp-1\": "
                        + "                  { \"%s\": 2, "
                        + "                    \"%s\": [\"comp-2\", \"comp-3\" ] }, "
                        + "     \"comp-2\": "
                        + "                  { \"%s\": [ \"comp-4\" ] }, "
                        + "     \"comp-3\": "
                        + "                  { \"%s\": \"comp-5\" } "
                        + "}",
                ConstraintSolverConfig.CONSTRAINT_TYPE_MAX_NODE_CO_LOCATION_CNT,
                ConstraintSolverConfig.CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS,
                ConstraintSolverConfig.CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS,
                ConstraintSolverConfig.CONSTRAINT_TYPE_INCOMPATIBLE_COMPONENTS
        );
        Object jsonValue = JSONValue.parse(s);
        Map<String, Object> config = Utils.readDefaultConfig();
        config.put(Config.TOPOLOGY_RAS_CONSTRAINTS, jsonValue);
        Set<String> allComps = new HashSet<>();
        allComps.addAll(Arrays.asList("comp-1", "comp-2", "comp-3", "comp-4", "comp-5"));
        ConstraintSolverConfig constraintSolverConfig = new ConstraintSolverConfig("test-topoid-1", config, allComps);

        Set<String> expectedSetComp1 = new HashSet<>();
        expectedSetComp1.addAll(Arrays.asList("comp-2", "comp-3"));
        Set<String> expectedSetComp2 = new HashSet<>();
        expectedSetComp2.addAll(Arrays.asList("comp-1", "comp-4"));
        Set<String> expectedSetComp3 = new HashSet<>();
        expectedSetComp3.addAll(Arrays.asList("comp-1", "comp-5"));
        Assert.assertEquals("comp-1 incompatible components", expectedSetComp1, constraintSolverConfig.getIncompatibleComponentSets().get("comp-1"));
        Assert.assertEquals("comp-2 incompatible components", expectedSetComp2, constraintSolverConfig.getIncompatibleComponentSets().get("comp-2"));
        Assert.assertEquals("comp-3 incompatible components", expectedSetComp3, constraintSolverConfig.getIncompatibleComponentSets().get("comp-3"));
        Assert.assertEquals("comp-1 maxNodeCoLocationCnt", 2, (int) constraintSolverConfig.getMaxNodeCoLocationCnts().getOrDefault("comp-1", -1));
        Assert.assertNull("comp-2 maxNodeCoLocationCnt", constraintSolverConfig.getMaxNodeCoLocationCnts().get("comp-2"));
    }

    @Test
    public void testConstraintSolverForceBacktrackWithSpreadCoLocation() {
        //The best way to force backtracking is to change the heuristic so the components are reversed, so it is hard
        // to find an answer.
        if (CO_LOCATION_CNT > 1 && !consolidatedConfigFlag) {
            LOG.info("INFO: Skipping Test {} with {}={} (required 1), and consolidatedConfigFlag={} (required false)",
                    "testConstraintSolverForceBacktrackWithSpreadCoLocation",
                    ConstraintSolverConfig.CONSTRAINT_TYPE_MAX_NODE_CO_LOCATION_CNT,
                    CO_LOCATION_CNT,
                    consolidatedConfigFlag);
            return;
        }

        ConstraintSolverStrategy cs = new ConstraintSolverStrategy() {
            protected void prepareForScheduling(Cluster cluster, TopologyDetails topologyDetails) {
                super.prepareForScheduling(cluster, topologyDetails);

                // set a reversing execSorter instance
                IExecSorter execSorter = new ExecSorterByConstraintSeverity(cluster, topologyDetails) {
                    @Override
                    public List<ExecutorDetails> sortExecutors(Set<ExecutorDetails> unassignedExecutors) {
                        List<ExecutorDetails> tmp = super.sortExecutors(unassignedExecutors);
                        List<ExecutorDetails> reversed = new ArrayList<>();
                        while (!tmp.isEmpty()) {
                            reversed.add(0, tmp.remove(0));
                        }
                        return reversed;
                    }
                };
                setExecSorter(execSorter);
            }
        };
        basicUnitTestWithKillAndRecover(cs, BACKTRACK_BOLT_PARALLEL, CO_LOCATION_CNT);
    }

    @Test
    public void testConstraintSolver() {
        basicUnitTestWithKillAndRecover(new ConstraintSolverStrategy(), NORMAL_BOLT_PARALLEL, 1);
    }

    @Test
    public void testConstraintSolverWithSpreadCoLocation() {
        if (CO_LOCATION_CNT > 1 && !consolidatedConfigFlag) {
            LOG.info("INFO: Skipping Test {} with {}={} (required 1), and consolidatedConfigFlag={} (required false)",
                    "testConstraintSolverWithSpreadCoLocation",
                    ConstraintSolverConfig.CONSTRAINT_TYPE_MAX_NODE_CO_LOCATION_CNT,
                    CO_LOCATION_CNT,
                    consolidatedConfigFlag);
            return;
        }

        basicUnitTestWithKillAndRecover(new ConstraintSolverStrategy(), NORMAL_BOLT_PARALLEL, CO_LOCATION_CNT);
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
                protected SchedulingResult scheduleExecutorsOnNodes(List<ExecutorDetails> orderedExecutors, Iterable<String> sortedNodes) {
                    //Each time we try to schedule a new component simulate taking 1 second longer
                    Time.advanceTime(1_001);
                    return super.scheduleExecutorsOnNodes(orderedExecutors, sortedNodes);
                }
            };
            basicFailureTest(Config.TOPOLOGY_RAS_CONSTRAINT_MAX_TIME_SECS, 1, cs);
        }
    }

    @Test
    public void testScheduleLargeExecutorConstraintCountSmall() {
        testScheduleLargeExecutorConstraintCount(1);
    }

    /*
     * Test scheduling large number of executors and constraints.
     * This test can succeed only with new style config that allows maxCoLocationCnt = parallelismMultiplier.
     * In prior code, this test would succeed because effectively the old code did not properly enforce the
     * SPREAD constraint.
     *
     * Cluster has sufficient resources for scheduling to succeed but can fail due to StackOverflowError.
     */
    @Test
    public void testScheduleLargeExecutorConstraintCountLarge() {
        testScheduleLargeExecutorConstraintCount(20);
    }

    private void testScheduleLargeExecutorConstraintCount(int parallelismMultiplier) {
        if (parallelismMultiplier > 1 && !consolidatedConfigFlag) {
            Assert.assertFalse("Large parallelism test requires new consolidated constraint format with maxCoLocationCnt=" + parallelismMultiplier, consolidatedConfigFlag);
            return;
        }

        // Add 1 topology with large number of executors and constraints. Too many can cause a java.lang.StackOverflowError
        Config config = createCSSClusterConfig(10, 10, 0, null);
        config.put(Config.TOPOLOGY_RAS_CONSTRAINT_MAX_STATE_SEARCH, 50000);
        config.put(Config.TOPOLOGY_RAS_CONSTRAINT_MAX_TIME_SECS, 120);
        config.put(DaemonConfig.SCHEDULING_TIMEOUT_SECONDS_PER_TOPOLOGY, 120);

        List<List<String>> constraints = new LinkedList<>();
        addConstraints("spout-0", "spout-0", constraints);
        addConstraints("bolt-1", "bolt-1", constraints);
        addConstraints("spout-0", "bolt-0", constraints);
        addConstraints("bolt-2", "spout-0", constraints);
        addConstraints("bolt-1", "bolt-2", constraints);
        addConstraints("bolt-1", "bolt-0", constraints);
        addConstraints("bolt-1", "spout-0", constraints);

        Map<String, Integer> spreads = new HashMap<>();
        spreads.put("spout-0", parallelismMultiplier);
        spreads.put("bolt-1", parallelismMultiplier);

        setConstraintConfig(constraints, spreads, config);

        TopologyDetails topo = genTopology("testTopo-" + parallelismMultiplier, config, 10, 10, 30 * parallelismMultiplier, 30 * parallelismMultiplier, 31414, 0, "user");
        Topologies topologies = new Topologies(topo);

        Map<String, SupervisorDetails> supMap = genSupervisors(30 * parallelismMultiplier, 30, 3500, 35000);
        Cluster cluster = makeCluster(topologies, supMap);

        ResourceAwareScheduler scheduler = new ResourceAwareScheduler();
        scheduler.prepare(config, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);

        boolean scheduleSuccess = isStatusSuccess(cluster.getStatus(topo.getId()));
        LOG.info("testScheduleLargeExecutorCount scheduling {} with {}x executor multiplier, consolidatedConfigFlag={}",
                scheduleSuccess ? "succeeds" : "fails", parallelismMultiplier, consolidatedConfigFlag);
        Assert.assertTrue(scheduleSuccess);
    }

    @Test
    public void testIntegrationWithRAS() {
        if (!consolidatedConfigFlag) {
            LOG.info("Skipping test since bolt-1 maxCoLocationCnt=10 requires consolidatedConfigFlag=true, current={}", consolidatedConfigFlag);
            return;
        }

        Map<String, Object> config = Utils.readDefaultConfig();
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, ConstraintSolverStrategy.class.getName());
        config.put(Config.TOPOLOGY_RAS_CONSTRAINT_MAX_STATE_SEARCH, MAX_TRAVERSAL_DEPTH);
        config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 100_000);
        config.put(Config.TOPOLOGY_PRIORITY, 1);
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 10);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 100);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 0.0);

        List<List<String>> constraints = new LinkedList<>();
        addConstraints("spout-0", "bolt-0", constraints);
        addConstraints("bolt-1", "bolt-1", constraints);
        addConstraints("bolt-1", "bolt-2", constraints);

        Map<String, Integer> spreads = new HashMap<String, Integer>();
        spreads.put("spout-0", 1);
        spreads.put("bolt-1", 10);

        setConstraintConfig(constraints, spreads, config);

        TopologyDetails topo = genTopology("testTopo", config, 2, 3, 30, 300, 0, 0, "user");
        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put(topo.getId(), topo);
        Topologies topologies = new Topologies(topoMap);
        // Fails with 36 supervisors, works with 37
        Map<String, SupervisorDetails> supMap = genSupervisors(37, 16, 400, 1024 * 4);
        Cluster cluster = makeCluster(topologies, supMap);
        ResourceAwareScheduler rs = new ResourceAwareScheduler();
        rs.prepare(config, new StormMetricsRegistry());
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

        rs.prepare(config, new StormMetricsRegistry());
        try {
            rs.schedule(topologies, cluster);
            assertStatusSuccess(cluster, topo.getId());
            Assert.assertEquals("topo all executors scheduled?", 0, cluster.getUnassignedExecutors(topo).size());
        } finally {
            rs.cleanup();
        }
    }

    @Test
    public void testZeroExecutorScheduling() {
        ConstraintSolverStrategy cs = new ConstraintSolverStrategy();
        cs.prepare(new HashMap<>());
        Map<String, Object> topoConf = Utils.readDefaultConfig();
        topoConf.put(Config.TOPOLOGY_RAS_CONSTRAINT_MAX_STATE_SEARCH, 1_000);
        topoConf.put(Config.TOPOLOGY_RAS_ONE_EXECUTOR_PER_WORKER, false);
        topoConf.put(Config.TOPOLOGY_RAS_ONE_COMPONENT_PER_WORKER, false);

        TopologyDetails topo = makeTopology(topoConf, 1);
        Cluster cluster = makeCluster(new Topologies(topo));
        cs.schedule(cluster, topo);
        LOG.info("********************* Scheduling Zero Unassigned Executors *********************");
        cs.schedule(cluster, topo); // reschedule a fully schedule topology
        LOG.info("********************* End of Scheduling Zero Unassigned Executors *********************");
    }

    @Test
    public void testGetMaxStateSearchFromTopoConf() {
        Map<String, Object> topoConf = new HashMap<>();

        Assert.assertEquals(10_000, ConstraintSolverStrategy.getMaxStateSearchFromTopoConf(topoConf));

        topoConf.put(Config.TOPOLOGY_RAS_CONSTRAINT_MAX_STATE_SEARCH, 40_000);
        Assert.assertEquals(40_000, ConstraintSolverStrategy.getMaxStateSearchFromTopoConf(topoConf));
    }
}
