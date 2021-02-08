/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.scheduler.resource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SchedulerAssignmentImpl;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.normalization.NormalizedResources;
import org.apache.storm.scheduler.resource.strategies.scheduling.BaseResourceAwareStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.ConstraintSolverStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategyOld;
import org.apache.storm.scheduler.resource.strategies.scheduling.GenericResourceAwareStrategy;
import org.apache.storm.testing.PerformanceTest;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.DisallowedStrategyException;
import org.apache.storm.utils.ReflectionUtils;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.apache.storm.validation.ConfigValidation;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.*;
import static org.junit.Assert.*;

import java.time.Duration;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.resource.normalization.ResourceMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestResourceAwareScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(TestResourceAwareScheduler.class);
    private final Config defaultTopologyConf;
    private int currentTime = 1450418597;
    private IScheduler scheduler = null;

    public TestResourceAwareScheduler() {
        defaultTopologyConf = createClusterConfig(10, 128, 0, null);
        defaultTopologyConf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 8192.0);
        defaultTopologyConf.put(Config.TOPOLOGY_PRIORITY, 0);
    }

    protected Class getDefaultResourceAwareStrategyClass() {
        return DefaultResourceAwareStrategy.class;
    }

    protected Class getGenericResourceAwareStrategyClass() {
        return GenericResourceAwareStrategy.class;
    }

    private Config createGrasClusterConfig(double compPcore, double compOnHeap, double compOffHeap,
                                           Map<String, Map<String, Number>> pools, Map<String, Double> genericResourceMap) {
        Config config = TestUtilsForResourceAwareScheduler.createGrasClusterConfig(compPcore, compOnHeap, compOffHeap, pools, genericResourceMap);
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, getGenericResourceAwareStrategyClass().getName());
        return config;
    }

    private Config createClusterConfig(double compPcore, double compOnHeap, double compOffHeap,
                                       Map<String, Map<String, Number>> pools) {
        Config config = TestUtilsForResourceAwareScheduler.createClusterConfig(compPcore, compOnHeap, compOffHeap, pools);
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, getDefaultResourceAwareStrategyClass().getName());
        return config;
    }

    @After
    public void cleanup() {
        if (scheduler != null) {
            scheduler.cleanup();
            scheduler = null;
        }
    }

    @Test
    public void testRASNodeSlotAssign() {
        Config config = new Config();
        config.putAll(defaultTopologyConf);

        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(5, 4, 400, 2000);
        TopologyDetails topology1 = genTopology("topology1", config, 1, 0, 2, 0, 0, 0, "user");
        TopologyDetails topology2 = genTopology("topology2", config, 1, 0, 2, 0, 0, 0, "user");
        Topologies topologies = new Topologies(topology1, topology2);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);
        Map<String, RasNode> nodes = RasNodes.getAllNodesFrom(cluster);
        assertEquals(5, nodes.size());
        RasNode node = nodes.get("r000s000");

        assertEquals("r000s000", node.getId());
        assertTrue(node.isAlive());
        assertEquals(0, node.getRunningTopologies().size());
        assertTrue(node.isTotallyFree());
        assertEquals(4, node.totalSlotsFree());
        assertEquals(0, node.totalSlotsUsed());
        assertEquals(4, node.totalSlots());

        List<ExecutorDetails> executors11 = new ArrayList<>();
        executors11.add(new ExecutorDetails(1, 1));
        node.assign(node.getFreeSlots().iterator().next(), topology1, executors11);
        assertEquals(1, node.getRunningTopologies().size());
        assertFalse(node.isTotallyFree());
        assertEquals(3, node.totalSlotsFree());
        assertEquals(1, node.totalSlotsUsed());
        assertEquals(4, node.totalSlots());

        List<ExecutorDetails> executors12 = new ArrayList<>();
        executors12.add(new ExecutorDetails(2, 2));
        node.assign(node.getFreeSlots().iterator().next(), topology1, executors12);
        assertEquals(1, node.getRunningTopologies().size());
        assertFalse(node.isTotallyFree());
        assertEquals(2, node.totalSlotsFree());
        assertEquals(2, node.totalSlotsUsed());
        assertEquals(4, node.totalSlots());

        List<ExecutorDetails> executors21 = new ArrayList<>();
        executors21.add(new ExecutorDetails(1, 1));
        node.assign(node.getFreeSlots().iterator().next(), topology2, executors21);
        assertEquals(2, node.getRunningTopologies().size());
        assertFalse(node.isTotallyFree());
        assertEquals(1, node.totalSlotsFree());
        assertEquals(3, node.totalSlotsUsed());
        assertEquals(4, node.totalSlots());

        List<ExecutorDetails> executors22 = new ArrayList<>();
        executors22.add(new ExecutorDetails(2, 2));
        node.assign(node.getFreeSlots().iterator().next(), topology2, executors22);
        assertEquals(2, node.getRunningTopologies().size());
        assertFalse(node.isTotallyFree());
        assertEquals(0, node.totalSlotsFree());
        assertEquals(4, node.totalSlotsUsed());
        assertEquals(4, node.totalSlots());

        node.freeAllSlots();
        assertEquals(0, node.getRunningTopologies().size());
        assertTrue(node.isTotallyFree());
        assertEquals(4, node.totalSlotsFree());
        assertEquals(0, node.totalSlotsUsed());
        assertEquals(4, node.totalSlots());
    }

    @Test
    public void sanityTestOfScheduling() {
        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(1, 2, 400, 2000);

        Config config = new Config();
        config.putAll(defaultTopologyConf);

        scheduler = new ResourceAwareScheduler();

        TopologyDetails topology1 = genTopology("topology1", config, 1, 1, 1, 1, 0, 0, "user");

        Topologies topologies = new Topologies(topology1);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);

        scheduler.prepare(config, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);

        SchedulerAssignment assignment = cluster.getAssignmentById(topology1.getId());
        Set<WorkerSlot> assignedSlots = assignment.getSlots();
        Set<String> nodesIDs = new HashSet<>();
        for (WorkerSlot slot : assignedSlots) {
            nodesIDs.add(slot.getNodeId());
        }
        Collection<ExecutorDetails> executors = assignment.getExecutors();

        assertEquals(1, assignedSlots.size());
        assertEquals(1, nodesIDs.size());
        assertEquals(2, executors.size());
        assertFalse(cluster.needsSchedulingRas(topology1));
        assertTrue(cluster.getStatusMap().get(topology1.getId()).startsWith("Running - Fully Scheduled by DefaultResourceAwareStrategy"));
    }

    @Test
    public void testTopologyWithMultipleSpouts() {
        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(2, 4, 400, 2000);

        TopologyBuilder builder1 = new TopologyBuilder(); // a topology with multiple spouts
        builder1.setSpout("wordSpout1", new TestWordSpout(), 1);
        builder1.setSpout("wordSpout2", new TestWordSpout(), 1);
        builder1.setBolt("wordCountBolt1", new TestWordCounter(), 1).shuffleGrouping("wordSpout1").shuffleGrouping("wordSpout2");
        builder1.setBolt("wordCountBolt2", new TestWordCounter(), 1).shuffleGrouping("wordCountBolt1");
        builder1.setBolt("wordCountBolt3", new TestWordCounter(), 1).shuffleGrouping("wordCountBolt1");
        builder1.setBolt("wordCountBolt4", new TestWordCounter(), 1).shuffleGrouping("wordCountBolt2");
        builder1.setBolt("wordCountBolt5", new TestWordCounter(), 1).shuffleGrouping("wordSpout2");
        StormTopology stormTopology1 = builder1.createTopology();

        Config config = new Config();
        config.putAll(defaultTopologyConf);
        Map<ExecutorDetails, String> executorMap1 = genExecsAndComps(stormTopology1);
        TopologyDetails topology1 = new TopologyDetails("topology1", config, stormTopology1, 0, executorMap1, 0, "user");

        TopologyBuilder builder2 = new TopologyBuilder(); // a topology with two unconnected partitions
        builder2.setSpout("wordSpoutX", new TestWordSpout(), 1);
        builder2.setSpout("wordSpoutY", new TestWordSpout(), 1);
        StormTopology stormTopology2 = builder2.createTopology();
        Map<ExecutorDetails, String> executorMap2 = genExecsAndComps(stormTopology2);
        TopologyDetails topology2 = new TopologyDetails("topology2", config, stormTopology2, 0, executorMap2, 0, "user");

        scheduler = new ResourceAwareScheduler();

        Topologies topologies = new Topologies(topology1, topology2);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);

        scheduler.prepare(config, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);

        SchedulerAssignment assignment1 = cluster.getAssignmentById(topology1.getId());
        Set<WorkerSlot> assignedSlots1 = assignment1.getSlots();
        Set<String> nodesIDs1 = new HashSet<>();
        for (WorkerSlot slot : assignedSlots1) {
            nodesIDs1.add(slot.getNodeId());
        }
        Collection<ExecutorDetails> executors1 = assignment1.getExecutors();

        assertEquals(1, assignedSlots1.size());
        assertEquals(1, nodesIDs1.size());
        assertEquals(7, executors1.size());
        assertFalse(cluster.needsSchedulingRas(topology1));
        assertTrue(cluster.getStatusMap().get(topology1.getId()).startsWith("Running - Fully Scheduled by DefaultResourceAwareStrategy"));

        SchedulerAssignment assignment2 = cluster.getAssignmentById(topology2.getId());
        Set<WorkerSlot> assignedSlots2 = assignment2.getSlots();
        Set<String> nodesIDs2 = new HashSet<>();
        for (WorkerSlot slot : assignedSlots2) {
            nodesIDs2.add(slot.getNodeId());
        }
        Collection<ExecutorDetails> executors2 = assignment2.getExecutors();

        assertEquals(1, assignedSlots2.size());
        assertEquals(1, nodesIDs2.size());
        assertEquals(2, executors2.size());
        assertFalse(cluster.needsSchedulingRas(topology2));
        assertTrue(cluster.getStatusMap().get(topology2.getId()).startsWith("Running - Fully Scheduled by DefaultResourceAwareStrategy"));
    }

    @Test
    public void testTopologySetCpuAndMemLoad() {
        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(2, 2, 400, 2000);

        TopologyBuilder builder1 = new TopologyBuilder(); // a topology with multiple spouts
        builder1.setSpout("wordSpout", new TestWordSpout(), 1).setCPULoad(20.0).setMemoryLoad(200.0);
        builder1.setBolt("wordCountBolt", new TestWordCounter(), 1).shuffleGrouping("wordSpout").setCPULoad(20.0).setMemoryLoad(200.0);
        StormTopology stormTopology1 = builder1.createTopology();

        Config config = new Config();
        config.putAll(defaultTopologyConf);
        Map<ExecutorDetails, String> executorMap1 = genExecsAndComps(stormTopology1);
        TopologyDetails topology1 = new TopologyDetails("topology1", config, stormTopology1, 0, executorMap1, 0, "user");

        ResourceAwareScheduler rs = new ResourceAwareScheduler();
        scheduler = rs;
        Topologies topologies = new Topologies(topology1);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);

        rs.prepare(config, new StormMetricsRegistry());
        rs.schedule(topologies, cluster);

        SchedulerAssignment assignment1 = cluster.getAssignmentById(topology1.getId());
        Map<WorkerSlot, WorkerResources> assignedSlots1 = assignment1.getScheduledResources();
        double assignedMemory = 0.0;
        double assignedCpu = 0.0;
        Set<String> nodesIDs1 = new HashSet<>();
        for (Entry<WorkerSlot, WorkerResources> entry : assignedSlots1.entrySet()) {
            WorkerResources wr = entry.getValue();
            nodesIDs1.add(entry.getKey().getNodeId());
            assignedMemory += wr.get_mem_on_heap() + wr.get_mem_off_heap();
            assignedCpu += wr.get_cpu();
        }
        Collection<ExecutorDetails> executors1 = assignment1.getExecutors();

        assertEquals(1, assignedSlots1.size());
        assertEquals(1, nodesIDs1.size());
        assertEquals(2, executors1.size());
        assertEquals(400.0, assignedMemory, 0.001);
        assertEquals(40.0, assignedCpu, 0.001);
        assertFalse(cluster.needsSchedulingRas(topology1));
        String expectedStatusPrefix = "Running - Fully Scheduled by DefaultResourceAwareStrategy";
        assertTrue(cluster.getStatusMap().get(topology1.getId()).startsWith(expectedStatusPrefix));
    }

    @Test
    public void testResourceLimitation() {
        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(2, 2, 400, 2000);

        TopologyBuilder builder1 = new TopologyBuilder(); // a topology with multiple spouts
        builder1.setSpout("wordSpout", new TestWordSpout(), 2).setCPULoad(250.0).setMemoryLoad(1000.0, 200.0);
        builder1.setBolt("wordCountBolt", new TestWordCounter(), 1).shuffleGrouping("wordSpout").setCPULoad(100.0)
                .setMemoryLoad(500.0, 100.0);
        StormTopology stormTopology1 = builder1.createTopology();

        Config config = new Config();
        config.putAll(defaultTopologyConf);
        Map<ExecutorDetails, String> executorMap1 = genExecsAndComps(stormTopology1);
        TopologyDetails topology1 = new TopologyDetails("topology1", config, stormTopology1, 2, executorMap1, 0, "user");

        ResourceAwareScheduler rs = new ResourceAwareScheduler();
        scheduler = rs;
        Topologies topologies = new Topologies(topology1);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);

        rs.prepare(config, new StormMetricsRegistry());
        rs.schedule(topologies, cluster);

        SchedulerAssignment assignment1 = cluster.getAssignmentById(topology1.getId());
        Set<WorkerSlot> assignedSlots1 = assignment1.getSlots();
        Set<String> nodesIDs1 = new HashSet<>();
        for (WorkerSlot slot : assignedSlots1) {
            nodesIDs1.add(slot.getNodeId());
        }
        Collection<ExecutorDetails> executors1 = assignment1.getExecutors();
        List<Double> assignedExecutorMemory = new ArrayList<>();
        List<Double> assignedExecutorCpu = new ArrayList<>();
        for (ExecutorDetails executor : executors1) {
            assignedExecutorMemory.add(topology1.getTotalMemReqTask(executor));
            assignedExecutorCpu.add(topology1.getTotalCpuReqTask(executor));
        }
        Collections.sort(assignedExecutorCpu);
        Collections.sort(assignedExecutorMemory);

        Map<ExecutorDetails, SupervisorDetails> executorToSupervisor = new HashMap<>();
        Map<SupervisorDetails, List<ExecutorDetails>> supervisorToExecutors = new HashMap<>();
        Map<Double, Double> cpuAvailableToUsed = new HashMap<>();
        Map<Double, Double> memoryAvailableToUsed = new HashMap<>();

        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : assignment1.getExecutorToSlot().entrySet()) {
            executorToSupervisor.put(entry.getKey(), cluster.getSupervisorById(entry.getValue().getNodeId()));
        }
        for (Map.Entry<ExecutorDetails, SupervisorDetails> entry : executorToSupervisor.entrySet()) {
            supervisorToExecutors
                    .computeIfAbsent(entry.getValue(), k -> new ArrayList<>())
                    .add(entry.getKey());
        }
        for (Map.Entry<SupervisorDetails, List<ExecutorDetails>> entry : supervisorToExecutors.entrySet()) {
            Double supervisorTotalCpu = entry.getKey().getTotalCpu();
            Double supervisorTotalMemory = entry.getKey().getTotalMemory();
            Double supervisorUsedCpu = 0.0;
            Double supervisorUsedMemory = 0.0;
            for (ExecutorDetails executor : entry.getValue()) {
                supervisorUsedMemory += topology1.getTotalCpuReqTask(executor);
                supervisorTotalCpu += topology1.getTotalMemReqTask(executor);
            }
            cpuAvailableToUsed.put(supervisorTotalCpu, supervisorUsedCpu);
            memoryAvailableToUsed.put(supervisorTotalMemory, supervisorUsedMemory);
        }
        // executor0 resides one one worker (on one), executor1 and executor2 on another worker (on the other node)
        assertEquals(2, assignedSlots1.size());
        assertEquals(2, nodesIDs1.size());
        assertEquals(3, executors1.size());

        assertEquals(100.0, assignedExecutorCpu.get(0), 0.001);
        assertEquals(250.0, assignedExecutorCpu.get(1), 0.001);
        assertEquals(250.0, assignedExecutorCpu.get(2), 0.001);
        assertEquals(600.0, assignedExecutorMemory.get(0), 0.001);
        assertEquals(1200.0, assignedExecutorMemory.get(1), 0.001);
        assertEquals(1200.0, assignedExecutorMemory.get(2), 0.001);

        for (Map.Entry<Double, Double> entry : memoryAvailableToUsed.entrySet()) {
            assertTrue(entry.getKey() - entry.getValue() >= 0);
        }
        for (Map.Entry<Double, Double> entry : cpuAvailableToUsed.entrySet()) {
            assertTrue(entry.getKey() - entry.getValue() >= 0);
        }
        assertFalse(cluster.needsSchedulingRas(topology1));
        assertTrue(cluster.getStatusMap().get(topology1.getId()).startsWith("Running - Fully Scheduled by DefaultResourceAwareStrategy"));
    }

    @Test
    public void testScheduleResilience() {
        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(2, 2, 400, 2000);

        TopologyBuilder builder1 = new TopologyBuilder();
        builder1.setSpout("wordSpout1", new TestWordSpout(), 3);
        StormTopology stormTopology1 = builder1.createTopology();
        Config config1 = new Config();
        config1.putAll(defaultTopologyConf);
        Map<ExecutorDetails, String> executorMap1 = genExecsAndComps(stormTopology1);
        TopologyDetails topology1 = new TopologyDetails("topology1", config1, stormTopology1, 3, executorMap1, 0, "user");

        TopologyBuilder builder2 = new TopologyBuilder();
        builder2.setSpout("wordSpout2", new TestWordSpout(), 2);
        StormTopology stormTopology2 = builder2.createTopology();
        Config config2 = new Config();
        config2.putAll(defaultTopologyConf);
        // memory requirement is large enough so that two executors can not be fully assigned to one node
        config2.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 1280.0);
        Map<ExecutorDetails, String> executorMap2 = genExecsAndComps(stormTopology2);
        TopologyDetails topology2 = new TopologyDetails("topology2", config2, stormTopology2, 2, executorMap2, 0, "user");

        // Test1: When a worker fails, RAS does not alter existing assignments on healthy workers
        scheduler = new ResourceAwareScheduler();
        Topologies topologies = new Topologies(topology2);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config1);

        scheduler.prepare(config1, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);

        SchedulerAssignment assignment = cluster.getAssignmentById(topology2.getId());
        // pick a worker to mock as failed
        WorkerSlot failedWorker = new ArrayList<>(assignment.getSlots()).get(0);
        Map<ExecutorDetails, WorkerSlot> executorToSlot = assignment.getExecutorToSlot();
        List<ExecutorDetails> failedExecutors = new ArrayList<>();
        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : executorToSlot.entrySet()) {
            if (entry.getValue().equals(failedWorker)) {
                failedExecutors.add(entry.getKey());
            }
        }
        for (ExecutorDetails executor : failedExecutors) {
            executorToSlot.remove(executor); // remove executor details assigned to the failed worker
        }
        Map<ExecutorDetails, WorkerSlot> copyOfOldMapping = new HashMap<>(executorToSlot);
        Set<ExecutorDetails> healthyExecutors = copyOfOldMapping.keySet();

        scheduler.schedule(topologies, cluster);
        SchedulerAssignment newAssignment = cluster.getAssignmentById(topology2.getId());
        Map<ExecutorDetails, WorkerSlot> newExecutorToSlot = newAssignment.getExecutorToSlot();

        for (ExecutorDetails executor : healthyExecutors) {
            assertEquals(copyOfOldMapping.get(executor), newExecutorToSlot.get(executor));
        }
        assertFalse(cluster.needsSchedulingRas(topology2));
        assertTrue(cluster.getStatusMap().get(topology2.getId()).startsWith("Running - Fully Scheduled by DefaultResourceAwareStrategy"));
        // end of Test1

        // Test2: When a supervisor fails, RAS does not alter existing assignments
        executorToSlot = new HashMap<>();
        executorToSlot.put(new ExecutorDetails(0, 0), new WorkerSlot("r000s000", 0));
        executorToSlot.put(new ExecutorDetails(1, 1), new WorkerSlot("r000s000", 1));
        executorToSlot.put(new ExecutorDetails(2, 2), new WorkerSlot("r000s001", 1));
        Map<String, SchedulerAssignment> existingAssignments = new HashMap<>();
        assignment = new SchedulerAssignmentImpl(topology1.getId(), executorToSlot, null, null);
        existingAssignments.put(topology1.getId(), assignment);
        copyOfOldMapping = new HashMap<>(executorToSlot);
        Set<ExecutorDetails> existingExecutors = copyOfOldMapping.keySet();
        Map<String, SupervisorDetails> supMap1 = new HashMap<>(supMap);
        supMap1.remove("r000s000"); // mock the supervisor r000s000 as a failed supervisor

        topologies = new Topologies(topology1);
        Cluster cluster1 = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap1, existingAssignments, topologies, config1);
        scheduler.schedule(topologies, cluster1);

        newAssignment = cluster1.getAssignmentById(topology1.getId());
        newExecutorToSlot = newAssignment.getExecutorToSlot();

        for (ExecutorDetails executor : existingExecutors) {
            assertEquals(copyOfOldMapping.get(executor), newExecutorToSlot.get(executor));
        }
        assertEquals("Fully Scheduled", cluster1.getStatusMap().get(topology1.getId()));
        // end of Test2

        // Test3: When a supervisor and a worker on it fails, RAS does not alter existing assignments
        executorToSlot = new HashMap<>();
        executorToSlot.put(new ExecutorDetails(0, 0), new WorkerSlot("r000s000", 1)); // the worker to orphan
        executorToSlot.put(new ExecutorDetails(1, 1), new WorkerSlot("r000s000", 2)); // the worker that fails
        executorToSlot.put(new ExecutorDetails(2, 2), new WorkerSlot("r000s001", 1)); // the healthy worker
        existingAssignments = new HashMap<>();
        assignment = new SchedulerAssignmentImpl(topology1.getId(), executorToSlot, null, null);
        existingAssignments.put(topology1.getId(), assignment);
        // delete one worker of r000s000 (failed) from topo1 assignment to enable actual schedule for testing
        executorToSlot.remove(new ExecutorDetails(1, 1));

        copyOfOldMapping = new HashMap<>(executorToSlot);
        existingExecutors = copyOfOldMapping.keySet(); // namely the two eds on the orphaned worker and the healthy worker
        supMap1 = new HashMap<>(supMap);
        supMap1.remove("r000s000"); // mock the supervisor r000s000 as a failed supervisor

        topologies = new Topologies(topology1);
        cluster1 = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap1, existingAssignments, topologies, config1);
        scheduler.schedule(topologies, cluster1);

        newAssignment = cluster1.getAssignmentById(topology1.getId());
        newExecutorToSlot = newAssignment.getExecutorToSlot();

        for (ExecutorDetails executor : existingExecutors) {
            assertEquals(copyOfOldMapping.get(executor), newExecutorToSlot.get(executor));
        }
        assertFalse(cluster1.needsSchedulingRas(topology1));
        assertEquals("Fully Scheduled", cluster1.getStatusMap().get(topology1.getId()));
        // end of Test3

        // Test4: Scheduling a new topology does not disturb other assignments unnecessarily
        topologies = new Topologies(topology1);
        cluster1 = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config1);
        scheduler.schedule(topologies, cluster1);
        assignment = cluster1.getAssignmentById(topology1.getId());
        executorToSlot = assignment.getExecutorToSlot();
        copyOfOldMapping = new HashMap<>(executorToSlot);

        topologies = addTopologies(topologies, topology2);
        cluster1 = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config1);
        scheduler.schedule(topologies, cluster1);

        newAssignment = cluster1.getAssignmentById(topology1.getId());
        newExecutorToSlot = newAssignment.getExecutorToSlot();

        for (ExecutorDetails executor : copyOfOldMapping.keySet()) {
            assertEquals(copyOfOldMapping.get(executor), newExecutorToSlot.get(executor));
        }
        assertFalse(cluster1.needsSchedulingRas(topology1));
        assertFalse(cluster1.needsSchedulingRas(topology2));
        String expectedStatusPrefix = "Running - Fully Scheduled by DefaultResourceAwareStrategy";
        assertTrue(cluster1.getStatusMap().get(topology1.getId()).startsWith(expectedStatusPrefix));
        assertTrue(cluster1.getStatusMap().get(topology2.getId()).startsWith(expectedStatusPrefix));
    }

    public void testHeterogeneousCluster(Config topologyConf, String strategyName) {
        LOG.info("\n\n\t\ttestHeterogeneousCluster");
        INimbus iNimbus = new INimbusTest();
        Map<String, Double> resourceMap1 = new HashMap<>(); // strong supervisor node
        resourceMap1.put(Config.SUPERVISOR_CPU_CAPACITY, 800.0);
        resourceMap1.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 4096.0);
        Map<String, Double> resourceMap2 = new HashMap<>(); // weak supervisor node
        resourceMap2.put(Config.SUPERVISOR_CPU_CAPACITY, 200.0);
        resourceMap2.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1024.0);

        resourceMap1 = NormalizedResources.RESOURCE_NAME_NORMALIZER.normalizedResourceMap(resourceMap1);
        resourceMap2 = NormalizedResources.RESOURCE_NAME_NORMALIZER.normalizedResourceMap(resourceMap2);

        Map<String, SupervisorDetails> supMap = new HashMap<>();
        for (int i = 0; i < 2; i++) {
            List<Number> ports = new LinkedList<>();
            for (int j = 0; j < 4; j++) {
                ports.add(j);
            }
            SupervisorDetails sup = new SupervisorDetails("r00s00" + i, "host-" + i, null, ports, i == 0 ? resourceMap1 : resourceMap2);
            supMap.put(sup.getId(), sup);
        }
        LOG.info("SUPERVISORS = {}", supMap);

        // topo1 has one single huge task that can not be handled by the small-super
        TopologyBuilder builder1 = new TopologyBuilder();
        builder1.setSpout("wordSpout1", new TestWordSpout(), 1).setCPULoad(300.0).setMemoryLoad(2000.0, 48.0);
        StormTopology stormTopology1 = builder1.createTopology();
        Config config1 = new Config();
        config1.putAll(topologyConf);
        Map<ExecutorDetails, String> executorMap1 = genExecsAndComps(stormTopology1);
        TopologyDetails topology1 = new TopologyDetails("topology1", config1, stormTopology1, 1, executorMap1, 0, "user");

        // topo2 has 4 large tasks
        TopologyBuilder builder2 = new TopologyBuilder();
        builder2.setSpout("wordSpout2", new TestWordSpout(), 4).setCPULoad(100.0).setMemoryLoad(500.0, 12.0);
        StormTopology stormTopology2 = builder2.createTopology();
        Config config2 = new Config();
        config2.putAll(topologyConf);
        Map<ExecutorDetails, String> executorMap2 = genExecsAndComps(stormTopology2);
        TopologyDetails topology2 = new TopologyDetails("topology2", config2, stormTopology2, 1, executorMap2, 0, "user");

        // topo3 has 4 large tasks
        TopologyBuilder builder3 = new TopologyBuilder();
        builder3.setSpout("wordSpout3", new TestWordSpout(), 4).setCPULoad(20.0).setMemoryLoad(200.0, 56.0);
        StormTopology stormTopology3 = builder3.createTopology();
        Config config3 = new Config();
        config3.putAll(topologyConf);
        Map<ExecutorDetails, String> executorMap3 = genExecsAndComps(stormTopology3);
        TopologyDetails topology3 = new TopologyDetails("topology3", config3, stormTopology3, 1, executorMap3, 0, "user");

        // topo4 has 12 small tasks, whose mem usage does not exactly divide a node's mem capacity
        TopologyBuilder builder4 = new TopologyBuilder();
        builder4.setSpout("wordSpout4", new TestWordSpout(), 12).setCPULoad(30.0).setMemoryLoad(100.0, 0.0);
        StormTopology stormTopology4 = builder4.createTopology();
        Config config4 = new Config();
        config4.putAll(topologyConf);
        Map<ExecutorDetails, String> executorMap4 = genExecsAndComps(stormTopology4);
        TopologyDetails topology4 = new TopologyDetails("topology4", config4, stormTopology4, 1, executorMap4, 0, "user");

        // topo5 has 40 small tasks, it should be able to exactly use up both the cpu and mem in the cluster
        TopologyBuilder builder5 = new TopologyBuilder();
        builder5.setSpout("wordSpout5", new TestWordSpout(), 40).setCPULoad(25.0).setMemoryLoad(100.0, 28.0);
        StormTopology stormTopology5 = builder5.createTopology();
        Config config5 = new Config();
        config5.putAll(topologyConf);
        Map<ExecutorDetails, String> executorMap5 = genExecsAndComps(stormTopology5);
        TopologyDetails topology5 = new TopologyDetails("topology5", config5, stormTopology5, 1, executorMap5, 0, "user");

        // Test1: Launch topo 1-3 together, it should be able to use up either mem or cpu resource due to exact division
        ResourceAwareScheduler rs = new ResourceAwareScheduler();
        LOG.info("\n\n\t\tScheduling topologies 1, 2 and 3");
        Topologies topologies = new Topologies(topology1, topology2, topology3);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config1);
        rs.prepare(config1, new StormMetricsRegistry());

        Map<SupervisorDetails, Double> superToCpu = null;
        Map<SupervisorDetails, Double> superToMem = null;

        try {
            rs.schedule(topologies, cluster);

            assertFalse(cluster.needsSchedulingRas(topology1));
            assertFalse(cluster.needsSchedulingRas(topology2));
            assertFalse(cluster.needsSchedulingRas(topology3));

            String expectedMsgPrefix = "Running - Fully Scheduled by " + strategyName;
            assertTrue(cluster.getStatusMap().get(topology1.getId()).startsWith(expectedMsgPrefix));
            assertTrue(cluster.getStatusMap().get(topology2.getId()).startsWith(expectedMsgPrefix));
            assertTrue(cluster.getStatusMap().get(topology3.getId()).startsWith(expectedMsgPrefix));

            superToCpu = getSupervisorToCpuUsage(cluster, topologies);
            superToMem = getSupervisorToMemoryUsage(cluster, topologies);

            final Double EPSILON = 0.0001;
            for (SupervisorDetails supervisor : supMap.values()) {
                Double cpuAvailable = supervisor.getTotalCpu();
                Double memAvailable = supervisor.getTotalMemory();
                Double cpuUsed = superToCpu.get(supervisor);
                Double memUsed = superToMem.get(supervisor);

                assertTrue(supervisor.getId() + " MEM: " + memAvailable + " == " + memUsed + " OR CPU: " + cpuAvailable + " == " + cpuUsed,
                        (Math.abs(memAvailable - memUsed) < EPSILON) || (Math.abs(cpuAvailable - cpuUsed) < EPSILON));
            }
        } finally {
            rs.cleanup();
        }

        // end of Test1

        LOG.warn("\n\n\t\tSwitching to topologies 1, 2 and 4");
        // Test2: Launch topo 1, 2 and 4, they together request a little more mem than available, so one of the 3 topos will not be
        // scheduled
        topologies = new Topologies(topology1, topology2, topology4);
        cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config1);
        rs.prepare(config1, new StormMetricsRegistry());
        try {
            rs.schedule(topologies, cluster);
            int numTopologiesAssigned = 0;
            if (cluster.getStatusMap().get(topology1.getId()).startsWith("Running - Fully Scheduled by " + strategyName)) {
                LOG.info("TOPO 1 scheduled");
                numTopologiesAssigned++;
            }
            if (cluster.getStatusMap().get(topology2.getId()).startsWith("Running - Fully Scheduled by " + strategyName)) {
                LOG.info("TOPO 2 scheduled");
                numTopologiesAssigned++;
            }
            if (cluster.getStatusMap().get(topology4.getId()).startsWith("Running - Fully Scheduled by " + strategyName)) {
                LOG.info("TOPO 3 scheduled");
                numTopologiesAssigned++;
            }
            assertEquals(2, numTopologiesAssigned);
        } finally {
            rs.cleanup();
        }
        //end of Test2

        LOG.info("\n\n\t\tScheduling just topo 5");
        //Test3: "Launch topo5 only, both mem and cpu should be exactly used up"
        topologies = new Topologies(topology5);
        cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config1);
        rs.prepare(config1, new StormMetricsRegistry());
        try {
            rs.schedule(topologies, cluster);
            superToCpu = getSupervisorToCpuUsage(cluster, topologies);
            superToMem = getSupervisorToMemoryUsage(cluster, topologies);
            for (SupervisorDetails supervisor : supMap.values()) {
                Double cpuAvailable = supervisor.getTotalCpu();
                Double memAvailable = supervisor.getTotalMemory();
                Double cpuUsed = superToCpu.get(supervisor);
                Double memUsed = superToMem.get(supervisor);
                assertEquals(cpuAvailable, cpuUsed, 0.0001);
                assertEquals(memAvailable, memUsed, 0.0001);
            }
        } finally {
            rs.cleanup();
        }
        //end of Test3
    }

    @Test
    public void testHeterogeneousClusterwithDefaultRas() {
        testHeterogeneousCluster(defaultTopologyConf, getDefaultResourceAwareStrategyClass().getSimpleName());
    }

    @Test
    public void testHeterogeneousClusterwithGras() {
        Config grasClusterConfig = (Config) defaultTopologyConf.clone();
        grasClusterConfig.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, getGenericResourceAwareStrategyClass().getName());
        testHeterogeneousCluster(grasClusterConfig, getGenericResourceAwareStrategyClass().getSimpleName());
    }

    @Test
    public void testTopologyWorkerMaxHeapSize() {
        // Test1: If RAS spreads executors across multiple workers based on the set limit for a worker used by the topology
        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(2, 2, 400, 2000);

        TopologyBuilder builder1 = new TopologyBuilder();
        builder1.setSpout("wordSpout1", new TestWordSpout(), 4);
        StormTopology stormTopology1 = builder1.createTopology();
        Config config1 = new Config();
        config1.putAll(defaultTopologyConf);
        config1.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 128.0);
        Map<ExecutorDetails, String> executorMap1 = genExecsAndComps(stormTopology1);
        TopologyDetails topology1 = new TopologyDetails("topology1", config1, stormTopology1, 1, executorMap1, 0, "user");
        ResourceAwareScheduler rs = new ResourceAwareScheduler();
        Topologies topologies = new Topologies(topology1);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config1);
        rs.prepare(config1, new StormMetricsRegistry());
        try {
            rs.schedule(topologies, cluster);
            assertFalse(cluster.needsSchedulingRas(topology1));
            assertTrue(cluster.getStatusMap().get(topology1.getId()).startsWith("Running - Fully Scheduled by DefaultResourceAwareStrategy"));
            assertEquals(4, cluster.getAssignedNumWorkers(topology1));
        } finally {
            rs.cleanup();
        }

        // Test2: test when no more workers are available due to topology worker max heap size limit but there is memory is still available
        // wordSpout2 is going to contain 5 executors that needs scheduling. Each of those executors has a memory requirement of 128.0 MB
        // The cluster contains 4 free WorkerSlots. For this topolology each worker is limited to a max heap size of 128.0
        // Thus, one executor not going to be able to get scheduled thus failing the scheduling of this topology and no executors of this
        // topology will be scheduled
        TopologyBuilder builder2 = new TopologyBuilder();
        builder2.setSpout("wordSpout2", new TestWordSpout(), 5);
        StormTopology stormTopology2 = builder2.createTopology();
        Config config2 = new Config();
        config2.putAll(defaultTopologyConf);
        config2.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 128.0);
        Map<ExecutorDetails, String> executorMap2 = genExecsAndComps(stormTopology2);
        TopologyDetails topology2 = new TopologyDetails("topology2", config2, stormTopology2, 1, executorMap2, 0, "user");
        topologies = new Topologies(topology2);
        cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config2);
        rs.prepare(config2, new StormMetricsRegistry());
        try {
            rs.schedule(topologies, cluster);
            assertTrue(cluster.needsSchedulingRas(topology2));
            String status = cluster.getStatusMap().get(topology2.getId());
            assert status.startsWith("Not enough resources to schedule") : status;
            //assert status.endsWith("5 executors not scheduled") : status;
            assertEquals(5, cluster.getUnassignedExecutors(topology2).size());
        } finally {
            rs.cleanup();
        }
    }

    @Test
    public void testReadInResourceAwareSchedulerUserPools() {
        Map<String, Object> fromFile = Utils.findAndReadConfigFile("user-resource-pools.yaml", false);
        LOG.info("fromFile: {}", fromFile);
        ConfigValidation.validateFields(fromFile);
    }

    @Test
    public void testSubmitUsersWithNoGuarantees() {
        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(4, 4, 100, 1000);
        Map<String, Map<String, Number>> resourceUserPool = userResourcePool(
            userRes("jerry", 200, 2000));

        Config config = createClusterConfig(100, 500, 500, resourceUserPool);

        Topologies topologies = new Topologies(
            genTopology("topo-1", config, 1, 0, 1, 0, currentTime - 2, 10, "jerry"),
            genTopology("topo-2", config, 1, 0, 1, 0, currentTime - 2, 20, "jerry"),
            genTopology("topo-3", config, 1, 0, 1, 0, currentTime - 2, 20, "jerry"),
            genTopology("topo-4", config, 1, 0, 1, 0, currentTime - 2, 10, "bobby"),
            genTopology("topo-5", config, 1, 0, 1, 0, currentTime - 2, 20, "bobby"));
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);

        scheduler = new ResourceAwareScheduler();
        scheduler.prepare(config, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);
        assertTopologiesFullyScheduled(cluster, "topo-1", "topo-2", "topo-3", "topo-4");
        assertTopologiesNotScheduled(cluster, "topo-5");
    }

    @Test
    public void testMultipleUsers() {
        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(20, 4, 1000, 1024 * 10);
        Map<String, Map<String, Number>> resourceUserPool = userResourcePool(
            userRes("jerry", 1_000, 8_192),
            userRes("bobby", 10_000, 32_768),
            userRes("derek", 5_000, 16_384));
        Config config = createClusterConfig(10, 128, 0, resourceUserPool);

        Topologies topologies = new Topologies(
            genTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2, 20, "jerry"),
            genTopology("topo-2", config, 5, 15, 1, 1, currentTime - 8, 29, "jerry"),
            genTopology("topo-3", config, 5, 15, 1, 1, currentTime - 16, 29, "jerry"),
            genTopology("topo-4", config, 5, 15, 1, 1, currentTime - 16, 20, "jerry"),
            genTopology("topo-5", config, 5, 15, 1, 1, currentTime - 24, 29, "jerry"),
            genTopology("topo-6", config, 5, 15, 1, 1, currentTime - 2, 20, "bobby"),
            genTopology("topo-7", config, 5, 15, 1, 1, currentTime - 8, 29, "bobby"),
            genTopology("topo-8", config, 5, 15, 1, 1, currentTime - 16, 29, "bobby"),
            genTopology("topo-9", config, 5, 15, 1, 1, currentTime - 16, 20, "bobby"),
            genTopology("topo-10", config, 5, 15, 1, 1, currentTime - 24, 29, "bobby"),
            genTopology("topo-11", config, 5, 15, 1, 1, currentTime - 2, 20, "derek"),
            genTopology("topo-12", config, 5, 15, 1, 1, currentTime - 8, 29, "derek"),
            genTopology("topo-13", config, 5, 15, 1, 1, currentTime - 16, 29, "derek"),
            genTopology("topo-14", config, 5, 15, 1, 1, currentTime - 16, 20, "derek"),
            genTopology("topo-15", config, 5, 15, 1, 1, currentTime - 24, 29, "derek"));
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);

        scheduler = new ResourceAwareScheduler();
        scheduler.prepare(config, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);
        for (TopologyDetails td : topologies) {
            assertTopologiesFullyScheduled(cluster, td.getName());
        }
    }

    @Test
    public void testHandlingClusterSubscription() {
        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(1, 4, 200, 1024 * 10);
        Map<String, Map<String, Number>> resourceUserPool = userResourcePool(
            userRes("jerry", 1_000, 8_192),
            userRes("bobby", 10_000, 32_768),
            userRes("derek", 5_000, 16_384));
        Config config = createClusterConfig(10, 128, 0, resourceUserPool);

        Topologies topologies = new Topologies(
            genTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2, 20, "jerry"),
            genTopology("topo-2", config, 5, 15, 1, 1, currentTime - 8, 29, "jerry"));
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);
        scheduler = new ResourceAwareScheduler();
        scheduler.prepare(config, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);

        assertTopologiesFullyScheduled(cluster, "topo-1");
        assertTopologiesNotScheduled(cluster, "topo-2");
    }

    /**
     * Test correct behavior when a supervisor dies.  Check if the scheduler handles it correctly and evicts the correct
     * topology when rescheduling the executors from the died supervisor
     */
    @Test
    public void testFaultTolerance() {
        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(6, 4, 100, 1000);
        Map<String, Map<String, Number>> resourceUserPool = userResourcePool(
            userRes("jerry", 50, 500),
            userRes("bobby", 200, 2_000),
            userRes("derek", 100, 1_000));
        Config config = createClusterConfig(100, 500, 500, resourceUserPool);

        Topologies topologies = new Topologies(
            genTopology("topo-1", config, 1, 0, 1, 0, currentTime - 2, 21, "jerry"),
            genTopology("topo-2", config, 1, 0, 1, 0, currentTime - 2, 20, "jerry"),
            genTopology("topo-3", config, 1, 0, 1, 0, currentTime - 2, 10, "bobby"),
            genTopology("topo-4", config, 1, 0, 1, 0, currentTime - 2, 10, "bobby"),
            genTopology("topo-5", config, 1, 0, 1, 0, currentTime - 2, 29, "derek"),
            genTopology("topo-6", config, 1, 0, 1, 0, currentTime - 2, 10, "derek"));
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);
        scheduler = new ResourceAwareScheduler();

        scheduler.prepare(config, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);

        assertTopologiesFullyScheduled(cluster, "topo-1", "topo-2", "topo-3", "topo-4", "topo-5", "topo-6");

        //fail supervisor
        SupervisorDetails supFailed = cluster.getSupervisors().values().iterator().next();
        LOG.info("/***** failing supervisor: {} ****/", supFailed.getHost());
        supMap.remove(supFailed.getId());
        Map<String, SchedulerAssignmentImpl> newAssignments = new HashMap<>();
        for (Map.Entry<String, SchedulerAssignment> topoToAssignment : cluster.getAssignments().entrySet()) {
            String topoId = topoToAssignment.getKey();
            SchedulerAssignment assignment = topoToAssignment.getValue();
            Map<ExecutorDetails, WorkerSlot> executorToSlots = new HashMap<>();
            for (Map.Entry<ExecutorDetails, WorkerSlot> execToWorker : assignment.getExecutorToSlot().entrySet()) {
                ExecutorDetails exec = execToWorker.getKey();
                WorkerSlot ws = execToWorker.getValue();
                if (!ws.getNodeId().equals(supFailed.getId())) {
                    executorToSlots.put(exec, ws);
                }
            }
            newAssignments.put(topoId, new SchedulerAssignmentImpl(topoId, executorToSlots, null, null));
        }
        Map<String, String> statusMap = cluster.getStatusMap();
        LOG.warn("Rescheduling with removed Supervisor....");
        cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, newAssignments, topologies, config);
        cluster.setStatusMap(statusMap);
        scheduler.schedule(topologies, cluster);

        assertTopologiesFullyScheduled(cluster, "topo-2", "topo-3", "topo-4", "topo-5", "topo-6");
        assertTopologiesNotScheduled(cluster, "topo-1");
    }

    /**
     * test if free slots on nodes work correctly
     */
    @Test
    public void testNodeFreeSlot() {
        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(4, 4, 100, 1000);
        Config config = createClusterConfig(100, 500, 500, null);

        Topologies topologies = new Topologies(
            genTopology("topo-1", config, 1, 0, 2, 0, currentTime - 2, 29, "user"),
            genTopology("topo-2", config, 1, 0, 2, 0, currentTime - 2, 10, "user"));
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);
        scheduler = new ResourceAwareScheduler();

        scheduler.prepare(config, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);

        Map<String, RasNode> nodes = RasNodes.getAllNodesFrom(cluster);

        for (SchedulerAssignment assignment : cluster.getAssignments().values()) {
            for (Entry<WorkerSlot, WorkerResources> entry : new HashMap<>(assignment.getScheduledResources()).entrySet()) {
                WorkerSlot ws = entry.getKey();
                WorkerResources wr = entry.getValue();
                double memoryBefore = nodes.get(ws.getNodeId()).getAvailableMemoryResources();
                double cpuBefore = nodes.get(ws.getNodeId()).getAvailableCpuResources();
                double memoryUsedByWorker = wr.get_mem_on_heap() + wr.get_mem_off_heap();
                assertEquals("Check if memory used by worker is calculated correctly", 1000.0, memoryUsedByWorker, 0.001);
                double cpuUsedByWorker = wr.get_cpu();
                assertEquals("Check if CPU used by worker is calculated correctly", 100.0, cpuUsedByWorker, 0.001);
                nodes.get(ws.getNodeId()).free(ws);
                double memoryAfter = nodes.get(ws.getNodeId()).getAvailableMemoryResources();
                double cpuAfter = nodes.get(ws.getNodeId()).getAvailableCpuResources();
                assertEquals("Check if free correctly frees amount of memory", memoryBefore + memoryUsedByWorker, memoryAfter, 0.001);
                assertEquals("Check if free correctly frees amount of memory", cpuBefore + cpuUsedByWorker, cpuAfter, 0.001);
                assertFalse("Check if worker was removed from assignments", assignment.getSlotToExecutors().containsKey(ws));
            }
        }
    }

    /**
     * When the first topology failed to be scheduled make sure subsequent schedulings can still succeed
     */
    @Test
    public void testSchedulingAfterFailedScheduling() {
        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(8, 4, 100, 1000);
        Config config = createClusterConfig(100, 500, 500, null);

        TopologyDetails topo1 = genTopology("topo-1", config, 8, 0, 2, 0, currentTime - 2, 10, "jerry");
        TopologyDetails topo2 = genTopology("topo-2", config, 2, 0, 2, 0, currentTime - 2, 20, "jerry");
        TopologyDetails topo3 = genTopology("topo-3", config, 1, 2, 1, 1, currentTime - 2, 20, "jerry");

        Topologies topologies = new Topologies(topo1, topo2, topo3);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
        scheduler = new ResourceAwareScheduler();

        scheduler.prepare(config, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);

        assertFalse("Topo-1 unscheduled?", cluster.getAssignmentById(topo1.getId()) != null);
        assertTrue("Topo-2 scheduled?", cluster.getAssignmentById(topo2.getId()) != null);
        assertEquals("Topo-2 all executors scheduled?", 4, cluster.getAssignmentById(topo2.getId()).getExecutorToSlot().size());
        assertTrue("Topo-3 scheduled?", cluster.getAssignmentById(topo3.getId()) != null);
        assertEquals("Topo-3 all executors scheduled?", 3, cluster.getAssignmentById(topo3.getId()).getExecutorToSlot().size());
    }

    /**
     * Min CPU for worker set to 50%.  1 supervisor with 100% CPU.
     * A topology with 10 10% components should schedule.
     */
    @Test
    public void minCpuWorkerJustFits() {
        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(1, 4, 100, 60000);
        Config config = createClusterConfig(10, 500, 500, null);
        config.put(DaemonConfig.STORM_WORKER_MIN_CPU_PCORE_PERCENT, 50.0);
        TopologyDetails topo1 = genTopology("topo-1", config, 10, 0, 1, 1, currentTime - 2, 20, "jerry");
        Topologies topologies = new Topologies(topo1);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
        scheduler = new ResourceAwareScheduler();
        scheduler.prepare(config, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);
        assertFalse(cluster.needsSchedulingRas(topo1));
        assertTrue("Topo-1 scheduled?", cluster.getAssignmentById(topo1.getId()) != null);
    }

    /**
     * Min CPU for worker set to 40%.  1 supervisor with 100% CPU.
     * 2 topologies with 2 10% components should schedule.  A third topology should then fail scheduling due to lack of CPU.
     */
    @Test
    public void minCpuPreventsThirdTopo() {
        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(1, 4, 100, 60000);
        Config config = createClusterConfig(10, 500, 500, null);
        config.put(DaemonConfig.STORM_WORKER_MIN_CPU_PCORE_PERCENT, 40.0);
        TopologyDetails topo1 = genTopology("topo-1", config, 2, 0, 1, 1, currentTime - 2, 20, "jerry");
        TopologyDetails topo2 = genTopology("topo-2", config, 2, 0, 1, 1, currentTime - 2, 20, "jerry");
        TopologyDetails topo3 = genTopology("topo-3", config, 2, 0, 1, 1, currentTime - 2, 20, "jerry");
        Topologies topologies = new Topologies(topo1, topo2, topo3);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
        scheduler = new ResourceAwareScheduler();
        scheduler.prepare(config, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);
        assertFalse(cluster.needsSchedulingRas(topo1));
        assertFalse(cluster.needsSchedulingRas(topo2));
        assertTrue(cluster.needsSchedulingRas(topo3));
        assertTrue("topo-1 scheduled?", cluster.getAssignmentById(topo1.getId()) != null);
        assertTrue("topo-2 scheduled?", cluster.getAssignmentById(topo2.getId()) != null);
        assertFalse("topo-3 unscheduled?", cluster.getAssignmentById(topo3.getId()) != null);

        SchedulerAssignment assignment1 = cluster.getAssignmentById(topo1.getId());
        assertEquals(1, assignment1.getSlots().size());
        Map<WorkerSlot, WorkerResources> assignedSlots1 = assignment1.getScheduledResources();
        double assignedCpu = 0.0;
        for (Entry<WorkerSlot, WorkerResources> entry : assignedSlots1.entrySet()) {
            WorkerResources wr = entry.getValue();
            assignedCpu += wr.get_cpu();
        }
        assertEquals(40.0, assignedCpu, 0.001);

        SchedulerAssignment assignment2 = cluster.getAssignmentById(topo2.getId());
        assertEquals(1, assignment2.getSlots().size());
        Map<WorkerSlot, WorkerResources> assignedSlots2 = assignment2.getScheduledResources();
        assignedCpu = 0.0;
        for (Entry<WorkerSlot, WorkerResources> entry : assignedSlots2.entrySet()) {
            WorkerResources wr = entry.getValue();
            assignedCpu += wr.get_cpu();
        }
        assertEquals(40.0, assignedCpu, 0.001);
    }

    @Test
    public void testMinCpuMaxMultipleSupervisors() {
        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(3, 4, 300, 60000);
        Config config = createClusterConfig(5, 50, 50, null);
        config.put(DaemonConfig.STORM_WORKER_MIN_CPU_PCORE_PERCENT, 100.0);
        TopologyDetails topo0 = genTopology("topo-0", config, 4, 5, 1, 1, currentTime - 2, 20, "jerry");
        TopologyDetails topo1 = genTopology("topo-1", config, 4, 5, 1, 1, currentTime - 2, 20, "jerry");
        TopologyDetails topo2 = genTopology("topo-2", config, 4, 5, 1, 1, currentTime - 2, 20, "jerry");
        TopologyDetails topo3 = genTopology("topo-3", config, 4, 5, 1, 1, currentTime - 2, 20, "jerry");
        TopologyDetails topo4 = genTopology("topo-4", config, 4, 5, 1, 1, currentTime - 2, 20, "jerry");
        TopologyDetails topo5 = genTopology("topo-5", config, 4, 5, 1, 1, currentTime - 2, 20, "jerry");
        TopologyDetails topo6 = genTopology("topo-6", config, 4, 5, 1, 1, currentTime - 2, 20, "jerry");
        TopologyDetails topo7 = genTopology("topo-7", config, 4, 5, 1, 1, currentTime - 2, 20, "jerry");
        TopologyDetails topo8 = genTopology("topo-8", config, 4, 5, 1, 1, currentTime - 2, 20, "jerry");
        TopologyDetails topo9 = genTopology("topo-9", config, 4, 5, 1, 1, currentTime - 2, 20, "jerry");
        Topologies topologies = new Topologies(topo0, topo1, topo2, topo3, topo4, topo5, topo6, topo7, topo8, topo9);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
        scheduler = new ResourceAwareScheduler();
        scheduler.prepare(config, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);

        assertFalse(cluster.needsSchedulingRas(topo0));
        assertFalse(cluster.needsSchedulingRas(topo1));
        assertFalse(cluster.needsSchedulingRas(topo2));
        assertFalse(cluster.needsSchedulingRas(topo3));
        assertFalse(cluster.needsSchedulingRas(topo4));
        assertFalse(cluster.needsSchedulingRas(topo5));
        assertFalse(cluster.needsSchedulingRas(topo6));
        assertFalse(cluster.needsSchedulingRas(topo7));
        assertFalse(cluster.needsSchedulingRas(topo8));
        assertTrue(cluster.needsSchedulingRas(topo9));

        assertTrue("topo-0 scheduled?", cluster.getAssignmentById(topo0.getId()) != null);
        assertTrue("topo-1 scheduled?", cluster.getAssignmentById(topo1.getId()) != null);
        assertTrue("topo-2 scheduled?", cluster.getAssignmentById(topo2.getId()) != null);
        assertTrue("topo-3 scheduled?", cluster.getAssignmentById(topo3.getId()) != null);
        assertTrue("topo-4 scheduled?", cluster.getAssignmentById(topo4.getId()) != null);
        assertTrue("topo-5 scheduled?", cluster.getAssignmentById(topo5.getId()) != null);
        assertTrue("topo-6 scheduled?", cluster.getAssignmentById(topo6.getId()) != null);
        assertTrue("topo-7 scheduled?", cluster.getAssignmentById(topo7.getId()) != null);
        assertTrue("topo-8 scheduled?", cluster.getAssignmentById(topo8.getId()) != null);
        assertFalse("topo-9 unscheduled?", cluster.getAssignmentById(topo9.getId()) != null);
    }

    /**
     * Min CPU for worker set to 50%.  1 supervisor with 100% CPU.
     * A topology with 3 workers should fail scheduling even if under CPU.
     */
    @Test
    public void minCpuWorkerSplitFails() {
        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(1, 4, 100, 60000);
        Config config = createClusterConfig(10, 500, 500, null);
        config.put(DaemonConfig.STORM_WORKER_MIN_CPU_PCORE_PERCENT, 50.0);
        TopologyDetails topo1 = genTopology("topo-1", config, 10, 0, 1, 1, currentTime - 2, 20,
                "jerry", 2000.0);
        Topologies topologies = new Topologies(topo1);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
        scheduler = new ResourceAwareScheduler();
        scheduler.prepare(config, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);
        assertTrue(cluster.needsSchedulingRas(topo1));
        assertFalse("Topo-1 unscheduled?", cluster.getAssignmentById(topo1.getId()) != null);
    }

    protected static class TimeBlockResult {
        List<Long> firstBlockTime;
        List<Long> lastBlockTime;

        TimeBlockResult() {
            firstBlockTime = new ArrayList<>();
            lastBlockTime = new ArrayList<>();
        }

        void append(TimeBlockResult other) {
            this.firstBlockTime.addAll(other.firstBlockTime);
            this.lastBlockTime.addAll(other.lastBlockTime);
        }
    }

    private long getMedianValue(List<Long> values) {
        final int numValues = values.size();
        assert(numValues % 2 == 1);     // number of values must be odd to compute median as below
        List<Long> sortedValues = new ArrayList<>();
        sortedValues.addAll(values);
        Collections.sort(sortedValues);

        final int medianIndex = (int) Math.floor(numValues / 2);
        return sortedValues.get(medianIndex);
    }

    /**
     * Check time to schedule a fragmented cluster using different strategies
     *
     * Simulate scheduling on a large production cluster. Find the ratio of time to schedule a set of topologies when
     * the cluster is empty and when the cluster is nearly full. While the cluster has sufficient resources to schedule
     * all topologies, when nearly full the cluster becomes fragmented and some topologies fail to schedule.
     */
    @Test
    public void TestLargeFragmentedClusterScheduling() {
        /*
        Without fragmentation, the cluster would be able to schedule both topologies on each node. Let's call each node
        with both topologies scheduled as 100% scheduled.

        We schedule the cluster in 3 blocks of topologies, measuring the time to schedule the blocks. The first, middle
        and last blocks attempt to schedule the following 0-10%, 10%-90%, 90%-100%. The last block has a number of
        scheduling failures due to cluster fragmentation and its time is dominated by attempting to evict topologies.

        Timing results for scheduling are noisy. As a result, we do multiple runs and use median values for FirstBlock
        and LastBlock times. (somewhere a statistician is crying). The ratio of LastBlock / FirstBlock remains fairly constant.


        TestLargeFragmentedClusterScheduling took 91118 ms
        DefaultResourceAwareStrategy, FirstBlock 249.0, LastBlock 1734.0 ratio 6.963855421686747
        GenericResourceAwareStrategy, FirstBlock 215.0, LastBlock 1673.0 ratio 7.78139534883721
        ConstraintSolverStrategy, FirstBlock 279.0, LastBlock 2200.0 ratio 7.885304659498208

        TestLargeFragmentedClusterScheduling took 98455 ms
        DefaultResourceAwareStrategy, FirstBlock 266.0, LastBlock 1812.0 ratio 6.81203007518797
        GenericResourceAwareStrategy, FirstBlock 235.0, LastBlock 1802.0 ratio 7.6680851063829785
        ConstraintSolverStrategy, FirstBlock 304.0, LastBlock 2320.0 ratio 7.631578947368421

        TestLargeFragmentedClusterScheduling took 97268 ms
        DefaultResourceAwareStrategy, FirstBlock 251.0, LastBlock 1826.0 ratio 7.274900398406374
        GenericResourceAwareStrategy, FirstBlock 220.0, LastBlock 1719.0 ratio 7.8136363636363635
        ConstraintSolverStrategy, FirstBlock 296.0, LastBlock 2469.0 ratio 8.341216216216216

        TestLargeFragmentedClusterScheduling took 97963 ms
        DefaultResourceAwareStrategy, FirstBlock 249.0, LastBlock 1788.0 ratio 7.180722891566265
        GenericResourceAwareStrategy, FirstBlock 240.0, LastBlock 1796.0 ratio 7.483333333333333
        ConstraintSolverStrategy, FirstBlock 328.0, LastBlock 2544.0 ratio 7.7560975609756095

        TestLargeFragmentedClusterScheduling took 93106 ms
        DefaultResourceAwareStrategy, FirstBlock 258.0, LastBlock 1714.0 ratio 6.6434108527131785
        GenericResourceAwareStrategy, FirstBlock 215.0, LastBlock 1692.0 ratio 7.869767441860465
        ConstraintSolverStrategy, FirstBlock 309.0, LastBlock 2342.0 ratio 7.5792880258899675

        Choose the median value of the values above
        DefaultResourceAwareStrategy    6.96
        GenericResourceAwareStrategy    7.78
        ConstraintSolverStrategy        7.75
        */

        final int numNodes = 500;
        final int numRuns = 5;

        Map<String, Config> strategyToConfigs = new HashMap<>();
        strategyToConfigs.put(getDefaultResourceAwareStrategyClass().getName(), createClusterConfig(10, 10, 0, null));
        strategyToConfigs.put(getGenericResourceAwareStrategyClass().getName(), createGrasClusterConfig(10, 10, 0, null, null));
        strategyToConfigs.put(ConstraintSolverStrategy.class.getName(), createCSSClusterConfig(10, 10, 0, null));

        Map<String, TimeBlockResult> strategyToTimeBlockResults = new HashMap<>();

        // AcceptedBlockTimeRatios obtained by empirical testing (see comment block above)
        Map<String, Double> strategyToAcceptedBlockTimeRatios = new HashMap<>();
        strategyToAcceptedBlockTimeRatios.put(getDefaultResourceAwareStrategyClass().getName(), 6.96);
        strategyToAcceptedBlockTimeRatios.put(getGenericResourceAwareStrategyClass().getName(), 7.78);
        strategyToAcceptedBlockTimeRatios.put(ConstraintSolverStrategy.class.getName(), 7.75);

        // Get first and last block times for multiple runs and strategies
        long startTime = Time.currentTimeMillis();
        for (Entry<String, Config> strategyConfig : strategyToConfigs.entrySet()) {
            TimeBlockResult strategyTimeBlockResult = strategyToTimeBlockResults.computeIfAbsent(strategyConfig.getKey(), (k) -> new TimeBlockResult());
            for (int run = 0; run < numRuns; ++run) {
                TimeBlockResult result = testLargeClusterSchedulingTiming(numNodes, strategyConfig.getValue());
                strategyTimeBlockResult.append(result);
            }
        }

        // Log median ratios for different strategies
        LOG.info("TestLargeFragmentedClusterScheduling took {} ms", Time.currentTimeMillis() - startTime);
        for (Entry<String, TimeBlockResult> strategyResult : strategyToTimeBlockResults.entrySet()) {
            TimeBlockResult strategyTimeBlockResult = strategyResult.getValue();
            double medianFirstBlockTime = getMedianValue(strategyTimeBlockResult.firstBlockTime);
            double medianLastBlockTime = getMedianValue(strategyTimeBlockResult.lastBlockTime);
            double ratio = medianLastBlockTime / medianFirstBlockTime;
            LOG.info("{}, FirstBlock {}, LastBlock {} ratio {}", strategyResult.getKey(), medianFirstBlockTime, medianLastBlockTime, ratio);
        }

        // Check last block scheduling time does not get significantly slower
        for (Entry<String, TimeBlockResult> strategyResult : strategyToTimeBlockResults.entrySet()) {
            TimeBlockResult strategyTimeBlockResult = strategyResult.getValue();
            double medianFirstBlockTime = getMedianValue(strategyTimeBlockResult.firstBlockTime);
            double medianLastBlockTime = getMedianValue(strategyTimeBlockResult.lastBlockTime);
            double ratio = medianLastBlockTime / medianFirstBlockTime;

            double slowSchedulingThreshold = 1.5;
            String msg = "Strategy " + strategyResult.getKey() + " scheduling is significantly slower for mostly full fragmented cluster\n";
            double ratioAccepted = strategyToAcceptedBlockTimeRatios.get(strategyResult.getKey());
            msg += String.format("Ratio was %.2f (high/low=%.2f/%.2f), max allowed is %.2f (%.2f * %.2f)",
                    ratio, medianLastBlockTime, medianFirstBlockTime,
                    ratioAccepted * slowSchedulingThreshold, ratioAccepted, slowSchedulingThreshold);
            assertTrue(msg, ratio < slowSchedulingThreshold * ratioAccepted);
        }
    }

    // Create multiple copies of a test topology
    private void addTopologyBlockToMap(Map<String, TopologyDetails> topologyMap, String baseName, Config config,
                                       double spoutMemoryLoad, int[] blockIndices) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("testSpout", new TestSpout(), 1).setMemoryLoad(spoutMemoryLoad);
        StormTopology stormTopology = builder.createTopology();
        Map<ExecutorDetails, String> executorMap = genExecsAndComps(stormTopology);

        for (int i = blockIndices[0]; i <= blockIndices[1]; ++i) {
            TopologyDetails topo = new TopologyDetails(baseName + i, config, stormTopology, 0, executorMap, 0, "user");
            topologyMap.put(topo.getId(), topo);
        }
    }

    /*
     * Test time to schedule large cluster scheduling with fragmentation
     */
    private TimeBlockResult testLargeClusterSchedulingTiming(int numNodes, Config config) {
        // Attempt to schedule multiple copies of 2 different topologies (topo-t0 and topo-t1) in 3 blocks.
        // Without fragmentation it is possible to schedule all topologies, but fragmentation causes topologies to not
        // schedule for the last block.

        // Get start/end indices for blocks
        int numTopologyPairs = numNodes;
        int increment = (int) Math.floor(numTopologyPairs * 0.1);
        int firstBlockIndices[] = {0, increment - 1};
        int midBlockIndices[] = {increment, numTopologyPairs - increment - 1};
        int lastBlockIndices[] = {numTopologyPairs - increment, numTopologyPairs - 1};

        // Memory is the constraining resource.
        double t0Mem = 70;   // memory required by topo-t0
        double t1Mem = 20;   // memory required by topo-t1
        double nodeMem = 100;

        // first block (0% - 10%)
        Map<String, TopologyDetails> topologyMap = new HashMap<>();
        addTopologyBlockToMap(topologyMap, "topo_t0-", config, t0Mem, firstBlockIndices);
        addTopologyBlockToMap(topologyMap, "topo_t1-", config, t1Mem, firstBlockIndices);
        Topologies topologies = new Topologies(topologyMap);

        Map<String, SupervisorDetails> supMap = genSupervisors(numNodes, 7, 3500, nodeMem);
        Cluster cluster = new Cluster(new INimbusTest(), new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
        TimeBlockResult timeBlockResult = new TimeBlockResult();

        // schedule first block (0% - 10%)
        {
            scheduler = new ResourceAwareScheduler();
            scheduler.prepare(config, new StormMetricsRegistry());

            long time = Time.currentTimeMillis();
            scheduler.schedule(topologies, cluster);
            timeBlockResult.firstBlockTime.add(Time.currentTimeMillis() - time);
        }

        // schedule mid block (10% - 90%)
        {
            addTopologyBlockToMap(topologyMap, "topo_t0-", config, t0Mem, midBlockIndices);
            addTopologyBlockToMap(topologyMap, "topo_t1-", config, t1Mem, midBlockIndices);

            topologies = new Topologies(topologyMap);
            cluster = new Cluster(cluster, topologies);

            scheduler.schedule(topologies, cluster);
        }

        // schedule last block (90% to 100%)
        {
            addTopologyBlockToMap(topologyMap, "topo_t0-", config, t0Mem, lastBlockIndices);
            addTopologyBlockToMap(topologyMap, "topo_t1-", config, t1Mem, lastBlockIndices);

            topologies = new Topologies(topologyMap);
            cluster = new Cluster(cluster, topologies);

            long time = Time.currentTimeMillis();
            scheduler.schedule(topologies, cluster);
            timeBlockResult.lastBlockTime.add(Time.currentTimeMillis() - time);
        }

        return timeBlockResult;
    }

    /**
     * Test multiple spouts and cyclic topologies
     */
    @Test
    public void testMultipleSpoutsAndCyclicTopologies() {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout-1", new TestSpout(),
                         5);
        builder.setSpout("spout-2", new TestSpout(),
                         5);
        builder.setBolt("bolt-1", new TestBolt(),
                        5).shuffleGrouping("spout-1").shuffleGrouping("bolt-3");
        builder.setBolt("bolt-2", new TestBolt(),
                        5).shuffleGrouping("bolt-1");
        builder.setBolt("bolt-3", new TestBolt(),
                        5).shuffleGrouping("bolt-2").shuffleGrouping("spout-2");

        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(25, 1, 100, 1000);
        Config config = createClusterConfig(100, 500, 500, null);

        StormTopology stormTopology = builder.createTopology();
        config.put(Config.TOPOLOGY_SUBMITTER_USER, "jerry");
        TopologyDetails topo = new TopologyDetails("topo-1", config, stormTopology,
                                                   0, genExecsAndComps(stormTopology), 0, "jerry");

        Topologies topologies = new Topologies(topo);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
        scheduler = new ResourceAwareScheduler();
        scheduler.prepare(config, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);

        assertTrue("Topo scheduled?", cluster.getAssignmentById(topo.getId()) != null);
        assertEquals("Topo all executors scheduled?", 25, cluster.getAssignmentById(topo.getId()).getExecutorToSlot().size());
    }

    @Test
    public void testSchedulerStrategyWhitelist() {
        Map<String, Object> config = ConfigUtils.readStormConfig();
        String allowed = getDefaultResourceAwareStrategyClass().getName();
        config.put(Config.NIMBUS_SCHEDULER_STRATEGY_CLASS_WHITELIST, Arrays.asList(allowed));

        Object sched = ReflectionUtils.newSchedulerStrategyInstance(allowed, config);
        assertEquals(sched.getClass().getName(), allowed);
    }

    @Test
    public void testSchedulerStrategyWhitelistException() {
        Map<String, Object> config = ConfigUtils.readStormConfig();
        String allowed = "org.apache.storm.scheduler.resource.strategies.scheduling.SomeNonExistantStrategy";
        String notAllowed = getDefaultResourceAwareStrategyClass().getName();
        config.put(Config.NIMBUS_SCHEDULER_STRATEGY_CLASS_WHITELIST, Arrays.asList(allowed));

        Assertions.assertThrows(DisallowedStrategyException.class, () -> ReflectionUtils.newSchedulerStrategyInstance(notAllowed, config));        
    }

    @Test
    public void testSchedulerStrategyEmptyWhitelist() {
        Map<String, Object> config = ConfigUtils.readStormConfig();
        String allowed = getDefaultResourceAwareStrategyClass().getName();

        Object sched = ReflectionUtils.newSchedulerStrategyInstance(allowed, config);
        assertEquals(sched.getClass().getName(), allowed);
    }

    @PerformanceTest
    @Test
    public void testLargeTopologiesOnLargeClusters() {
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(30), 
            () -> testLargeTopologiesCommon(getDefaultResourceAwareStrategyClass().getName(), false, 1));
        
    }
    
    @PerformanceTest
    @Test
    public void testLargeTopologiesOnLargeClustersGras() {
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(75),
            () -> testLargeTopologiesCommon(getGenericResourceAwareStrategyClass().getName(), true, 1));
    }

    public static class NeverEndingSchedulingStrategy extends BaseResourceAwareStrategy {

        @Override
        public void prepare(Map<String, Object> config) {
        }

        @Override
        public SchedulingResult schedule(Cluster schedulingState, TopologyDetails td) {
            while (true) {
                if (Thread.currentThread().isInterrupted()) {
                    LOG.info("scheduling interrupted");
                    return null;
                }
            }
        }
    }

    @Test
    public void testStrategyTakingTooLong() {
        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(8, 4, 100, 1000);
        Config config = createClusterConfig(100, 500, 500, null);
        List<String> allowedSchedulerStrategies = new ArrayList<>();
        allowedSchedulerStrategies.add(getDefaultResourceAwareStrategyClass().getName());
        allowedSchedulerStrategies.add(DefaultResourceAwareStrategyOld.class.getName());
        allowedSchedulerStrategies.add(NeverEndingSchedulingStrategy.class.getName());
        config.put(Config.NIMBUS_SCHEDULER_STRATEGY_CLASS_WHITELIST, allowedSchedulerStrategies);
        config.put(DaemonConfig.SCHEDULING_TIMEOUT_SECONDS_PER_TOPOLOGY, 30);

        TopologyDetails topo1 = genTopology("topo-1", config, 1, 0, 2, 0, currentTime - 2, 10, "jerry");
        TopologyDetails topo3 = genTopology("topo-3", config, 1, 2, 1, 1, currentTime - 2, 20, "jerry");

        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, NeverEndingSchedulingStrategy.class.getName());
        TopologyDetails topo2 = genTopology("topo-2", config, 2, 0, 2, 0, currentTime - 2, 20, "jerry");

        Topologies topologies = new Topologies(topo1, topo2, topo3);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);
        scheduler = new ResourceAwareScheduler();

        scheduler.prepare(config, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);

        assertFalse(cluster.needsSchedulingRas(topo1));
        assertTrue(cluster.needsSchedulingRas(topo2));
        assertFalse(cluster.needsSchedulingRas(topo3));

        assertTrue("Topo-1 scheduled?", cluster.getAssignmentById(topo1.getId()) != null);
        assertEquals("Topo-1 all executors scheduled?", 2, cluster.getAssignmentById(topo1.getId()).getExecutorToSlot().size());
        assertTrue("Topo-2 not scheduled", cluster.getAssignmentById(topo2.getId()) == null);
        assertEquals("Scheduling took too long for " + topo2.getId() + " using strategy "
                + NeverEndingSchedulingStrategy.class.getName()
                + " timeout after 30 seconds using config scheduling.timeout.seconds.per.topology.", cluster.getStatusMap().get(topo2.getId()));
        assertTrue("Topo-3 scheduled?", cluster.getAssignmentById(topo3.getId()) != null);
        assertEquals("Topo-3 all executors scheduled?", 3, cluster.getAssignmentById(topo3.getId()).getExecutorToSlot().size());
    }

    public void testLargeTopologiesCommon(final String strategy, final boolean includeGpu, final int multiplier) {
        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisorsWithRacks(25 * multiplier, 40, 66, 3 * multiplier, 0, 4700, 226200, new HashMap<>());
        if (includeGpu) {
            HashMap<String, Double> extraResources = new HashMap<>();
            extraResources.put("my.gpu", 1.0);
            supMap.putAll(genSupervisorsWithRacks(3 * multiplier, 40, 66, 0, 0, 4700, 226200, extraResources));
        }

        Config config = new Config();
        config.putAll(createClusterConfig(88, 775, 25, null));
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, strategy);

        scheduler = new ResourceAwareScheduler();

        Map<String, TopologyDetails> topologyDetailsMap = new HashMap<>();
        for (int i = 0; i < 11 * multiplier; i++) {
            TopologyDetails td = genTopology(String.format("topology-%05d", i), config, 5,
                40, 30, 114, 0, 0, "user", 8192);
            topologyDetailsMap.put(td.getId(), td);
        }
        if (includeGpu) {
            for (int i = 0; i < multiplier; i++) {
                TopologyBuilder builder = topologyBuilder(5, 40, 30, 114);
                builder.setBolt("gpu-bolt", new TestBolt(), 40)
                    .addResource("my.gpu", 1.0)
                    .shuffleGrouping("spout-0");
                TopologyDetails td = topoToTopologyDetails(String.format("topology-gpu-%05d", i), config, builder.createTopology(), 0, 0,
                    "user", 8192);
                topologyDetailsMap.put(td.getId(), td);
            }
        }
        Topologies topologies = new Topologies(topologyDetailsMap);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);

        long startTime = Time.currentTimeMillis();
        scheduler.prepare(config, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);
        long schedulingDuration = Time.currentTimeMillis() - startTime;
        LOG.info("Scheduling took " + schedulingDuration + " ms");
        LOG.info("HAS {} SLOTS USED", cluster.getUsedSlots().size());

        Map<String, SchedulerAssignment> assignments = new TreeMap<>(cluster.getAssignments());

        for (Entry<String, SchedulerAssignment> entry: assignments.entrySet()) {
            SchedulerAssignment sa = entry.getValue();
            Map<String, AtomicLong> slotsPerRack = new TreeMap<>();
            for (WorkerSlot slot : sa.getSlots()) {
                String nodeId = slot.getNodeId();
                String rack = supervisorIdToRackName(nodeId);
                slotsPerRack.computeIfAbsent(rack, (r) -> new AtomicLong(0)).incrementAndGet();
            }
            LOG.info("{} => {}", entry.getKey(), slotsPerRack);
        }
    }

    public static void main(String[] args) {
        String strategy = DefaultResourceAwareStrategy.class.getName();
        if (args.length > 0) {
            strategy = args[0];
        }
        boolean includeGpu = false;
        if (args.length > 1) {
            includeGpu = Boolean.valueOf(args[1]);
        }
        int multiplier = 1;
        if (args.length > 2) {
            multiplier = Integer.valueOf(args[2]);
        }
        TestResourceAwareScheduler trs = new TestResourceAwareScheduler();
        trs.testLargeTopologiesCommon(strategy, includeGpu, multiplier);
        System.exit(0);
    }
}
