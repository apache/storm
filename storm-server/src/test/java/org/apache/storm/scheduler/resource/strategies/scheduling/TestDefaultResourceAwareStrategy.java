/*
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

import org.apache.storm.daemon.StormCommon;
import org.apache.storm.daemon.nimbus.Nimbus;
import org.apache.storm.daemon.nimbus.TopologyResources;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourcesExtension;
import java.util.Collections;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.networktopography.DNSToSwitchMapping;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.SupervisorResources;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.RasNode;
import org.apache.storm.scheduler.resource.ResourceAwareScheduler;
import org.apache.storm.scheduler.resource.SchedulingResult;
import org.apache.storm.scheduler.resource.strategies.scheduling.sorter.INodeSorter;
import org.apache.storm.scheduler.resource.strategies.scheduling.sorter.NodeSorter;
import org.apache.storm.scheduler.resource.strategies.scheduling.sorter.NodeSorterHostProximity;
import org.apache.storm.topology.SharedOffHeapWithinNode;
import org.apache.storm.topology.SharedOffHeapWithinWorker;
import org.apache.storm.topology.SharedOnHeap;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.ServerUtils;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.resource.normalization.ResourceMetrics;

@ExtendWith({NormalizedResourcesExtension.class})
public class TestDefaultResourceAwareStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(TestDefaultResourceAwareStrategy.class);

    private static final int CURRENT_TIME = 1450418597;
    private static IScheduler scheduler = null;
    private enum SharedMemoryType {
        SHARED_OFF_HEAP_NODE,
        SHARED_OFF_HEAP_WORKER,
        SHARED_ON_HEAP_WORKER
    };
    protected enum WorkerRestrictionType {
        WORKER_RESTRICTION_ONE_EXECUTOR,
        WORKER_RESTRICTION_ONE_COMPONENT,
        WORKER_RESTRICTION_NONE
    };

    protected Class getDefaultResourceAwareStrategyClass() {
        return DefaultResourceAwareStrategy.class;
    }

    private Config createClusterConfig(double compPcore, double compOnHeap, double compOffHeap,
                                             Map<String, Map<String, Number>> pools) {
        Config config = TestUtilsForResourceAwareScheduler.createClusterConfig(compPcore, compOnHeap, compOffHeap, pools);
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, getDefaultResourceAwareStrategyClass().getName());
        return config;
    }

    private static class TestDNSToSwitchMapping implements DNSToSwitchMapping {
        private final Map<String, String> result;

        public TestDNSToSwitchMapping(Map<String, SupervisorDetails> ... racks) {
            Map<String, String> ret = new HashMap<>();
            for (int rackNum = 0; rackNum < racks.length; rackNum++) {
                String rack = "rack-" + rackNum;
                for (SupervisorDetails sup : racks[rackNum].values()) {
                    ret.put(sup.getHost(), rack);
                }
            }
            result = Collections.unmodifiableMap(ret);
        }

        @Override
        public Map<String, String> resolve(List<String> names) {
            return result;
        }
    };

    @AfterEach
    public void cleanup() {
        if (scheduler != null) {
            scheduler.cleanup();
            scheduler = null;
        }
    }

    /*
     * test assigned memory with shared memory types and oneWorkerPerExecutor
     */
    @ParameterizedTest
    @EnumSource(SharedMemoryType.class)
    public void testMultipleSharedMemoryWithOneExecutorPerWorker(SharedMemoryType memoryType) {
        int spoutParallelism = 4;
        double cpuPercent = 10;
        double memoryOnHeap = 10;
        double memoryOffHeap = 10;
        double sharedOnHeapWithinWorker = 450;
        double sharedOffHeapWithinNode = 600;
        double sharedOffHeapWithinWorker = 400;

        TopologyBuilder builder = new TopologyBuilder();
        switch (memoryType) {
            case SHARED_OFF_HEAP_NODE:
                builder.setSpout("spout", new TestSpout(), spoutParallelism)
                        .addSharedMemory(new SharedOffHeapWithinNode(sharedOffHeapWithinNode, "spout shared off heap within node"));
                break;
            case SHARED_OFF_HEAP_WORKER:
                builder.setSpout("spout", new TestSpout(), spoutParallelism)
                        .addSharedMemory(new SharedOffHeapWithinWorker(sharedOffHeapWithinWorker, "spout shared off heap within worker"));
                break;
            case SHARED_ON_HEAP_WORKER:
                builder.setSpout("spout", new TestSpout(), spoutParallelism)
                        .addSharedMemory(new SharedOnHeap(sharedOnHeapWithinWorker, "spout shared on heap within worker"));
                break;
        }
        StormTopology stormToplogy = builder.createTopology();
        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(4, 4, 500, 1000);
        Config conf = createClusterConfig(cpuPercent, memoryOnHeap, memoryOffHeap, null);

        conf.put(Config.TOPOLOGY_PRIORITY, 0);
        conf.put(Config.TOPOLOGY_NAME, "testTopology");
        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 2000);
        conf.put(Config.TOPOLOGY_RAS_ONE_EXECUTOR_PER_WORKER, true);
        TopologyDetails topo = new TopologyDetails("testTopology-id", conf, stormToplogy, 0,
                genExecsAndComps(stormToplogy), CURRENT_TIME, "user");

        Topologies topologies = new Topologies(topo);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, conf);

        scheduler = new ResourceAwareScheduler();
        scheduler.prepare(conf, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);

        TopologyResources topologyResources = cluster.getTopologyResourcesMap().get(topo.getId());
        SchedulerAssignment assignment = cluster.getAssignmentById(topo.getId());
        long numNodes = assignment.getSlotToExecutors().keySet().stream().map(ws -> ws.getNodeId()).distinct().count();

        switch (memoryType) {
            case SHARED_OFF_HEAP_NODE:
                // 4 workers on single node. OffHeapNode memory is shared
                assertThat(topologyResources.getAssignedMemOnHeap(), closeTo(spoutParallelism * memoryOnHeap, 0.01));
                assertThat(topologyResources.getAssignedMemOffHeap(), closeTo(spoutParallelism * memoryOffHeap + sharedOffHeapWithinNode, 0.01));
                assertThat(topologyResources.getAssignedSharedMemOnHeap(), closeTo(0, 0.01));
                assertThat(topologyResources.getAssignedSharedMemOffHeap(), closeTo(sharedOffHeapWithinNode, 0.01));
                assertThat(topologyResources.getAssignedNonSharedMemOnHeap(), closeTo(spoutParallelism * memoryOnHeap, 0.01));
                assertThat(topologyResources.getAssignedNonSharedMemOffHeap(), closeTo(spoutParallelism * memoryOffHeap, 0.01));
                assertThat(numNodes, is(1L));
                assertThat(cluster.getAssignedNumWorkers(topo), is(spoutParallelism));
                break;
            case SHARED_OFF_HEAP_WORKER:
                // 4 workers on 2 nodes. OffHeapWorker memory not shared -- consumed 4x, once for each worker)
                assertThat(topologyResources.getAssignedMemOnHeap(), closeTo(spoutParallelism * memoryOnHeap, 0.01));
                assertThat(topologyResources.getAssignedMemOffHeap(), closeTo(spoutParallelism * (memoryOffHeap + sharedOffHeapWithinWorker), 0.01));
                assertThat(topologyResources.getAssignedSharedMemOnHeap(), closeTo(0, 0.01));
                assertThat(topologyResources.getAssignedSharedMemOffHeap(), closeTo(spoutParallelism * sharedOffHeapWithinWorker, 0.01));
                assertThat(topologyResources.getAssignedNonSharedMemOnHeap(), closeTo(spoutParallelism * memoryOnHeap, 0.01));
                assertThat(topologyResources.getAssignedNonSharedMemOffHeap(), closeTo(spoutParallelism * memoryOffHeap, 0.01));
                assertThat(numNodes, is(2L));
                assertThat(cluster.getAssignedNumWorkers(topo), is(spoutParallelism));
                break;
            case SHARED_ON_HEAP_WORKER:
                // 4 workers on 2 nodes. onHeap memory not shared -- consumed 4x, once for each worker
                assertThat(topologyResources.getAssignedMemOnHeap(), closeTo(spoutParallelism * (memoryOnHeap + sharedOnHeapWithinWorker), 0.01));
                assertThat(topologyResources.getAssignedMemOffHeap(), closeTo(spoutParallelism * memoryOffHeap, 0.01));
                assertThat(topologyResources.getAssignedSharedMemOnHeap(), closeTo(spoutParallelism * sharedOnHeapWithinWorker, 0.01));
                assertThat(topologyResources.getAssignedSharedMemOffHeap(), closeTo(0, 0.01));
                assertThat(topologyResources.getAssignedNonSharedMemOnHeap(), closeTo(spoutParallelism * memoryOnHeap, 0.01));
                assertThat(topologyResources.getAssignedNonSharedMemOffHeap(), closeTo(spoutParallelism * memoryOffHeap, 0.01));
                assertThat(numNodes, is(2L));
                assertThat(cluster.getAssignedNumWorkers(topo), is(spoutParallelism));
                break;
        }
    }

    /*
     * test scheduling does not cause negative resources
     */
    @Test
    public void testSchedulingNegativeResources() {
        int spoutParallelism = 2;
        int boltParallelism = 2;
        double cpuPercent = 10;
        double memoryOnHeap = 10;
        double memoryOffHeap = 10;
        double sharedOnHeapWithinWorker = 400;
        double sharedOffHeapWithinNode = 700;
        double sharedOffHeapWithinWorker = 500;

        Config conf = createClusterConfig(cpuPercent, memoryOnHeap, memoryOffHeap, null);
        TopologyDetails[] topo = new TopologyDetails[2];

        // 1st topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new TestSpout(),
                spoutParallelism);
        builder.setBolt("bolt-1", new TestBolt(),
                boltParallelism).addSharedMemory(new SharedOffHeapWithinWorker(sharedOffHeapWithinWorker, "bolt-1 shared off heap within worker")).shuffleGrouping("spout");
        builder.setBolt("bolt-2", new TestBolt(),
                boltParallelism).addSharedMemory(new SharedOffHeapWithinNode(sharedOffHeapWithinNode, "bolt-2 shared off heap within node")).shuffleGrouping("bolt-1");
        builder.setBolt("bolt-3", new TestBolt(),
                boltParallelism).addSharedMemory(new SharedOnHeap(sharedOnHeapWithinWorker, "bolt-3 shared on heap within worker")).shuffleGrouping("bolt-2");
        StormTopology stormToplogy = builder.createTopology();

        conf.put(Config.TOPOLOGY_PRIORITY, 1);
        conf.put(Config.TOPOLOGY_NAME, "testTopology-0");
        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 2000);
        topo[0] = new TopologyDetails("testTopology-id-0", conf, stormToplogy, 0,
                genExecsAndComps(stormToplogy), CURRENT_TIME, "user");

        // 2nd topology
        builder = new TopologyBuilder();
        builder.setSpout("spout", new TestSpout(),
                spoutParallelism).addSharedMemory(new SharedOffHeapWithinNode(sharedOffHeapWithinNode, "spout shared off heap within node"));
        stormToplogy = builder.createTopology();

        conf.put(Config.TOPOLOGY_PRIORITY, 0);
        conf.put(Config.TOPOLOGY_NAME, "testTopology-1");
        topo[1] = new TopologyDetails("testTopology-id-1", conf, stormToplogy, 0,
                genExecsAndComps(stormToplogy), CURRENT_TIME, "user");

        Map<String, SupervisorDetails> supMap = genSupervisors(1, 4, 500, 2000);
        Topologies topologies = new Topologies(topo[0]);
        Cluster cluster = new Cluster(new INimbusTest(), new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, conf);

        // schedule 1st topology
        scheduler = new ResourceAwareScheduler();
        scheduler.prepare(conf, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);
        assertTopologiesFullyScheduled(cluster, topo[0].getName());

        // attempt scheduling both topologies.
        // this triggered negative resource event as the second topology incorrectly scheduled with the first in place
        // first topology should get evicted for higher priority (lower value) second topology to successfully schedule
        topologies = new Topologies(topo[0], topo[1]);
        cluster = new Cluster(cluster, topologies);
        scheduler.schedule(topologies, cluster);
        assertTopologiesNotScheduled(cluster, topo[0].getName());
        assertTopologiesFullyScheduled(cluster, topo[1].getName());

        // check negative resource count
        assertThat(cluster.getResourceMetrics().getNegativeResourceEventsMeter().getCount(), is(0L));
    }

    /**
     * test if the scheduling shared memory is correct with/without oneExecutorPerWorker enabled
     */
    @ParameterizedTest
    @EnumSource(WorkerRestrictionType.class)
    public void testDefaultResourceAwareStrategySharedMemory(WorkerRestrictionType schedulingLimitation) {
        int spoutParallelism = 2;
        int boltParallelism = 2;
        int numBolts = 3;
        double cpuPercent = 10;
        double memoryOnHeap = 10;
        double memoryOffHeap = 10;
        double sharedOnHeapWithinWorker = 400;
        double sharedOffHeapWithinNode = 700;
        double sharedOffHeapWithinWorker = 600;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new TestSpout(),
                spoutParallelism);
        builder.setBolt("bolt-1", new TestBolt(),
                boltParallelism).addSharedMemory(new SharedOffHeapWithinWorker(sharedOffHeapWithinWorker, "bolt-1 shared off heap within worker")).shuffleGrouping("spout");
        builder.setBolt("bolt-2", new TestBolt(),
                boltParallelism).addSharedMemory(new SharedOffHeapWithinNode(sharedOffHeapWithinNode, "bolt-2 shared off heap within node")).shuffleGrouping("bolt-1");
        builder.setBolt("bolt-3", new TestBolt(),
                boltParallelism).addSharedMemory(new SharedOnHeap(sharedOnHeapWithinWorker, "bolt-3 shared on heap within worker")).shuffleGrouping("bolt-2");

        StormTopology stormToplogy = builder.createTopology();

        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(4, 4, 500, 2000);
        Config conf = createClusterConfig(cpuPercent, memoryOnHeap, memoryOffHeap, null);

        conf.put(Config.TOPOLOGY_PRIORITY, 0);
        conf.put(Config.TOPOLOGY_NAME, "testTopology");
        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 2000);
        switch (schedulingLimitation) {
            case WORKER_RESTRICTION_ONE_EXECUTOR:
                conf.put(Config.TOPOLOGY_RAS_ONE_EXECUTOR_PER_WORKER, true);
                break;
            case WORKER_RESTRICTION_ONE_COMPONENT:
                conf.put(Config.TOPOLOGY_RAS_ONE_COMPONENT_PER_WORKER, true);
                break;
        }
        TopologyDetails topo = new TopologyDetails("testTopology-id", conf, stormToplogy, 0,
                genExecsAndComps(stormToplogy), CURRENT_TIME, "user");

        Topologies topologies = new Topologies(topo);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, conf);

        scheduler = new ResourceAwareScheduler();
        scheduler.prepare(conf, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);

        // [3,3] [7,7], [0,0] [2,2] [6,6] [1,1] [5,5] [4,4] sorted executor ordering
        // spout  [0,0] [1,1]
        // bolt-1 [2,2] [3,3]
        // bolt-2 [6,6] [7,7]
        // bolt-3 [4,4] [5,5]

        // WorkerRestrictionType.WORKER_RESTRICTION_NONE
        // expect 1 worker, 1 node

        // WorkerRestrictionType.WORKER_RESTRICTION_ONE_EXECUTOR
        // expect 8 workers, 2 nodes
        // node r000s000 workers: bolt-1 bolt-2 spout bolt-1 (no memory sharing)
        // node r000s001 workers: bolt-2 spout bolt-3 bolt-3 (no memory sharing)

        // WorkerRestrictionType.WORKER_RESTRICTION_ONE_COMPONENT
        // expect 4 workers, 1 node

        for (Entry<String, SupervisorResources> entry: cluster.getSupervisorsResourcesMap().entrySet()) {
            String supervisorId = entry.getKey();
            SupervisorResources resources = entry.getValue();
            assertTrue(supervisorId, resources.getTotalCpu() >= resources.getUsedCpu());
            assertTrue(supervisorId, resources.getTotalMem() >= resources.getUsedMem());
        }

        int totalNumberOfTasks = spoutParallelism + boltParallelism * numBolts;
        SchedulerAssignment assignment = cluster.getAssignmentById(topo.getId());
        TopologyResources topologyResources = cluster.getTopologyResourcesMap().get(topo.getId());
        long numNodes = assignment.getSlotToExecutors().keySet().stream().map(WorkerSlot::getNodeId).distinct().count();
        String assignmentString = "Assignments:\n\t" + assignment.getSlotToExecutors().entrySet().stream()
                .map(x -> String.format("Node=%s, components=%s",
                        x.getKey().getNodeId(),
                        x.getValue().stream()
                                .map(y -> topo.getComponentFromExecutor(y))
                                .collect(Collectors.joining(","))
                        )
                )
                .collect(Collectors.joining("\n\t"));

        if (schedulingLimitation == WorkerRestrictionType.WORKER_RESTRICTION_NONE) {
            // Everything should fit in a single slot
            double totalExpectedCPU = totalNumberOfTasks * cpuPercent;
            double totalExpectedOnHeap = (totalNumberOfTasks * memoryOnHeap) + sharedOnHeapWithinWorker;
            double totalExpectedWorkerOffHeap = (totalNumberOfTasks * memoryOffHeap) + sharedOffHeapWithinWorker;

            assertThat(assignment.getSlots().size(), is(1));
            WorkerSlot ws = assignment.getSlots().iterator().next();
            String nodeId = ws.getNodeId();
            assertThat(assignment.getNodeIdToTotalSharedOffHeapNodeMemory().size(), is(1));
            assertThat(assignment.getNodeIdToTotalSharedOffHeapNodeMemory().get(nodeId), closeTo(sharedOffHeapWithinNode, 0.01));
            assertThat(assignment.getScheduledResources().size(), is(1));
            WorkerResources resources = assignment.getScheduledResources().get(ws);
            assertThat(resources.get_cpu(), closeTo(totalExpectedCPU, 0.01));
            assertThat(resources.get_mem_on_heap(), closeTo(totalExpectedOnHeap, 0.01));
            assertThat(resources.get_mem_off_heap(), closeTo(totalExpectedWorkerOffHeap, 0.01));
            assertThat(resources.get_shared_mem_on_heap(), closeTo(sharedOnHeapWithinWorker, 0.01));
            assertThat(resources.get_shared_mem_off_heap(), closeTo(sharedOffHeapWithinWorker, 0.01));
        } else if (schedulingLimitation == WorkerRestrictionType.WORKER_RESTRICTION_ONE_EXECUTOR) {
            double expectedMemOnHeap = (totalNumberOfTasks * memoryOnHeap) + 2 * sharedOnHeapWithinWorker;
            double expectedMemOffHeap = (totalNumberOfTasks * memoryOffHeap) + 2 * sharedOffHeapWithinWorker + 2 * sharedOffHeapWithinNode;
            double expectedMemSharedOnHeap = 2 * sharedOnHeapWithinWorker;
            double expectedMemSharedOffHeap = 2 * sharedOffHeapWithinWorker + 2 * sharedOffHeapWithinNode;
            double expectedMemNonSharedOnHeap = totalNumberOfTasks * memoryOnHeap;
            double expectedMemNonSharedOffHeap = totalNumberOfTasks * memoryOffHeap;
            assertThat(topologyResources.getAssignedMemOnHeap(), closeTo(expectedMemOnHeap, 0.01));
            assertThat(topologyResources.getAssignedMemOffHeap(), closeTo(expectedMemOffHeap, 0.01));
            assertThat(topologyResources.getAssignedSharedMemOnHeap(), closeTo(expectedMemSharedOnHeap, 0.01));
            assertThat(topologyResources.getAssignedSharedMemOffHeap(), closeTo(expectedMemSharedOffHeap, 0.01));
            assertThat(topologyResources.getAssignedNonSharedMemOnHeap(), closeTo(expectedMemNonSharedOnHeap, 0.01));
            assertThat(topologyResources.getAssignedNonSharedMemOffHeap(), closeTo(expectedMemNonSharedOffHeap, 0.01));

            double totalExpectedCPU = totalNumberOfTasks * cpuPercent;
            assertThat(topologyResources.getAssignedCpu(), closeTo(totalExpectedCPU, 0.01));
            int numAssignedWorkers = cluster.getAssignedNumWorkers(topo);
            assertThat(numAssignedWorkers, is(8));
            assertThat(assignment.getSlots().size(), is(8));
            assertThat(assignmentString, numNodes, is(2L));
        } else if (schedulingLimitation == WorkerRestrictionType.WORKER_RESTRICTION_ONE_COMPONENT) {
            double expectedMemOnHeap = (totalNumberOfTasks * memoryOnHeap) + sharedOnHeapWithinWorker;
            double expectedMemOffHeap = (totalNumberOfTasks * memoryOffHeap) + sharedOffHeapWithinWorker + sharedOffHeapWithinNode;
            double expectedMemSharedOnHeap = sharedOnHeapWithinWorker;
            double expectedMemSharedOffHeap = sharedOffHeapWithinWorker + sharedOffHeapWithinNode;
            double expectedMemNonSharedOnHeap = totalNumberOfTasks * memoryOnHeap;
            double expectedMemNonSharedOffHeap = totalNumberOfTasks * memoryOffHeap;
            assertThat(topologyResources.getAssignedMemOnHeap(), closeTo(expectedMemOnHeap, 0.01));
            assertThat(topologyResources.getAssignedMemOffHeap(), closeTo(expectedMemOffHeap, 0.01));
            assertThat(topologyResources.getAssignedSharedMemOnHeap(), closeTo(expectedMemSharedOnHeap, 0.01));
            assertThat(topologyResources.getAssignedSharedMemOffHeap(), closeTo(expectedMemSharedOffHeap, 0.01));
            assertThat(topologyResources.getAssignedNonSharedMemOnHeap(), closeTo(expectedMemNonSharedOnHeap, 0.01));
            assertThat(topologyResources.getAssignedNonSharedMemOffHeap(), closeTo(expectedMemNonSharedOffHeap, 0.01));

            double totalExpectedCPU = totalNumberOfTasks * cpuPercent;
            assertThat(topologyResources.getAssignedCpu(), closeTo(totalExpectedCPU, 0.01));
            int numAssignedWorkers = cluster.getAssignedNumWorkers(topo);
            assertThat(numAssignedWorkers, is(4));
            assertThat(assignment.getSlots().size(), is(4));
            assertThat(numNodes, is(1L));
        }
    }

    /**
     * test if the scheduling logic for the DefaultResourceAwareStrategy is correct
     * when topology.acker.executors.per.worker is set to different values.
     *
     * If {@link Config#TOPOLOGY_ACKER_EXECUTORS} is not set,
     * it will be calculated by Nimbus as (num of estimated worker * topology.acker.executors.per.worker).
     * In this test, {@link Config#TOPOLOGY_ACKER_EXECUTORS} is set to 2 (num of estimated workers based on topo resources usage)
     *
     * For different value for {@link Config#TOPOLOGY_RAS_ACKER_EXECUTORS_PER_WORKER}:
     * -1: Note we don't really set it to be -1.
     *     It is just a special case in this test that topology.acker.executors.per.worker is unset, nimbus will set to 1 by default.
     * 0:  Since {@link Config#TOPOLOGY_ACKER_EXECUTORS} is not set either, acking is disabled.
     * 1:  2 ackers in total. Distribute 1 acker per worker. With ackers being added, this topology will now need 3 workers.
     *     Then first two worker will get 1 acker and last worker get 0.
     * 2:  4 ackers in total. First two workers will get 2 acker per worker respectively.
     */
    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 1, 2})
    public void testDefaultResourceAwareStrategyWithoutSettingAckerExecutors(int numOfAckersPerWorker)
        throws InvalidTopologyException {
        int spoutParallelism = 1;
        int boltParallelism = 2;
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new TestSpout(),
            spoutParallelism);
        builder.setBolt("bolt-1", new TestBolt(),
            boltParallelism).shuffleGrouping("spout");
        builder.setBolt("bolt-2", new TestBolt(),
            boltParallelism).shuffleGrouping("bolt-1");
        builder.setBolt("bolt-3", new TestBolt(),
            boltParallelism).shuffleGrouping("bolt-2");

        String topoName = "testTopology";

        StormTopology stormToplogy = builder.createTopology();

        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(4, 4, 200, 2000);
        Config conf = createClusterConfig(50, 450, 0, null);
        conf.put(Config.TOPOLOGY_PRIORITY, 0);
        conf.put(Config.TOPOLOGY_NAME, topoName);
        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 2000);
        conf.put(Config.TOPOLOGY_SUBMITTER_USER, "user");

        // Topology needs 2 workers (estimated by nimbus based on resources),
        // but with ackers added, probably more worker will be launched.
        // Parameterized test on different numOfAckersPerWorker
        if (numOfAckersPerWorker == -1) {
            // Both Config.TOPOLOGY_ACKER_EXECUTORS and Config.TOPOLOGY_RAS_ACKER_EXECUTORS_PER_WORKER are not set
            // Default will be 2 (estimate num of workers) and 1 respectively
        } else {
            conf.put(Config.TOPOLOGY_RAS_ACKER_EXECUTORS_PER_WORKER, numOfAckersPerWorker);
        }

        int estimatedNumWorker = ServerUtils.getEstimatedWorkerCountForRasTopo(conf, stormToplogy);
        Nimbus.setUpAckerExecutorConfigs(topoName, conf, conf, estimatedNumWorker);

        conf.put(Config.TOPOLOGY_ACKER_RESOURCES_ONHEAP_MEMORY_MB, 250);
        conf.put(Config.TOPOLOGY_ACKER_CPU_PCORE_PERCENT, 50);

        TopologyDetails topo = new TopologyDetails("testTopology-id", conf, stormToplogy, 0,
            genExecsAndComps(StormCommon.systemTopology(conf, stormToplogy)), CURRENT_TIME, "user");

        Topologies topologies = new Topologies(topo);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, conf);

        scheduler = new ResourceAwareScheduler();

        scheduler.prepare(conf, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);

        // Ordered execs: [[6, 6], [2, 2], [4, 4], [5, 5], [1, 1], [3, 3], [0, 0], [8, 8], [7, 7]]
        // Ackers: [[8, 8], [7, 7]] (+ [[9, 9], [10, 10]] when numOfAckersPerWorker=2)
        HashSet<HashSet<ExecutorDetails>> expectedScheduling = new HashSet<>();
        if (numOfAckersPerWorker == -1 || numOfAckersPerWorker == 1) {
            // Setting topology.acker.executors = null and topology.acker.executors.per.worker = null
            // are equivalent to topology.acker.executors = null and topology.acker.executors.per.worker = 1
            expectedScheduling.add(new HashSet<>(Arrays.asList(
                new ExecutorDetails(6, 6), //bolt-3
                new ExecutorDetails(2, 2), //bolt-1
                new ExecutorDetails(4, 4), //bolt-2
                new ExecutorDetails(8, 8)))); //acker
            expectedScheduling.add(new HashSet<>(Arrays.asList(
                new ExecutorDetails(5, 5), //bolt-3
                new ExecutorDetails(1, 1), //bolt-1
                new ExecutorDetails(3, 3), //bolt-2
                new ExecutorDetails(7, 7)))); //acker
            expectedScheduling.add(new HashSet<>(Arrays.asList(
                new ExecutorDetails(0, 0)))); //spout
        } else if (numOfAckersPerWorker == 0) {
            expectedScheduling.add(new HashSet<>(Arrays.asList(
                new ExecutorDetails(6, 6), //bolt-3
                new ExecutorDetails(2, 2), //bolt-1
                new ExecutorDetails(4, 4), //bolt-2
                new ExecutorDetails(5, 5)))); //bolt-3
            expectedScheduling.add(new HashSet<>(Arrays.asList(
                new ExecutorDetails(0, 0), //spout
                new ExecutorDetails(3, 3), //bolt-2
                new ExecutorDetails(1, 1)))); //bolt-1
        } else if (numOfAckersPerWorker == 2) {
            expectedScheduling.add(new HashSet<>(Arrays.asList(
                new ExecutorDetails(6, 6), //bolt-3
                new ExecutorDetails(2, 2), //bolt-1
                new ExecutorDetails(7, 7), //acker
                new ExecutorDetails(8, 8)))); //acker
            expectedScheduling.add(new HashSet<>(Arrays.asList(
                new ExecutorDetails(4, 4), //bolt-2
                new ExecutorDetails(5, 5), //bolt-3
                new ExecutorDetails(9, 9), //acker
                new ExecutorDetails(10, 10)))); //acker
            expectedScheduling.add(new HashSet<>(Arrays.asList(
                new ExecutorDetails(1, 1), //bolt-1
                new ExecutorDetails(3, 3), //bolt-2
                new ExecutorDetails(0, 0)))); //spout
        }
        HashSet<HashSet<ExecutorDetails>> foundScheduling = new HashSet<>();
        SchedulerAssignment assignment = cluster.getAssignmentById("testTopology-id");
        for (Collection<ExecutorDetails> execs : assignment.getSlotToExecutors().values()) {
            foundScheduling.add(new HashSet<>(execs));
        }

        Assert.assertEquals(expectedScheduling, foundScheduling);
    }

    /**
     * test if the scheduling logic for the DefaultResourceAwareStrategy is correct
     * when topology.acker.executors is set.
     *
     * If yes, topology.acker.executors.per.worker setting will be ignored and calculated as
     * Math.ceil(topology.acker.executors / estimate num of workers) by Nimbus
     */
    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 2, 300})
    public void testDefaultResourceAwareStrategyWithSettingAckerExecutors(int numOfAckersPerWorker)
        throws InvalidTopologyException {

        int spoutParallelism = 1;
        int boltParallelism = 2;
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new TestSpout(),
            spoutParallelism);
        builder.setBolt("bolt-1", new TestBolt(),
            boltParallelism).shuffleGrouping("spout");
        builder.setBolt("bolt-2", new TestBolt(),
            boltParallelism).shuffleGrouping("bolt-1");
        builder.setBolt("bolt-3", new TestBolt(),
            boltParallelism).shuffleGrouping("bolt-2");

        String topoName = "testTopology";

        StormTopology stormToplogy = builder.createTopology();

        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(4, 4, 200, 2000);
        Config conf = createClusterConfig(50, 450, 0, null);
        conf.put(Config.TOPOLOGY_PRIORITY, 0);
        conf.put(Config.TOPOLOGY_NAME, topoName);
        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 2000);
        conf.put(Config.TOPOLOGY_SUBMITTER_USER, "user");

        conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 4);
        conf.put(Config.TOPOLOGY_RAS_ACKER_EXECUTORS_PER_WORKER, numOfAckersPerWorker);


        if (numOfAckersPerWorker == -1) {
            // Leave topology.acker.executors.per.worker unset
        } else {
            conf.put(Config.TOPOLOGY_RAS_ACKER_EXECUTORS_PER_WORKER, numOfAckersPerWorker);
        }

        int estimatedNumWorker = ServerUtils.getEstimatedWorkerCountForRasTopo(conf, stormToplogy);
        Nimbus.setUpAckerExecutorConfigs(topoName, conf, conf, estimatedNumWorker);

        conf.put(Config.TOPOLOGY_ACKER_RESOURCES_ONHEAP_MEMORY_MB, 250);
        conf.put(Config.TOPOLOGY_ACKER_CPU_PCORE_PERCENT, 50);

        TopologyDetails topo = new TopologyDetails("testTopology-id", conf, stormToplogy, 0,
            genExecsAndComps(StormCommon.systemTopology(conf, stormToplogy)), CURRENT_TIME, "user");

        Topologies topologies = new Topologies(topo);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, conf);

        scheduler = new ResourceAwareScheduler();

        scheduler.prepare(conf, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);

        // Sorted execs: [[6, 6], [2, 2], [4, 4], [5, 5], [1, 1], [3, 3], [0, 0], [8, 8], [7, 7], [10, 10], [9, 9]]
        // Ackers: [[8, 8], [7, 7], [10, 10], [9, 9]]

        HashSet<HashSet<ExecutorDetails>> expectedScheduling = new HashSet<>();
        expectedScheduling.add(new HashSet<>(Arrays.asList(
            new ExecutorDetails(6, 6), //bolt-3
            new ExecutorDetails(2, 2), //bolt-1
            new ExecutorDetails(7, 7), //acker
            new ExecutorDetails(8, 8)))); //acker
        expectedScheduling.add(new HashSet<>(Arrays.asList(
            new ExecutorDetails(5, 5), //bolt-3
            new ExecutorDetails(4, 4), //bolt-2
            new ExecutorDetails(9, 9), //acker
            new ExecutorDetails(10, 10)))); //acker
        expectedScheduling.add(new HashSet<>(Arrays.asList(
            new ExecutorDetails(0, 0), //spout
            new ExecutorDetails(3, 3), //bolt-2
            new ExecutorDetails(1, 1)))); //bolt-1

        HashSet<HashSet<ExecutorDetails>> foundScheduling = new HashSet<>();
        SchedulerAssignment assignment = cluster.getAssignmentById("testTopology-id");
        for (Collection<ExecutorDetails> execs : assignment.getSlotToExecutors().values()) {
            foundScheduling.add(new HashSet<>(execs));
        }

        Assert.assertEquals(expectedScheduling, foundScheduling);
    }

    /**
     * test if the scheduling logic for the DefaultResourceAwareStrategy (when made by network proximity needs.) is correct
     */
    @Test
    public void testDefaultResourceAwareStrategyInFavorOfShuffle()
        throws InvalidTopologyException {
        int spoutParallelism = 1;
        int boltParallelism = 2;
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new TestSpout(),
            spoutParallelism);
        builder.setBolt("bolt-1", new TestBolt(),
            boltParallelism).shuffleGrouping("spout");
        builder.setBolt("bolt-2", new TestBolt(),
            boltParallelism).shuffleGrouping("bolt-1");
        builder.setBolt("bolt-3", new TestBolt(),
            boltParallelism).shuffleGrouping("bolt-2");

        StormTopology stormToplogy = builder.createTopology();

        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(4, 4, 200, 2000);
        Config conf = createClusterConfig(50, 250, 250, null);
        conf.put(Config.TOPOLOGY_PRIORITY, 0);
        conf.put(Config.TOPOLOGY_NAME, "testTopology");
        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, Double.MAX_VALUE);
        conf.put(Config.TOPOLOGY_SUBMITTER_USER, "user");
        conf.put(Config.TOPOLOGY_RAS_ORDER_EXECUTORS_BY_PROXIMITY_NEEDS, true);

        TopologyDetails topo = new TopologyDetails("testTopology-id", conf, stormToplogy, 0,
            genExecsAndComps(StormCommon.systemTopology(conf, stormToplogy)), CURRENT_TIME, "user");

        Topologies topologies = new Topologies(topo);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, conf);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(conf, new StormMetricsRegistry());
        rs.schedule(topologies, cluster);
        // Sorted execs: [[0, 0], [2, 2], [6, 6], [4, 4], [1, 1], [5, 5], [3, 3], [7, 7]]
        // Ackers: [[7, 7]]]

        HashSet<HashSet<ExecutorDetails>> expectedScheduling = new HashSet<>();
        expectedScheduling.add(new HashSet<>(Arrays.asList(
            new ExecutorDetails(0, 0), //spout
            new ExecutorDetails(6, 6), //bolt-2
            new ExecutorDetails(2, 2), //bolt-1
            new ExecutorDetails(7, 7)))); //acker
        expectedScheduling.add(new HashSet<>(Arrays.asList(
            new ExecutorDetails(3, 3), //bolt-3
            new ExecutorDetails(5, 5), //bolt-2
            new ExecutorDetails(4, 4), //bolt-3
            new ExecutorDetails(1, 1)))); //bolt-1
        HashSet<HashSet<ExecutorDetails>> foundScheduling = new HashSet<>();
        SchedulerAssignment assignment = cluster.getAssignmentById("testTopology-id");
        for (Collection<ExecutorDetails> execs : assignment.getSlotToExecutors().values()) {
            foundScheduling.add(new HashSet<>(execs));
        }

        Assert.assertEquals(expectedScheduling, foundScheduling);
    }

    /**
     * Test whether strategy will choose correct rack
     */
    @Test
    public void testMultipleRacks() {
        final Map<String, SupervisorDetails> supMap = new HashMap<>();
        final Map<String, SupervisorDetails> supMapRack0 = genSupervisors(10, 4, 0, 400, 8000);
        //generate another rack of supervisors with less resources
        final Map<String, SupervisorDetails> supMapRack1 = genSupervisors(10, 4, 10, 200, 4000);

        //generate some supervisors that are depleted of one resource
        final Map<String, SupervisorDetails> supMapRack2 = genSupervisors(10, 4, 20, 0, 8000);

        //generate some that has alot of memory but little of cpu
        final Map<String, SupervisorDetails> supMapRack3 = genSupervisors(10, 4, 30, 10, 8000 * 2 + 4000);

        //generate some that has alot of cpu but little of memory
        final Map<String, SupervisorDetails> supMapRack4 = genSupervisors(10, 4, 40, 400 + 200 + 10, 1000);

        //Generate some that have neither resource, to verify that the strategy will prioritize this last
        //Also put a generic resource with 0 value in the resources list, to verify that it doesn't affect the sorting
        final Map<String, SupervisorDetails> supMapRack5 = genSupervisors(10, 4, 50, 0.0, 0.0, Collections.singletonMap("gpu.count", 0.0));

        supMap.putAll(supMapRack0);
        supMap.putAll(supMapRack1);
        supMap.putAll(supMapRack2);
        supMap.putAll(supMapRack3);
        supMap.putAll(supMapRack4);
        supMap.putAll(supMapRack5);

        Config config = createClusterConfig(100, 500, 500, null);
        config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, Double.MAX_VALUE);
        INimbus iNimbus = new INimbusTest();

        //create test DNSToSwitchMapping plugin
        DNSToSwitchMapping TestNetworkTopographyPlugin =
            new TestDNSToSwitchMapping(supMapRack0, supMapRack1, supMapRack2, supMapRack3, supMapRack4, supMapRack5);

        //generate topologies
        TopologyDetails topo1 = genTopology("topo-1", config, 8, 0, 2, 0, CURRENT_TIME - 2, 10, "user");
        TopologyDetails topo2 = genTopology("topo-2", config, 8, 0, 2, 0, CURRENT_TIME - 2, 10, "user");
        
        Topologies topologies = new Topologies(topo1, topo2);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);
        
        List<String> supHostnames = new LinkedList<>();
        for (SupervisorDetails sup : supMap.values()) {
            supHostnames.add(sup.getHost());
        }
        Map<String, List<String>> rackToNodes = new HashMap<>();
        Map<String, String> resolvedSuperVisors =  TestNetworkTopographyPlugin.resolve(supHostnames);
        for (Map.Entry<String, String> entry : resolvedSuperVisors.entrySet()) {
            String hostName = entry.getKey();
            String rack = entry.getValue();
            rackToNodes.computeIfAbsent(rack, rid -> new ArrayList<>()).add(hostName);
        }
        cluster.setNetworkTopography(rackToNodes);

        DefaultResourceAwareStrategyOld rs = new DefaultResourceAwareStrategyOld();
        
        rs.prepareForScheduling(cluster, topo1);
        INodeSorter nodeSorter = new NodeSorterHostProximity(cluster, topo1, BaseResourceAwareStrategy.NodeSortType.DEFAULT_RAS);
        nodeSorter.prepare(null);
        Iterable<ObjectResourcesItem> sortedRacks = nodeSorter.getSortedRacks();

        Iterator<ObjectResourcesItem> it = sortedRacks.iterator();
        // Ranked first since rack-0 has the most balanced set of resources
        Assert.assertEquals("rack-0 should be ordered first", "rack-0", it.next().id);
        // Ranked second since rack-1 has a balanced set of resources but less than rack-0
        Assert.assertEquals("rack-1 should be ordered second", "rack-1", it.next().id);
        // Ranked third since rack-4 has a lot of cpu but not a lot of memory
        Assert.assertEquals("rack-4 should be ordered third", "rack-4", it.next().id);
        // Ranked fourth since rack-3 has alot of memory but not cpu
        Assert.assertEquals("rack-3 should be ordered fourth", "rack-3", it.next().id);
        //Ranked fifth since rack-2 has not cpu resources
        Assert.assertEquals("rack-2 should be ordered fifth", "rack-2", it.next().id);
        //Ranked last since rack-5 has neither CPU nor memory available
        assertEquals("Rack-5 should be ordered sixth", "rack-5", it.next().id);

        SchedulingResult schedulingResult = rs.schedule(cluster, topo1);
        assert(schedulingResult.isSuccess());
        SchedulerAssignment assignment = cluster.getAssignmentById(topo1.getId());
        for (WorkerSlot ws : assignment.getSlotToExecutors().keySet()) {
            //make sure all workers on scheduled in rack-0
            Assert.assertEquals("assert worker scheduled on rack-0", "rack-0",
                    resolvedSuperVisors.get(rs.idToNode(ws.getNodeId()).getHostname()));
        }
        Assert.assertEquals("All executors in topo-1 scheduled", 0, cluster.getUnassignedExecutors(topo1).size());

        //Test if topology is already partially scheduled on one rack
        Iterator<ExecutorDetails> executorIterator = topo2.getExecutors().iterator();
        List<String> nodeHostnames = rackToNodes.get("rack-1");
        for (int i = 0; i< topo2.getExecutors().size()/2; i++) {
            String nodeHostname = nodeHostnames.get(i % nodeHostnames.size());
            RasNode node = rs.hostnameToNodes(nodeHostname).get(0);
            WorkerSlot targetSlot = node.getFreeSlots().iterator().next();
            ExecutorDetails targetExec = executorIterator.next();
            // to keep track of free slots
            node.assign(targetSlot, topo2, Arrays.asList(targetExec));
        }

        rs = new DefaultResourceAwareStrategyOld();
        // schedule topo2
        schedulingResult = rs.schedule(cluster, topo2);
        assert(schedulingResult.isSuccess());
        assignment = cluster.getAssignmentById(topo2.getId());
        for (WorkerSlot ws : assignment.getSlotToExecutors().keySet()) {
            //make sure all workers on scheduled in rack-1
            Assert.assertEquals("assert worker scheduled on rack-1", "rack-1",
                    resolvedSuperVisors.get(rs.idToNode(ws.getNodeId()).getHostname()));
        }
        Assert.assertEquals("All executors in topo-2 scheduled", 0, cluster.getUnassignedExecutors(topo2).size());
    }

    /**
     * Test whether strategy will choose correct rack
     */
    @Test
    public void testMultipleRacksWithFavoritism() {
        final Map<String, SupervisorDetails> supMap = new HashMap<>();
        final Map<String, SupervisorDetails> supMapRack0 = genSupervisors(10, 4, 0, 400, 8000);
        //generate another rack of supervisors with less resources
        final Map<String, SupervisorDetails> supMapRack1 = genSupervisors(10, 4, 10, 200, 4000);

        //generate some supervisors that are depleted of one resource
        final Map<String, SupervisorDetails> supMapRack2 = genSupervisors(10, 4, 20, 0, 8000);

        //generate some that has alot of memory but little of cpu
        final Map<String, SupervisorDetails> supMapRack3 = genSupervisors(10, 4, 30, 10, 8000 * 2 + 4000);

        //generate some that has alot of cpu but little of memory
        final Map<String, SupervisorDetails> supMapRack4 = genSupervisors(10, 4, 40, 400 + 200 + 10, 1000);

        supMap.putAll(supMapRack0);
        supMap.putAll(supMapRack1);
        supMap.putAll(supMapRack2);
        supMap.putAll(supMapRack3);
        supMap.putAll(supMapRack4);

        Config config = createClusterConfig(100, 500, 500, null);
        config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, Double.MAX_VALUE);
        INimbus iNimbus = new INimbusTest();

        //create test DNSToSwitchMapping plugin
        DNSToSwitchMapping TestNetworkTopographyPlugin =
            new TestDNSToSwitchMapping(supMapRack0, supMapRack1, supMapRack2, supMapRack3, supMapRack4);

        Config t1Conf = new Config();
        t1Conf.putAll(config);
        final List<String> t1FavoredHostNames = Arrays.asList("host-41", "host-42", "host-43");
        t1Conf.put(Config.TOPOLOGY_SCHEDULER_FAVORED_NODES, t1FavoredHostNames);
        final List<String> t1UnfavoredHostIds = Arrays.asList("host-1", "host-2", "host-3");
        t1Conf.put(Config.TOPOLOGY_SCHEDULER_UNFAVORED_NODES, t1UnfavoredHostIds);
        //generate topologies
        TopologyDetails topo1 = genTopology("topo-1", t1Conf, 8, 0, 2, 0, CURRENT_TIME - 2, 10, "user");


        Config t2Conf = new Config();
        t2Conf.putAll(config);
        t2Conf.put(Config.TOPOLOGY_SCHEDULER_FAVORED_NODES, Arrays.asList("host-31", "host-32", "host-33"));
        t2Conf.put(Config.TOPOLOGY_SCHEDULER_UNFAVORED_NODES, Arrays.asList("host-11", "host-12", "host-13"));
        TopologyDetails topo2 = genTopology("topo-2", t2Conf, 8, 0, 2, 0, CURRENT_TIME - 2, 10, "user");

        Topologies topologies = new Topologies(topo1, topo2);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);

        List<String> supHostnames = new LinkedList<>();
        for (SupervisorDetails sup : supMap.values()) {
            supHostnames.add(sup.getHost());
        }
        Map<String, List<String>> rackToNodes = new HashMap<>();
        Map<String, String> resolvedSuperVisors =  TestNetworkTopographyPlugin.resolve(supHostnames);
        for (Map.Entry<String, String> entry : resolvedSuperVisors.entrySet()) {
            String hostName = entry.getKey();
            String rack = entry.getValue();
            List<String> nodesForRack = rackToNodes.get(rack);
            if (nodesForRack == null) {
                nodesForRack = new ArrayList<>();
                rackToNodes.put(rack, nodesForRack);
            }
            nodesForRack.add(hostName);
        }
        cluster.setNetworkTopography(rackToNodes);

        DefaultResourceAwareStrategyOld rs = new DefaultResourceAwareStrategyOld();

        rs.prepareForScheduling(cluster, topo1);
        INodeSorter nodeSorter = new NodeSorterHostProximity(cluster, topo1, BaseResourceAwareStrategy.NodeSortType.DEFAULT_RAS);
        nodeSorter.prepare(null);
        Iterable<ObjectResourcesItem> sortedRacks= nodeSorter.getSortedRacks();

        Iterator<ObjectResourcesItem> it = sortedRacks.iterator();
        // Ranked first since rack-0 has the most balanced set of resources
        Assert.assertEquals("rack-0 should be ordered first", "rack-0", it.next().id);
        // Ranked second since rack-1 has a balanced set of resources but less than rack-0
        Assert.assertEquals("rack-1 should be ordered second", "rack-1", it.next().id);
        // Ranked third since rack-4 has a lot of cpu but not a lot of memory
        Assert.assertEquals("rack-4 should be ordered third", "rack-4", it.next().id);
        // Ranked fourth since rack-3 has alot of memory but not cpu
        Assert.assertEquals("rack-3 should be ordered fourth", "rack-3", it.next().id);
        //Ranked last since rack-2 has not cpu resources
        Assert.assertEquals("rack-2 should be ordered fifth", "rack-2", it.next().id);

        SchedulingResult schedulingResult = rs.schedule(cluster, topo1);
        assert(schedulingResult.isSuccess());
        SchedulerAssignment assignment = cluster.getAssignmentById(topo1.getId());
        for (WorkerSlot ws : assignment.getSlotToExecutors().keySet()) {
            String hostName = rs.idToNode(ws.getNodeId()).getHostname();
            String rackId = resolvedSuperVisors.get(hostName);
            Assert.assertTrue(ws + " is neither on a favored node " + t1FavoredHostNames + " nor the highest priority rack (rack-0)",
                t1FavoredHostNames.contains(hostName) || "rack-0".equals(rackId));
            Assert.assertFalse(ws + " is a part of an unfavored node " + t1UnfavoredHostIds,
                t1UnfavoredHostIds.contains(hostName));
        }
        Assert.assertEquals("All executors in topo-1 scheduled", 0, cluster.getUnassignedExecutors(topo1).size());

        //Test if topology is already partially scheduled on one rack
        Iterator<ExecutorDetails> executorIterator = topo2.getExecutors().iterator();
        List<String> nodeHostnames = rackToNodes.get("rack-1");
        for (int i = 0; i< topo2.getExecutors().size()/2; i++) {
            String nodeHostname = nodeHostnames.get(i % nodeHostnames.size());
            RasNode node = rs.hostnameToNodes(nodeHostname).get(0);
            WorkerSlot targetSlot = node.getFreeSlots().iterator().next();
            ExecutorDetails targetExec = executorIterator.next();
            // to keep track of free slots
            node.assign(targetSlot, topo2, Arrays.asList(targetExec));
        }

        rs = new DefaultResourceAwareStrategyOld();
        // schedule topo2
        schedulingResult = rs.schedule(cluster, topo2);
        assert(schedulingResult.isSuccess());
        assignment = cluster.getAssignmentById(topo2.getId());
        for (WorkerSlot ws : assignment.getSlotToExecutors().keySet()) {
            //make sure all workers on scheduled in rack-1
            // The favored nodes would have put it on a different rack, but because that rack does not have free space to run the
            // topology it falls back to this rack
            Assert.assertEquals("assert worker scheduled on rack-1", "rack-1",
                    resolvedSuperVisors.get(rs.idToNode(ws.getNodeId()).getHostname()));
        }
        Assert.assertEquals("All executors in topo-2 scheduled", 0, cluster.getUnassignedExecutors(topo2).size());
    }
}
