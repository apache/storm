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

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.networktopography.DNSToSwitchMapping;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.Cluster.SupervisorResources;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SchedulerAssignmentImpl;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.RAS_Node;
import org.apache.storm.scheduler.resource.ResourceAwareScheduler;
import org.apache.storm.scheduler.resource.SchedulingResult;
import org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler;
import org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.ObjectResources;
import org.apache.storm.topology.SharedOffHeapWithinNode;
import org.apache.storm.topology.SharedOffHeapWithinWorker;
import org.apache.storm.topology.SharedOnHeap;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.TreeSet;

public class TestDefaultResourceAwareStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(TestDefaultResourceAwareStrategy.class);

    private static int currentTime = 1450418597;

    /**
     * test if the scheduling logic for the DefaultResourceAwareStrategy is correct
     */
    @Test
    public void testDefaultResourceAwareStrategySharedMemory() {
        int spoutParallelism = 2;
        int boltParallelism = 2;
        int numBolts = 3;
        double cpuPercent = 10;
        double memoryOnHeap = 10;
        double memoryOffHeap = 10;
        double sharedOnHeap = 500;
        double sharedOffHeapNode = 700;
        double sharedOffHeapWorker = 500;
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new TestSpout(),
                spoutParallelism);
        builder.setBolt("bolt-1", new TestBolt(),
                boltParallelism).addSharedMemory(new SharedOffHeapWithinWorker(sharedOffHeapWorker, "bolt-1 shared off heap worker")).shuffleGrouping("spout");
        builder.setBolt("bolt-2", new TestBolt(),
                boltParallelism).addSharedMemory(new SharedOffHeapWithinNode(sharedOffHeapNode, "bolt-2 shared node")).shuffleGrouping("bolt-1");
        builder.setBolt("bolt-3", new TestBolt(),
                boltParallelism).addSharedMemory(new SharedOnHeap(sharedOnHeap, "bolt-3 shared worker")).shuffleGrouping("bolt-2");

        StormTopology stormToplogy = builder.createTopology();

        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(4, 4, 500, 2000);
        Config conf = createClusterConfig(cpuPercent, memoryOnHeap, memoryOffHeap, null);
        
        conf.put(Config.TOPOLOGY_PRIORITY, 0);
        conf.put(Config.TOPOLOGY_NAME, "testTopology");
        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 2000);
        TopologyDetails topo = new TopologyDetails("testTopology-id", conf, stormToplogy, 0,
                genExecsAndComps(stormToplogy), currentTime, "user");

        Topologies topologies = new Topologies(topo);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, conf);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(conf);
        rs.schedule(topologies, cluster);
        
        for (Entry<String, SupervisorResources> entry: cluster.getSupervisorsResourcesMap().entrySet()) {
            String supervisorId = entry.getKey();
            SupervisorResources resources = entry.getValue();
            assertTrue(supervisorId, resources.getTotalCpu() >= resources.getUsedCpu());
            assertTrue(supervisorId, resources.getTotalMem() >= resources.getUsedMem());
        }

        // Everything should fit in a single slot
        int totalNumberOfTasks = (spoutParallelism + (boltParallelism * numBolts));
        double totalExpectedCPU = totalNumberOfTasks * cpuPercent;
        double totalExpectedOnHeap = (totalNumberOfTasks * memoryOnHeap) + sharedOnHeap;
        double totalExpectedWorkerOffHeap = (totalNumberOfTasks * memoryOffHeap) + sharedOffHeapWorker;
        
        SchedulerAssignment assignment = cluster.getAssignmentById(topo.getId());
        assertEquals(1, assignment.getSlots().size());
        WorkerSlot ws = assignment.getSlots().iterator().next();
        String nodeId = ws.getNodeId();
        assertEquals(1, assignment.getNodeIdToTotalSharedOffHeapMemory().size());
        assertEquals(sharedOffHeapNode, assignment.getNodeIdToTotalSharedOffHeapMemory().get(nodeId), 0.01);
        assertEquals(1, assignment.getScheduledResources().size());
        WorkerResources resources = assignment.getScheduledResources().get(ws);
        assertEquals(totalExpectedCPU, resources.get_cpu(), 0.01);
        assertEquals(totalExpectedOnHeap, resources.get_mem_on_heap(), 0.01);
        assertEquals(totalExpectedWorkerOffHeap, resources.get_mem_off_heap(), 0.01);
        assertEquals(sharedOnHeap, resources.get_shared_mem_on_heap(), 0.01);
        assertEquals(sharedOffHeapWorker, resources.get_shared_mem_off_heap(), 0.01);
    }
    
    
    /**
     * test if the scheduling logic for the DefaultResourceAwareStrategy is correct
     */
    @Test
    public void testDefaultResourceAwareStrategy() {
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
        Map<String, SupervisorDetails> supMap = genSupervisors(4, 4, 150, 1500);
        Config conf = createClusterConfig(50, 250, 250, null);
        conf.put(Config.TOPOLOGY_PRIORITY, 0);
        conf.put(Config.TOPOLOGY_NAME, "testTopology");
        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, Double.MAX_VALUE);
        conf.put(Config.TOPOLOGY_SUBMITTER_USER, "user");

        TopologyDetails topo = new TopologyDetails("testTopology-id", conf, stormToplogy, 0,
                genExecsAndComps(stormToplogy), currentTime, "user");

        Topologies topologies = new Topologies(topo);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, conf);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(conf);
        rs.schedule(topologies, cluster);

        HashSet<HashSet<ExecutorDetails>> expectedScheduling = new HashSet<>();
        expectedScheduling.add(new HashSet<>(Arrays.asList(new ExecutorDetails(0, 0)))); //Spout
        expectedScheduling.add(new HashSet<>(Arrays.asList(
            new ExecutorDetails(2, 2), //bolt-1
            new ExecutorDetails(4, 4), //bolt-2
            new ExecutorDetails(6, 6)))); //bolt-3
        expectedScheduling.add(new HashSet<>(Arrays.asList(
            new ExecutorDetails(1, 1), //bolt-1
            new ExecutorDetails(3, 3), //bolt-2
            new ExecutorDetails(5, 5)))); //bolt-3
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

        final Map<String, SupervisorDetails> supMap = new HashMap<String, SupervisorDetails>();
        final Map<String, SupervisorDetails> supMapRack1 = genSupervisors(10, 4, 0, 400, 8000);
        //generate another rack of supervisors with less resources
        final Map<String, SupervisorDetails> supMapRack2 = genSupervisors(10, 4, 10, 200, 4000);

        //generate some supervisors that are depleted of one resource
        final Map<String, SupervisorDetails> supMapRack3 = genSupervisors(10, 4, 20, 0, 8000);

        //generate some that has alot of memory but little of cpu
        final Map<String, SupervisorDetails> supMapRack4 = genSupervisors(10, 4, 30, 10, 8000 * 2 + 4000);

        //generate some that has alot of cpu but little of memory
        final Map<String, SupervisorDetails> supMapRack5 = genSupervisors(10, 4, 40, 400 + 200 + 10, 1000);

        supMap.putAll(supMapRack1);
        supMap.putAll(supMapRack2);
        supMap.putAll(supMapRack3);
        supMap.putAll(supMapRack4);
        supMap.putAll(supMapRack5);

        Config config = createClusterConfig(100, 500, 500, null);
        config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, Double.MAX_VALUE);
        INimbus iNimbus = new INimbusTest();

        //create test DNSToSwitchMapping plugin
        DNSToSwitchMapping TestNetworkTopographyPlugin = new DNSToSwitchMapping() {
            @Override
            public Map<String, String> resolve(List<String> names) {
                Map<String, String> ret = new HashMap<String, String>();
                for (SupervisorDetails sup : supMapRack1.values()) {
                    ret.put(sup.getHost(), "rack-0");
                }
                for (SupervisorDetails sup : supMapRack2.values()) {
                    ret.put(sup.getHost(), "rack-1");
                }
                for (SupervisorDetails sup : supMapRack3.values()) {
                    ret.put(sup.getHost(), "rack-2");
                }
                for (SupervisorDetails sup : supMapRack4.values()) {
                    ret.put(sup.getHost(), "rack-3");
                }
                for (SupervisorDetails sup : supMapRack5.values()) {
                    ret.put(sup.getHost(), "rack-4");
                }
                return ret;
            }
        };

        //generate topologies
        TopologyDetails topo1 = genTopology("topo-1", config, 8, 0, 2, 0, currentTime - 2, 10, "user");
        TopologyDetails topo2 = genTopology("topo-2", config, 8, 0, 2, 0, currentTime - 2, 10, "user");
        
        Topologies topologies = new Topologies(topo1, topo2);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
        
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
                nodesForRack = new ArrayList<String>();
                rackToNodes.put(rack, nodesForRack);
            }
            nodesForRack.add(hostName);
        }
        cluster.setNetworkTopography(rackToNodes);

        DefaultResourceAwareStrategy rs = new DefaultResourceAwareStrategy();

        rs.prepare(cluster);
        TreeSet<ObjectResources> sortedRacks= rs.sortRacks(topo1.getId());

        Assert.assertEquals("# of racks sorted", 5, sortedRacks.size());
        Iterator<ObjectResources> it = sortedRacks.iterator();
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
            //make sure all workers on scheduled in rack-0
            Assert.assertEquals("assert worker scheduled on rack-0", "rack-0", resolvedSuperVisors.get(rs.idToNode(ws.getNodeId()).getHostname()));
        }
        Assert.assertEquals("All executors in topo-1 scheduled", 0, cluster.getUnassignedExecutors(topo1).size());

        //Test if topology is already partially scheduled on one rack
        Iterator<ExecutorDetails> executorIterator = topo2.getExecutors().iterator();
        List<String> nodeHostnames = rackToNodes.get("rack-1");
        for (int i = 0; i< topo2.getExecutors().size()/2; i++) {
            String nodeHostname = nodeHostnames.get(i % nodeHostnames.size());
            RAS_Node node = rs.idToNode(rs.nodeHostnameToId(nodeHostname));
            WorkerSlot targetSlot = node.getFreeSlots().iterator().next();
            ExecutorDetails targetExec = executorIterator.next();
            // to keep track of free slots
            node.assign(targetSlot, topo2, Arrays.asList(targetExec));
        }

        rs = new DefaultResourceAwareStrategy();
        // schedule topo2
        schedulingResult = rs.schedule(cluster, topo2);
        assert(schedulingResult.isSuccess());
        assignment = cluster.getAssignmentById(topo2.getId());
        for (WorkerSlot ws : assignment.getSlotToExecutors().keySet()) {
            //make sure all workers on scheduled in rack-1
            Assert.assertEquals("assert worker scheduled on rack-1", "rack-1", resolvedSuperVisors.get(rs.idToNode(ws.getNodeId()).getHostname()));
        }
        Assert.assertEquals("All executors in topo-2 scheduled", 0, cluster.getUnassignedExecutors(topo1).size());
    }

    /**
     * Test whether strategy will choose correct rack
     */
    @Test
    public void testMultipleRacksWithFavoritism() {
        final Map<String, SupervisorDetails> supMap = new HashMap<String, SupervisorDetails>();
        final Map<String, SupervisorDetails> supMapRack1 = genSupervisors(10, 4, 0, 400, 8000);
        //generate another rack of supervisors with less resources
        final Map<String, SupervisorDetails> supMapRack2 = genSupervisors(10, 4, 10, 200, 4000);

        //generate some supervisors that are depleted of one resource
        final Map<String, SupervisorDetails> supMapRack3 = genSupervisors(10, 4, 20, 0, 8000);

        //generate some that has alot of memory but little of cpu
        final Map<String, SupervisorDetails> supMapRack4 = genSupervisors(10, 4, 30, 10, 8000 * 2 + 4000);

        //generate some that has alot of cpu but little of memory
        final Map<String, SupervisorDetails> supMapRack5 = genSupervisors(10, 4, 40, 400 + 200 + 10, 1000);

        supMap.putAll(supMapRack1);
        supMap.putAll(supMapRack2);
        supMap.putAll(supMapRack3);
        supMap.putAll(supMapRack4);
        supMap.putAll(supMapRack5);

        Config config = createClusterConfig(100, 500, 500, null);
        config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, Double.MAX_VALUE);
        INimbus iNimbus = new INimbusTest();

        //create test DNSToSwitchMapping plugin
        DNSToSwitchMapping TestNetworkTopographyPlugin = new DNSToSwitchMapping() {
            @Override
            public Map<String, String> resolve(List<String> names) {
                Map<String, String> ret = new HashMap<>();
                for (SupervisorDetails sup : supMapRack1.values()) {
                    ret.put(sup.getHost(), "rack-0");
                }
                for (SupervisorDetails sup : supMapRack2.values()) {
                    ret.put(sup.getHost(), "rack-1");
                }
                for (SupervisorDetails sup : supMapRack3.values()) {
                    ret.put(sup.getHost(), "rack-2");
                }
                for (SupervisorDetails sup : supMapRack4.values()) {
                    ret.put(sup.getHost(), "rack-3");
                }
                for (SupervisorDetails sup : supMapRack5.values()) {
                    ret.put(sup.getHost(), "rack-4");
                }
                return ret;
            }
        };

        Config t1Conf = new Config();
        t1Conf.putAll(config);
        final List<String> t1FavoredHostNames = Arrays.asList("host-41", "host-42", "host-43");
        t1Conf.put(Config.TOPOLOGY_SCHEDULER_FAVORED_NODES, t1FavoredHostNames);
        final List<String> t1UnfavoredHostIds = Arrays.asList("host-1", "host-2", "host-3");
        t1Conf.put(Config.TOPOLOGY_SCHEDULER_UNFAVORED_NODES, t1UnfavoredHostIds);
        //generate topologies
        TopologyDetails topo1 = genTopology("topo-1", t1Conf, 8, 0, 2, 0, currentTime - 2, 10, "user");


        Config t2Conf = new Config();
        t2Conf.putAll(config);
        t2Conf.put(Config.TOPOLOGY_SCHEDULER_FAVORED_NODES, Arrays.asList("host-31", "host-32", "host-33"));
        t2Conf.put(Config.TOPOLOGY_SCHEDULER_UNFAVORED_NODES, Arrays.asList("host-11", "host-12", "host-13"));
        TopologyDetails topo2 = genTopology("topo-2", t2Conf, 8, 0, 2, 0, currentTime - 2, 10, "user");

        Topologies topologies = new Topologies(topo1, topo2);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);

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
                nodesForRack = new ArrayList<String>();
                rackToNodes.put(rack, nodesForRack);
            }
            nodesForRack.add(hostName);
        }
        cluster.setNetworkTopography(rackToNodes);

        DefaultResourceAwareStrategy rs = new DefaultResourceAwareStrategy();

        rs.prepare(cluster);
        TreeSet<ObjectResources> sortedRacks= rs.sortRacks(topo1.getId());

        Assert.assertEquals("# of racks sorted", 5, sortedRacks.size());
        Iterator<ObjectResources> it = sortedRacks.iterator();
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
            RAS_Node node = rs.idToNode(rs.nodeHostnameToId(nodeHostname));
            WorkerSlot targetSlot = node.getFreeSlots().iterator().next();
            ExecutorDetails targetExec = executorIterator.next();
            // to keep track of free slots
            node.assign(targetSlot, topo2, Arrays.asList(targetExec));
        }

        rs = new DefaultResourceAwareStrategy();
        // schedule topo2
        schedulingResult = rs.schedule(cluster, topo2);
        assert(schedulingResult.isSuccess());
        assignment = cluster.getAssignmentById(topo2.getId());
        for (WorkerSlot ws : assignment.getSlotToExecutors().keySet()) {
            //make sure all workers on scheduled in rack-1
            // The favored nodes would have put it on a different rack, but because that rack does not have free space to run the
            // topology it falls back to this rack
            Assert.assertEquals("assert worker scheduled on rack-1", "rack-1", resolvedSuperVisors.get(rs.idToNode(ws.getNodeId()).getHostname()));
        }
        Assert.assertEquals("All executors in topo-2 scheduled", 0, cluster.getUnassignedExecutors(topo1).size());
    }
}
