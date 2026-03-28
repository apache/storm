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

package org.apache.storm.scheduler.multitenant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.daemon.nimbus.Nimbus.StandaloneINimbus;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SchedulerAssignmentImpl;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.normalization.ResourceMetrics;
import org.junit.jupiter.api.Test;

public class MultitenantSchedulerTest {

    private static ExecutorDetails ed(int id) {
        return new ExecutorDetails(id, id);
    }

    private static Map<String, SupervisorDetails> genSupervisors(int count) {
        Map<String, SupervisorDetails> supervisors = new LinkedHashMap<>();
        for (int id = 0; id < count; id++) {
            SupervisorDetails supervisor = new SupervisorDetails(
                "super" + id, "host" + id, Collections.emptyList(), Arrays.asList(1, 2, 3, 4));
            supervisors.put(supervisor.getId(), supervisor);
        }
        return supervisors;
    }

    private static Map<String, TopologyDetails> toTopMap(TopologyDetails... topologies) {
        Map<String, TopologyDetails> map = new HashMap<>();
        for (TopologyDetails top : topologies) {
            map.put(top.getId(), top);
        }
        return map;
    }

    private static Map<ExecutorDetails, String> mkEdMap(Object[]... args) {
        Map<ExecutorDetails, String> map = new HashMap<>();
        for (Object[] entry : args) {
            String name = (String) entry[0];
            int start = (int) entry[1];
            int end = (int) entry[2];
            for (int at = start; at < end; at++) {
                map.put(ed(at), name);
            }
        }
        return map;
    }

    private static Set<String> getNodeIds(Set<WorkerSlot> slots) {
        Set<String> nodeIds = new HashSet<>();
        for (WorkerSlot slot : slots) {
            nodeIds.add(slot.getNodeId());
        }
        return nodeIds;
    }

    private static ResourceMetrics newResourceMetrics() {
        return new ResourceMetrics(new StormMetricsRegistry());
    }

    @Test
    public void testNode() {
        Map<String, SupervisorDetails> supers = genSupervisors(5);
        TopologyDetails topology1 = new TopologyDetails("topology1", new HashMap<>(), null, 1, "user");
        TopologyDetails topology2 = new TopologyDetails("topology2", new HashMap<>(), null, 1, "user");
        Map<String, TopologyDetails> topMap = new HashMap<>();
        topMap.put("topology1", topology1);
        topMap.put("topology2", topology2);
        Cluster cluster = new Cluster(new StandaloneINimbus(), newResourceMetrics(), supers, new HashMap<>(),
            new Topologies(topMap), new HashMap<>());
        Map<String, Node> nodeMap = Node.getAllNodesFrom(cluster);

        assertEquals(5, nodeMap.size());

        Node node = nodeMap.get("super0");
        assertEquals("super0", node.getId());
        assertTrue(node.isAlive());
        assertEquals(0, node.getRunningTopologies().size());
        assertTrue(node.isTotallyFree());
        assertEquals(4, node.totalSlotsFree());
        assertEquals(0, node.totalSlotsUsed());
        assertEquals(4, node.totalSlots());

        node.assign("topology1", Arrays.asList(new ExecutorDetails(1, 1)), cluster);
        assertEquals(1, node.getRunningTopologies().size());
        assertFalse(node.isTotallyFree());
        assertEquals(3, node.totalSlotsFree());
        assertEquals(1, node.totalSlotsUsed());
        assertEquals(4, node.totalSlots());

        node.assign("topology1", Arrays.asList(new ExecutorDetails(2, 2)), cluster);
        assertEquals(1, node.getRunningTopologies().size());
        assertFalse(node.isTotallyFree());
        assertEquals(2, node.totalSlotsFree());
        assertEquals(2, node.totalSlotsUsed());
        assertEquals(4, node.totalSlots());

        node.assign("topology2", Arrays.asList(new ExecutorDetails(1, 1)), cluster);
        assertEquals(2, node.getRunningTopologies().size());
        assertFalse(node.isTotallyFree());
        assertEquals(1, node.totalSlotsFree());
        assertEquals(3, node.totalSlotsUsed());
        assertEquals(4, node.totalSlots());

        node.assign("topology2", Arrays.asList(new ExecutorDetails(2, 2)), cluster);
        assertEquals(2, node.getRunningTopologies().size());
        assertFalse(node.isTotallyFree());
        assertEquals(0, node.totalSlotsFree());
        assertEquals(4, node.totalSlotsUsed());
        assertEquals(4, node.totalSlots());

        node.freeAllSlots(cluster);
        assertEquals(0, node.getRunningTopologies().size());
        assertTrue(node.isTotallyFree());
        assertEquals(4, node.totalSlotsFree());
        assertEquals(0, node.totalSlotsUsed());
        assertEquals(4, node.totalSlots());
    }

    @Test
    public void testFreePool() {
        Map<String, SupervisorDetails> supers = genSupervisors(5);
        TopologyDetails topology1 = new TopologyDetails("topology1", new HashMap<>(), null, 1, "user");
        Map<String, TopologyDetails> topMap = new HashMap<>();
        topMap.put("topology1", topology1);
        Cluster cluster = new Cluster(new StandaloneINimbus(), newResourceMetrics(), supers, new HashMap<>(),
            new Topologies(topMap), new HashMap<>());
        Map<String, Node> nodeMap = Node.getAllNodesFrom(cluster);
        FreePool freePool = new FreePool();

        // assign one node so it is not in the pool
        nodeMap.get("super0").assign("topology1", Arrays.asList(new ExecutorDetails(1, 1)), cluster);
        freePool.init(cluster, nodeMap);

        assertEquals(4, freePool.nodesAvailable());
        assertEquals(4 * 4, freePool.slotsAvailable());

        NodePool.NodeAndSlotCounts nsCount1 = freePool.getNodeAndSlotCountIfSlotsWereTaken(1);
        NodePool.NodeAndSlotCounts nsCount3 = freePool.getNodeAndSlotCountIfSlotsWereTaken(3);
        NodePool.NodeAndSlotCounts nsCount4 = freePool.getNodeAndSlotCountIfSlotsWereTaken(4);
        NodePool.NodeAndSlotCounts nsCount5 = freePool.getNodeAndSlotCountIfSlotsWereTaken(5);

        assertEquals(1, nsCount1.nodes);
        assertEquals(4, nsCount1.slots);
        assertEquals(1, nsCount3.nodes);
        assertEquals(4, nsCount3.slots);
        assertEquals(1, nsCount4.nodes);
        assertEquals(4, nsCount4.slots);
        assertEquals(2, nsCount5.nodes);
        assertEquals(8, nsCount5.slots);

        Collection<Node> nodes = freePool.takeNodesBySlots(5);
        assertEquals(2, nodes.size());
        assertEquals(8, Node.countFreeSlotsAlive(nodes));
        assertEquals(8, Node.countTotalSlotsAlive(nodes));
        assertEquals(2, freePool.nodesAvailable());
        assertEquals(2 * 4, freePool.slotsAvailable());

        Collection<Node> nodes2 = freePool.takeNodes(3); // Only 2 should be left
        assertEquals(2, nodes2.size());
        assertEquals(8, Node.countFreeSlotsAlive(nodes2));
        assertEquals(8, Node.countTotalSlotsAlive(nodes2));
        assertEquals(0, freePool.nodesAvailable());
        assertEquals(0, freePool.slotsAvailable());
    }

    @Test
    public void testDefaultPoolSimple() {
        Map<String, SupervisorDetails> supers = genSupervisors(5);
        FreePool freePool = new FreePool();
        DefaultPool defaultPool = new DefaultPool();
        ExecutorDetails executor1 = ed(1);
        ExecutorDetails executor2 = ed(2);
        ExecutorDetails executor3 = ed(3);
        Map<String, Object> conf1 = new HashMap<>();
        conf1.put(Config.TOPOLOGY_NAME, "topology-name-1");
        Map<ExecutorDetails, String> execMap1 = new HashMap<>();
        execMap1.put(executor1, "spout1");
        execMap1.put(executor2, "bolt1");
        execMap1.put(executor3, "bolt2");
        TopologyDetails topology1 = new TopologyDetails("topology1", conf1, new StormTopology(), 2, execMap1, "user");
        Topologies topologies = new Topologies(toTopMap(topology1));
        Cluster cluster = new Cluster(new StandaloneINimbus(), newResourceMetrics(), supers, new HashMap<>(),
            topologies, new HashMap<>());
        Map<String, Node> nodeMap = Node.getAllNodesFrom(cluster);

        // assign one node so it is not in the pool
        nodeMap.get("super0").assign("topology1", Arrays.asList(executor1), cluster);
        freePool.init(cluster, nodeMap);
        defaultPool.init(cluster, nodeMap);

        assertTrue(defaultPool.canAdd(topology1));
        defaultPool.addTopology(topology1);

        // Only 1 node is in the default-pool because only one node was scheduled already
        assertEquals(4, defaultPool.slotsAvailable());
        assertEquals(1, defaultPool.nodesAvailable());
        assertEquals(4 * 4, freePool.slotsAvailable());
        assertEquals(4, freePool.nodesAvailable());
        assertEquals(1, cluster.getAssignmentById("topology1").getSlots().size());

        defaultPool.scheduleAsNeeded(new NodePool[]{freePool});
        assertEquals(4, defaultPool.slotsAvailable());
        assertEquals(1, defaultPool.nodesAvailable());
        assertEquals(4 * 4, freePool.slotsAvailable());
        assertEquals(4, freePool.nodesAvailable());
        assertEquals(2, cluster.getAssignmentById("topology1").getSlots().size());
        assertEquals("Fully Scheduled", cluster.getStatusMap().get("topology1"));
    }

    @Test
    public void testDefaultPoolBigRequest() {
        Map<String, SupervisorDetails> supers = genSupervisors(5);
        FreePool freePool = new FreePool();
        DefaultPool defaultPool = new DefaultPool();
        ExecutorDetails executor1 = ed(1);
        ExecutorDetails executor2 = ed(2);
        ExecutorDetails executor3 = ed(3);
        Map<String, Object> conf1 = new HashMap<>();
        conf1.put(Config.TOPOLOGY_NAME, "topology-name-1");
        Map<ExecutorDetails, String> execMap1 = new HashMap<>();
        execMap1.put(executor1, "spout1");
        execMap1.put(executor2, "bolt1");
        execMap1.put(executor3, "bolt2");
        TopologyDetails topology1 = new TopologyDetails("topology1", conf1, new StormTopology(), 5, execMap1, "user");
        Topologies topologies = new Topologies(toTopMap(topology1));
        Cluster cluster = new Cluster(new StandaloneINimbus(), newResourceMetrics(), supers, new HashMap<>(),
            topologies, new HashMap<>());
        Map<String, Node> nodeMap = Node.getAllNodesFrom(cluster);

        // assign one node so it is not in the pool
        nodeMap.get("super0").assign("topology1", Arrays.asList(executor1), cluster);
        freePool.init(cluster, nodeMap);
        defaultPool.init(cluster, nodeMap);

        assertTrue(defaultPool.canAdd(topology1));
        defaultPool.addTopology(topology1);

        assertEquals(4, defaultPool.slotsAvailable());
        assertEquals(1, defaultPool.nodesAvailable());
        assertEquals(4 * 4, freePool.slotsAvailable());
        assertEquals(4, freePool.nodesAvailable());
        assertEquals(1, cluster.getAssignmentById("topology1").getSlots().size());

        defaultPool.scheduleAsNeeded(new NodePool[]{freePool});
        assertEquals(4, defaultPool.slotsAvailable());
        assertEquals(1, defaultPool.nodesAvailable());
        assertEquals(4 * 4, freePool.slotsAvailable());
        assertEquals(4, freePool.nodesAvailable());
        assertEquals(3, cluster.getAssignmentById("topology1").getSlots().size());
        assertEquals("Fully Scheduled (requested 5 slots, but could only use 3)",
            cluster.getStatusMap().get("topology1"));
    }

    @Test
    public void testDefaultPoolBigRequest2() {
        Map<String, SupervisorDetails> supers = genSupervisors(1);
        FreePool freePool = new FreePool();
        DefaultPool defaultPool = new DefaultPool();
        ExecutorDetails executor1 = ed(1);
        ExecutorDetails executor2 = ed(2);
        ExecutorDetails executor3 = ed(3);
        ExecutorDetails executor4 = ed(4);
        ExecutorDetails executor5 = ed(5);
        Map<String, Object> conf1 = new HashMap<>();
        conf1.put(Config.TOPOLOGY_NAME, "topology-name-1");
        Map<ExecutorDetails, String> execMap1 = new HashMap<>();
        execMap1.put(executor1, "spout1");
        execMap1.put(executor2, "bolt1");
        execMap1.put(executor3, "bolt1");
        execMap1.put(executor4, "bolt1");
        execMap1.put(executor5, "bolt2");
        TopologyDetails topology1 = new TopologyDetails("topology1", conf1, new StormTopology(), 5, execMap1, "user");
        Cluster cluster = new Cluster(new StandaloneINimbus(), newResourceMetrics(), supers, new HashMap<>(),
            new Topologies(toTopMap(topology1)), new HashMap<>());
        Map<String, Node> nodeMap = Node.getAllNodesFrom(cluster);

        // assign one node so it is not in the pool
        nodeMap.get("super0").assign("topology1", Arrays.asList(executor1), cluster);
        freePool.init(cluster, nodeMap);
        defaultPool.init(cluster, nodeMap);

        assertTrue(defaultPool.canAdd(topology1));
        defaultPool.addTopology(topology1);

        assertEquals(4, defaultPool.slotsAvailable());
        assertEquals(1, defaultPool.nodesAvailable());
        assertEquals(0, freePool.slotsAvailable());
        assertEquals(0, freePool.nodesAvailable());
        assertEquals(1, cluster.getAssignmentById("topology1").getSlots().size());

        defaultPool.scheduleAsNeeded(new NodePool[]{freePool});
        assertEquals(4, defaultPool.slotsAvailable());
        assertEquals(1, defaultPool.nodesAvailable());
        assertEquals(0, freePool.slotsAvailable());
        assertEquals(0, freePool.nodesAvailable());
        assertEquals(4, cluster.getAssignmentById("topology1").getSlots().size());
        assertEquals("Running with fewer slots than requested (4/5)",
            cluster.getStatusMap().get("topology1"));
    }

    @Test
    public void testDefaultPoolFull() {
        Map<String, SupervisorDetails> supers = genSupervisors(2);
        // make 2 supervisors but only schedule with one of them
        Map.Entry<String, SupervisorDetails> firstEntry = supers.entrySet().iterator().next();
        Map<String, SupervisorDetails> singleSuper = new HashMap<>();
        singleSuper.put(firstEntry.getKey(), firstEntry.getValue());

        ExecutorDetails executor1 = ed(1);
        ExecutorDetails executor2 = ed(2);
        ExecutorDetails executor3 = ed(3);
        ExecutorDetails executor4 = ed(4);
        ExecutorDetails executor5 = ed(5);
        Map<String, Object> conf1 = new HashMap<>();
        conf1.put(Config.TOPOLOGY_NAME, "topology-name-1");
        Map<ExecutorDetails, String> execMap1 = new HashMap<>();
        execMap1.put(executor1, "spout1");
        execMap1.put(executor2, "bolt1");
        execMap1.put(executor3, "bolt2");
        execMap1.put(executor4, "bolt3");
        execMap1.put(executor5, "bolt4");
        TopologyDetails topology1 = new TopologyDetails("topology1", conf1, new StormTopology(), 5, execMap1, "user");
        Topologies topologies = new Topologies(toTopMap(topology1));
        Cluster singleCluster = new Cluster(new StandaloneINimbus(), newResourceMetrics(), singleSuper,
            new HashMap<>(), topologies, new HashMap<>());

        {
            Map<String, Node> nodeMap = Node.getAllNodesFrom(singleCluster);
            FreePool freePool = new FreePool();
            DefaultPool defaultPool = new DefaultPool();
            freePool.init(singleCluster, nodeMap);
            defaultPool.init(singleCluster, nodeMap);
            defaultPool.addTopology(topology1);
            defaultPool.scheduleAsNeeded(new NodePool[]{freePool});
            // The cluster should be full and have 4 slots used, but the topology would like 1 more
            assertEquals(4, singleCluster.getUsedSlots().size());
            assertEquals("Running with fewer slots than requested (4/5)",
                singleCluster.getStatusMap().get("topology1"));
        }

        {
            Cluster cluster = new Cluster(new StandaloneINimbus(), newResourceMetrics(), supers,
                singleCluster.getAssignments(), topologies, new HashMap<>());
            Map<String, Node> nodeMap = Node.getAllNodesFrom(cluster);
            FreePool freePool = new FreePool();
            DefaultPool defaultPool = new DefaultPool();
            freePool.init(cluster, nodeMap);
            defaultPool.init(cluster, nodeMap);
            defaultPool.addTopology(topology1);
            defaultPool.scheduleAsNeeded(new NodePool[]{freePool});
            // The cluster should now have 5 slots used
            assertEquals(5, cluster.getUsedSlots().size());
            assertEquals("Fully Scheduled", cluster.getStatusMap().get("topology1"));
        }
    }

    @Test
    public void testDefaultPoolComplex() {
        Map<String, SupervisorDetails> supers = genSupervisors(5);
        FreePool freePool = new FreePool();
        DefaultPool defaultPool = new DefaultPool();
        ExecutorDetails executor1 = ed(1);
        ExecutorDetails executor2 = ed(2);
        ExecutorDetails executor3 = ed(3);
        ExecutorDetails executor11 = ed(11);
        ExecutorDetails executor12 = ed(12);
        ExecutorDetails executor13 = ed(13);
        ExecutorDetails executor14 = ed(14);

        Map<String, Object> conf1 = new HashMap<>();
        conf1.put(Config.TOPOLOGY_NAME, "topology-name-1");
        Map<ExecutorDetails, String> execMap1 = new HashMap<>();
        execMap1.put(executor1, "spout1");
        execMap1.put(executor2, "bolt1");
        execMap1.put(executor3, "bolt2");
        TopologyDetails topology1 = new TopologyDetails("topology1", conf1, new StormTopology(), 2, execMap1, "user");

        Map<String, Object> conf2 = new HashMap<>();
        conf2.put(Config.TOPOLOGY_NAME, "topology-name-2");
        Map<ExecutorDetails, String> execMap2 = new HashMap<>();
        execMap2.put(executor11, "spout11");
        execMap2.put(executor12, "bolt12");
        execMap2.put(executor13, "bolt13");
        execMap2.put(executor14, "bolt14");
        TopologyDetails topology2 = new TopologyDetails("topology2", conf2, new StormTopology(), 4, execMap2, "user");

        Topologies topologies = new Topologies(toTopMap(topology1, topology2));
        Cluster cluster = new Cluster(new StandaloneINimbus(), newResourceMetrics(), supers, new HashMap<>(),
            topologies, new HashMap<>());
        Map<String, Node> nodeMap = Node.getAllNodesFrom(cluster);

        // assign one node so it is not in the pool
        nodeMap.get("super0").assign("topology1", Arrays.asList(executor1), cluster);
        freePool.init(cluster, nodeMap);
        defaultPool.init(cluster, nodeMap);

        assertTrue(defaultPool.canAdd(topology1));
        defaultPool.addTopology(topology1);
        assertTrue(defaultPool.canAdd(topology2));
        defaultPool.addTopology(topology2);

        // Only 1 node is in the default-pool because only one node was scheduled already
        assertEquals(4, defaultPool.slotsAvailable());
        assertEquals(1, defaultPool.nodesAvailable());
        assertEquals(4 * 4, freePool.slotsAvailable());
        assertEquals(4, freePool.nodesAvailable());
        assertEquals(1, cluster.getAssignmentById("topology1").getSlots().size());
        assertNull(cluster.getAssignmentById("topology2"));

        defaultPool.scheduleAsNeeded(new NodePool[]{freePool});
        // We steal a node from the free pool to handle the extra
        assertEquals(8, defaultPool.slotsAvailable());
        assertEquals(2, defaultPool.nodesAvailable());
        assertEquals(3 * 4, freePool.slotsAvailable());
        assertEquals(3, freePool.nodesAvailable());
        assertEquals(2, cluster.getAssignmentById("topology1").getSlots().size());
        assertEquals(4, cluster.getAssignmentById("topology2").getSlots().size());

        NodePool.NodeAndSlotCounts nsCount1 = defaultPool.getNodeAndSlotCountIfSlotsWereTaken(1);
        NodePool.NodeAndSlotCounts nsCount3 = defaultPool.getNodeAndSlotCountIfSlotsWereTaken(3);
        NodePool.NodeAndSlotCounts nsCount4 = defaultPool.getNodeAndSlotCountIfSlotsWereTaken(4);
        NodePool.NodeAndSlotCounts nsCount5 = defaultPool.getNodeAndSlotCountIfSlotsWereTaken(5);
        assertEquals(1, nsCount1.nodes);
        assertEquals(4, nsCount1.slots);
        assertEquals(1, nsCount3.nodes);
        assertEquals(4, nsCount3.slots);
        assertEquals(1, nsCount4.nodes);
        assertEquals(4, nsCount4.slots);
        assertEquals(2, nsCount5.nodes);
        assertEquals(8, nsCount5.slots);

        Collection<Node> nodes = defaultPool.takeNodesBySlots(3);
        assertEquals(1, nodes.size());
        assertEquals(4, Node.countFreeSlotsAlive(nodes));
        assertEquals(4, Node.countTotalSlotsAlive(nodes));
        assertEquals(1, defaultPool.nodesAvailable());
        assertEquals(1 * 4, defaultPool.slotsAvailable());

        Collection<Node> nodes2 = defaultPool.takeNodes(3); // Only 1 should be left
        assertEquals(1, nodes2.size());
        assertEquals(4, Node.countFreeSlotsAlive(nodes2));
        assertEquals(4, Node.countTotalSlotsAlive(nodes2));
        assertEquals(0, defaultPool.nodesAvailable());
        assertEquals(0, defaultPool.slotsAvailable());

        assertEquals("Fully Scheduled", cluster.getStatusMap().get("topology1"));
        assertEquals("Fully Scheduled", cluster.getStatusMap().get("topology2"));
    }

    @Test
    public void testIsolatedPoolSimple() {
        Map<String, SupervisorDetails> supers = genSupervisors(5);
        FreePool freePool = new FreePool();
        IsolatedPool isolatedPool = new IsolatedPool(5);
        ExecutorDetails executor1 = ed(1);
        ExecutorDetails executor2 = ed(2);
        ExecutorDetails executor3 = ed(3);
        ExecutorDetails executor4 = ed(4);

        Map<String, Object> conf1 = new HashMap<>();
        conf1.put(Config.TOPOLOGY_NAME, "topology-name-1");
        conf1.put(Config.TOPOLOGY_ISOLATED_MACHINES, 4);
        Map<ExecutorDetails, String> execMap1 = new HashMap<>();
        execMap1.put(executor1, "spout1");
        execMap1.put(executor2, "bolt1");
        execMap1.put(executor3, "bolt2");
        execMap1.put(executor4, "bolt4");
        TopologyDetails topology1 = new TopologyDetails("topology1", conf1, new StormTopology(), 4, execMap1, "user");
        Topologies topologies = new Topologies(toTopMap(topology1));
        Cluster cluster = new Cluster(new StandaloneINimbus(), newResourceMetrics(), supers, new HashMap<>(),
            topologies, new HashMap<>());
        Map<String, Node> nodeMap = Node.getAllNodesFrom(cluster);

        // assign one node so it is not in the pool
        nodeMap.get("super0").assign("topology1", Arrays.asList(executor1), cluster);
        freePool.init(cluster, nodeMap);
        isolatedPool.init(cluster, nodeMap);

        assertTrue(isolatedPool.canAdd(topology1));
        isolatedPool.addTopology(topology1);

        // Isolated topologies cannot have their resources stolen
        assertEquals(0, isolatedPool.slotsAvailable());
        assertEquals(0, isolatedPool.nodesAvailable());
        assertEquals(4 * 4, freePool.slotsAvailable());
        assertEquals(4, freePool.nodesAvailable());
        assertEquals(1, cluster.getAssignmentById("topology1").getSlots().size());

        isolatedPool.scheduleAsNeeded(new NodePool[]{freePool});
        assertEquals(0, isolatedPool.slotsAvailable());
        assertEquals(0, isolatedPool.nodesAvailable());
        assertEquals(1 * 4, freePool.slotsAvailable());
        assertEquals(1, freePool.nodesAvailable());

        Set<WorkerSlot> assignedSlots = cluster.getAssignmentById("topology1").getSlots();
        // 4 slots on 4 machines
        assertEquals(4, assignedSlots.size());
        assertEquals(4, getNodeIds(assignedSlots).size());

        assertEquals("Scheduled Isolated on 4 Nodes", cluster.getStatusMap().get("topology1"));
    }

    @Test
    public void testIsolatedPoolBigAsk() {
        Map<String, SupervisorDetails> supers = genSupervisors(5);
        FreePool freePool = new FreePool();
        IsolatedPool isolatedPool = new IsolatedPool(5);
        ExecutorDetails executor1 = ed(1);
        ExecutorDetails executor2 = ed(2);
        ExecutorDetails executor3 = ed(3);
        ExecutorDetails executor4 = ed(4);

        Map<String, Object> conf1 = new HashMap<>();
        conf1.put(Config.TOPOLOGY_NAME, "topology-name-1");
        conf1.put(Config.TOPOLOGY_ISOLATED_MACHINES, 4);
        Map<ExecutorDetails, String> execMap1 = new HashMap<>();
        execMap1.put(executor1, "spout1");
        execMap1.put(executor2, "bolt1");
        execMap1.put(executor3, "bolt2");
        execMap1.put(executor4, "bolt4");
        TopologyDetails topology1 = new TopologyDetails("topology1", conf1, new StormTopology(), 10, execMap1, "user");
        Topologies topologies = new Topologies(toTopMap(topology1));
        Cluster cluster = new Cluster(new StandaloneINimbus(), newResourceMetrics(), supers, new HashMap<>(),
            topologies, new HashMap<>());
        Map<String, Node> nodeMap = Node.getAllNodesFrom(cluster);

        // assign one node so it is not in the pool
        nodeMap.get("super0").assign("topology1", Arrays.asList(executor1), cluster);
        freePool.init(cluster, nodeMap);
        isolatedPool.init(cluster, nodeMap);

        assertTrue(isolatedPool.canAdd(topology1));
        isolatedPool.addTopology(topology1);

        // Isolated topologies cannot have their resources stolen
        assertEquals(0, isolatedPool.slotsAvailable());
        assertEquals(0, isolatedPool.nodesAvailable());
        assertEquals(4 * 4, freePool.slotsAvailable());
        assertEquals(4, freePool.nodesAvailable());
        assertEquals(1, cluster.getAssignmentById("topology1").getSlots().size());

        isolatedPool.scheduleAsNeeded(new NodePool[]{freePool});
        assertEquals(0, isolatedPool.slotsAvailable());
        assertEquals(0, isolatedPool.nodesAvailable());
        assertEquals(1 * 4, freePool.slotsAvailable());
        assertEquals(1, freePool.nodesAvailable());

        Set<WorkerSlot> assignedSlots = cluster.getAssignmentById("topology1").getSlots();
        // 4 slots on 4 machines
        assertEquals(4, assignedSlots.size());
        assertEquals(4, getNodeIds(assignedSlots).size());

        assertEquals("Scheduled Isolated on 4 Nodes", cluster.getStatusMap().get("topology1"));
    }

    @Test
    public void testIsolatedPoolComplex() {
        Map<String, SupervisorDetails> supers = genSupervisors(5);
        FreePool freePool = new FreePool();
        IsolatedPool isolatedPool = new IsolatedPool(5);
        ExecutorDetails executor1 = ed(1);
        ExecutorDetails executor2 = ed(2);
        ExecutorDetails executor3 = ed(3);
        ExecutorDetails executor4 = ed(4);
        ExecutorDetails executor11 = ed(11);
        ExecutorDetails executor12 = ed(12);
        ExecutorDetails executor13 = ed(13);
        ExecutorDetails executor14 = ed(14);

        Map<String, Object> conf1 = new HashMap<>();
        conf1.put(Config.TOPOLOGY_NAME, "topology-name-1");
        Map<ExecutorDetails, String> execMap1 = new HashMap<>();
        execMap1.put(executor1, "spout1");
        execMap1.put(executor2, "bolt1");
        execMap1.put(executor3, "bolt2");
        execMap1.put(executor4, "bolt4");
        TopologyDetails topology1 = new TopologyDetails("topology1", conf1, new StormTopology(), 4, execMap1, "user");

        Map<String, Object> conf2 = new HashMap<>();
        conf2.put(Config.TOPOLOGY_NAME, "topology-name-2");
        conf2.put(Config.TOPOLOGY_ISOLATED_MACHINES, 2);
        Map<ExecutorDetails, String> execMap2 = new HashMap<>();
        execMap2.put(executor11, "spout11");
        execMap2.put(executor12, "bolt12");
        execMap2.put(executor13, "bolt13");
        execMap2.put(executor14, "bolt14");
        TopologyDetails topology2 = new TopologyDetails("topology2", conf2, new StormTopology(), 4, execMap2, "user");

        Topologies topologies = new Topologies(toTopMap(topology1, topology2));
        Cluster cluster = new Cluster(new StandaloneINimbus(), newResourceMetrics(), supers, new HashMap<>(),
            topologies, new HashMap<>());
        Map<String, Node> nodeMap = Node.getAllNodesFrom(cluster);

        // assign one node so it is not in the pool
        nodeMap.get("super0").assign("topology1", Arrays.asList(executor1), cluster);
        freePool.init(cluster, nodeMap);
        isolatedPool.init(cluster, nodeMap);

        assertTrue(isolatedPool.canAdd(topology1));
        isolatedPool.addTopology(topology1);
        assertTrue(isolatedPool.canAdd(topology2));
        isolatedPool.addTopology(topology2);

        // nodes can be stolen from non-isolated tops in the pool
        assertEquals(4, isolatedPool.slotsAvailable());
        assertEquals(1, isolatedPool.nodesAvailable());
        assertEquals(4 * 4, freePool.slotsAvailable());
        assertEquals(4, freePool.nodesAvailable());
        assertEquals(1, cluster.getAssignmentById("topology1").getSlots().size());
        assertNull(cluster.getAssignmentById("topology2"));

        isolatedPool.scheduleAsNeeded(new NodePool[]{freePool});
        // We steal 2 nodes from the free pool to handle the extra (but still only 1 node for the non-isolated top)
        assertEquals(4, isolatedPool.slotsAvailable());
        assertEquals(1, isolatedPool.nodesAvailable());
        assertEquals(2 * 4, freePool.slotsAvailable());
        assertEquals(2, freePool.nodesAvailable());

        Set<WorkerSlot> assignedSlots1 = cluster.getAssignmentById("topology1").getSlots();
        // 4 slots on 1 machine
        assertEquals(4, assignedSlots1.size());
        assertEquals(1, getNodeIds(assignedSlots1).size());

        Set<WorkerSlot> assignedSlots2 = cluster.getAssignmentById("topology2").getSlots();
        // 4 slots on 2 machines
        assertEquals(4, assignedSlots2.size());
        assertEquals(2, getNodeIds(assignedSlots2).size());

        NodePool.NodeAndSlotCounts nsCount1 = isolatedPool.getNodeAndSlotCountIfSlotsWereTaken(1);
        NodePool.NodeAndSlotCounts nsCount3 = isolatedPool.getNodeAndSlotCountIfSlotsWereTaken(3);
        NodePool.NodeAndSlotCounts nsCount4 = isolatedPool.getNodeAndSlotCountIfSlotsWereTaken(4);
        NodePool.NodeAndSlotCounts nsCount5 = isolatedPool.getNodeAndSlotCountIfSlotsWereTaken(5);
        assertEquals(1, nsCount1.nodes);
        assertEquals(4, nsCount1.slots);
        assertEquals(1, nsCount3.nodes);
        assertEquals(4, nsCount3.slots);
        assertEquals(1, nsCount4.nodes);
        assertEquals(4, nsCount4.slots);
        assertEquals(1, nsCount5.nodes); // Only 1 node can be stolen right now
        assertEquals(4, nsCount5.slots);

        Collection<Node> nodes = isolatedPool.takeNodesBySlots(3);
        assertEquals(1, nodes.size());
        assertEquals(4, Node.countFreeSlotsAlive(nodes));
        assertEquals(4, Node.countTotalSlotsAlive(nodes));
        assertEquals(0, isolatedPool.nodesAvailable());
        assertEquals(0 * 4, isolatedPool.slotsAvailable());

        Set<WorkerSlot> assignedSlots1After = cluster.getAssignmentById("topology1").getSlots();
        // 0 slots on 0 machines (topology1 was evicted)
        assertEquals(0, assignedSlots1After.size());
        assertEquals(0, getNodeIds(assignedSlots1After).size());

        Set<WorkerSlot> assignedSlots2After = cluster.getAssignmentById("topology2").getSlots();
        // 4 slots on 2 machines
        assertEquals(4, assignedSlots2After.size());
        assertEquals(2, getNodeIds(assignedSlots2After).size());

        Collection<Node> nodes2 = isolatedPool.takeNodes(3); // Cannot steal from the isolated scheduler
        assertEquals(0, nodes2.size());
        assertEquals(0, Node.countFreeSlotsAlive(nodes2));
        assertEquals(0, Node.countTotalSlotsAlive(nodes2));
        assertEquals(0, isolatedPool.nodesAvailable());
        assertEquals(0, isolatedPool.slotsAvailable());

        assertEquals("Scheduled Isolated on 1 Nodes", cluster.getStatusMap().get("topology1"));
        assertEquals("Scheduled Isolated on 2 Nodes", cluster.getStatusMap().get("topology2"));
    }

    @Test
    public void testIsolatedPoolComplex2() {
        Map<String, SupervisorDetails> supers = genSupervisors(5);
        FreePool freePool = new FreePool();
        // like before but now we can only hold 2 nodes max. Don't go over
        IsolatedPool isolatedPool = new IsolatedPool(2);
        ExecutorDetails executor1 = ed(1);
        ExecutorDetails executor2 = ed(2);
        ExecutorDetails executor3 = ed(3);
        ExecutorDetails executor4 = ed(4);
        ExecutorDetails executor11 = ed(11);
        ExecutorDetails executor12 = ed(12);
        ExecutorDetails executor13 = ed(13);
        ExecutorDetails executor14 = ed(14);

        Map<String, Object> conf1 = new HashMap<>();
        conf1.put(Config.TOPOLOGY_NAME, "topology-name-1");
        Map<ExecutorDetails, String> execMap1 = new HashMap<>();
        execMap1.put(executor1, "spout1");
        execMap1.put(executor2, "bolt1");
        execMap1.put(executor3, "bolt2");
        execMap1.put(executor4, "bolt4");
        TopologyDetails topology1 = new TopologyDetails("topology1", conf1, new StormTopology(), 4, execMap1, "user");

        Map<String, Object> conf2 = new HashMap<>();
        conf2.put(Config.TOPOLOGY_NAME, "topology-name-2");
        conf2.put(Config.TOPOLOGY_ISOLATED_MACHINES, 2);
        Map<ExecutorDetails, String> execMap2 = new HashMap<>();
        execMap2.put(executor11, "spout11");
        execMap2.put(executor12, "bolt12");
        execMap2.put(executor13, "bolt13");
        execMap2.put(executor14, "bolt14");
        TopologyDetails topology2 = new TopologyDetails("topology2", conf2, new StormTopology(), 4, execMap2, "user");

        Topologies topologies = new Topologies(toTopMap(topology1, topology2));
        Cluster cluster = new Cluster(new StandaloneINimbus(), newResourceMetrics(), supers, new HashMap<>(),
            topologies, new HashMap<>());
        Map<String, Node> nodeMap = Node.getAllNodesFrom(cluster);

        // assign one node so it is not in the pool
        nodeMap.get("super0").assign("topology1", Arrays.asList(executor1), cluster);
        freePool.init(cluster, nodeMap);
        isolatedPool.init(cluster, nodeMap);

        assertTrue(isolatedPool.canAdd(topology1));
        isolatedPool.addTopology(topology1);
        assertTrue(isolatedPool.canAdd(topology2));
        isolatedPool.addTopology(topology2);

        // nodes can be stolen from non-isolated tops in the pool
        assertEquals(4, isolatedPool.slotsAvailable());
        assertEquals(1, isolatedPool.nodesAvailable());
        assertEquals(4 * 4, freePool.slotsAvailable());
        assertEquals(4, freePool.nodesAvailable());
        assertEquals(1, cluster.getAssignmentById("topology1").getSlots().size());
        assertNull(cluster.getAssignmentById("topology2"));

        isolatedPool.scheduleAsNeeded(new NodePool[]{freePool});
        // We steal 1 node from the free pool and 1 from ourself to handle the extra
        assertEquals(0, isolatedPool.slotsAvailable());
        assertEquals(0, isolatedPool.nodesAvailable());
        assertEquals(3 * 4, freePool.slotsAvailable());
        assertEquals(3, freePool.nodesAvailable());

        Set<WorkerSlot> assignedSlots1 = cluster.getAssignmentById("topology1").getSlots();
        // 0 slots on 0 machines
        assertEquals(0, assignedSlots1.size());
        assertEquals(0, getNodeIds(assignedSlots1).size());

        Set<WorkerSlot> assignedSlots2 = cluster.getAssignmentById("topology2").getSlots();
        // 4 slots on 2 machines
        assertEquals(4, assignedSlots2.size());
        assertEquals(2, getNodeIds(assignedSlots2).size());

        // The text can be off for a bit until we schedule again
        isolatedPool.scheduleAsNeeded(new NodePool[]{freePool});
        assertEquals("Max Nodes(2) for this user would be exceeded. 1 more nodes needed to run topology.",
            cluster.getStatusMap().get("topology1"));
        assertEquals("Scheduled Isolated on 2 Nodes", cluster.getStatusMap().get("topology2"));
    }

    @Test
    public void testMultitenantScheduler() {
        Map<String, SupervisorDetails> supers = genSupervisors(10);
        Map<String, Object> conf1 = new HashMap<>();
        conf1.put(Config.TOPOLOGY_NAME, "topology-name-1");
        TopologyDetails topology1 = new TopologyDetails("topology1", conf1, new StormTopology(), 4,
            mkEdMap(new Object[]{"spout1", 0, 5}, new Object[]{"bolt1", 5, 10},
                new Object[]{"bolt2", 10, 15}, new Object[]{"bolt3", 15, 20}), "userC");

        Map<String, Object> conf2 = new HashMap<>();
        conf2.put(Config.TOPOLOGY_NAME, "topology-name-2");
        conf2.put(Config.TOPOLOGY_ISOLATED_MACHINES, 2);
        TopologyDetails topology2 = new TopologyDetails("topology2", conf2, new StormTopology(), 4,
            mkEdMap(new Object[]{"spout11", 0, 5}, new Object[]{"bolt12", 5, 6},
                new Object[]{"bolt13", 6, 7}, new Object[]{"bolt14", 7, 10}), "userA");

        Map<String, Object> conf3 = new HashMap<>();
        conf3.put(Config.TOPOLOGY_NAME, "topology-name-3");
        conf3.put(Config.TOPOLOGY_ISOLATED_MACHINES, 5);
        TopologyDetails topology3 = new TopologyDetails("topology3", conf3, new StormTopology(), 10,
            mkEdMap(new Object[]{"spout21", 0, 10}, new Object[]{"bolt22", 10, 20},
                new Object[]{"bolt23", 20, 30}, new Object[]{"bolt24", 30, 40}), "userB");

        Topologies topologies = new Topologies(toTopMap(topology1, topology2, topology3));
        Cluster cluster = new Cluster(new StandaloneINimbus(), newResourceMetrics(), supers, new HashMap<>(),
            topologies, new HashMap<>());
        Map<String, Node> nodeMap = Node.getAllNodesFrom(cluster);

        Map<String, Object> conf = new HashMap<>();
        Map<String, Integer> userPools = new HashMap<>();
        userPools.put("userA", 5);
        userPools.put("userB", 5);
        conf.put(DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS, userPools);

        MultitenantScheduler scheduler = new MultitenantScheduler();
        nodeMap.get("super0").assign("topology1", Arrays.asList(ed(1)), cluster);
        nodeMap.get("super1").assign("topology2", Arrays.asList(ed(5)), cluster);
        scheduler.prepare(conf, new StormMetricsRegistry());
        try {
            scheduler.schedule(topologies, cluster);

            SchedulerAssignment assignment = cluster.getAssignmentById("topology1");
            Set<WorkerSlot> assignedSlots = assignment.getSlots();
            // 4 slots on 1 machine, all executors assigned
            assertEquals(4, assignedSlots.size());
            assertEquals(1, getNodeIds(assignedSlots).size());
            assertEquals(20, assignment.getExecutors().size());

            assertEquals("Fully Scheduled", cluster.getStatusMap().get("topology1"));
            assertEquals("Scheduled Isolated on 2 Nodes", cluster.getStatusMap().get("topology2"));
            assertEquals("Scheduled Isolated on 5 Nodes", cluster.getStatusMap().get("topology3"));
        } finally {
            scheduler.cleanup();
        }
    }

    @Test
    public void testForceFreeSlotInBadState() {
        Map<String, SupervisorDetails> supers = genSupervisors(1);
        Map<String, Object> conf1 = new HashMap<>();
        conf1.put(Config.TOPOLOGY_NAME, "topology-name-1");
        TopologyDetails topology1 = new TopologyDetails("topology1", conf1, new StormTopology(), 4,
            mkEdMap(new Object[]{"spout1", 0, 5}, new Object[]{"bolt1", 5, 10},
                new Object[]{"bolt2", 10, 15}, new Object[]{"bolt3", 15, 20}), "userC");

        Map<ExecutorDetails, WorkerSlot> executorToSlot = new HashMap<>();
        executorToSlot.put(new ExecutorDetails(0, 5), new WorkerSlot("super0", 1));
        executorToSlot.put(new ExecutorDetails(5, 10), new WorkerSlot("super0", 20));
        executorToSlot.put(new ExecutorDetails(10, 15), new WorkerSlot("super0", 1));
        executorToSlot.put(new ExecutorDetails(15, 20), new WorkerSlot("super0", 1));
        Map<String, SchedulerAssignmentImpl> existingAssignments = new HashMap<>();
        existingAssignments.put("topology1", new SchedulerAssignmentImpl("topology1", executorToSlot, null, null));

        Topologies topologies = new Topologies(toTopMap(topology1));
        Cluster cluster = new Cluster(new StandaloneINimbus(), newResourceMetrics(), supers, existingAssignments,
            topologies, new HashMap<>());
        Map<String, Node> nodeMap = Node.getAllNodesFrom(cluster);

        Map<String, Object> conf = new HashMap<>();
        Map<String, Integer> userPools = new HashMap<>();
        userPools.put("userA", 5);
        userPools.put("userB", 5);
        conf.put(DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS, userPools);

        MultitenantScheduler scheduler = new MultitenantScheduler();
        nodeMap.get("super0").assign("topology1", Arrays.asList(ed(1)), cluster);
        scheduler.prepare(conf, new StormMetricsRegistry());
        try {
            scheduler.schedule(topologies, cluster);

            SchedulerAssignment assignment = cluster.getAssignmentById("topology1");
            Set<WorkerSlot> assignedSlots = assignment.getSlots();
            // 4 slots on 1 machine, all executors assigned
            assertEquals(4, assignedSlots.size());
            assertEquals(1, getNodeIds(assignedSlots).size());

            assertEquals("Fully Scheduled", cluster.getStatusMap().get("topology1"));
        } finally {
            scheduler.cleanup();
        }
    }

    @Test
    public void testMultitenantSchedulerBadStartingState() {
        Map<String, SupervisorDetails> supers = genSupervisors(5);

        Map<String, Object> conf1 = new HashMap<>();
        conf1.put(Config.TOPOLOGY_NAME, "topology-name-1");
        TopologyDetails topology1 = new TopologyDetails("topology1", conf1, new StormTopology(), 1,
            mkEdMap(new Object[]{"spout1", 0, 1}), "userC");

        Map<String, Object> conf2 = new HashMap<>();
        conf2.put(Config.TOPOLOGY_NAME, "topology-name-2");
        conf2.put(Config.TOPOLOGY_ISOLATED_MACHINES, 2);
        TopologyDetails topology2 = new TopologyDetails("topology2", conf2, new StormTopology(), 2,
            mkEdMap(new Object[]{"spout11", 1, 2}, new Object[]{"bolt11", 3, 4}), "userA");

        Map<String, Object> conf3 = new HashMap<>();
        conf3.put(Config.TOPOLOGY_NAME, "topology-name-3");
        conf3.put(Config.TOPOLOGY_ISOLATED_MACHINES, 1);
        TopologyDetails topology3 = new TopologyDetails("topology3", conf3, new StormTopology(), 1,
            mkEdMap(new Object[]{"spout21", 2, 3}), "userB");

        WorkerSlot workerSlotWithMultipleAssignments = new WorkerSlot("super1", 1);
        Map<String, SchedulerAssignmentImpl> existingAssignments = new HashMap<>();
        Map<ExecutorDetails, WorkerSlot> execToSlot2 = new HashMap<>();
        execToSlot2.put(new ExecutorDetails(1, 1), workerSlotWithMultipleAssignments);
        existingAssignments.put("topology2", new SchedulerAssignmentImpl("topology2", execToSlot2, null, null));
        Map<ExecutorDetails, WorkerSlot> execToSlot3 = new HashMap<>();
        execToSlot3.put(new ExecutorDetails(2, 2), workerSlotWithMultipleAssignments);
        existingAssignments.put("topology3", new SchedulerAssignmentImpl("topology3", execToSlot3, null, null));

        Topologies topologies = new Topologies(toTopMap(topology1, topology2, topology3));
        Cluster cluster = new Cluster(new StandaloneINimbus(), newResourceMetrics(), supers, existingAssignments,
            topologies, new HashMap<>());

        Map<String, Object> conf = new HashMap<>();
        Map<String, Integer> userPools = new HashMap<>();
        userPools.put("userA", 2);
        userPools.put("userB", 1);
        conf.put(DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS, userPools);

        MultitenantScheduler scheduler = new MultitenantScheduler();
        scheduler.prepare(conf, new StormMetricsRegistry());
        try {
            scheduler.schedule(topologies, cluster);

            SchedulerAssignment assignment = cluster.getAssignmentById("topology1");
            Set<WorkerSlot> assignedSlots = assignment.getSlots();
            assertEquals(1, assignedSlots.size());

            assertEquals("Fully Scheduled", cluster.getStatusMap().get("topology1"));
            assertEquals("Scheduled Isolated on 2 Nodes", cluster.getStatusMap().get("topology2"));
            assertEquals("Scheduled Isolated on 1 Nodes", cluster.getStatusMap().get("topology3"));
        } finally {
            scheduler.cleanup();
        }
    }

    @Test
    public void testExistingAssignmentSlotNotFoundInSupervisor() {
        Map<String, SupervisorDetails> supers = genSupervisors(1);
        int portNotReportedBySupervisor = 6;

        Map<String, Object> conf1 = new HashMap<>();
        conf1.put(Config.TOPOLOGY_NAME, "topology-name-1");
        TopologyDetails topology1 = new TopologyDetails("topology1", conf1, new StormTopology(), 1,
            mkEdMap(new Object[]{"spout11", 0, 1}), "userA");

        Map<ExecutorDetails, WorkerSlot> execToSlot = new HashMap<>();
        execToSlot.put(new ExecutorDetails(0, 0), new WorkerSlot("super0", portNotReportedBySupervisor));
        Map<String, SchedulerAssignmentImpl> existingAssignments = new HashMap<>();
        existingAssignments.put("topology1", new SchedulerAssignmentImpl("topology1", execToSlot, null, null));

        Topologies topologies = new Topologies(toTopMap(topology1));
        Cluster cluster = new Cluster(new StandaloneINimbus(), newResourceMetrics(), supers, existingAssignments,
            topologies, new HashMap<>());

        Map<String, Object> conf = new HashMap<>();
        MultitenantScheduler scheduler = new MultitenantScheduler();
        scheduler.prepare(conf, new StormMetricsRegistry());
        try {
            scheduler.schedule(topologies, cluster);

            SchedulerAssignment assignment = cluster.getAssignmentById("topology1");
            Set<WorkerSlot> assignedSlots = assignment.getSlots();
            assertEquals(1, assignedSlots.size());

            assertEquals("Fully Scheduled", cluster.getStatusMap().get("topology1"));
        } finally {
            scheduler.cleanup();
        }
    }

    @Test
    public void testExistingAssignmentSlotOnDeadSupervisor() {
        Map<String, SupervisorDetails> supers = genSupervisors(1);
        String deadSupervisor = "super1";
        int portNotReportedBySupervisor = 6;

        Map<String, Object> conf1 = new HashMap<>();
        conf1.put(Config.TOPOLOGY_NAME, "topology-name-1");
        TopologyDetails topology1 = new TopologyDetails("topology1", conf1, new StormTopology(), 2,
            mkEdMap(new Object[]{"spout11", 0, 1}, new Object[]{"bolt12", 1, 2}), "userA");

        Map<String, Object> conf2 = new HashMap<>();
        conf2.put(Config.TOPOLOGY_NAME, "topology-name-2");
        TopologyDetails topology2 = new TopologyDetails("topology2", conf2, new StormTopology(), 2,
            mkEdMap(new Object[]{"spout21", 4, 5}, new Object[]{"bolt22", 5, 6}), "userA");

        WorkerSlot workerSlotWithMultipleAssignments = new WorkerSlot(deadSupervisor, 1);
        Map<String, SchedulerAssignmentImpl> existingAssignments = new HashMap<>();

        Map<ExecutorDetails, WorkerSlot> execToSlot1 = new HashMap<>();
        execToSlot1.put(new ExecutorDetails(0, 0), workerSlotWithMultipleAssignments);
        execToSlot1.put(new ExecutorDetails(1, 1), new WorkerSlot(deadSupervisor, 3));
        existingAssignments.put("topology1", new SchedulerAssignmentImpl("topology1", execToSlot1, null, null));

        Map<ExecutorDetails, WorkerSlot> execToSlot2 = new HashMap<>();
        execToSlot2.put(new ExecutorDetails(4, 4), workerSlotWithMultipleAssignments);
        execToSlot2.put(new ExecutorDetails(5, 5), new WorkerSlot(deadSupervisor, portNotReportedBySupervisor));
        existingAssignments.put("topology2", new SchedulerAssignmentImpl("topology2", execToSlot2, null, null));

        Topologies topologies = new Topologies(toTopMap(topology1, topology2));
        Cluster cluster = new Cluster(new StandaloneINimbus(), newResourceMetrics(), supers, existingAssignments,
            topologies, new HashMap<>());

        Map<String, Object> conf = new HashMap<>();
        MultitenantScheduler scheduler = new MultitenantScheduler();
        scheduler.prepare(conf, new StormMetricsRegistry());
        try {
            scheduler.schedule(topologies, cluster);

            SchedulerAssignment assignment1 = cluster.getAssignmentById("topology1");
            assertEquals(2, assignment1.getSlots().size());
            assertEquals(2, assignment1.getExecutors().size());
            assertEquals("Fully Scheduled", cluster.getStatusMap().get("topology1"));

            SchedulerAssignment assignment2 = cluster.getAssignmentById("topology2");
            assertEquals(2, assignment2.getSlots().size());
            assertEquals(2, assignment2.getExecutors().size());
            assertEquals("Fully Scheduled", cluster.getStatusMap().get("topology2"));
        } finally {
            scheduler.cleanup();
        }
    }

    @Test
    public void testIsolatedPoolSchedulingWithNodesWithDifferentNumberOfSlots() {
        SupervisorDetails super1 = new SupervisorDetails("super1", "host2", Collections.emptyList(),
            Arrays.asList(1, 2, 3, 4, 5));
        SupervisorDetails super2 = new SupervisorDetails("super2", "host2", Collections.emptyList(),
            Arrays.asList(1, 2));
        Map<String, SupervisorDetails> supers = new HashMap<>();
        supers.put("super1", super1);
        supers.put("super2", super2);

        Map<String, Object> conf1 = new HashMap<>();
        conf1.put(Config.TOPOLOGY_NAME, "topology-name-1");
        conf1.put(Config.TOPOLOGY_ISOLATED_MACHINES, 1);
        TopologyDetails topology1 = new TopologyDetails("topology1", conf1, new StormTopology(), 7,
            mkEdMap(new Object[]{"spout21", 0, 7}), "userA");

        Map<ExecutorDetails, WorkerSlot> execToSlot = new HashMap<>();
        execToSlot.put(new ExecutorDetails(0, 0), new WorkerSlot("super1", 1));
        execToSlot.put(new ExecutorDetails(1, 1), new WorkerSlot("super1", 2));
        execToSlot.put(new ExecutorDetails(2, 2), new WorkerSlot("super1", 3));
        execToSlot.put(new ExecutorDetails(3, 3), new WorkerSlot("super1", 4));
        execToSlot.put(new ExecutorDetails(4, 4), new WorkerSlot("super2", 1));
        execToSlot.put(new ExecutorDetails(5, 5), new WorkerSlot("super2", 2));
        Map<String, SchedulerAssignmentImpl> existingAssignments = new HashMap<>();
        existingAssignments.put("topology1", new SchedulerAssignmentImpl("topology1", execToSlot, null, null));

        Topologies topologies = new Topologies(toTopMap(topology1));
        Cluster cluster = new Cluster(new StandaloneINimbus(), newResourceMetrics(), supers, existingAssignments,
            topologies, new HashMap<>());

        Map<String, Object> conf = new HashMap<>();
        Map<String, Integer> userPools = new HashMap<>();
        userPools.put("userA", 2);
        conf.put(DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS, userPools);

        MultitenantScheduler scheduler = new MultitenantScheduler();
        scheduler.prepare(conf, new StormMetricsRegistry());
        try {
            scheduler.schedule(topologies, cluster);
        } finally {
            scheduler.cleanup();
        }
        assertEquals("Scheduled Isolated on 2 Nodes", cluster.getStatusMap().get("topology1"));
    }
}
