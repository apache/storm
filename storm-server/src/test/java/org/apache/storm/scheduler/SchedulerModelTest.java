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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.storm.Config;
import org.apache.storm.daemon.nimbus.Nimbus.StandaloneINimbus;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.resource.normalization.ResourceMetrics;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SchedulerModelTest {

    @Test
    public void testSupervisorDetails() {
        Map<ExecutorDetails, WorkerSlot> executorToSlot = new HashMap<>();
        executorToSlot.put(new ExecutorDetails(1, 5), new WorkerSlot("supervisor1", 1));
        executorToSlot.put(new ExecutorDetails(6, 10), new WorkerSlot("supervisor2", 2));

        String topologyId = "topology1";
        SchedulerAssignmentImpl assignment = new SchedulerAssignmentImpl(topologyId, executorToSlot, null, null);

        // test assign
        assignment.assign(new WorkerSlot("supervisor1", 1),
                Arrays.asList(new ExecutorDetails(11, 15), new ExecutorDetails(16, 20)));

        Map<ExecutorDetails, WorkerSlot> result = assignment.getExecutorToSlot();
        assertEquals(new WorkerSlot("supervisor1", 1), result.get(new ExecutorDetails(1, 5)));
        assertEquals(new WorkerSlot("supervisor2", 2), result.get(new ExecutorDetails(6, 10)));
        assertEquals(new WorkerSlot("supervisor1", 1), result.get(new ExecutorDetails(11, 15)));
        assertEquals(new WorkerSlot("supervisor1", 1), result.get(new ExecutorDetails(16, 20)));
        assertEquals(4, result.size());

        // test isSlotOccupied
        assertTrue(assignment.isSlotOccupied(new WorkerSlot("supervisor2", 2)));
        assertTrue(assignment.isSlotOccupied(new WorkerSlot("supervisor1", 1)));

        // test isExecutorAssigned
        assertTrue(assignment.isExecutorAssigned(new ExecutorDetails(1, 5)));
        assertFalse(assignment.isExecutorAssigned(new ExecutorDetails(21, 25)));

        // test unassignBySlot
        assignment.unassignBySlot(new WorkerSlot("supervisor1", 1));
        Map<ExecutorDetails, WorkerSlot> afterUnassign = assignment.getExecutorToSlot();
        assertEquals(1, afterUnassign.size());
        assertEquals(new WorkerSlot("supervisor2", 2), afterUnassign.get(new ExecutorDetails(6, 10)));
    }

    @Test
    public void testTopologies() {
        ExecutorDetails executor1 = new ExecutorDetails(1, 5);
        ExecutorDetails executor2 = new ExecutorDetails(6, 10);

        Map<String, Object> conf1 = new HashMap<>();
        conf1.put(Config.TOPOLOGY_NAME, "topology-name-1");

        Map<ExecutorDetails, String> executorToComponent1 = new HashMap<>();
        executorToComponent1.put(executor1, "spout1");
        executorToComponent1.put(executor2, "bolt1");

        TopologyDetails topology1 = new TopologyDetails("topology1", conf1, new StormTopology(), 1,
                executorToComponent1, "user");

        // test topology.selectExecutorToComponent
        Map<ExecutorDetails, String> selected = topology1.selectExecutorToComponent(Arrays.asList(executor1));
        assertEquals(1, selected.size());
        assertEquals("spout1", selected.get(executor1));

        // test topologies.getById
        Map<String, Object> conf2 = new HashMap<>();
        conf2.put(Config.TOPOLOGY_NAME, "topology-name-2");

        TopologyDetails topology2 = new TopologyDetails("topology2", conf2, new StormTopology(), 1,
                new HashMap<>(), "user");

        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put("topology1", topology1);
        topoMap.put("topology2", topology2);
        Topologies topologies = new Topologies(topoMap);

        assertEquals("topology1", topologies.getById("topology1").getId());

        // test topologies.getByName
        assertEquals("topology2", topologies.getByName("topology-name-2").getId());
    }

    @Test
    public void testCluster() {
        SupervisorDetails supervisor1 = new SupervisorDetails("supervisor1", "192.168.0.1",
                Collections.emptyList(), Arrays.asList(1, 3, 5, 7, 9));
        SupervisorDetails supervisor2 = new SupervisorDetails("supervisor2", "192.168.0.2",
                Collections.emptyList(), Arrays.asList(2, 4, 6, 8, 10));

        ExecutorDetails executor1 = new ExecutorDetails(1, 5);
        ExecutorDetails executor2 = new ExecutorDetails(6, 10);
        ExecutorDetails executor3 = new ExecutorDetails(11, 15);
        ExecutorDetails executor11 = new ExecutorDetails(100, 105);
        ExecutorDetails executor12 = new ExecutorDetails(106, 110);
        ExecutorDetails executor21 = new ExecutorDetails(201, 205);
        ExecutorDetails executor22 = new ExecutorDetails(206, 210);

        // topology1 needs scheduling: executor3 is NOT assigned a slot.
        Map<String, Object> conf1 = new HashMap<>();
        conf1.put(Config.TOPOLOGY_NAME, "topology-name-1");
        Map<ExecutorDetails, String> exec2comp1 = new HashMap<>();
        exec2comp1.put(executor1, "spout1");
        exec2comp1.put(executor2, "bolt1");
        exec2comp1.put(executor3, "bolt2");
        TopologyDetails topology1 = new TopologyDetails("topology1", conf1, new StormTopology(), 2,
                exec2comp1, "user");

        // topology2 is fully scheduled
        Map<String, Object> conf2 = new HashMap<>();
        conf2.put(Config.TOPOLOGY_NAME, "topology-name-2");
        Map<ExecutorDetails, String> exec2comp2 = new HashMap<>();
        exec2comp2.put(executor11, "spout11");
        exec2comp2.put(executor12, "bolt12");
        TopologyDetails topology2 = new TopologyDetails("topology2", conf2, new StormTopology(), 2,
                exec2comp2, "user");

        // topology3 needs scheduling, since the assignment is squeezed
        Map<String, Object> conf3 = new HashMap<>();
        conf3.put(Config.TOPOLOGY_NAME, "topology-name-3");
        Map<ExecutorDetails, String> exec2comp3 = new HashMap<>();
        exec2comp3.put(executor21, "spout21");
        exec2comp3.put(executor22, "bolt22");
        TopologyDetails topology3 = new TopologyDetails("topology3", conf3, new StormTopology(), 2,
                exec2comp3, "user");

        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put("topology1", topology1);
        topoMap.put("topology2", topology2);
        topoMap.put("topology3", topology3);
        Topologies topologies = new Topologies(topoMap);

        Map<ExecutorDetails, WorkerSlot> executorToSlot1 = new HashMap<>();
        executorToSlot1.put(executor1, new WorkerSlot("supervisor1", 1));
        executorToSlot1.put(executor2, new WorkerSlot("supervisor2", 2));

        Map<ExecutorDetails, WorkerSlot> executorToSlot2 = new HashMap<>();
        executorToSlot2.put(executor11, new WorkerSlot("supervisor1", 3));
        executorToSlot2.put(executor12, new WorkerSlot("supervisor2", 4));

        Map<ExecutorDetails, WorkerSlot> executorToSlot3 = new HashMap<>();
        executorToSlot3.put(executor21, new WorkerSlot("supervisor1", 5));
        executorToSlot3.put(executor22, new WorkerSlot("supervisor1", 5));

        SchedulerAssignmentImpl assignment1 = new SchedulerAssignmentImpl("topology1", executorToSlot1, null, null);
        SchedulerAssignmentImpl assignment2 = new SchedulerAssignmentImpl("topology2", executorToSlot2, null, null);
        SchedulerAssignmentImpl assignment3 = new SchedulerAssignmentImpl("topology3", executorToSlot3, null, null);

        Map<String, SupervisorDetails> supervisors = new HashMap<>();
        supervisors.put("supervisor1", supervisor1);
        supervisors.put("supervisor2", supervisor2);

        Map<String, SchedulerAssignmentImpl> assignments = new HashMap<>();
        assignments.put("topology1", assignment1);
        assignments.put("topology2", assignment2);
        assignments.put("topology3", assignment3);

        Cluster cluster = new Cluster(new StandaloneINimbus(),
                new ResourceMetrics(new StormMetricsRegistry()),
                supervisors, assignments, topologies, new HashMap<>());

        // test Cluster constructor
        assertEquals(new HashSet<>(Arrays.asList("supervisor1", "supervisor2")),
                cluster.getSupervisors().keySet());
        assertEquals(new HashSet<>(Arrays.asList("topology1", "topology2", "topology3")),
                cluster.getAssignments().keySet());

        // test Cluster.getUnassignedExecutors
        assertEquals(new HashSet<>(Arrays.asList(executor3)),
                new HashSet<>(cluster.getUnassignedExecutors(topology1)));
        assertTrue(cluster.getUnassignedExecutors(topology2).isEmpty());

        // test Cluster.needsScheduling
        assertTrue(cluster.needsScheduling(topology1));
        assertFalse(cluster.needsScheduling(topology2));
        assertTrue(cluster.needsScheduling(topology3));

        // test Cluster.needsSchedulingTopologies
        Set<String> needsSchedulingIds = cluster.needsSchedulingTopologies().stream()
                .map(TopologyDetails::getId)
                .collect(Collectors.toSet());
        assertEquals(new HashSet<>(Arrays.asList("topology1", "topology3")), needsSchedulingIds);

        // test Cluster.getNeedsSchedulingExecutorToComponents
        Map<ExecutorDetails, String> needsSched1 = cluster.getNeedsSchedulingExecutorToComponents(topology1);
        assertEquals(1, needsSched1.size());
        assertEquals("bolt2", needsSched1.get(executor3));
        assertTrue(cluster.getNeedsSchedulingExecutorToComponents(topology2).isEmpty());
        assertTrue(cluster.getNeedsSchedulingExecutorToComponents(topology3).isEmpty());

        // test Cluster.getNeedsSchedulingComponentToExecutors
        Map<String, List<ExecutorDetails>> needsSchedComp1 =
                cluster.getNeedsSchedulingComponentToExecutors(topology1);
        assertEquals(1, needsSchedComp1.size());
        assertEquals(new HashSet<>(Arrays.asList(executor3)),
                new HashSet<>(needsSchedComp1.get("bolt2")));
        assertTrue(cluster.getNeedsSchedulingComponentToExecutors(topology2).isEmpty());
        assertTrue(cluster.getNeedsSchedulingComponentToExecutors(topology3).isEmpty());

        // test Cluster.getUsedPorts
        assertEquals(new HashSet<>(Arrays.asList(1, 3, 5)),
                new HashSet<>(cluster.getUsedPorts(supervisor1)));
        assertEquals(new HashSet<>(Arrays.asList(2, 4)),
                new HashSet<>(cluster.getUsedPorts(supervisor2)));

        // test Cluster.getAvailablePorts
        assertEquals(new HashSet<>(Arrays.asList(7, 9)),
                new HashSet<>(cluster.getAvailablePorts(supervisor1)));
        assertEquals(new HashSet<>(Arrays.asList(6, 8, 10)),
                new HashSet<>(cluster.getAvailablePorts(supervisor2)));

        // test Cluster.getAvailableSlots (per supervisor)
        Set<String> availSlots1 = cluster.getAvailableSlots(supervisor1).stream()
                .map(s -> s.getNodeId() + ":" + s.getPort())
                .collect(Collectors.toSet());
        assertEquals(new HashSet<>(Arrays.asList("supervisor1:7", "supervisor1:9")), availSlots1);

        Set<String> availSlots2 = cluster.getAvailableSlots(supervisor2).stream()
                .map(s -> s.getNodeId() + ":" + s.getPort())
                .collect(Collectors.toSet());
        assertEquals(new HashSet<>(Arrays.asList("supervisor2:6", "supervisor2:8", "supervisor2:10")), availSlots2);

        // test Cluster.getAvailableSlots (all)
        Set<String> allAvailSlots = cluster.getAvailableSlots().stream()
                .map(s -> s.getNodeId() + ":" + s.getPort())
                .collect(Collectors.toSet());
        assertEquals(new HashSet<>(Arrays.asList(
                "supervisor1:7", "supervisor1:9",
                "supervisor2:6", "supervisor2:8", "supervisor2:10")),
                allAvailSlots);

        // test Cluster.getAssignedNumWorkers
        assertEquals(2, cluster.getAssignedNumWorkers(topology1));
        assertEquals(2, cluster.getAssignedNumWorkers(topology2));
        assertEquals(1, cluster.getAssignedNumWorkers(topology3));

        // test Cluster.isSlotOccupied
        assertTrue(cluster.isSlotOccupied(new WorkerSlot("supervisor1", 1)));
        assertTrue(cluster.isSlotOccupied(new WorkerSlot("supervisor1", 3)));
        assertTrue(cluster.isSlotOccupied(new WorkerSlot("supervisor1", 5)));
        assertFalse(cluster.isSlotOccupied(new WorkerSlot("supervisor1", 7)));
        assertFalse(cluster.isSlotOccupied(new WorkerSlot("supervisor1", 9)));
        assertTrue(cluster.isSlotOccupied(new WorkerSlot("supervisor2", 2)));
        assertTrue(cluster.isSlotOccupied(new WorkerSlot("supervisor2", 4)));
        assertFalse(cluster.isSlotOccupied(new WorkerSlot("supervisor2", 6)));
        assertFalse(cluster.isSlotOccupied(new WorkerSlot("supervisor2", 8)));
        assertFalse(cluster.isSlotOccupied(new WorkerSlot("supervisor2", 10)));

        // test Cluster.getAssignmentById
        assertTrue(assignment1.equalsIgnoreResources(cluster.getAssignmentById("topology1")));
        assertTrue(assignment2.equalsIgnoreResources(cluster.getAssignmentById("topology2")));
        assertTrue(assignment3.equalsIgnoreResources(cluster.getAssignmentById("topology3")));

        // test Cluster.getSupervisorById
        assertEquals(supervisor1, cluster.getSupervisorById("supervisor1"));
        assertEquals(supervisor2, cluster.getSupervisorById("supervisor2"));

        // test Cluster.getSupervisorsByHost
        assertEquals(new HashSet<>(Arrays.asList(supervisor1)),
                new HashSet<>(cluster.getSupervisorsByHost("192.168.0.1")));
        assertEquals(new HashSet<>(Arrays.asList(supervisor2)),
                new HashSet<>(cluster.getSupervisorsByHost("192.168.0.2")));

        // ==== the following tests will change the state of the cluster ====

        // test Cluster.assign
        cluster.assign(new WorkerSlot("supervisor1", 7), "topology1", Arrays.asList(executor3));
        assertFalse(cluster.needsScheduling(topology1));
        assertTrue(cluster.isSlotOccupied(new WorkerSlot("supervisor1", 7)));

        // revert the change
        cluster.freeSlot(new WorkerSlot("supervisor1", 7));

        // test Cluster.assign: if an executor is already assigned, there will be an exception
        assertThrows(Exception.class,
                () -> cluster.assign(new WorkerSlot("supervisor1", 9), "topology1", Arrays.asList(executor1)));

        // test Cluster.assign: if a slot is occupied, there will be an exception
        assertThrows(Exception.class,
                () -> cluster.assign(new WorkerSlot("supervisor2", 4), "topology1", Arrays.asList(executor3)));

        // test Cluster.freeSlot
        cluster.freeSlot(new WorkerSlot("supervisor1", 7));
        assertFalse(cluster.isSlotOccupied(new WorkerSlot("supervisor1", 7)));

        // test Cluster.freeSlots
        assertTrue(cluster.isSlotOccupied(new WorkerSlot("supervisor1", 1)));
        assertTrue(cluster.isSlotOccupied(new WorkerSlot("supervisor1", 3)));
        assertTrue(cluster.isSlotOccupied(new WorkerSlot("supervisor1", 5)));
        cluster.freeSlots(Arrays.asList(
                new WorkerSlot("supervisor1", 1),
                new WorkerSlot("supervisor1", 3),
                new WorkerSlot("supervisor1", 5)));
        assertFalse(cluster.isSlotOccupied(new WorkerSlot("supervisor1", 1)));
        assertFalse(cluster.isSlotOccupied(new WorkerSlot("supervisor1", 3)));
        assertFalse(cluster.isSlotOccupied(new WorkerSlot("supervisor1", 5)));
    }

    @Test
    public void testSortSlots() {
        // test supervisor2 has more free slots
        List<WorkerSlot> sorted1 = EvenScheduler.sortSlots(Arrays.asList(
                new WorkerSlot("supervisor1", 6700), new WorkerSlot("supervisor1", 6701),
                new WorkerSlot("supervisor2", 6700), new WorkerSlot("supervisor2", 6701),
                new WorkerSlot("supervisor2", 6702)));

        List<WorkerSlot> expected1 = Arrays.asList(
                new WorkerSlot("supervisor2", 6700), new WorkerSlot("supervisor1", 6700),
                new WorkerSlot("supervisor2", 6701), new WorkerSlot("supervisor1", 6701),
                new WorkerSlot("supervisor2", 6702));
        assertEquals(expected1, sorted1);

        // test supervisor3 has more free slots
        List<WorkerSlot> sorted2 = EvenScheduler.sortSlots(Arrays.asList(
                new WorkerSlot("supervisor1", 6700), new WorkerSlot("supervisor1", 6701),
                new WorkerSlot("supervisor2", 6700), new WorkerSlot("supervisor2", 6701),
                new WorkerSlot("supervisor2", 6702),
                new WorkerSlot("supervisor3", 6700), new WorkerSlot("supervisor3", 6703),
                new WorkerSlot("supervisor3", 6702), new WorkerSlot("supervisor3", 6701)));

        List<WorkerSlot> expected2 = Arrays.asList(
                new WorkerSlot("supervisor3", 6700), new WorkerSlot("supervisor2", 6700),
                new WorkerSlot("supervisor1", 6700),
                new WorkerSlot("supervisor3", 6701), new WorkerSlot("supervisor2", 6701),
                new WorkerSlot("supervisor1", 6701),
                new WorkerSlot("supervisor3", 6702), new WorkerSlot("supervisor2", 6702),
                new WorkerSlot("supervisor3", 6703));
        assertEquals(expected2, sorted2);
    }
}
