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

import com.google.common.collect.Sets;
import org.apache.storm.Config;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.networktopography.DNSToSwitchMapping;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.ResourceAwareScheduler;
import org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourcesExtension;
import org.apache.storm.scheduler.resource.normalization.ResourceMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.INimbusTest;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.createRoundRobinClusterConfig;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.genSupervisorsWithRacksAndNuma;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.genTopology;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.supervisorIdToRackName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith({NormalizedResourcesExtension.class})
public class TestRoundRobinNodeSorterHostIsolation {
    private static final Logger LOG = LoggerFactory.getLogger(TestRoundRobinNodeSorterHostIsolation.class);
    private static final int CURRENT_TIME = 1450418597;
    private static final Class strategyClass = RoundRobinResourceAwareStrategy.class;

    private Config createClusterConfig(double compPcore, double compOnHeap, double compOffHeap,
                                       Map<String, Map<String, Number>> pools) {
        Config config = TestUtilsForResourceAwareScheduler.createClusterConfig(compPcore, compOnHeap, compOffHeap, pools);
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, strategyClass.getName());
        return config;
    }

    private static class TestDNSToSwitchMapping implements DNSToSwitchMapping {
        private final Map<String, String> hostToRackMap;
        private final Map<String, List<String>> rackToHosts;

        @SafeVarargs
        public TestDNSToSwitchMapping(Map<String, SupervisorDetails>... racks) {
            Set<String> seenHosts = new HashSet<>();
            Map<String, String> hostToRackMap = new HashMap<>();
            Map<String, List<String>> rackToHosts = new HashMap<>();
            for (int rackNum = 0; rackNum < racks.length; rackNum++) {
                String rack = String.format("rack-%03d", rackNum);
                for (SupervisorDetails sup : racks[rackNum].values()) {
                    hostToRackMap.put(sup.getHost(), rack);
                    String host = sup.getHost();
                    if (!seenHosts.contains(host)) {
                        rackToHosts.computeIfAbsent(rack, rid -> new ArrayList<>()).add(host);
                        seenHosts.add(host);
                    }
                }
            }
            this.hostToRackMap = Collections.unmodifiableMap(hostToRackMap);
            this.rackToHosts = Collections.unmodifiableMap(rackToHosts);
        }

        /**
         * Use the "rack-%03d" embedded in the name of the supervisor to determine the rack number.
         *
         * @param supervisorDetailsCollection
         */
        public TestDNSToSwitchMapping(Collection<SupervisorDetails> supervisorDetailsCollection) {
            Set<String> seenHosts = new HashSet<>();
            Map<String, String> hostToRackMap = new HashMap<>();
            Map<String, List<String>> rackToHosts = new HashMap<>();

            for (SupervisorDetails supervisorDetails: supervisorDetailsCollection) {
                String rackId = supervisorIdToRackName(supervisorDetails.getId());
                hostToRackMap.put(supervisorDetails.getHost(), rackId);
                String host = supervisorDetails.getHost();
                if (!seenHosts.contains(host)) {
                    rackToHosts.computeIfAbsent(rackId, rid -> new ArrayList<>()).add(host);
                    seenHosts.add(host);
                }
            }
            this.hostToRackMap = Collections.unmodifiableMap(hostToRackMap);
            this.rackToHosts = Collections.unmodifiableMap(rackToHosts);
        }

        @Override
        public Map<String, String> resolve(List<String> names) {
            return hostToRackMap;
        }

        public Map<String, List<String>> getRackToHosts() {
            return rackToHosts;
        }
    }

    /**
     * Free one-fifth of WorkerSlots.
     */
    private void freeSomeWorkerSlots(Cluster cluster) {
        Map<String, SchedulerAssignment> assignmentMap = cluster.getAssignments();
        for (SchedulerAssignment schedulerAssignment: assignmentMap.values()) {
            int i = 0;
            List<WorkerSlot> slotsToKill = new ArrayList<>();
            for (WorkerSlot workerSlot: schedulerAssignment.getSlots()) {
                i++;
                if (i % 5 == 0) {
                    slotsToKill.add(workerSlot);
                }
            }
            cluster.freeSlots(slotsToKill);
        }
    }

    /**
     * Test whether number of nodes is limited by {@link Config#TOPOLOGY_ISOLATED_MACHINES} by scheduling
     * two topologies and verifying the number of nodes that each one occupies and are not overlapping.
     */
    @Test
    void testTopologyIsolation() {
        INimbus iNimbus = new INimbusTest();
        double compPcore = 100;
        double compOnHeap = 775;
        double compOffHeap = 25;
        int[] topoNumSpouts = {1,1};
        int[] topoNumBolts = {1,1};
        int[] topoSpoutParallelism = {100, 100};
        int[] topoBoltParallelism = {200, 200};
        final int numRacks = 3;
        final int numSupersPerRack = 10;
        final int numPortsPerSuper = 6;
        final int numZonesPerHost = 1;
        final double numaResourceMultiplier = 1.0;
        int rackStartNum = 0;
        int supStartNum = 0;
        long compPerRack = (topoNumSpouts[0] * topoSpoutParallelism[0] + topoNumBolts[0] * topoBoltParallelism[0]
                + topoNumSpouts[1] * topoSpoutParallelism[1]); // enough for topo1 but not topo1+topo2
        long compPerSuper =  compPerRack / numSupersPerRack;
        double cpuPerSuper = compPcore * compPerSuper;
        double memPerSuper = (compOnHeap + compOffHeap) * compPerSuper;
        double[] topoMaxHeapSize = {memPerSuper, memPerSuper};
        final String[] topoNames = {"topology1", "topology2"};
        int[] maxNodes = {15, 13};

        Map<String, SupervisorDetails> supMap = genSupervisorsWithRacksAndNuma(
                numRacks, numSupersPerRack, numZonesPerHost, numPortsPerSuper, rackStartNum, supStartNum,
                cpuPerSuper, memPerSuper, Collections.emptyMap(), numaResourceMultiplier);
        TestDNSToSwitchMapping testDNSToSwitchMapping = new TestDNSToSwitchMapping(supMap.values());

        Config[] configs = new Config[topoNames.length];
        TopologyDetails[] topos = new TopologyDetails[topoNames.length];
        for (int i = 0 ; i < topoNames.length ; i++) {
            configs[i] = new Config();
            configs[i].putAll(createRoundRobinClusterConfig(compPcore, compOnHeap, compOffHeap, null, null));
            configs[i].put(Config.TOPOLOGY_ISOLATED_MACHINES, maxNodes[i]);
            topos[i] = genTopology(topoNames[i], configs[i], topoNumSpouts[i],
                    topoNumBolts[i], topoSpoutParallelism[i], topoBoltParallelism[i], 0, 0, "user", topoMaxHeapSize[i]);
        }
        TopologyDetails td1 = topos[0];
        TopologyDetails td2 = topos[1];

        IScheduler scheduler = new ResourceAwareScheduler();
        scheduler.prepare(configs[0], new StormMetricsRegistry());

        //Schedule the topo1 topology and ensure it uses limited number of nodes
        Topologies topologies = new Topologies(td1);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, configs[0]);
        cluster.setNetworkTopography(testDNSToSwitchMapping.getRackToHosts());

        scheduler.schedule(topologies, cluster);
        Set<String> assignedRacks = cluster.getAssignedRacks(topos[0].getId());
        assertEquals(2 , assignedRacks.size(), "Racks for topology=" + td1.getId() + " is " + assignedRacks);

        //Now schedule GPU but with the simple topology in place.
        topologies = new Topologies(td1, td2);
        cluster = new Cluster(cluster, topologies);
        scheduler.schedule(topologies, cluster);

        assignedRacks = cluster.getAssignedRacks(td1.getId(), td2.getId());
        assertEquals(numRacks, assignedRacks.size(), "Racks for topologies=" + td1.getId() + "/" + td2.getId() + " is " + assignedRacks);

        SchedulerAssignment[] assignments = new SchedulerAssignment[topoNames.length];
        Collection<String>[] assignmentNodes = new Collection[topoNames.length];
        for (int i = 0 ; i < topoNames.length ; i++) {
            assignments[i] = cluster.getAssignmentById(topos[i].getId());
            if (assignments[i] == null) {
                fail("Topology " + topoNames[i] + " cannot be scheduled");
            }
            assignmentNodes[i] = assignments[i].getSlots().stream().map(WorkerSlot::getNodeId).collect(Collectors.toList());
            assertEquals(maxNodes[i], assignmentNodes[i].size(), "Max Nodes for " + topoNames[i] + " assignment");
        }
        // confirm no overlap in nodes
        Set<String> nodes1 = new HashSet<>(assignmentNodes[0]);
        Set<String> nodes2 = new HashSet<>(assignmentNodes[1]);
        Set<String> dupNodes = Sets.intersection(nodes1, nodes2);
        if (dupNodes.size() > 0) {
            List<String> lines = new ArrayList<>();
            lines.add("Topologies shared nodes when not expected to");
            lines.add("Duplicated nodes are " + String.join(",", dupNodes));
            fail(String.join("\n\t", lines));
        }
        nodes2.removeAll(nodes1);

        // topo2 gets scheduled on across the two racks even if there is one rack with enough capacity
        assignedRacks = cluster.getAssignedRacks(td2.getId());
        assertEquals(numRacks -1, assignedRacks.size(), "Racks for topologies=" + td2.getId() + " is " + assignedRacks);

        // now unassign topo2, expect only two of three racks to be in use; free some slots and reschedule topo1 some topo1 executors
        cluster.unassign(td2.getId());
        assignedRacks = cluster.getAssignedRacks(td2.getId());
        assertEquals(0, assignedRacks.size(),
                "After unassigning topology " + td2.getId() + ", racks for topology=" + td2.getId() + " is " + assignedRacks);
        assignedRacks = cluster.getAssignedRacks(td1.getId());
        assertEquals(numRacks - 1, assignedRacks.size(),
                "After unassigning topology " + td2.getId() + ", racks for topology=" + td1.getId() + " is " + assignedRacks);
        assertFalse(cluster.needsSchedulingRas(td1),
                "Topology " + td1.getId() + " should be fully assigned before freeing slots");
        freeSomeWorkerSlots(cluster);
        assertTrue(cluster.needsSchedulingRas(td1),
                "Topology " + td1.getId() + " should need scheduling after freeing slots");

        // then reschedule executors
        scheduler.schedule(topologies, cluster);

        // only two of three racks should be in use still
        assignedRacks = cluster.getAssignedRacks(td1.getId());
        assertEquals(numRacks - 1, assignedRacks.size(),
                "After reassigning topology " + td2.getId() + ", racks for topology=" + td1.getId() + " is " + assignedRacks);
    }
}