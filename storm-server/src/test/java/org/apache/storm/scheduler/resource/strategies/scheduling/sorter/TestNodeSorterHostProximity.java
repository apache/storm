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

package org.apache.storm.scheduler.resource.strategies.scheduling.sorter;

import org.apache.storm.Config;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.networktopography.DNSToSwitchMapping;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.RasNodes;
import org.apache.storm.scheduler.resource.ResourceAwareScheduler;
import org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourceRequest;
import org.apache.storm.scheduler.resource.normalization.NormalizedResourcesExtension;
import org.apache.storm.scheduler.resource.normalization.ResourceMetrics;
import org.apache.storm.scheduler.resource.strategies.scheduling.BaseResourceAwareStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.GenericResourceAwareStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.ObjectResourcesItem;
import org.apache.storm.topology.TopologyBuilder;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@ExtendWith({NormalizedResourcesExtension.class})
public class TestNodeSorterHostProximity {
    private static final Logger LOG = LoggerFactory.getLogger(TestNodeSorterHostProximity.class);
    private static final int CURRENT_TIME = 1450418597;

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
     * Test whether strategy will choose correct rack.
     */
    @Test
    public void testMultipleRacks() {
        final Map<String, SupervisorDetails> supMap = new HashMap<>();
        final int numRacks = 1;
        final int numSupersPerRack = 10;
        final int numPortsPerSuper = 4;
        final int numZonesPerHost = 1;
        final double numaResourceMultiplier = 1.0;
        int rackStartNum = 0;
        int supStartNum = 0;

        final Map<String, SupervisorDetails> supMapRack0 = genSupervisorsWithRacksAndNuma(
                numRacks, numSupersPerRack, numZonesPerHost, numPortsPerSuper, rackStartNum++, supStartNum,
                400, 8000, Collections.emptyMap(), numaResourceMultiplier);

        //generate another rack of supervisors with less resources
        supStartNum += numSupersPerRack;
        final Map<String, SupervisorDetails> supMapRack1 = genSupervisorsWithRacksAndNuma(
                numRacks, numSupersPerRack, numZonesPerHost, numPortsPerSuper, rackStartNum++, supStartNum,
                200, 4000, Collections.emptyMap(), numaResourceMultiplier);

        //generate some supervisors that are depleted of one resource
        supStartNum += numSupersPerRack;
        final Map<String, SupervisorDetails> supMapRack2 = genSupervisorsWithRacksAndNuma(
                numRacks, numSupersPerRack, numZonesPerHost, numPortsPerSuper, rackStartNum++, supStartNum,
                0, 8000, Collections.emptyMap(), numaResourceMultiplier);

        //generate some that has a lot of memory but little of cpu
        supStartNum += numSupersPerRack;
        final Map<String, SupervisorDetails> supMapRack3 = genSupervisorsWithRacksAndNuma(
                numRacks, numSupersPerRack, numZonesPerHost, numPortsPerSuper, rackStartNum++, supStartNum,
                10, 8000 * 2 + 4000, Collections.emptyMap(),numaResourceMultiplier);

        //generate some that has a lot of cpu but little of memory
        supStartNum += numSupersPerRack;
        final Map<String, SupervisorDetails> supMapRack4 = genSupervisorsWithRacksAndNuma(
                numRacks, numSupersPerRack, numZonesPerHost, numPortsPerSuper, rackStartNum++, supStartNum,
                400 + 200 + 10, 1000, Collections.emptyMap(), numaResourceMultiplier);

        //Generate some that have neither resource, to verify that the strategy will prioritize this last
        //Also put a generic resource with 0 value in the resources list, to verify that it doesn't affect the sorting
        supStartNum += numSupersPerRack;
        final Map<String, SupervisorDetails> supMapRack5 = genSupervisorsWithRacksAndNuma(
                numRacks, numSupersPerRack, numZonesPerHost, numPortsPerSuper, rackStartNum++, supStartNum,
                0.0, 0.0, Collections.singletonMap("gpu.count", 0.0), numaResourceMultiplier);

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
        TestDNSToSwitchMapping testDNSToSwitchMapping =
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
        Map<String, List<String>> rackToHosts = testDNSToSwitchMapping.getRackToHosts();
        cluster.setNetworkTopography(rackToHosts);

        NodeSorterHostProximity nodeSorter = new NodeSorterHostProximity(cluster, topo1, BaseResourceAwareStrategy.NodeSortType.DEFAULT_RAS);
        nodeSorter.prepare(null);
        List<ObjectResourcesItem> sortedRacks = StreamSupport.stream(nodeSorter.getSortedRacks().spliterator(), false)
                .collect(Collectors.toList());
        String rackSummaries = sortedRacks.stream()
                .map(x -> String.format("Rack %s -> scheduled-cnt %d, min-avail %f, avg-avail %f, cpu %f, mem %f",
                        x.id, nodeSorter.getScheduledExecCntByRackId().getOrDefault(x.id, new AtomicInteger(-1)).get(),
                        x.minResourcePercent, x.avgResourcePercent,
                        x.availableResources.getTotalCpu(),
                        x.availableResources.getTotalMemoryMb()))
                .collect(Collectors.joining("\n\t"));
        Assert.assertEquals(rackSummaries + "\n# of racks sorted", 6, sortedRacks.size());
        Iterator<ObjectResourcesItem> it = sortedRacks.iterator();
        Assert.assertEquals(rackSummaries + "\nrack-000 should be ordered first since it has the most balanced set of resources", "rack-000", it.next().id);
        Assert.assertEquals(rackSummaries + "\nrack-001 should be ordered second since it has a balanced set of resources but less than rack-000", "rack-001", it.next().id);
        Assert.assertEquals(rackSummaries + "\nrack-004 should be ordered third since it has a lot of cpu but not a lot of memory", "rack-004", it.next().id);
        Assert.assertEquals(rackSummaries + "\nrack-003 should be ordered fourth since it has a lot of memory but not cpu", "rack-003", it.next().id);
        Assert.assertEquals(rackSummaries + "\nrack-002 should be ordered fifth since it has not cpu resources", "rack-002", it.next().id);
        Assert.assertEquals(rackSummaries + "\nRack-005 should be ordered sixth since it has neither CPU nor memory available", "rack-005", it.next().id);
    }

    /**
     * Test whether strategy will choose correct rack.
     */
    @Test
    public void testMultipleRacksWithFavoritism() {
        final Map<String, SupervisorDetails> supMap = new HashMap<>();
        final int numRacks = 1;
        final int numSupersPerRack = 10;
        final int numPortsPerSuper = 4;
        final int numZonesPerHost = 2;
        int rackStartNum = 0;
        int supStartNum = 0;
        final Map<String, SupervisorDetails> supMapRack0 = genSupervisorsWithRacksAndNuma(
                numRacks, numSupersPerRack, numZonesPerHost, numPortsPerSuper, rackStartNum++, supStartNum,
                400, 8000, Collections.emptyMap(), 1.0);

        //generate another rack of supervisors with less resources
        supStartNum += numSupersPerRack;
        final Map<String, SupervisorDetails> supMapRack1 = genSupervisorsWithRacksAndNuma(
                numRacks, numSupersPerRack, numZonesPerHost, numPortsPerSuper, rackStartNum++, supStartNum,
                200, 4000, Collections.emptyMap(), 1.0);

        //generate some supervisors that are depleted of one resource
        supStartNum += numSupersPerRack;
        final Map<String, SupervisorDetails> supMapRack2 = genSupervisorsWithRacksAndNuma(
                numRacks, numSupersPerRack, numZonesPerHost, numPortsPerSuper, rackStartNum++, supStartNum,
                0, 8000, Collections.emptyMap(), 1.0);

        //generate some that has a lot of memory but little of cpu
        supStartNum += numSupersPerRack;
        final Map<String, SupervisorDetails> supMapRack3 = genSupervisorsWithRacksAndNuma(
                numRacks, numSupersPerRack, numZonesPerHost, numPortsPerSuper, rackStartNum++, supStartNum,
                10, 8000 * 2 + 4000, Collections.emptyMap(), 1.0);

        //generate some that has a lot of cpu but little of memory
        supStartNum += numSupersPerRack;
        final Map<String, SupervisorDetails> supMapRack4 = genSupervisorsWithRacksAndNuma(
                numRacks, numSupersPerRack, numZonesPerHost, numPortsPerSuper, rackStartNum++, supStartNum,
                400 + 200 + 10, 1000, Collections.emptyMap(), 1.0);

        supMap.putAll(supMapRack0);
        supMap.putAll(supMapRack1);
        supMap.putAll(supMapRack2);
        supMap.putAll(supMapRack3);
        supMap.putAll(supMapRack4);

        Config config = createClusterConfig(100, 500, 500, null);
        config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, Double.MAX_VALUE);
        INimbus iNimbus = new INimbusTest();

        //create test DNSToSwitchMapping plugin
        TestDNSToSwitchMapping testDNSToSwitchMapping =
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
        Map<String, List<String>> rackToHosts = testDNSToSwitchMapping.getRackToHosts();
        cluster.setNetworkTopography(rackToHosts);

        NodeSorterHostProximity nodeSorter = new NodeSorterHostProximity(cluster, topo1, BaseResourceAwareStrategy.NodeSortType.DEFAULT_RAS);
        nodeSorter.prepare(null);
        List<ObjectResourcesItem> sortedRacks = StreamSupport.stream(nodeSorter.getSortedRacks().spliterator(), false)
                .collect(Collectors.toList());
        String rackSummaries = sortedRacks.stream()
                .map(x -> String.format("Rack %s -> scheduled-cnt %d, min-avail %f, avg-avail %f, cpu %f, mem %f",
                        x.id, nodeSorter.getScheduledExecCntByRackId().getOrDefault(x.id, new AtomicInteger(-1)).get(),
                        x.minResourcePercent, x.avgResourcePercent,
                        x.availableResources.getTotalCpu(),
                        x.availableResources.getTotalMemoryMb()))
                .collect(Collectors.joining("\n\t"));

        Iterator<ObjectResourcesItem> it = sortedRacks.iterator();
        // Ranked first since rack-000 has the most balanced set of resources
        Assert.assertEquals("rack-000 should be ordered first", "rack-000", it.next().id);
        // Ranked second since rack-1 has a balanced set of resources but less than rack-0
        Assert.assertEquals("rack-001 should be ordered second", "rack-001", it.next().id);
        // Ranked third since rack-4 has a lot of cpu but not a lot of memory
        Assert.assertEquals("rack-004 should be ordered third", "rack-004", it.next().id);
        // Ranked fourth since rack-3 has alot of memory but not cpu
        Assert.assertEquals("rack-003 should be ordered fourth", "rack-003", it.next().id);
        //Ranked last since rack-2 has not cpu resources
        Assert.assertEquals("rack-00s2 should be ordered fifth", "rack-002", it.next().id);
    }

    /**
     * Test if hosts are presented together regardless of resource availability.
     * Supervisors are created with multiple Numa zones in such a manner that resources on two numa zones on the same host
     * differ widely in resource availability.
     */
    @Test
    public void testMultipleRacksWithHostProximity() {
        final Map<String, SupervisorDetails> supMap = new HashMap<>();
        final int numRacks = 1;
        final int numSupersPerRack = 12;
        final int numPortsPerSuper = 4;
        final int numZonesPerHost = 3;
        final double numaResourceMultiplier = 0.4;
        int rackStartNum = 0;
        int supStartNum = 0;

        final Map<String, SupervisorDetails> supMapRack0 = genSupervisorsWithRacksAndNuma(
                numRacks, numSupersPerRack, numZonesPerHost, numPortsPerSuper, rackStartNum++, supStartNum,
                400, 8000, Collections.emptyMap(), numaResourceMultiplier);

        //generate another rack of supervisors with less resources
        supStartNum += numSupersPerRack;
        final Map<String, SupervisorDetails> supMapRack1 = genSupervisorsWithRacksAndNuma(
                numRacks, numSupersPerRack, numZonesPerHost, numPortsPerSuper, rackStartNum++, supStartNum,
                200, 4000, Collections.emptyMap(), numaResourceMultiplier);

        //generate some supervisors that are depleted of one resource
        supStartNum += numSupersPerRack;
        final Map<String, SupervisorDetails> supMapRack2 = genSupervisorsWithRacksAndNuma(
                numRacks, numSupersPerRack, numZonesPerHost, numPortsPerSuper, rackStartNum++, supStartNum,
                0, 8000, Collections.emptyMap(), numaResourceMultiplier);

        //generate some that has a lot of memory but little of cpu
        supStartNum += numSupersPerRack;
        final Map<String, SupervisorDetails> supMapRack3 = genSupervisorsWithRacksAndNuma(
                numRacks, numSupersPerRack, numZonesPerHost, numPortsPerSuper, rackStartNum++, supStartNum,
                10, 8000 * 2 + 4000, Collections.emptyMap(),numaResourceMultiplier);

        //generate some that has a lot of cpu but little of memory
        supStartNum += numSupersPerRack;
        final Map<String, SupervisorDetails> supMapRack4 = genSupervisorsWithRacksAndNuma(
                numRacks, numSupersPerRack, numZonesPerHost, numPortsPerSuper, rackStartNum++, supStartNum,
                400 + 200 + 10, 1000, Collections.emptyMap(), numaResourceMultiplier);

        supMap.putAll(supMapRack0);
        supMap.putAll(supMapRack1);
        supMap.putAll(supMapRack2);
        supMap.putAll(supMapRack3);
        supMap.putAll(supMapRack4);

        Config config = createClusterConfig(100, 500, 500, null);
        config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, Double.MAX_VALUE);
        INimbus iNimbus = new INimbusTest();

        //create test DNSToSwitchMapping plugin
        TestDNSToSwitchMapping testDNSToSwitchMapping =
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

        cluster.setNetworkTopography(testDNSToSwitchMapping.getRackToHosts());

        INodeSorter nodeSorter = new NodeSorterHostProximity(cluster, topo1);
        nodeSorter.prepare(null);

        Set<String> seenHosts = new HashSet<>();
        String prevHost = null;
        List<String> errLines = new ArrayList();
        Map<String, String> nodeToHost = new RasNodes(cluster).getNodeIdToHostname();
        for (String nodeId: nodeSorter.sortAllNodes()) {
            String host = nodeToHost.getOrDefault(nodeId, "no-host-for-node-" + nodeId);
            errLines.add(String.format("\tnodeId:%s, host:%s", nodeId, host));
            if (!host.equals(prevHost) && seenHosts.contains(host)) {
                String err = String.format("Host %s for node %s is out of order:\n\t%s", host, nodeId, String.join("\n\t", errLines));
                Assert.fail(err);
            }
            seenHosts.add(host);
            prevHost = host;
        }
    }

    /**
     * Racks should be returned in order of decreasing capacity.
     */
    @Test
    public void testMultipleRacksOrderedByCapacity() {
        final Map<String, SupervisorDetails> supMap = new HashMap<>();
        final int numRacks = 1;
        final int numSupersPerRack = 10;
        final int numPortsPerSuper = 4;
        final int numZonesPerHost = 1;
        final double numaResourceMultiplier = 1.0;
        int rackStartNum = 0;
        int supStartNum = 0;

        final Map<String, SupervisorDetails> supMapRack0 = genSupervisorsWithRacksAndNuma(
                numRacks, numSupersPerRack, numZonesPerHost, numPortsPerSuper, rackStartNum++, supStartNum,
                600, 8000 - rackStartNum, Collections.emptyMap(), numaResourceMultiplier);

        supStartNum += numSupersPerRack;
        final Map<String, SupervisorDetails> supMapRack1 = genSupervisorsWithRacksAndNuma(
                numRacks, numSupersPerRack, numZonesPerHost, numPortsPerSuper, rackStartNum++, supStartNum,
                500, 8000 - rackStartNum, Collections.emptyMap(), numaResourceMultiplier);

        supStartNum += numSupersPerRack;
        final Map<String, SupervisorDetails> supMapRack2 = genSupervisorsWithRacksAndNuma(
                numRacks, numSupersPerRack, numZonesPerHost, numPortsPerSuper, rackStartNum++, supStartNum,
                400, 8000 - rackStartNum, Collections.emptyMap(), numaResourceMultiplier);

        supStartNum += numSupersPerRack;
        final Map<String, SupervisorDetails> supMapRack3 = genSupervisorsWithRacksAndNuma(
                numRacks, numSupersPerRack, numZonesPerHost, numPortsPerSuper, rackStartNum++, supStartNum,
                300, 8000 - rackStartNum, Collections.emptyMap(),numaResourceMultiplier);

        supStartNum += numSupersPerRack;
        final Map<String, SupervisorDetails> supMapRack4 = genSupervisorsWithRacksAndNuma(
                numRacks, numSupersPerRack, numZonesPerHost, numPortsPerSuper, rackStartNum++, supStartNum,
                200, 8000 - rackStartNum, Collections.emptyMap(), numaResourceMultiplier);

        // too small to hold topology
        supStartNum += numSupersPerRack;
        final Map<String, SupervisorDetails> supMapRack5 = genSupervisorsWithRacksAndNuma(
                numRacks, numSupersPerRack, numZonesPerHost, numPortsPerSuper, rackStartNum++, supStartNum,
                100, 8000 - rackStartNum, Collections.singletonMap("gpu.count", 0.0), numaResourceMultiplier);

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
        TestDNSToSwitchMapping testDNSToSwitchMapping =
                new TestDNSToSwitchMapping(supMapRack0, supMapRack1, supMapRack2, supMapRack3, supMapRack4, supMapRack5);

        //generate topologies
        TopologyDetails topo1 = genTopology("topo-1", config, 8, 0, 2, 0, CURRENT_TIME - 2, 10, "user");
        TopologyDetails topo2 = genTopology("topo-2", config, 8, 0, 2, 0, CURRENT_TIME - 2, 10, "user");

        Topologies topologies = new Topologies(topo1, topo2);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);

        cluster.setNetworkTopography(testDNSToSwitchMapping.getRackToHosts());

        NodeSorterHostProximity nodeSorter = new NodeSorterHostProximity(cluster, topo1);
        nodeSorter.prepare(null);
        List<ObjectResourcesItem> sortedRacks = StreamSupport.stream(nodeSorter.getSortedRacks().spliterator(), false)
                .collect(Collectors.toList());
        String rackSummaries = sortedRacks
                .stream()
                .map(x -> String.format("Rack %s -> scheduled-cnt %d, min-avail %f, avg-avail %f, cpu %f, mem %f",
                        x.id, nodeSorter.getScheduledExecCntByRackId().getOrDefault(x.id, new AtomicInteger(-1)).get(),
                        x.minResourcePercent, x.avgResourcePercent,
                        x.availableResources.getTotalCpu(),
                        x.availableResources.getTotalMemoryMb()))
                .collect(Collectors.joining("\n\t"));
        NormalizedResourceRequest topoResourceRequest = topo1.getApproximateTotalResources();
        String topoRequest = String.format("Topo %s, approx-requested-resources %s", topo1.getId(), topoResourceRequest.toString());
        Iterator<ObjectResourcesItem> it = sortedRacks.iterator();
        Assert.assertEquals(topoRequest + "\n\t" + rackSummaries + "\nRack-000 should be ordered first since it has the largest capacity", "rack-000", it.next().id);
        Assert.assertEquals(topoRequest + "\n\t" + rackSummaries + "\nrack-001 should be ordered second since it smaller than rack-000", "rack-001", it.next().id);
        Assert.assertEquals(topoRequest + "\n\t" + rackSummaries + "\nrack-002 should be ordered third since it is smaller than rack-001", "rack-002", it.next().id);
        Assert.assertEquals(topoRequest + "\n\t" + rackSummaries + "\nrack-003 should be ordered fourth since it since it is smaller than rack-002", "rack-003", it.next().id);
        Assert.assertEquals(topoRequest + "\n\t" + rackSummaries + "\nrack-004 should be ordered fifth since it since it is smaller than rack-003", "rack-004", it.next().id);
        Assert.assertEquals(topoRequest + "\n\t" + rackSummaries + "\nrack-005 should be ordered last since it since it is has smallest capacity", "rack-005", it.next().id);
    }

    /**
     * Schedule two topologies, once with special resources and another without.
     * There are enough special resources to hold one topology with special resource ("my.gpu").
     * If the sort order is incorrect, scheduling will not succeed.
     */
    @Test
    public void testAntiAffinityWithMultipleTopologies() {
        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisorsWithRacks(1, 40, 66, 0, 0, 4700, 226200, new HashMap<>());
        HashMap<String, Double> extraResources = new HashMap<>();
        extraResources.put("my.gpu", 1.0);
        supMap.putAll(genSupervisorsWithRacks(1, 40, 66, 1, 0, 4700, 226200, extraResources));

        Config config = new Config();
        config.putAll(createGrasClusterConfig(88, 775, 25, null, null));

        IScheduler scheduler = new ResourceAwareScheduler();
        scheduler.prepare(config, new StormMetricsRegistry());

        TopologyDetails tdSimple = genTopology("topology-simple", config, 1,
                5, 100, 300, 0, 0, "user", 8192);

        //Schedule the simple topology first
        Topologies topologies = new Topologies(tdSimple);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);

        {
            NodeSorterHostProximity nodeSorter = new NodeSorterHostProximity(cluster, tdSimple);
            for (ExecutorDetails exec : tdSimple.getExecutors()) {
                nodeSorter.prepare(exec);
                List<ObjectResourcesItem> sortedRacks = StreamSupport
                        .stream(nodeSorter.getSortedRacks().spliterator(), false)
                        .collect(Collectors.toList());
                String rackSummaries = StreamSupport
                        .stream(sortedRacks.spliterator(), false)
                        .map(x -> String.format("Rack %s -> scheduled-cnt %d, min-avail %f, avg-avail %f, cpu %f, mem %f",
                                x.id, nodeSorter.getScheduledExecCntByRackId().getOrDefault(x.id, new AtomicInteger(-1)).get(),
                                x.minResourcePercent, x.avgResourcePercent,
                                x.availableResources.getTotalCpu(),
                                x.availableResources.getTotalMemoryMb()))
                        .collect(Collectors.joining("\n\t"));
                NormalizedResourceRequest topoResourceRequest = tdSimple.getApproximateTotalResources();
                String topoRequest = String.format("Topo %s, approx-requested-resources %s", tdSimple.getId(), topoResourceRequest.toString());
                Assert.assertEquals(rackSummaries + "\n# of racks sorted", 2, sortedRacks.size());
                Assert.assertEquals(rackSummaries + "\nFirst rack sorted", "rack-000", sortedRacks.get(0).id);
                Assert.assertEquals(rackSummaries + "\nSecond rack sorted", "rack-001", sortedRacks.get(1).id);
            }
        }

        scheduler.schedule(topologies, cluster);

        TopologyBuilder builder = topologyBuilder(1, 5, 100, 300);
        builder.setBolt("gpu-bolt", new TestBolt(), 40)
                .addResource("my.gpu", 1.0)
                .shuffleGrouping("spout-0");
        TopologyDetails tdGpu = topoToTopologyDetails("topology-gpu", config, builder.createTopology(), 0, 0,"user", 8192);

        //Now schedule GPU but with the simple topology in place.
        topologies = new Topologies(tdSimple, tdGpu);
        cluster = new Cluster(cluster, topologies);
        {
            NodeSorterHostProximity nodeSorter = new NodeSorterHostProximity(cluster, tdGpu);
            for (ExecutorDetails exec : tdGpu.getExecutors()) {
                String comp = tdGpu.getComponentFromExecutor(exec);
                nodeSorter.prepare(exec);
                List<ObjectResourcesItem> sortedRacks = StreamSupport
                        .stream(nodeSorter.getSortedRacks().spliterator(), false).collect(Collectors.toList());
                String rackSummaries = sortedRacks.stream()
                        .map(x -> String.format("Rack %s -> scheduled-cnt %d, min-avail %f, avg-avail %f, cpu %f, mem %f",
                                x.id, nodeSorter.getScheduledExecCntByRackId().getOrDefault(x.id, new AtomicInteger(-1)).get(),
                                x.minResourcePercent, x.avgResourcePercent,
                                x.availableResources.getTotalCpu(),
                                x.availableResources.getTotalMemoryMb()))
                        .collect(Collectors.joining("\n\t"));
                NormalizedResourceRequest topoResourceRequest = tdSimple.getApproximateTotalResources();
                String topoRequest = String.format("Topo %s, approx-requested-resources %s", tdSimple.getId(), topoResourceRequest.toString());
                Assert.assertEquals(rackSummaries + "\n# of racks sorted", 2, sortedRacks.size());
                if (comp.equals("gpu-bolt")) {
                    Assert.assertEquals(rackSummaries + "\nFirst rack sorted for " + comp, "rack-001", sortedRacks.get(0).id);
                    Assert.assertEquals(rackSummaries + "\nSecond rack sorted for " + comp, "rack-000", sortedRacks.get(1).id);
                } else {
                    Assert.assertEquals(rackSummaries + "\nFirst rack sorted for " + comp, "rack-000", sortedRacks.get(0).id);
                    Assert.assertEquals(rackSummaries + "\nSecond rack sorted for " + comp, "rack-001", sortedRacks.get(1).id);
                }
            }
        }

        scheduler.schedule(topologies, cluster);

        Map<String, SchedulerAssignment> assignments = new TreeMap<>(cluster.getAssignments());
        assertEquals(2, assignments.size());

        Map<String, Map<String, AtomicLong>> topoPerRackCount = new HashMap<>();
        for (Map.Entry<String, SchedulerAssignment> entry: assignments.entrySet()) {
            SchedulerAssignment sa = entry.getValue();
            Map<String, AtomicLong> slotsPerRack = new TreeMap<>();
            for (WorkerSlot slot : sa.getSlots()) {
                String nodeId = slot.getNodeId();
                String rack = supervisorIdToRackName(nodeId);
                slotsPerRack.computeIfAbsent(rack, (r) -> new AtomicLong(0)).incrementAndGet();
            }
            LOG.info("{} => {}", entry.getKey(), slotsPerRack);
            topoPerRackCount.put(entry.getKey(), slotsPerRack);
        }

        Map<String, AtomicLong> simpleCount = topoPerRackCount.get("topology-simple-0");
        assertNotNull(simpleCount);
        //Because the simple topology was scheduled first we want to be sure that it didn't put anything on
        // the GPU nodes.
        assertEquals(1, simpleCount.size()); //Only 1 rack is in use
        assertFalse(simpleCount.containsKey("r001")); //r001 is the second rack with GPUs
        assertTrue(simpleCount.containsKey("r000")); //r000 is the first rack with no GPUs

        //We don't really care too much about the scheduling of topology-gpu-0, because it was scheduled.
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
     * If the topology is too large for one rack, it should be partially scheduled onto the next rack (and next rack only).
     */
    @Test
    public void testFillUpRackAndSpilloverToNextRack() {
        INimbus iNimbus = new INimbusTest();
        double compPcore = 100;
        double compOnHeap = 775;
        double compOffHeap = 25;
        int topo1NumSpouts = 1;
        int topo1NumBolts = 5;
        int topo1SpoutParallelism = 100;
        int topo1BoltParallelism = 200;
        final int numRacks = 3;
        final int numSupersPerRack = 10;
        final int numPortsPerSuper = 6;
        final int numZonesPerHost = 1;
        final double numaResourceMultiplier = 1.0;
        int rackStartNum = 0;
        int supStartNum = 0;
        long compPerRack = (topo1NumSpouts * topo1SpoutParallelism + topo1NumBolts * topo1BoltParallelism) * 4/5; // not enough for topo1
        long compPerSuper =  compPerRack / numSupersPerRack;
        double cpuPerSuper = compPcore * compPerSuper;
        double memPerSuper = (compOnHeap + compOffHeap) * compPerSuper;
        double topo1MaxHeapSize = memPerSuper;
        final String topoName1 = "topology1";

        Map<String, SupervisorDetails> supMap = genSupervisorsWithRacksAndNuma(
                numRacks, numSupersPerRack, numZonesPerHost, numPortsPerSuper, rackStartNum, supStartNum,
                cpuPerSuper, memPerSuper, Collections.emptyMap(), numaResourceMultiplier);
        TestDNSToSwitchMapping testDNSToSwitchMapping = new TestDNSToSwitchMapping(supMap.values());

        Config config = new Config();
        config.putAll(createGrasClusterConfig(compPcore, compOnHeap, compOffHeap, null, null));
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, GenericResourceAwareStrategy.class.getName());

        IScheduler scheduler = new ResourceAwareScheduler();
        scheduler.prepare(config, new StormMetricsRegistry());

        TopologyDetails td1 = genTopology(topoName1, config, topo1NumSpouts,
                topo1NumBolts, topo1SpoutParallelism, topo1BoltParallelism, 0, 0, "user", topo1MaxHeapSize);

        //Schedule the topo1 topology and ensure it fits on 2 racks
        Topologies topologies = new Topologies(td1);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);
        cluster.setNetworkTopography(testDNSToSwitchMapping.getRackToHosts());

        scheduler.schedule(topologies, cluster);
        Set<String> assignedRacks = cluster.getAssignedRacks(td1.getId());
        assertEquals("Racks for topology=" + td1.getId() + " is " + assignedRacks, 2, assignedRacks.size());
    }

    /**
     * Rack with low resources should be used to schedule an executor if it has other executors for the same topology.
     * <li>Schedule topo1 on one rack</li>
     * <li>unassign some executors</li>
     * <li>schedule another topology to partially fill up rack1</li>
     * <li>Add another rack and schedule topology 1 remaining executors again</li>
     * <li>scheduling should utilize all resources on rack1 before before trying next rack</li>
     */
    @Test
    public void testPreferRackWithTopoExecutors() {
        INimbus iNimbus = new INimbusTest();
        double compPcore = 100;
        double compOnHeap = 775;
        double compOffHeap = 25;
        int topo1NumSpouts = 1;
        int topo1NumBolts = 5;
        int topo1SpoutParallelism = 100;
        int topo1BoltParallelism = 200;
        int topo2NumSpouts = 1;
        int topo2NumBolts = 5;
        int topo2SpoutParallelism = 10;
        int topo2BoltParallelism = 20;
        final int numRacks = 3;
        final int numSupersPerRack = 10;
        final int numPortsPerSuper = 6;
        final int numZonesPerHost = 1;
        final double numaResourceMultiplier = 1.0;
        int rackStartNum = 0;
        int supStartNum = 0;
        long compPerRack = (topo1NumSpouts * topo1SpoutParallelism + topo1NumBolts * topo1BoltParallelism
                + topo2NumSpouts * topo2SpoutParallelism); // enough for topo1 but not topo1+topo2
        long compPerSuper =  compPerRack / numSupersPerRack;
        double cpuPerSuper = compPcore * compPerSuper;
        double memPerSuper = (compOnHeap + compOffHeap) * compPerSuper;
        double topo1MaxHeapSize = memPerSuper;
        double topo2MaxHeapSize = memPerSuper;
        final String topoName1 = "topology1";
        final String topoName2 = "topology2";

        Map<String, SupervisorDetails> supMap = genSupervisorsWithRacksAndNuma(
                numRacks, numSupersPerRack, numZonesPerHost, numPortsPerSuper, rackStartNum, supStartNum,
                cpuPerSuper, memPerSuper, Collections.emptyMap(), numaResourceMultiplier);
        TestDNSToSwitchMapping testDNSToSwitchMapping = new TestDNSToSwitchMapping(supMap.values());

        Config config = new Config();
        config.putAll(createGrasClusterConfig(compPcore, compOnHeap, compOffHeap, null, null));
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, GenericResourceAwareStrategy.class.getName());

        IScheduler scheduler = new ResourceAwareScheduler();
        scheduler.prepare(config, new StormMetricsRegistry());

        TopologyDetails td1 = genTopology(topoName1, config, topo1NumSpouts,
                topo1NumBolts, topo1SpoutParallelism, topo1BoltParallelism, 0, 0, "user", topo1MaxHeapSize);

        //Schedule the topo1 topology and ensure it fits on 1 rack
        Topologies topologies = new Topologies(td1);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);
        cluster.setNetworkTopography(testDNSToSwitchMapping.getRackToHosts());

        scheduler.schedule(topologies, cluster);
        Set<String> assignedRacks = cluster.getAssignedRacks(td1.getId());
        assertEquals("Racks for topology=" + td1.getId() + " is " + assignedRacks, 1, assignedRacks.size());

        TopologyBuilder builder = topologyBuilder(topo2NumSpouts, topo2NumBolts, topo2SpoutParallelism, topo2BoltParallelism);
        TopologyDetails td2 = topoToTopologyDetails(topoName2, config, builder.createTopology(), 0, 0,"user", topo2MaxHeapSize);

        //Now schedule GPU but with the simple topology in place.
        topologies = new Topologies(td1, td2);
        cluster = new Cluster(cluster, topologies);
        scheduler.schedule(topologies, cluster);

        assignedRacks = cluster.getAssignedRacks(td1.getId(), td2.getId());
        assertEquals("Racks for topologies=" + td1.getId() + "/" + td2.getId() + " is " + assignedRacks, 2, assignedRacks.size());

        // topo2 gets scheduled on its own rack because it is empty and available
        assignedRacks = cluster.getAssignedRacks(td2.getId());
        assertEquals("Racks for topologies=" + td2.getId() + " is " + assignedRacks, 1, assignedRacks.size());

        // now unassign topo2, expect only one rack to be in use; free some slots and reschedule topo1 some topo1 executors
        cluster.unassign(td2.getId());
        assignedRacks = cluster.getAssignedRacks(td2.getId());
        assertEquals("After unassigning topology " + td2.getId() + ", racks for topology=" + td2.getId() + " is " + assignedRacks,
                0, assignedRacks.size());
        assignedRacks = cluster.getAssignedRacks(td1.getId());
        assertEquals("After unassigning topology " + td2.getId() + ", racks for topology=" + td1.getId() + " is " + assignedRacks,
                1, assignedRacks.size());
        assertFalse("Topology " + td1.getId() + " should be fully assigned before freeing slots", cluster.needsSchedulingRas(td1));
        freeSomeWorkerSlots(cluster);
        assertTrue("Topology " + td1.getId() + " should need scheduling after freeing slots", cluster.needsSchedulingRas(td1));

        // then reschedule executors
        scheduler.schedule(topologies, cluster);

        // only one rack should be in use by topology1
        assignedRacks = cluster.getAssignedRacks(td1.getId());
        assertEquals("After reassigning topology " + td2.getId() + ", racks for topology=" + td1.getId() + " is " + assignedRacks,
                1, assignedRacks.size());
    }

    /**
     * Assign and then clear out a rack to host list mapping in cluster.networkTopography.
     * Expected behavior is that:
     *  <li>the rack without hosts does not show up in {@link NodeSorterHostProximity#getSortedRacks()}</li>
     *  <li>all the supervisor nodes still get returned in {@link NodeSorterHostProximity#sortAllNodes()} ()}</li>
     *  <li>supervisors on cleared rack show up under {@link DNSToSwitchMapping#DEFAULT_RACK}</li>
     *
     *  <p>
     *      Force an usual condition, where one of the racks is still passed to LazyNodeSortingIterator with
     *      an empty list and then ensure that code is resilient.
     *  </p>
     */
    @Test
    void testWithImpairedClusterNetworkTopography() {
        INimbus iNimbus = new INimbusTest();
        double compPcore = 100;
        double compOnHeap = 775;
        double compOffHeap = 25;
        int topo1NumSpouts = 1;
        int topo1NumBolts = 5;
        int topo1SpoutParallelism = 100;
        int topo1BoltParallelism = 200;
        final int numSupersPerRack = 10;
        final int numPortsPerSuper = 66;
        long compPerRack = (topo1NumSpouts * topo1SpoutParallelism + topo1NumBolts * topo1BoltParallelism + 10);
        long compPerSuper =  compPerRack / numSupersPerRack;
        double cpuPerSuper = compPcore * compPerSuper;
        double memPerSuper = (compOnHeap + compOffHeap) * compPerSuper;
        double topo1MaxHeapSize = memPerSuper;
        final String topoName1 = "topology1";
        int numRacks = 3;

        Map<String, SupervisorDetails> supMap = genSupervisorsWithRacks(numRacks, numSupersPerRack,  numPortsPerSuper,
            0, 0, cpuPerSuper, memPerSuper, new HashMap<>());
        TestDNSToSwitchMapping testDNSToSwitchMapping = new TestDNSToSwitchMapping(supMap.values());

        Config config = new Config();
        config.putAll(createGrasClusterConfig(compPcore, compOnHeap, compOffHeap, null, null));
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, GenericResourceAwareStrategy.class.getName());

        IScheduler scheduler = new ResourceAwareScheduler();
        scheduler.prepare(config, new StormMetricsRegistry());

        TopologyDetails td1 = genTopology(topoName1, config, topo1NumSpouts,
            topo1NumBolts, topo1SpoutParallelism, topo1BoltParallelism, 0, 0, "user", topo1MaxHeapSize);

        Topologies topologies = new Topologies(td1);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);
        cluster.setNetworkTopography(testDNSToSwitchMapping.getRackToHosts());

        Map<String, List<String>> networkTopography = cluster.getNetworkTopography();
        assertEquals("Expecting " + numRacks + " racks found " + networkTopography.size(), numRacks, networkTopography.size());
        assertTrue("Expecting racks count to be >= 3, found " + networkTopography.size(), networkTopography.size() >= 3);

        // Impair cluster.networkTopography and set one rack to have zero hosts, getSortedRacks should exclude this rack.
        // Keep, the supervisorDetails unchanged - confirm that these nodes are not lost even with incomplete networkTopography
        String rackIdToZero = networkTopography.keySet().stream().findFirst().get();
        impairClusterRack(cluster, rackIdToZero, true, false);

        NodeSorterHostProximity nodeSorterHostProximity = new NodeSorterHostProximity(cluster, td1);
        nodeSorterHostProximity.getSortedRacks().forEach(x -> assertNotEquals(x.id, rackIdToZero));

        // confirm that the above action has not lost the hosts and that they appear under the DEFAULT rack
        {
            Set<String> seenRacks = new HashSet<>();
            nodeSorterHostProximity.getSortedRacks().forEach(x -> seenRacks.add(x.id));
            assertEquals("Expecting rack cnt to be still " + numRacks, numRacks, seenRacks.size());
            assertTrue("Expecting to see default-rack=" + DNSToSwitchMapping.DEFAULT_RACK + " in sortedRacks",
                seenRacks.contains(DNSToSwitchMapping.DEFAULT_RACK));
        }

        // now check if node/supervisor is missing when sorting all nodes
        Set<String> expectedNodes = supMap.keySet();
        Set<String> seenNodes = new HashSet<>();
        nodeSorterHostProximity.prepare(null);
        nodeSorterHostProximity.sortAllNodes().forEach( n -> seenNodes.add(n));
        assertEquals("Expecting see all supervisors ", expectedNodes, seenNodes);

        // Now fully impair the cluster - confirm no default rack
        {
            cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);
            cluster.setNetworkTopography(new TestDNSToSwitchMapping(supMap.values()).getRackToHosts());
            impairClusterRack(cluster, rackIdToZero, true, true);
            Set<String> seenRacks = new HashSet<>();
            NodeSorterHostProximity nodeSorterHostProximity2 = new NodeSorterHostProximity(cluster, td1);
            nodeSorterHostProximity2.getSortedRacks().forEach(x -> seenRacks.add(x.id));
            Map<String, Set<String>> rackIdToHosts = nodeSorterHostProximity2.getRackIdToHosts();
            String dumpOfRacks = rackIdToHosts.entrySet().stream()
                .map(x -> String.format("rack %s -> hosts [%s]", x.getKey(), String.join(",", x.getValue())))
                .collect(Collectors.joining("\n\t"));
            assertEquals("Expecting rack cnt to be " + (numRacks - 1) + " but found " + seenRacks.size() + "\n\t" + dumpOfRacks,
                numRacks - 1, seenRacks.size());
            assertFalse("Found default-rack=" + DNSToSwitchMapping.DEFAULT_RACK + " in \n\t" + dumpOfRacks,
                seenRacks.contains(DNSToSwitchMapping.DEFAULT_RACK));
        }
    }

    /**
     * Black list all nodes for a rack before sorting nodes.
     * Confirm that {@link NodeSorterHostProximity#sortAllNodes()} still works.
     *
     */
    @Test
    void testWithBlackListedHosts() {
        INimbus iNimbus = new INimbusTest();
        double compPcore = 100;
        double compOnHeap = 775;
        double compOffHeap = 25;
        int topo1NumSpouts = 1;
        int topo1NumBolts = 5;
        int topo1SpoutParallelism = 100;
        int topo1BoltParallelism = 200;
        final int numSupersPerRack = 10;
        final int numPortsPerSuper = 66;
        long compPerRack = (topo1NumSpouts * topo1SpoutParallelism + topo1NumBolts * topo1BoltParallelism + 10);
        long compPerSuper =  compPerRack / numSupersPerRack;
        double cpuPerSuper = compPcore * compPerSuper;
        double memPerSuper = (compOnHeap + compOffHeap) * compPerSuper;
        double topo1MaxHeapSize = memPerSuper;
        final String topoName1 = "topology1";
        int numRacks = 3;

        Map<String, SupervisorDetails> supMap = genSupervisorsWithRacks(numRacks, numSupersPerRack,  numPortsPerSuper,
            0, 0, cpuPerSuper, memPerSuper, new HashMap<>());
        TestDNSToSwitchMapping testDNSToSwitchMapping = new TestDNSToSwitchMapping(supMap.values());

        Config config = new Config();
        config.putAll(createGrasClusterConfig(compPcore, compOnHeap, compOffHeap, null, null));
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, GenericResourceAwareStrategy.class.getName());

        IScheduler scheduler = new ResourceAwareScheduler();
        scheduler.prepare(config, new StormMetricsRegistry());

        TopologyDetails td1 = genTopology(topoName1, config, topo1NumSpouts,
            topo1NumBolts, topo1SpoutParallelism, topo1BoltParallelism, 0, 0, "user", topo1MaxHeapSize);

        Topologies topologies = new Topologies(td1);
        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);
        cluster.setNetworkTopography(testDNSToSwitchMapping.getRackToHosts());

        Map<String, List<String>> networkTopography = cluster.getNetworkTopography();
        assertEquals("Expecting " + numRacks + " racks found " + networkTopography.size(), numRacks, networkTopography.size());
        assertTrue("Expecting racks count to be >= 3, found " + networkTopography.size(), networkTopography.size() >= 3);

        Set<String> blackListedHosts = new HashSet<>();
        List<SupervisorDetails> supArray = new ArrayList<>(supMap.values());
        for (int i = 0 ; i < numSupersPerRack ; i++) {
            blackListedHosts.add(supArray.get(i).getHost());
        }
        blacklistHostsAndSortNodes(blackListedHosts, supMap.values(), cluster, td1);

        String rackToClear = cluster.getNetworkTopography().keySet().stream().findFirst().get();
        blackListedHosts = new HashSet<>(cluster.getNetworkTopography().get(rackToClear));
        blacklistHostsAndSortNodes(blackListedHosts, supMap.values(), cluster, td1);
    }

    // Impair cluster by blacklisting some hosts
    private void blacklistHostsAndSortNodes(
        Set<String> blackListedHosts, Collection<SupervisorDetails> sups, Cluster cluster, TopologyDetails td1) {
        LOG.info("blackListedHosts={}", blackListedHosts);
        cluster.setBlacklistedHosts(blackListedHosts);

        NodeSorterHostProximity nodeSorterHostProximity = new NodeSorterHostProximity(cluster, td1);
        // confirm that the above action loses hosts
        {
            Set<String> allHosts = sups.stream().map(x -> x.getHost()).collect(Collectors.toSet());
            Set<String> seenRacks = new HashSet<>();
            nodeSorterHostProximity.getSortedRacks().forEach(x -> seenRacks.add(x.id));
            Set<String> seenHosts = new HashSet<>();
            nodeSorterHostProximity.getRackIdToHosts().forEach((k,v) -> seenHosts.addAll(v));
            allHosts.removeAll(seenHosts);
            assertEquals("Expecting only blacklisted hosts removed", allHosts, blackListedHosts);
        }

        // now check if sortAllNodes still works
        Set<String> expectedNodes = sups.stream()
            .filter(x -> !blackListedHosts.contains(x.getHost()))
            .map(x ->x.getId())
            .collect(Collectors.toSet());
        Set<String> seenNodes = new HashSet<>();
            nodeSorterHostProximity.prepare(null);
            nodeSorterHostProximity.sortAllNodes().forEach( n -> seenNodes.add(n));
        assertEquals("Expecting see all supervisors ", expectedNodes, seenNodes);
    }

    /**
     * Impair the cluster for a specified rackId.
     *  <li>making the host list a zero length</li>
     *  <li>removing supervisors for the hosts on the rack</li>
     *
     * @param cluster cluster to impair
     * @param rackId rackId to clear
     * @param clearNetworkTopography if true, then clear (but not remove) the hosts in list for the rack.
     * @param clearSupervisorMap if true, then remove supervisors for the rack.
     */
    private void impairClusterRack(Cluster cluster, String rackId, boolean clearNetworkTopography, boolean clearSupervisorMap) {
        Set<String> hostIds = new HashSet<>(cluster.getNetworkTopography().computeIfAbsent(rackId, k -> new ArrayList<>()));
        if (clearNetworkTopography) {
            cluster.getNetworkTopography().computeIfAbsent(rackId, k -> new ArrayList<>()).clear();
        }
        if (clearSupervisorMap) {
            Set<String> supToRemove = new HashSet<>();
            for (String hostId: hostIds) {
                cluster.getSupervisorsByHost(hostId).forEach(s -> supToRemove.add(s.getId()));
            }
            Map<String, SupervisorDetails> supervisorDetailsMap = cluster.getSupervisors();
            for (String supId: supToRemove) {
                supervisorDetailsMap.remove(supId);
            }
        }
    }
}