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
import org.apache.storm.networktopography.DNSToSwitchMapping;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.SchedulerAssignmentImpl;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.RAS_Node;
import org.apache.storm.scheduler.resource.RAS_Nodes;
import org.apache.storm.scheduler.resource.ResourceAwareScheduler;
import org.apache.storm.scheduler.resource.SchedulingResult;
import org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.ObjectResources;
import org.apache.storm.scheduler.resource.SchedulingState;
import org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler;
import org.apache.storm.scheduler.resource.User;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;


public class TestDefaultResourceAwareStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(TestDefaultResourceAwareStrategy.class);

    private static int currentTime = 1450418597;

    /**
     * test if the scheduling logic for the DefaultResourceAwareStrategy is correct
     */
    @Test
    public void testDefaultResourceAwareStrategy() {
        int spoutParallelism = 1;
        int boltParallelism = 2;
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new TestUtilsForResourceAwareScheduler.TestSpout(),
                spoutParallelism);
        builder.setBolt("bolt-1", new TestUtilsForResourceAwareScheduler.TestBolt(),
                boltParallelism).shuffleGrouping("spout");
        builder.setBolt("bolt-2", new TestUtilsForResourceAwareScheduler.TestBolt(),
                boltParallelism).shuffleGrouping("bolt-1");
        builder.setBolt("bolt-3", new TestUtilsForResourceAwareScheduler.TestBolt(),
                boltParallelism).shuffleGrouping("bolt-2");

        StormTopology stormToplogy = builder.createTopology();

        Config conf = new Config();
        INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 150.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1500.0);
        Map<String, SupervisorDetails> supMap = TestUtilsForResourceAwareScheduler.genSupervisors(4, 4, resourceMap);
        conf.putAll(Utils.readDefaultConfig());
        conf.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, org.apache.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        conf.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        conf.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
        conf.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 50.0);
        conf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 250);
        conf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 250);
        conf.put(Config.TOPOLOGY_PRIORITY, 0);
        conf.put(Config.TOPOLOGY_NAME, "testTopology");
        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, Double.MAX_VALUE);

        TopologyDetails topo = new TopologyDetails("testTopology-id", conf, stormToplogy, 0,
                TestUtilsForResourceAwareScheduler.genExecsAndComps(stormToplogy)
                , this.currentTime);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo.getId(), topo);
        Topologies topologies = new Topologies(topoMap);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), conf);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(conf);
        rs.schedule(topologies, cluster);

        Map<String, List<String>> nodeToComps = new HashMap<String, List<String>>();
        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : cluster.getAssignments().get("testTopology-id").getExecutorToSlot().entrySet()) {
            WorkerSlot ws = entry.getValue();
            ExecutorDetails exec = entry.getKey();
            if (!nodeToComps.containsKey(ws.getNodeId())) {
                nodeToComps.put(ws.getNodeId(), new LinkedList<String>());
            }
            nodeToComps.get(ws.getNodeId()).add(topo.getExecutorToComponent().get(exec));
        }

        /**
         * check for correct scheduling
         * Since all the resource availabilites on nodes are the same in the beginining
         * DefaultResourceAwareStrategy can arbitrarily pick one thus we must find if a particular scheduling
         * exists on a node the the cluster.
         */

        //one node should have the below scheduling
        List<String> node1 = new LinkedList<>();
        node1.add("spout");
        node1.add("bolt-1");
        node1.add("bolt-2");
        Assert.assertTrue("Check DefaultResourceAwareStrategy scheduling", checkDefaultStrategyScheduling(nodeToComps, node1));

        //one node should have the below scheduling
        List<String> node2 = new LinkedList<>();
        node2.add("bolt-3");
        node2.add("bolt-1");
        node2.add("bolt-2");

        Assert.assertTrue("Check DefaultResourceAwareStrategy scheduling", checkDefaultStrategyScheduling(nodeToComps, node2));

        //one node should have the below scheduling
        List<String> node3 = new LinkedList<>();
        node3.add("bolt-3");

        Assert.assertTrue("Check DefaultResourceAwareStrategy scheduling", checkDefaultStrategyScheduling(nodeToComps, node3));

        //three used and one node should be empty
        Assert.assertEquals("only three nodes should be used", 3, nodeToComps.size());
    }

    private boolean checkDefaultStrategyScheduling(Map<String, List<String>> nodeToComps, List<String> schedulingToFind) {
        for (List<String> entry : nodeToComps.values()) {
            if (schedulingToFind.containsAll(entry) && entry.containsAll(schedulingToFind)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Test whether strategy will choose correct rack
     */
    @Test
    public void testMultipleRacks() {

        final Map<String, SupervisorDetails> supMap = new HashMap<String, SupervisorDetails>();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        //generate a rack of supervisors
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 400.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 8000.0);
        final Map<String, SupervisorDetails> supMapRack1 = TestUtilsForResourceAwareScheduler.genSupervisors(10, 4, 0, resourceMap);

        //generate another rack of supervisors with less resources
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 200.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 4000.0);
        final Map<String, SupervisorDetails> supMapRack2 = TestUtilsForResourceAwareScheduler.genSupervisors(10, 4, 10, resourceMap);

        //generate some supervisors that are depleted of one resource
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 0.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 8000.0);
        final Map<String, SupervisorDetails> supMapRack3 = TestUtilsForResourceAwareScheduler.genSupervisors(10, 4, 20, resourceMap);

        //generate some that has alot of memory but little of cpu
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 10.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 8000.0 *2 + 4000.0);
        final Map<String, SupervisorDetails> supMapRack4 = TestUtilsForResourceAwareScheduler.genSupervisors(10, 4, 30, resourceMap);

        //generate some that has alot of cpu but little of memory
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 400.0 + 200.0 + 10.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1000.0);
        final Map<String, SupervisorDetails> supMapRack5 = TestUtilsForResourceAwareScheduler.genSupervisors(10, 4, 40, resourceMap);


        supMap.putAll(supMapRack1);
        supMap.putAll(supMapRack2);
        supMap.putAll(supMapRack3);
        supMap.putAll(supMapRack4);
        supMap.putAll(supMapRack5);

        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, org.apache.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 100.0);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 500);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 500);
        config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, Double.MAX_VALUE);
        INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);

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

        //generate topologies
        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        TopologyDetails topo1 = TestUtilsForResourceAwareScheduler.getTopology("topo-1", config, 8, 0, 2, 0, currentTime - 2, 10);
        TopologyDetails topo2 = TestUtilsForResourceAwareScheduler.getTopology("topo-2", config, 8, 0, 2, 0, currentTime - 2, 10);

        topoMap.put(topo1.getId(), topo1);

        Topologies topologies = new Topologies(topoMap);

        DefaultResourceAwareStrategy rs = new DefaultResourceAwareStrategy();

        rs.prepare(new SchedulingState(new HashMap<String, User>(), cluster, topologies, config));
        TreeSet<ObjectResources> sortedRacks= rs.sortRacks(topo1.getId(), new HashMap<WorkerSlot, Collection<ExecutorDetails>>());

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

        SchedulingResult schedulingResult = rs.schedule(topo1);
        for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> entry : schedulingResult.getSchedulingResultMap().entrySet()) {
            WorkerSlot ws = entry.getKey();
            Collection<ExecutorDetails> execs = entry.getValue();
            //make sure all workers on scheduled in rack-0
            Assert.assertEquals("assert worker scheduled on rack-0", "rack-0", resolvedSuperVisors.get(rs.idToNode(ws.getNodeId()).getHostname()));
            // make actual assignments
            cluster.assign(ws, topo1.getId(), execs);
        }
        Assert.assertEquals("All executors in topo-1 scheduled", 0, cluster.getUnassignedExecutors(topo1).size());

        //Test if topology is already partially scheduled on one rack

        topoMap.put(topo2.getId(), topo2);
        topologies = new Topologies(topoMap);
        RAS_Nodes nodes = new RAS_Nodes(cluster, topologies);
        Iterator<ExecutorDetails> executorIterator = topo2.getExecutors().iterator();
        List<String> nodeHostnames = rackToNodes.get("rack-1");
        for (int i=0 ; i< topo2.getExecutors().size()/2; i++) {
            String nodeHostname = nodeHostnames.get(i % nodeHostnames.size());
            RAS_Node node = rs.idToNode(rs.NodeHostnameToId(nodeHostname));
            WorkerSlot targetSlot = node.getFreeSlots().iterator().next();
            ExecutorDetails targetExec = executorIterator.next();
            // to keep track of free slots
            node.assign(targetSlot, topo2, Arrays.asList(targetExec));
            // to actually assign
            cluster.assign(targetSlot, topo2.getId(), Arrays.asList(targetExec));
        }

        rs = new DefaultResourceAwareStrategy();
        rs.prepare(new SchedulingState(new HashMap<String, User>(), cluster, topologies, config));
        // schedule topo2
        schedulingResult = rs.schedule(topo2);

        // checking assignments
        for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> entry : schedulingResult.getSchedulingResultMap().entrySet()) {
            WorkerSlot ws = entry.getKey();
            Collection<ExecutorDetails> execs = entry.getValue();
            //make sure all workers on scheduled in rack-1
            Assert.assertEquals("assert worker scheduled on rack-1", "rack-1", resolvedSuperVisors.get(rs.idToNode(ws.getNodeId()).getHostname()));
            // make actual assignments
            cluster.assign(ws, topo2.getId(), execs);
        }
        Assert.assertEquals("All executors in topo-2 scheduled", 0, cluster.getUnassignedExecutors(topo1).size());
    }
}
