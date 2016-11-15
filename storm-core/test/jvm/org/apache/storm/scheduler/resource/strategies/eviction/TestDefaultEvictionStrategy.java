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

package org.apache.storm.scheduler.resource.strategies.eviction;

import org.apache.storm.Config;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.SchedulerAssignmentImpl;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.ResourceAwareScheduler;
import org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestDefaultEvictionStrategy {

    private static int currentTime = 1450418597;

    /**
     * The resources in the cluster are limited. In the first round of scheduling, all resources in the cluster is used.
     * User jerry submits another toploogy.  Since user jerry has his resource guarantees satisfied, and user bobby
     * has exceeded his resource guarantee, topo-3 from user bobby should be evicted.
     */
    @Test
    public void testEviction() {
        INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 100.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1000.0);
        Map<String, SupervisorDetails> supMap = TestUtilsForResourceAwareScheduler.genSupervisors(4, 4, resourceMap);
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, org.apache.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 100.0);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 500);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 500);
        Map<String, Map<String, Number>> resourceUserPool = new HashMap<String, Map<String, Number>>();
        resourceUserPool.put("jerry", new HashMap<String, Number>());
        resourceUserPool.get("jerry").put("cpu", 200.0);
        resourceUserPool.get("jerry").put("memory", 2000.0);

        resourceUserPool.put("bobby", new HashMap<String, Number>());
        resourceUserPool.get("bobby").put("cpu", 100.0);
        resourceUserPool.get("bobby").put("memory", 1000.0);

        resourceUserPool.put("derek", new HashMap<String, Number>());
        resourceUserPool.get("derek").put("cpu", 200.0);
        resourceUserPool.get("derek").put("memory", 2000.0);

        config.put(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS, resourceUserPool);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "jerry");

        TopologyDetails topo1 = TestUtilsForResourceAwareScheduler.getTopology("topo-1", config, 1, 0, 1, 0, currentTime - 2, 10);
        TopologyDetails topo6 = TestUtilsForResourceAwareScheduler.getTopology("topo-6", config, 1, 0, 1, 0, currentTime - 2, 20);


        config.put(Config.TOPOLOGY_SUBMITTER_USER, "bobby");

        TopologyDetails topo2 = TestUtilsForResourceAwareScheduler.getTopology("topo-2", config, 1, 0, 1, 0, currentTime - 2, 10);
        TopologyDetails topo3 = TestUtilsForResourceAwareScheduler.getTopology("topo-3", config, 1, 0, 1, 0, currentTime - 2, 20);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "derek");

        TopologyDetails topo4 = TestUtilsForResourceAwareScheduler.getTopology("topo-4", config, 1, 0, 1, 0, currentTime - 2, 29);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo1.getId(), topo1);
        topoMap.put(topo2.getId(), topo2);
        topoMap.put(topo3.getId(), topo3);
        topoMap.put(topo4.getId(), topo4);

        Topologies topologies = new Topologies(topoMap);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("bobby").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());

        //user jerry submits another topology
        topoMap.put(topo6.getId(), topo6);
        topologies = new Topologies(topoMap);

        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("bobby").getTopologiesRunning().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesAttempted()) {
            Assert.assertFalse("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of attempted topologies", 1, rs.getUser("bobby").getTopologiesAttempted().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
        Assert.assertEquals("correct topology to evict", "topo-3", rs.getUser("bobby").getTopologiesAttempted().iterator().next().getName());
    }

    @Test
    public void TestEvictMultipleTopologies() {
        INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 100.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1000.0);
        Map<String, SupervisorDetails> supMap = TestUtilsForResourceAwareScheduler.genSupervisors(4, 4, resourceMap);
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, org.apache.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 100.0);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 500);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 500);
        Map<String, Map<String, Number>> resourceUserPool = new HashMap<String, Map<String, Number>>();
        resourceUserPool.put("jerry", new HashMap<String, Number>());
        resourceUserPool.get("jerry").put("cpu", 200.0);
        resourceUserPool.get("jerry").put("memory", 2000.0);

        resourceUserPool.put("derek", new HashMap<String, Number>());
        resourceUserPool.get("derek").put("cpu", 100.0);
        resourceUserPool.get("derek").put("memory", 1000.0);

        config.put(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS, resourceUserPool);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "jerry");

        TopologyDetails topo1 = TestUtilsForResourceAwareScheduler.getTopology("topo-1", config, 2, 0, 1, 0, currentTime - 2, 10);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "bobby");

        TopologyDetails topo2 = TestUtilsForResourceAwareScheduler.getTopology("topo-2", config, 1, 0, 1, 0, currentTime - 2, 10);
        TopologyDetails topo3 = TestUtilsForResourceAwareScheduler.getTopology("topo-3", config, 1, 0, 1, 0, currentTime - 2, 20);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "derek");

        TopologyDetails topo4 = TestUtilsForResourceAwareScheduler.getTopology("topo-4", config, 1, 0, 1, 0, currentTime - 2, 29);
        TopologyDetails topo5 = TestUtilsForResourceAwareScheduler.getTopology("topo-5", config, 1, 0, 1, 0, currentTime - 2, 29);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo2.getId(), topo2);
        topoMap.put(topo3.getId(), topo3);
        topoMap.put(topo4.getId(), topo4);
        topoMap.put(topo5.getId(), topo5);

        Topologies topologies = new Topologies(topoMap);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("bobby").getTopologiesAttempted().size());

        //user jerry submits another topology
        topoMap.put(topo1.getId(), topo1);
        topologies = new Topologies(topoMap);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesAttempted()) {
            Assert.assertFalse("assert topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of attempted topologies", 2, rs.getUser("bobby").getTopologiesAttempted().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
        Assert.assertEquals("# of running topologies", 0, rs.getUser("bobby").getTopologiesRunning().size());
    }

    /**
     * Eviction order:
     * topo-3: since user bobby don't have any resource guarantees and topo-3 is the lowest priority for user bobby
     * topo-2: since user bobby don't have any resource guarantees and topo-2 is the next lowest priority for user bobby
     * topo-5: since user derek has exceeded his resource guarantee while user jerry has not.  topo-5 and topo-4 has the same priority
     * but topo-4 was submitted earlier thus we choose that one to evict
     */
    @Test
    public void TestEvictMultipleTopologiesFromMultipleUsersInCorrectOrder() {
        INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 100.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1000.0);
        Map<String, SupervisorDetails> supMap = TestUtilsForResourceAwareScheduler.genSupervisors(4, 4, resourceMap);
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, org.apache.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 100.0);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 500);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 500);
        Map<String, Map<String, Number>> resourceUserPool = new HashMap<String, Map<String, Number>>();
        resourceUserPool.put("jerry", new HashMap<String, Number>());
        resourceUserPool.get("jerry").put("cpu", 300.0);
        resourceUserPool.get("jerry").put("memory", 3000.0);

        resourceUserPool.put("derek", new HashMap<String, Number>());
        resourceUserPool.get("derek").put("cpu", 100.0);
        resourceUserPool.get("derek").put("memory", 1000.0);

        config.put(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS, resourceUserPool);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "jerry");

        TopologyDetails topo1 = TestUtilsForResourceAwareScheduler.getTopology("topo-1", config, 1, 0, 1, 0, currentTime - 2, 10);
        TopologyDetails topo6 = TestUtilsForResourceAwareScheduler.getTopology("topo-6", config, 1, 0, 1, 0, currentTime - 2, 10);
        TopologyDetails topo7 = TestUtilsForResourceAwareScheduler.getTopology("topo-7", config, 1, 0, 1, 0, currentTime - 2, 10);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "bobby");

        TopologyDetails topo2 = TestUtilsForResourceAwareScheduler.getTopology("topo-2", config, 1, 0, 1, 0, currentTime - 2, 10);
        TopologyDetails topo3 = TestUtilsForResourceAwareScheduler.getTopology("topo-3", config, 1, 0, 1, 0, currentTime - 2, 20);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "derek");

        TopologyDetails topo4 = TestUtilsForResourceAwareScheduler.getTopology("topo-4", config, 1, 0, 1, 0, currentTime - 2, 29);
        TopologyDetails topo5 = TestUtilsForResourceAwareScheduler.getTopology("topo-5", config, 1, 0, 1, 0, currentTime - 15, 29);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo2.getId(), topo2);
        topoMap.put(topo3.getId(), topo3);
        topoMap.put(topo4.getId(), topo4);
        topoMap.put(topo5.getId(), topo5);

        Topologies topologies = new Topologies(topoMap);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("bobby").getTopologiesAttempted().size());

        //user jerry submits another topology
        topoMap.put(topo1.getId(), topo1);
        topologies = new Topologies(topoMap);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesAttempted()) {
            Assert.assertFalse("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of attempted topologies", 1, rs.getUser("bobby").getTopologiesAttempted().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
        Assert.assertEquals("# of running topologies", 1, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("correct topology to evict", rs.getUser("bobby").getTopologiesAttempted().iterator().next().getName(), "topo-3");

        topoMap.put(topo6.getId(), topo6);
        topologies = new Topologies(topoMap);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesAttempted()) {
            Assert.assertFalse("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of attempted topologies", 2, rs.getUser("bobby").getTopologiesAttempted().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
        Assert.assertEquals("# of running topologies", 0, rs.getUser("bobby").getTopologiesRunning().size());

        Assert.assertTrue("correct topology to evict", TestUtilsForResourceAwareScheduler.findTopologyInSetFromName("topo-2", rs.getUser("bobby").getTopologiesAttempted()) != null);
        Assert.assertTrue("correct topology to evict", TestUtilsForResourceAwareScheduler.findTopologyInSetFromName("topo-3", rs.getUser("bobby").getTopologiesAttempted()) != null);

        topoMap.put(topo7.getId(), topo7);
        topologies = new Topologies(topoMap);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 3, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        for (TopologyDetails topo : rs.getUser("derek").getTopologiesAttempted()) {
            Assert.assertFalse("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 1, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());
        Assert.assertEquals("correct topology to evict", rs.getUser("derek").getTopologiesAttempted().iterator().next().getName(), "topo-4");

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesAttempted()) {
            Assert.assertFalse("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of attempted topologies", 2, rs.getUser("bobby").getTopologiesAttempted().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
        Assert.assertEquals("# of running topologies", 0, rs.getUser("bobby").getTopologiesRunning().size());

        Assert.assertTrue("correct topology to evict", TestUtilsForResourceAwareScheduler.findTopologyInSetFromName("topo-2", rs.getUser("bobby").getTopologiesAttempted()) != null);
        Assert.assertTrue("correct topology to evict", TestUtilsForResourceAwareScheduler.findTopologyInSetFromName("topo-3", rs.getUser("bobby").getTopologiesAttempted()) != null);
    }

    /**
     * If topologies from other users cannot be evicted to make space
     * check if there is a topology with lower priority that can be evicted from the current user
     */
    @Test
    public void TestEvictTopologyFromItself() {
        INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 100.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1000.0);
        Map<String, SupervisorDetails> supMap = TestUtilsForResourceAwareScheduler.genSupervisors(4, 4, resourceMap);
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, org.apache.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 100.0);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 500);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 500);
        Map<String, Map<String, Number>> resourceUserPool = new HashMap<String, Map<String, Number>>();
        resourceUserPool.put("jerry", new HashMap<String, Number>());
        resourceUserPool.get("jerry").put("cpu", 200.0);
        resourceUserPool.get("jerry").put("memory", 2000.0);

        resourceUserPool.put("bobby", new HashMap<String, Number>());
        resourceUserPool.get("bobby").put("cpu", 100.0);
        resourceUserPool.get("bobby").put("memory", 1000.0);

        resourceUserPool.put("derek", new HashMap<String, Number>());
        resourceUserPool.get("derek").put("cpu", 100.0);
        resourceUserPool.get("derek").put("memory", 1000.0);

        config.put(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS, resourceUserPool);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "jerry");

        TopologyDetails topo1 = TestUtilsForResourceAwareScheduler.getTopology("topo-1", config, 1, 0, 1, 0, currentTime - 2, 20);
        TopologyDetails topo2 = TestUtilsForResourceAwareScheduler.getTopology("topo-2", config, 1, 0, 1, 0, currentTime - 2, 20);
        TopologyDetails topo3 = TestUtilsForResourceAwareScheduler.getTopology("topo-3", config, 1, 0, 1, 0, currentTime - 2, 29);
        TopologyDetails topo4 = TestUtilsForResourceAwareScheduler.getTopology("topo-4", config, 1, 0, 1, 0, currentTime - 2, 10);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "bobby");

        TopologyDetails topo5 = TestUtilsForResourceAwareScheduler.getTopology("topo-5", config, 1, 0, 1, 0, currentTime - 2, 10);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "derek");

        TopologyDetails topo6 = TestUtilsForResourceAwareScheduler.getTopology("topo-6", config, 1, 0, 1, 0, currentTime - 2, 29);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo1.getId(), topo1);
        topoMap.put(topo2.getId(), topo2);
        topoMap.put(topo5.getId(), topo5);
        topoMap.put(topo6.getId(), topo6);

        Topologies topologies = new Topologies(topoMap);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());


        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("bobby").getTopologiesAttempted().size());

        //user jerry submits another topology into a full cluster
        // topo3 should not be able to scheduled
        topoMap.put(topo3.getId(), topo3);
        topologies = new Topologies(topoMap);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 1, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());
        //make sure that topo-3 didn't get scheduled.
        Assert.assertEquals("correct topology in attempted queue", rs.getUser("jerry").getTopologiesAttempted().iterator().next().getName(), "topo-3");


        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("bobby").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());

        //user jerry submits another topology but this one should be scheduled since it has higher priority than than the
        //rest of jerry's running topologies
        topoMap.put(topo4.getId(), topo4);
        topologies = new Topologies(topoMap);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 2, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());
        Assert.assertTrue("correct topology in attempted queue", TestUtilsForResourceAwareScheduler.findTopologyInSetFromName("topo-3", rs.getUser("jerry").getTopologiesAttempted()) != null);
        //Either topo-1 or topo-2 should have gotten evicted
        Assert.assertTrue("correct topology in attempted queue", ((TestUtilsForResourceAwareScheduler.findTopologyInSetFromName("topo-1", rs.getUser("jerry").getTopologiesAttempted())) != null)
                || (TestUtilsForResourceAwareScheduler.findTopologyInSetFromName("topo-2", rs.getUser("jerry").getTopologiesAttempted()) != null));
        //assert that topo-4 got scheduled
        Assert.assertTrue("correct topology in running queue", TestUtilsForResourceAwareScheduler.findTopologyInSetFromName("topo-4", rs.getUser("jerry").getTopologiesRunning()) != null);

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("bobby").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
    }

    /**
     * If users are above his or her guarantee, check if topology eviction works correct
     */
    @Test
    public void TestOverGuaranteeEviction() {
        INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 100.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1000.0);
        Map<String, SupervisorDetails> supMap = TestUtilsForResourceAwareScheduler.genSupervisors(4, 4, resourceMap);
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, org.apache.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 100.0);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 500);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 500);
        Map<String, Map<String, Number>> resourceUserPool = new HashMap<String, Map<String, Number>>();
        resourceUserPool.put("jerry", new HashMap<String, Number>());
        resourceUserPool.get("jerry").put("cpu", 70.0);
        resourceUserPool.get("jerry").put("memory", 700.0);

        resourceUserPool.put("bobby", new HashMap<String, Number>());
        resourceUserPool.get("bobby").put("cpu", 100.0);
        resourceUserPool.get("bobby").put("memory", 1000.0);

        resourceUserPool.put("derek", new HashMap<String, Number>());
        resourceUserPool.get("derek").put("cpu", 25.0);
        resourceUserPool.get("derek").put("memory", 250.0);

        config.put(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS, resourceUserPool);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "jerry");

        TopologyDetails topo1 = TestUtilsForResourceAwareScheduler.getTopology("topo-1", config, 1, 0, 1, 0, currentTime - 2, 20);
        TopologyDetails topo2 = TestUtilsForResourceAwareScheduler.getTopology("topo-2", config, 1, 0, 1, 0, currentTime - 2, 20);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "bobby");

        TopologyDetails topo3 = TestUtilsForResourceAwareScheduler.getTopology("topo-3", config, 1, 0, 1, 0, currentTime - 2, 10);
        TopologyDetails topo4 = TestUtilsForResourceAwareScheduler.getTopology("topo-4", config, 1, 0, 1, 0, currentTime - 2, 10);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "derek");

        TopologyDetails topo5 = TestUtilsForResourceAwareScheduler.getTopology("topo-5", config, 1, 0, 1, 0, currentTime - 2, 29);
        TopologyDetails topo6 = TestUtilsForResourceAwareScheduler.getTopology("topo-6", config, 1, 0, 1, 0, currentTime - 2, 10);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo1.getId(), topo1);
        topoMap.put(topo3.getId(), topo3);
        topoMap.put(topo4.getId(), topo4);
        topoMap.put(topo5.getId(), topo5);

        Topologies topologies = new Topologies(topoMap);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("bobby").getTopologiesAttempted().size());

        //user derek submits another topology into a full cluster
        //topo6 should not be able to scheduled intially, but since topo6 has higher priority than topo5
        //topo5 will be evicted so that topo6 can be scheduled
        topoMap.put(topo6.getId(), topo6);
        topologies = new Topologies(topoMap);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 1, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());
        //topo5 will be evicted since topo6 has higher priority
        Assert.assertEquals("correct topology in attempted queue", "topo-5", rs.getUser("derek").getTopologiesAttempted().iterator().next().getName());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("bobby").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());

        //user jerry submits topo2
        topoMap.put(topo2.getId(), topo2);
        topologies = new Topologies(topoMap);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 0, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 2, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());
        Assert.assertEquals("correct topology in attempted queue", "topo-6", rs.getUser("derek").getTopologiesAttempted().iterator().next().getName());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("bobby").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
    }
}
