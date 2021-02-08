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

package org.apache.storm.scheduler.resource.strategies.priority;

import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.resource.ResourceAwareScheduler;
import org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler;
import org.apache.storm.scheduler.resource.normalization.ResourceMetrics;
import org.apache.storm.scheduler.resource.strategies.scheduling.GenericResourceAwareStrategy;
import org.apache.storm.utils.Time;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.addTopologies;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.assertTopologiesBeenEvicted;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.assertTopologiesFullyScheduled;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.assertTopologiesNotBeenEvicted;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.assertTopologiesNotScheduled;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.genSupervisors;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.genTopology;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.userRes;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.userResourcePool;

public class TestGenericResourceAwareSchedulingPriorityStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(TestGenericResourceAwareSchedulingPriorityStrategy.class);
    private int currentTime = Time.currentTimeSecs();
    private IScheduler scheduler = null;

    @After
    public void cleanup() {
        if (scheduler != null) {
            scheduler.cleanup();
            scheduler = null;
        }
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

    /*
     * DefaultSchedulingPriorityStrategy will not evict topo as long as the resources request can be met
     *
     *  Ethan asks for heavy cpu and memory while Rui asks for little cpu and memory but heavy generic resource
     *  Since Rui's all types of resources request can be met, no eviction will happen.
    */
    @Test
    public void testDefaultSchedulingPriorityStrategyNotEvicting() {
        Map<String, Double> requestedgenericResourcesMap = new HashMap<>();
        requestedgenericResourcesMap.put("generic.resource.1", 40.0);
        // Use full memory and cpu of the cluster capacity
        Config ruiConf = createGrasClusterConfig(20, 50, 50, null, requestedgenericResourcesMap);
        Config ethanConf = createGrasClusterConfig(80, 400, 500, null, Collections.emptyMap());
        Topologies topologies = new Topologies(
            genTopology("ethan-topo-1", ethanConf, 1, 0, 1, 0, currentTime - 2, 10, "ethan"),
            genTopology("ethan-topo-2", ethanConf, 1, 0, 1, 0, currentTime - 2, 20, "ethan"),
            genTopology("ethan-topo-3", ethanConf, 1, 0, 1, 0, currentTime - 2, 28, "ethan"),
            genTopology("ethan-topo-4", ethanConf, 1, 0, 1, 0, currentTime - 2, 29, "ethan"));

        Topologies withNewTopo = addTopologies(topologies,
            genTopology("rui-topo-1", ruiConf, 1, 0, 4, 0, currentTime - 2, 10, "rui"));

        Config config = mkClusterConfig(DefaultSchedulingPriorityStrategy.class.getName());
        Cluster cluster = mkTestCluster(topologies, config);
        scheduler = new ResourceAwareScheduler();
        scheduler.prepare(config, new StormMetricsRegistry());

        scheduler.schedule(topologies, cluster);

        assertTopologiesFullyScheduled(cluster, "ethan-topo-1", "ethan-topo-2", "ethan-topo-3", "ethan-topo-4");

        cluster = new Cluster(cluster, withNewTopo);
        scheduler.schedule(withNewTopo, cluster);
        Map<String, Set<String>> evictedTopos = ((ResourceAwareScheduler) scheduler).getEvictedTopologiesMap();

        assertTopologiesFullyScheduled(cluster, "ethan-topo-1", "ethan-topo-2", "ethan-topo-3", "ethan-topo-4");
        assertTopologiesNotBeenEvicted(cluster, collectMapValues(evictedTopos), "ethan-topo-1", "ethan-topo-2", "ethan-topo-3", "ethan-topo-4");
        assertTopologiesFullyScheduled(cluster, "rui-topo-1");
    }

    /*
     * DefaultSchedulingPriorityStrategy does not take generic resources into account when calculating score
     * So even if a user is requesting a lot of generic resources other than CPU and memory, scheduler will still score it very low and kick out other topologies
     *
     *  Ethan asks for medium cpu and memory while Rui asks for little cpu and memory but heavy generic resource
     *  However, Rui's generic request can not be met and default scoring system is not taking generic resources into account,
     *  so the score of Rui's new topo will be much lower than all Ethan's topos'.
     *  Then all Ethan's topo will be evicted in trying to make rooms for Rui.
     */
    @Test
    public void testDefaultSchedulingPriorityStrategyEvicting() {
        Map<String, Double> requestedgenericResourcesMap = new HashMap<>();
        requestedgenericResourcesMap.put("generic.resource.1", 40.0);
        Config ruiConf = createGrasClusterConfig(10, 10, 10, null, requestedgenericResourcesMap);
        Config ethanConf = createGrasClusterConfig(60, 200, 300, null, Collections.emptyMap());
        Topologies topologies = new Topologies(
            genTopology("ethan-topo-1", ethanConf, 1, 0, 1, 0, currentTime - 2, 10, "ethan"),
            genTopology("ethan-topo-2", ethanConf, 1, 0, 1, 0, currentTime - 2, 20, "ethan"),
            genTopology("ethan-topo-3", ethanConf, 1, 0, 1, 0, currentTime - 2, 28, "ethan"),
            genTopology("ethan-topo-4", ethanConf, 1, 0, 1, 0, currentTime - 2, 29, "ethan"));

        Topologies withNewTopo = addTopologies(topologies,
            genTopology("rui-topo-1", ruiConf, 1, 0, 5, 0, currentTime - 2, 10, "rui"));

        Config config = mkClusterConfig(DefaultSchedulingPriorityStrategy.class.getName());
        Cluster cluster = mkTestCluster(topologies, config);
        scheduler = new ResourceAwareScheduler();
        scheduler.prepare(config, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);

        assertTopologiesFullyScheduled(cluster, "ethan-topo-1", "ethan-topo-2", "ethan-topo-3", "ethan-topo-4");

        cluster = new Cluster(cluster, withNewTopo);
        scheduler.schedule(withNewTopo, cluster);
        Map<String, Set<String>> evictedTopos = ((ResourceAwareScheduler) scheduler).getEvictedTopologiesMap();

        assertTopologiesFullyScheduled(cluster, "ethan-topo-1", "ethan-topo-2", "ethan-topo-3", "ethan-topo-4");
        assertTopologiesBeenEvicted(cluster, collectMapValues(evictedTopos), "ethan-topo-1", "ethan-topo-2", "ethan-topo-3", "ethan-topo-4");
        assertTopologiesNotScheduled(cluster, "rui-topo-1");
    }

    /*
     * GenericResourceAwareSchedulingPriorityStrategy extend scoring formula to accommodate generic resources
     *
     *   Same setting as testDefaultSchedulingPriorityStrategyEvicting, but this time, new scoring system is taking generic resources into account,
     *   the score of rui's new topo will be higher than all Ethan's topos' due to its crazy generic request.
     *   At the end, all Ethan's topo will not be evicted as expected.
     */
    @Test
    public void testGenericSchedulingPriorityStrategyEvicting() {
        Map<String, Double> requestedgenericResourcesMap = new HashMap<>();
        requestedgenericResourcesMap.put("generic.resource.1", 40.0);
        Config ruiConf = createGrasClusterConfig(10, 10, 10, null, requestedgenericResourcesMap);
        Config ethanConf = createGrasClusterConfig(60, 200, 300, null, Collections.emptyMap());
        Topologies topologies = new Topologies(
            genTopology("ethan-topo-1", ethanConf, 1, 0, 1, 0, currentTime - 2, 10, "ethan"),
            genTopology("ethan-topo-2", ethanConf, 1, 0, 1, 0, currentTime - 2, 20, "ethan"),
            genTopology("ethan-topo-3", ethanConf, 1, 0, 1, 0, currentTime - 2, 28, "ethan"),
            genTopology("ethan-topo-4", ethanConf, 1, 0, 1, 0, currentTime - 2, 29, "ethan"));

        Topologies withNewTopo = addTopologies(topologies,
            genTopology("rui-topo-1", ruiConf, 1, 0, 5, 0, currentTime - 2, 10, "rui"));


        Config config = mkClusterConfig(GenericResourceAwareSchedulingPriorityStrategy.class.getName());
        Cluster cluster = mkTestCluster(topologies, config);
        scheduler = new ResourceAwareScheduler();
        scheduler.prepare(config, new StormMetricsRegistry());
        scheduler.schedule(topologies, cluster);

        assertTopologiesFullyScheduled(cluster, "ethan-topo-1", "ethan-topo-2", "ethan-topo-3", "ethan-topo-4");

        cluster = new Cluster(cluster, withNewTopo);
        scheduler.schedule(withNewTopo, cluster);
        Map<String, Set<String>> evictedTopos = ((ResourceAwareScheduler) scheduler).getEvictedTopologiesMap();

        assertTopologiesFullyScheduled(cluster, "ethan-topo-1", "ethan-topo-2", "ethan-topo-3", "ethan-topo-4");
        assertTopologiesNotBeenEvicted(cluster, collectMapValues(evictedTopos),"ethan-topo-1", "ethan-topo-2", "ethan-topo-3", "ethan-topo-4");
        assertTopologiesNotScheduled(cluster, "rui-topo-1");
    }


    private Config mkClusterConfig(String SchedulingPriorityStrategy) {
        Map<String, Map<String, Number>> resourceUserPool = userResourcePool(
            userRes("rui", 200, 2000),
            userRes("ethan", 200, 2000));

        Map<String, Double> genericResourcesOfferedMap = new HashMap<>();
        genericResourcesOfferedMap.put("generic.resource.1", 50.0);

        Config config = createGrasClusterConfig(100, 500, 500, resourceUserPool, genericResourcesOfferedMap);
        config.put(DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, SchedulingPriorityStrategy);
        config.put(DaemonConfig.RESOURCE_AWARE_SCHEDULER_MAX_TOPOLOGY_SCHEDULING_ATTEMPTS, 2);    // allow 1 round of evictions

        return config;
    }

    private Cluster mkTestCluster(Topologies topologies, Config config) {
        INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();

        Map<String, Double> genericResourcesOfferedMap = (Map<String, Double>) config.get(Config.TOPOLOGY_COMPONENT_RESOURCES_MAP);
        if (genericResourcesOfferedMap == null || genericResourcesOfferedMap.isEmpty()) {
            throw new IllegalArgumentException("Generic resources map must contain something in this test: "
                    + TestGenericResourceAwareSchedulingPriorityStrategy.class.getName());
        }
        Map<String, SupervisorDetails> supMap = genSupervisors(4, 4, 100, 1000, genericResourcesOfferedMap);

        return new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);
    }

    private Set<String> collectMapValues(Map<String, Set<String>> map) {
        Set<String> set = new HashSet<>();
        map.values().forEach((s) -> set.addAll(s));
        return set;
    }
}
