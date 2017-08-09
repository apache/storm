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
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.resource.ResourceAwareScheduler;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.*;

public class TestDefaultEvictionStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(TestDefaultEvictionStrategy.class);
    private static int currentTime = 1450418597;
    
    /**
     * The resources in the cluster are limited. In the first round of scheduling, all resources in the cluster is used.
     * User jerry submits another topology.  Since user jerry has his resource guarantees satisfied, and user bobby
     * has exceeded his resource guarantee, topo-3 from user bobby should be evicted.
     */
    @Test
    public void testEviction() {
      INimbus iNimbus = new INimbusTest();
      Map<String, SupervisorDetails> supMap = genSupervisors(4, 4, 100, 1000);
        Map<String, Map<String, Number>> resourceUserPool = userResourcePool(
            userRes("jerry", 200, 2000),
            userRes("bobby", 100, 1000),
            userRes("derek", 200, 2000));
        Config config = createClusterConfig(100, 500, 500, resourceUserPool);
        Topologies topologies = new Topologies(
            genTopology("topo-1", config, 1, 0, 1, 0, currentTime - 2, 10, "jerry"),
            genTopology("topo-2", config, 1, 0, 1, 0, currentTime - 2, 10, "bobby"),
            genTopology("topo-3", config, 1, 0, 1, 0, currentTime - 2, 20, "bobby"),
            genTopology("topo-4", config, 1, 0, 1, 0, currentTime - 2, 29, "derek"));

        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<>(), topologies, config);
        ResourceAwareScheduler rs = new ResourceAwareScheduler();
        rs.prepare(config);
        rs.schedule(topologies, cluster);
        
        assertTopologiesFullyScheduled(cluster, "topo-1", "topo-2", "topo-3", "topo-4");

        //user jerry submits another topology
        topologies = addTopologies(topologies,
            genTopology("topo-6", config, 1, 0, 1, 0, currentTime - 2, 20, "jerry"));
        cluster = new Cluster(cluster, topologies);
        rs.schedule(topologies, cluster);
        
        //topo-3 evicted (lowest priority)
        assertTopologiesFullyScheduled(cluster, "topo-1", "topo-2", "topo-4", "topo-6");
        assertTopologiesNotScheduled(cluster, "topo-3");
    }

    @Test
    public void testEvictMultipleTopologies() {
        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(4, 4, 100, 1000);
        Map<String, Map<String, Number>> resourceUserPool = userResourcePool(
            userRes("jerry", 200, 2000),
            userRes("derek", 100, 1000));
        Config config = createClusterConfig(100, 500, 500, resourceUserPool);

        Topologies topologies = new Topologies(
            genTopology("topo-2", config, 1, 0, 1, 0, currentTime - 2, 10, "bobby"),
            genTopology("topo-3", config, 1, 0, 1, 0, currentTime - 2, 20, "bobby"),
            genTopology("topo-4", config, 1, 0, 1, 0, currentTime - 2, 29, "derek"),
            genTopology("topo-5", config, 1, 0, 1, 0, currentTime - 2, 29, "derek"));
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<>(), topologies, config);
        ResourceAwareScheduler rs = new ResourceAwareScheduler();
        rs.prepare(config);
        rs.schedule(topologies, cluster);

        assertTopologiesFullyScheduled(cluster, "topo-2", "topo-3", "topo-4", "topo-5");

        //user jerry submits another topology
        topologies = addTopologies(topologies,
            genTopology("topo-1", config, 2, 0, 1, 0, currentTime - 2, 10, "jerry"));
        cluster = new Cluster(cluster, topologies);
        rs.schedule(topologies, cluster);

        //bobby has no guarantee so topo-2 and topo-3 evicted
        assertTopologiesFullyScheduled(cluster, "topo-1", "topo-4", "topo-5");
        assertTopologiesNotScheduled(cluster, "topo-2", "topo-3");
    }

    @Test
    public void testEvictMultipleTopologiesFromMultipleUsersInCorrectOrder() {
        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(4, 4, 100, 1000);
        Map<String, Map<String, Number>> resourceUserPool = userResourcePool(
            userRes("jerry", 300, 3000),
            userRes("derek", 100, 1000));
        Config config = createClusterConfig(100, 500, 500, resourceUserPool);

        Topologies topologies = new Topologies(
            genTopology("topo-2", config, 1, 0, 1, 0, currentTime - 2, 10, "bobby"),
            genTopology("topo-3", config, 1, 0, 1, 0, currentTime - 2, 20, "bobby"),
            genTopology("topo-4", config, 1, 0, 1, 0, currentTime - 2, 29, "derek"),
            genTopology("topo-5", config, 1, 0, 1, 0, currentTime - 15, 29, "derek"));
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<>(), topologies, config);
        ResourceAwareScheduler rs = new ResourceAwareScheduler();
        rs.prepare(config);
        rs.schedule(topologies, cluster);
        
        assertTopologiesFullyScheduled(cluster, "topo-2", "topo-3", "topo-4", "topo-5");

        //user jerry submits another topology
        topologies = addTopologies(topologies,
            genTopology("topo-1", config, 1, 0, 1, 0, currentTime - 2, 10, "jerry"));
        cluster = new Cluster(cluster, topologies);
        rs.schedule(topologies, cluster);

        //topo-3 evicted since user bobby don't have any resource guarantees and topo-3 is the lowest priority for user bobby
        assertTopologiesFullyScheduled(cluster, "topo-1", "topo-2", "topo-4", "topo-5");
        assertTopologiesNotScheduled(cluster, "topo-3");
        
        topologies = addTopologies(topologies, 
            genTopology("topo-6", config, 1, 0, 1, 0, currentTime - 2, 10, "jerry"));
        cluster = new Cluster(cluster, topologies);
        rs.schedule(topologies, cluster);
        
        //topo-2 evicted since user bobby don't have any resource guarantees and topo-2 is the next lowest priority for user bobby
        assertTopologiesFullyScheduled(cluster, "topo-1", "topo-4", "topo-5");
        assertTopologiesNotScheduled(cluster, "topo-2", "topo-3");

        topologies = addTopologies(topologies,
            genTopology("topo-7", config, 1, 0, 1, 0, currentTime - 2, 10, "jerry"));
        cluster = new Cluster(cluster, topologies);
        rs.schedule(topologies, cluster);

        // since user derek has exceeded his resource guarantee while user jerry has not topo-5 or topo-4 could be evicted because they have the same priority
        // but topo-4 was submitted earlier thus we choose that one to evict (somewhat arbitrary)
        assertTopologiesFullyScheduled(cluster, "topo-1", "topo-5", "topo-7");
        assertTopologiesNotScheduled(cluster, "topo-2", "topo-3", "topo-4");
    }

    /**
     * If topologies from other users cannot be evicted to make space
     * check if there is a topology with lower priority that can be evicted from the current user
     */
    @Test
    public void testEvictTopologyFromItself() {
        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(4, 4, 100, 1000);
        Map<String, Map<String, Number>> resourceUserPool = userResourcePool(
            userRes("jerry", 200, 2000),
            userRes("bobby", 100, 1000),
            userRes("derek", 100, 1000));
        Config config = createClusterConfig(100, 500, 500, resourceUserPool);

        Topologies topologies = new Topologies(
            genTopology("topo-1", config, 1, 0, 1, 0, currentTime - 2, 20, "jerry"),
            genTopology("topo-2", config, 1, 0, 1, 0, currentTime - 2, 20, "jerry"),
            genTopology("topo-5", config, 1, 0, 1, 0, currentTime - 2, 10, "bobby"),
            genTopology("topo-6", config, 1, 0, 1, 0, currentTime - 2, 29, "derek"));
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<>(), topologies, config);
        ResourceAwareScheduler rs = new ResourceAwareScheduler();
        rs.prepare(config);
        rs.schedule(topologies, cluster);
        
        assertTopologiesFullyScheduled(cluster, "topo-1", "topo-2", "topo-5", "topo-6");

        //user jerry submits another topology into a full cluster
        // topo3 should not be able to scheduled
        topologies = addTopologies(topologies,
            genTopology("topo-3", config, 1, 0, 1, 0, currentTime - 2, 29, "jerry"));
        cluster = new Cluster(cluster, topologies);
        rs.schedule(topologies, cluster);

        assertTopologiesFullyScheduled(cluster, "topo-1", "topo-2", "topo-5", "topo-6");
        assertTopologiesNotScheduled(cluster, "topo-3");

        //user jerry submits another topology but this one should be scheduled since it has higher priority than than the
        //rest of jerry's running topologies
        topologies = addTopologies(topologies,
            genTopology("topo-4", config, 1, 0, 1, 0, currentTime - 2, 10, "jerry"));
        cluster = new Cluster(cluster, topologies);
        rs.schedule(topologies, cluster);
        
        assertTopologiesFullyScheduled(cluster, "topo-2", "topo-4", "topo-5", "topo-6");
        assertTopologiesNotScheduled(cluster, "topo-1", "topo-3");
    }

    /**
     * If users are above his or her guarantee, check if topology eviction works correct
     */
    @Test
    public void testOverGuaranteeEviction() {
        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(4, 4, 100, 1000);
        Map<String, Map<String, Number>> resourceUserPool = userResourcePool(
            userRes("jerry", 70, 700),
            userRes("bobby", 100, 1000),
            userRes("derek", 25, 250));
        Config config = createClusterConfig(100, 500, 500, resourceUserPool);

        Topologies topologies = new Topologies(
            genTopology("topo-1", config, 1, 0, 1, 0, currentTime - 2, 20, "jerry"),
            genTopology("topo-3", config, 1, 0, 1, 0, currentTime - 2, 10, "bobby"),
            genTopology("topo-4", config, 1, 0, 1, 0, currentTime - 2, 10, "bobby"),
            genTopology("topo-5", config, 1, 0, 1, 0, currentTime - 2, 29, "derek"));
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<>(), topologies, config);
        ResourceAwareScheduler rs = new ResourceAwareScheduler();
        rs.prepare(config);
        rs.schedule(topologies, cluster);
        
        assertTopologiesFullyScheduled(cluster, "topo-1", "topo-3", "topo-4", "topo-5");

        //user derek submits another topology into a full cluster
        //topo6 should not be able to scheduled initially, but since topo6 has higher priority than topo5
        //topo5 will be evicted so that topo6 can be scheduled
        topologies = addTopologies(topologies,
            genTopology("topo-6", config, 1, 0, 1, 0, currentTime - 2, 10, "derek"));
        cluster = new Cluster(cluster, topologies);
        rs.schedule(topologies, cluster);
        
        assertTopologiesFullyScheduled(cluster, "topo-1", "topo-3", "topo-4", "topo-6");
        assertTopologiesNotScheduled(cluster, "topo-5");

        //user jerry submits topo2
        topologies = addTopologies(topologies,
            genTopology("topo-2", config, 1, 0, 1, 0, currentTime - 2, 20, "jerry"));
        cluster = new Cluster(cluster, topologies);
        rs.schedule(topologies, cluster);
        
        assertTopologiesFullyScheduled(cluster, "topo-1", "topo-2", "topo-3", "topo-4");
        assertTopologiesNotScheduled(cluster, "topo-5", "topo-6");
    }
}
