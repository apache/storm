/**
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
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.resource.ResourceAwareScheduler;
import org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler;
import org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy;
import org.apache.storm.utils.Time;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.*;

import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.resource.normalization.ResourceMetrics;

public class TestFIFOSchedulingPriorityStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(TestFIFOSchedulingPriorityStrategy.class);

    protected Class getDefaultResourceAwareStrategyClass() {
        return DefaultResourceAwareStrategy.class;
    }

    private Config createClusterConfig(double compPcore, double compOnHeap, double compOffHeap,
                                             Map<String, Map<String, Number>> pools) {
        Config config = TestUtilsForResourceAwareScheduler.createClusterConfig(compPcore, compOnHeap, compOffHeap, pools);
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, getDefaultResourceAwareStrategyClass().getName());
        return config;
    }

    @Test
    public void testFIFOEvictionStrategy() {
        try (Time.SimulatedTime sim = new Time.SimulatedTime()) {
            INimbus iNimbus = new INimbusTest();
            Map<String, SupervisorDetails> supMap = genSupervisors(4, 4, 100.0, 1000.0);
            Map<String, Map<String, Number>> resourceUserPool = userResourcePool(
                userRes("jerry", 200.0, 2000.0));
            Config config = createClusterConfig(100, 500, 500, resourceUserPool);
            config.put(DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, FIFOSchedulingPriorityStrategy.class.getName());

            Topologies topologies = new Topologies(
                genTopology("topo-1-jerry", config, 1, 0, 1, 0, Time.currentTimeSecs() - 250, 20, "jerry"),
                genTopology("topo-2-bobby", config, 1, 0, 1, 0, Time.currentTimeSecs() - 200, 10, "bobby"),
                genTopology("topo-3-bobby", config, 1, 0, 1, 0, Time.currentTimeSecs() - 300, 20, "bobby"),
                genTopology("topo-4-derek", config, 1, 0, 1, 0, Time.currentTimeSecs() - 201, 29, "derek"));
            Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);

            ResourceAwareScheduler rs = new ResourceAwareScheduler();
            rs.prepare(config, new StormMetricsRegistry());
            try {
                rs.schedule(topologies, cluster);

                assertTopologiesFullyScheduled(cluster, "topo-1-jerry", "topo-2-bobby", "topo-3-bobby", "topo-4-derek");

                LOG.info("\n\n\t\tINSERTING topo-5");
                //new topology needs to be scheduled
                //topo-3 should be evicted since its been up the longest
                topologies = addTopologies(topologies,
                    genTopology("topo-5-derek", config, 1, 0, 1, 0, Time.currentTimeSecs() - 15, 29, "derek"));

                cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);
                rs.schedule(topologies, cluster);

                assertTopologiesFullyScheduled(cluster, "topo-1-jerry", "topo-2-bobby", "topo-4-derek", "topo-5-derek");
                assertTopologiesNotScheduled(cluster, "topo-3-bobby");

                LOG.info("\n\n\t\tINSERTING topo-6");
                //new topology needs to be scheduled.  topo-4 should be evicted. Even though topo-1 from user jerry is older, topo-1 will not be evicted
                //since user jerry has enough resource guarantee
                topologies = addTopologies(topologies,
                    genTopology("topo-6-bobby", config, 1, 0, 1, 0, Time.currentTimeSecs() - 10, 29, "bobby"));

                cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);
                rs.schedule(topologies, cluster);

                assertTopologiesFullyScheduled(cluster, "topo-1-jerry", "topo-2-bobby", "topo-5-derek", "topo-6-bobby");
                assertTopologiesNotScheduled(cluster, "topo-3-bobby", "topo-4-derek");
            } finally {
                rs.cleanup();
            }
        }
    }
}
