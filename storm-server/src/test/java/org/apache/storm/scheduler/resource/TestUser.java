/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.scheduler.resource;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.INimbusTest;
import org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy;
import org.apache.storm.utils.Time;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.genSupervisors;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.genTopology;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.toDouble;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.userRes;
import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.userResourcePool;

import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.resource.normalization.ResourceMetrics;

public class TestUser {
    private static final Logger LOG = LoggerFactory.getLogger(TestUser.class);

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
    public void testResourcePoolUtilization() {
        INimbus iNimbus = new INimbusTest();
        Map<String, SupervisorDetails> supMap = genSupervisors(4, 4, 100, 1000);
        Double cpuGuarantee = 400.0;
        Double memoryGuarantee = 1000.0;
        Map<String, Map<String, Number>> resourceUserPool = userResourcePool(
            userRes("user1", cpuGuarantee, memoryGuarantee));
        Config config = createClusterConfig(100, 200, 200, resourceUserPool);
        TopologyDetails topo1 = genTopology("topo-1", config, 1, 1, 2, 1, Time.currentTimeSecs() - 24, 9, "user1");
        Topologies topologies = new Topologies(topo1);

        Cluster cluster = new Cluster(iNimbus, new ResourceMetrics(new StormMetricsRegistry()), supMap, new HashMap<>(), topologies, config);
        User user1 = new User("user1", toDouble(resourceUserPool.get("user1")));
        WorkerSlot slot = cluster.getAvailableSlots().get(0);
        cluster.assign(slot, topo1.getId(), topo1.getExecutors());

        Assert.assertEquals("check cpu resource guarantee", cpuGuarantee, user1.getCpuResourceGuaranteed(), 0.001);
        Assert.assertEquals("check memory resource guarantee", memoryGuarantee, user1.getMemoryResourceGuaranteed(), 0.001);

        Assert.assertEquals("check cpu resource pool utilization", ((100.0 * 3.0) / cpuGuarantee),
                            user1.getCpuResourcePoolUtilization(cluster), 0.001);
        Assert.assertEquals("check memory resource pool utilization", ((200.0 + 200.0) * 3.0) / memoryGuarantee,
                            user1.getMemoryResourcePoolUtilization(cluster), 0.001);
    }
}
