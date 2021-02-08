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

package org.apache.storm.daemon.nimbus;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.Time;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.fail;

public class NimbusTest {
    protected Class getDefaultResourceAwareStrategyClass() {
        return DefaultResourceAwareStrategy.class;
    }

    @Test
    public void testMemoryLoadLargerThanMaxHeapSize() throws Exception {
        // Topology will not be able to be successfully scheduled: Config TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB=128.0 < 129.0,
        // Largest memory requirement of a component in the topology).
        TopologyBuilder builder1 = new TopologyBuilder();
        builder1.setSpout("wordSpout1", new TestWordSpout(), 4);
        StormTopology stormTopology1 = builder1.createTopology();
        Config config1 = new Config();
        config1.put(Config.STORM_NETWORK_TOPOGRAPHY_PLUGIN, "org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping");
        config1.put(DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, DefaultSchedulingPriorityStrategy.class.getName());

        config1.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, getDefaultResourceAwareStrategyClass().getName());
        config1.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 10.0);
        config1.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 0.0);
        config1.put(Config.TOPOLOGY_PRIORITY, 0);
        config1.put(Config.TOPOLOGY_SUBMITTER_USER, "zhuo");
        config1.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 128.0);
        config1.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 129.0);
        try {
            ServerUtils.validateTopologyWorkerMaxHeapSizeConfigs(config1, stormTopology1, 768.0);
            fail("Expected exception not thrown");
        } catch (InvalidTopologyException e) {
            //Expected...
        }
    }

    @Test
    public void uploadedBlobPersistsMinimumTime() {
        Set<String> idleTopologies = new HashSet<>();
        idleTopologies.add("topology1");
        Map<String, Object> conf = new HashMap<>();
        conf.put(DaemonConfig.NIMBUS_TOPOLOGY_BLOBSTORE_DELETION_DELAY_MS, 300000);

        try (Time.SimulatedTime t = new Time.SimulatedTime(null)) {
            Set<String> toDelete = Nimbus.getExpiredTopologyIds(idleTopologies, conf);
            Assert.assertTrue(toDelete.isEmpty());

            Time.advanceTime(10 * 60 * 1000L);

            toDelete = Nimbus.getExpiredTopologyIds(idleTopologies, conf);
            Assert.assertTrue(toDelete.contains("topology1"));
            Assert.assertEquals(1, toDelete.size());

        }
    }
}
