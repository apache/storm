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

package org.apache.storm;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.scheduler.resource.ResourceAwareScheduler;
import org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler;
import org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.SpoutDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRebalance {

    static final int SLEEP_TIME_BETWEEN_RETRY = 1000;

    private static final Logger LOG = LoggerFactory.getLogger(TestRebalance.class);

    public static String topoNameToId(String topoName, ILocalCluster cluster) throws TException {
        try {
            TopologySummary topoSum = cluster.getTopologySummaryByName(topoName);
            return topoSum.get_id();
        } catch (NotAliveException e) {
            LOG.error("Failed to getTopologySummaryByName from " + topoName, e);
        }
        return null;
    }

    protected Class getDefaultResourceAwareStrategyClass() {
        return DefaultResourceAwareStrategy.class;
    }

    @Test
    public void testRebalanceTopologyResourcesAndConfigs()
        throws Exception {

        LOG.info("Starting local cluster...");

        Config conf = new Config();
        conf.put(DaemonConfig.STORM_SCHEDULER, ResourceAwareScheduler.class.getName());
        conf.put(DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, DefaultSchedulingPriorityStrategy.class.getName());
        conf.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, getDefaultResourceAwareStrategyClass().getName());
        conf.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 10.0);
        conf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 10.0);
        conf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 100.0);
        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, Double.MAX_VALUE);
        Map<String, Double> resourcesMap = new HashMap();
        resourcesMap.put("gpu.count", 5.0);
        conf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_MAP, resourcesMap);

        try (ILocalCluster cluster = new LocalCluster.Builder().withDaemonConf(conf).build()) {

            TopologyBuilder builder = new TopologyBuilder();
            SpoutDeclarer s1 = builder.setSpout("spout-1", new TestUtilsForResourceAwareScheduler.TestSpout(),
                                                2);
            BoltDeclarer b1 = builder.setBolt("bolt-1", new TestUtilsForResourceAwareScheduler.TestBolt(),
                                              2).shuffleGrouping("spout-1");
            BoltDeclarer b2 = builder.setBolt("bolt-2", new TestUtilsForResourceAwareScheduler.TestBolt(),
                                              2).shuffleGrouping("bolt-1");

            StormTopology stormTopology = builder.createTopology();

            LOG.info("submitting topologies....");
            String topoName = "topo1";
            cluster.submitTopology(topoName, new HashMap<>(), stormTopology);

            waitTopologyScheduled(topoName, cluster, 20);

            RebalanceOptions opts = new RebalanceOptions();

            Map<String, Map<String, Double>> resources = new HashMap<String, Map<String, Double>>();
            resources.put("spout-1", new HashMap<String, Double>());
            resources.get("spout-1").put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 120.0);
            resources.get("spout-1").put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 25.0);
            resources.get("spout-1").put("gpu.count", 5.0);

            opts.set_topology_resources_overrides(resources);
            opts.set_wait_secs(0);

            JSONObject jsonObject = new JSONObject();
            jsonObject.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 768.0);

            opts.set_topology_conf_overrides(jsonObject.toJSONString());

            LOG.info("rebalancing....");
            cluster.rebalance("topo1", opts);

            waitTopologyScheduled(topoName, cluster, 10);

            boolean topologyUpdated = false;
            JSONParser parser = new JSONParser();

            for (int i = 0; i < 5; i++) {
                Utils.sleep(SLEEP_TIME_BETWEEN_RETRY);

                String confRaw = cluster.getTopologyConf(topoNameToId(topoName, cluster));


                JSONObject readConf = (JSONObject) parser.parse(confRaw);
                if (768.0 == (double) readConf.get(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB)) {
                    topologyUpdated = true;
                    break;
                }
            }

            StormTopology readStormTopology = cluster.getTopology(topoNameToId(topoName, cluster));
            String componentConfRaw = readStormTopology.get_spouts().get("spout-1").get_common().get_json_conf();

            JSONObject readTopologyConf = (JSONObject) parser.parse(componentConfRaw);

            Map<String, Double> componentResources = (Map<String, Double>) readTopologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_MAP);
            assertTrue("Topology has been updated", topologyUpdated);
            assertEquals("Updated CPU correct", 25.0, componentResources.get(Constants.COMMON_CPU_RESOURCE_NAME), 0.001);
            assertEquals("Updated Memory correct", 120.0, componentResources.get(Constants.COMMON_ONHEAP_MEMORY_RESOURCE_NAME), 0.001);
            assertEquals("Updated Generic resource correct", 5.0, componentResources.get("gpu.count"), 0.001);
        }
    }

    public void waitTopologyScheduled(String topoName, ILocalCluster cluster, int retryAttempts) throws TException {
        for (int i = 0; i < retryAttempts; i++) {
            if (checkTopologyScheduled(topoName, cluster)) {
                //sleep to prevent race conditions
                Utils.sleep(SLEEP_TIME_BETWEEN_RETRY);
                return;
            }
            Utils.sleep(SLEEP_TIME_BETWEEN_RETRY);
        }
        throw new RuntimeException("Error: Wait for topology " + topoName + " to be ACTIVE has timed out!");
    }

    public boolean checkTopologyScheduled(String topoName, ILocalCluster cluster) throws TException {
        if (checkTopologyUp(topoName, cluster)) {
            TopologySummary topoSum = cluster.getTopologySummaryByName(topoName);
            String status = topoSum.get_status();
            String sched_status = topoSum.get_sched_status();
            if (status.equals("ACTIVE") && (sched_status != null && !sched_status.equals(""))) {
                return true;
            }
        }
        return false;
    }

    public boolean checkTopologyUp(String topoName, ILocalCluster cluster) throws TException {
        ClusterSummary sum = cluster.getClusterInfo();
        TopologySummary topoSum = cluster.getTopologySummaryByName(topoName);
            if (topoSum != null) {
                return true;
            }
        return false;
    }
}
