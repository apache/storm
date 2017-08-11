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

package org.apache.storm.task;

import java.util.Arrays;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.PatternSyntaxException;
import com.google.common.collect.Sets;
import org.apache.storm.Config;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.networktopography.DNSToSwitchMapping;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DefaultTaskNetworkDistanceCalculatorTest {

    private ITaskNetworkDistanceCalculator iTaskNetworkDistanceCalculator;
    private Map<Integer, Set<Integer>> localTaskToOutboundTasks;
    @Before
    public void initializeAndPrepare() {
        iTaskNetworkDistanceCalculator = new DefaultTaskNetworkDistanceCalculator();
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_NETWORK_TOPOGRAPHY_PLUGIN, DNSToSwitchMappingMock.class.getName());
        localTaskToOutboundTasks = new HashMap<>();
        //mock localTask to outbound task
        localTaskToOutboundTasks.put(1, Sets.newHashSet(2,3,4,5));
        iTaskNetworkDistanceCalculator.prepare(conf, null,
                new NodeInfo("rack1_node1", new HashSet<>(Arrays.asList(6700L))), localTaskToOutboundTasks);
    }

    @Test
    public void testCalculateOrUpdate() throws Exception {
        Map<Integer, NodeInfo> taskToNodePort = new HashMap<>();
        //task on the same worker
        taskToNodePort.put(2, new NodeInfo("rack1_node1", new HashSet<>(Arrays.asList(6700L))));
        //task on the same node, different worker
        taskToNodePort.put(3, new NodeInfo("rack1_node1", new HashSet<>(Arrays.asList(6701L))));
        //task on the same rack, different node
        taskToNodePort.put(4, new NodeInfo("rack1_node2", new HashSet<>(Arrays.asList(6701L))));
        //task on different rack
        taskToNodePort.put(5, new NodeInfo("rack2_node3", new HashSet<>(Arrays.asList(6702L))));

        ConcurrentMap<Integer, ConcurrentMap<Integer, Double>> taskNetworkDistance = new ConcurrentHashMap<>();

        iTaskNetworkDistanceCalculator.calculateOrUpdate(taskToNodePort, taskNetworkDistance);

        Assert.assertTrue(taskNetworkDistance.get(1).get(2) ==
                DefaultTaskNetworkDistanceCalculator.DistanceType.WORKER_LOCAL.distance());
        Assert.assertTrue(taskNetworkDistance.get(1).get(3) ==
                DefaultTaskNetworkDistanceCalculator.DistanceType.HOST_LOCAL.distance());
        Assert.assertTrue(taskNetworkDistance.get(1).get(4) ==
                DefaultTaskNetworkDistanceCalculator.DistanceType.RACK_LOCAL.distance());
        Assert.assertTrue(taskNetworkDistance.get(1).get(5) ==
                DefaultTaskNetworkDistanceCalculator.DistanceType.UNKNOWN.distance());

    }

    public static class DNSToSwitchMappingMock implements DNSToSwitchMapping {

        private final String UNKNOWN_RACK = "unknown_rack";

        /**
         * Identify the rack the node is on.
         * @param names the list of hosts to resolve (can be empty)
         * @return Map of hosts to resolved network paths
         */
        @Override
        public Map<String, String> resolve(List<String> names) {
            Map<String, String> ret = new HashMap<>();

            for (String hostName: names) {
                try {
                    ret.put(hostName, hostName.split("_")[0]);
                } catch (PatternSyntaxException e) {
                    ret.put(hostName, UNKNOWN_RACK);
                }
            }

            return ret;
        }
    }
}