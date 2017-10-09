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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.storm.Config;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.networktopography.DNSToSwitchMapping;
import org.apache.storm.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This calculator calculates the network distance between tasks based on their physical locations:
 * WORKER_LOCAL, HOST_LOCAL, RACK_LOCAL, UNKNOWN.
 */
public class DefaultTaskNetworkDistanceCalculator implements ITaskNetworkDistanceCalculator {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultTaskNetworkDistanceCalculator.class);

    private DNSToSwitchMapping dnsToSwitchMapping;
    private Map<Integer, Set<Integer>> localTaskToOutboundTasks;
    private NodeInfo localNodeInfo;

    public void prepare(Map<String, Object> conf, IStormClusterState stormClusterState,
                        NodeInfo localNodeInfo, Map<Integer, Set<Integer>> localTaskToOutboundTasks) {
        dnsToSwitchMapping = ReflectionUtils.newInstance((String) conf.get(Config.STORM_NETWORK_TOPOGRAPHY_PLUGIN));
        this.localTaskToOutboundTasks = localTaskToOutboundTasks;
        this.localNodeInfo = localNodeInfo;
    }

    public void calculateOrUpdate(Map<Integer, NodeInfo> taskToNodePort,
                                  ConcurrentMap<Integer, ConcurrentMap<Integer, Double>> taskNetworkDistance) {

        Map<String, String> hostToRack = getHostToRackMapping(taskToNodePort);

        for (Map.Entry<Integer, Set<Integer>> entry: localTaskToOutboundTasks.entrySet()) {
            Integer sourceTaskId = entry.getKey();
            Set<Integer> outboundTasks = entry.getValue();

            ConcurrentMap<Integer, Double> targetTaskDistances;
            if (!taskNetworkDistance.containsKey(sourceTaskId)) {
                targetTaskDistances = new ConcurrentHashMap<>();
                taskNetworkDistance.put(sourceTaskId, targetTaskDistances);
            } else {
                targetTaskDistances = taskNetworkDistance.get(sourceTaskId);
            }

            for (Integer targetTaskId: outboundTasks) {
                NodeInfo targetNodeInfo = taskToNodePort.get(targetTaskId);
                double dist = calculateDistance(this.localNodeInfo, targetNodeInfo, hostToRack).distance();
                targetTaskDistances.put(targetTaskId, dist);

                LOG.debug("Source: taskId: {}; nodePort: {}", sourceTaskId, localNodeInfo);
                LOG.debug("Target: taskId: {}; nodePort: {}", targetTaskId, taskToNodePort.get(targetTaskId));
                LOG.debug("SourceTask: {}; TargetTask: {}; Distance: {}", sourceTaskId, targetTaskId, dist);
            }
        }
    }

    private DistanceType calculateDistance(NodeInfo source, NodeInfo target, Map<String, String> hostToRack) {
        if (source == null || target == null) {
            return DistanceType.UNKNOWN;
        }

        String sourceRack = hostToRack.get(source.get_node());
        String targetRack = hostToRack.get(target.get_node());

        if(sourceRack != null && targetRack != null && sourceRack.equals(targetRack)) {
            if(source.get_node().equals(target.get_node())) {
                if(source.get_port().equals(target.get_port())) {
                    return DistanceType.WORKER_LOCAL;
                }
                return DistanceType.HOST_LOCAL;
            }
            return DistanceType.RACK_LOCAL;
        } else {
            return DistanceType.UNKNOWN;
        }
    }

    private Map<String, String> getHostToRackMapping(Map<Integer, NodeInfo> taskToNodePort) {
        Set<String> hosts = new HashSet();

        for (Map.Entry<Integer, NodeInfo> entry: taskToNodePort.entrySet()) {
            hosts.add(entry.getValue().get_node());
        }

        return dnsToSwitchMapping.resolve(new ArrayList<>(hosts));
    }

    /**
     * Hard coded distance between tasks.
     */
    enum DistanceType {
        WORKER_LOCAL(0), // tasks in the same worker
        HOST_LOCAL(10),  // tasks in the same host
        RACK_LOCAL(50),  // tasks in the same rack
        UNKNOWN(100);    // everything else

        private final double distance;
        DistanceType(double distance) {
            this.distance = distance;
        }

        public double distance() {
            return distance;
        }
    }
}
