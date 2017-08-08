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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.NodeInfo;

public interface ITaskNetworkDistanceCalculator {

    /**
     * Prepare before doing any calculation.
     * @param conf The configuration
     * @param stormClusterState The storm cluster state
     */
    void prepare(Map<String, Object> conf, IStormClusterState stormClusterState);

    /**
     * Calculate or update taskNetworkDistance.
     * @param taskToNodePort The map from taskId to NodeInfo
     * @param taskNetworkDistance The map from taskId to the map from targetTaskId to distance
     */
    void calculateOrUpdate(Map<Integer, NodeInfo> taskToNodePort,
                                  ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Double>> taskNetworkDistance);

}
