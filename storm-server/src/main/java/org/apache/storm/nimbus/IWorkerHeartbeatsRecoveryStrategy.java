/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.nimbus;

import java.util.Map;
import java.util.Set;

/**
 * Interface for strategy to recover heartbeats when master gains leadership.
 */
public interface IWorkerHeartbeatsRecoveryStrategy {

    /**
     * Function to prepare the strategy.
     * @param conf config
     */
    void prepare(Map conf);

    /**
     * Function to decide if the heartbeats is ready.
     * @param nodeIds all the node ids from current physical plan[assignments], read from {@code ClusterState}
     * @return true if all node worker heartbeats reported
     */
    boolean isReady(Set<String> nodeIds);

    /**
     * report the node id to this strategy to help to decide {@code isReady}.
     * @param nodeId the node id from reported SupervisorWorkerHeartbeats
     */
    void reportNodeId(String nodeId);
}

