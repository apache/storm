/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.nimbus;

import org.apache.storm.Config;
import org.apache.storm.utils.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Wait for a node to report worker heartbeats until a configured timeout. For cases below we have strategies:
 * <p>
 * 1: When nimbus gains leader ship, it will decide if the heartbeats are ready based on the reported node ids,
 * supervisors/nodes will take care of the worker heartbeats recovery, a reported node id means all the workers heartbeats on the node are reported.
 * <p>
 * 2: If several supervisor also crush and will never recover[or all crush for some unknown reason],
 * workers will report their heartbeats directly to master, so it has not any effect.
 */
public class TimeOutWorkerHeartbeatsRecoveryStrategy implements IWorkerHeartbeatsRecoveryStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(TimeOutWorkerHeartbeatsRecoveryStrategy.class);

    private static int NODE_MAX_TIMEOUT_SECS = 600;

    private long startTimeSecs;

    private Set<String> reportedIDs;

    @Override
    public void prepare(Map conf) {
        NODE_MAX_TIMEOUT_SECS = ObjectReader.getInt(conf.get(Config.SUPERVISOR_WORKER_HEARTBEATS_MAX_TIMEOUT_SECS), 600);
        this.startTimeSecs = System.currentTimeMillis() / 1000L;
        this.reportedIDs = new HashSet<>();
    }

    @Override
    public boolean isReady(Set<String> nodeIds) {
        if (isMaxTimeOut()) {
            HashSet<String> tmp = new HashSet<>();
            for(String nodeID : nodeIds) {
                if (!this.reportedIDs.contains(nodeID))
                tmp.add(nodeID);
            }
            LOG.warn("Failed to recover heartbeats for nodes: {} with timeout {}s", tmp, NODE_MAX_TIMEOUT_SECS);
            return true;
        }
        for (String nodeID : nodeIds) {
            if (this.reportedIDs.contains(nodeID)) {
                continue;
            } else {
                return false;
            }

        }

        return true;
    }

    @Override
    public void reportNodeId(String nodeId) {
        this.reportedIDs.add(nodeId);
    }

    private boolean isMaxTimeOut() {
        return (System.currentTimeMillis() / 1000L - this.startTimeSecs) > NODE_MAX_TIMEOUT_SECS;
    }

}
