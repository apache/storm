/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.nimbus;

import static java.util.stream.Collectors.toSet;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.storm.Config;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wait for a node to report worker heartbeats until a configured timeout. For cases below we have strategies:
 *
 * <p>1: When nimbus gains leader ship, it will decide if the heartbeats are ready based on the reported node ids,
 * supervisors/nodes will take care of the worker heartbeats recovery, a reported node id means all the workers
 * heartbeats on the node are reported.
 *
 * <p>2: If several supervisor also crush and will never recover[or all crush for some unknown reason],
 * workers will report their heartbeats directly to master, so it has not any effect.
 */
public class TimeOutWorkerHeartbeatsRecoveryStrategy implements IWorkerHeartbeatsRecoveryStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(TimeOutWorkerHeartbeatsRecoveryStrategy.class);

    private static int NODE_MAX_TIMEOUT_SECS = 600;

    private long startTimeSecs;

    private Set<String> reportedIds;

    @Override
    public void prepare(Map conf) {
        NODE_MAX_TIMEOUT_SECS = ObjectReader.getInt(conf.get(Config.SUPERVISOR_WORKER_HEARTBEATS_MAX_TIMEOUT_SECS), 600);
        this.startTimeSecs = Time.currentTimeMillis() / 1000L;
        this.reportedIds = new HashSet<>();
    }

    @Override
    public boolean isReady(Set<String> nodeIds) {
        if (exceedsMaxTimeOut()) {
            Set<String> tmp = nodeIds.stream().filter(id -> !this.reportedIds.contains(id)).collect(toSet());
            LOG.warn("Failed to recover heartbeats for nodes: {} with timeout {}s", tmp, NODE_MAX_TIMEOUT_SECS);
            return true;
        }

        return nodeIds.stream().allMatch(id -> this.reportedIds.contains(id));
    }

    @Override
    public void reportNodeId(String nodeId) {
        this.reportedIds.add(nodeId);
    }

    private boolean exceedsMaxTimeOut() {
        return (Time.currentTimeMillis() / 1000L - this.startTimeSecs) > NODE_MAX_TIMEOUT_SECS;
    }

}
