/*
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

package org.apache.storm.daemon.supervisor.timer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.storm.Config;
import org.apache.storm.daemon.supervisor.Supervisor;
import org.apache.storm.generated.LSWorkerHeartbeat;
import org.apache.storm.generated.SupervisorWorkerHeartbeat;
import org.apache.storm.generated.SupervisorWorkerHeartbeats;
import org.apache.storm.utils.Time;
import org.junit.jupiter.api.Test;

public class ReportWorkerHeartbeatsTest {

    private static final int WORKER_TIMEOUT_SECS = 30;
    private static final String SUPERVISOR_ID = "test-supervisor";

    private static LSWorkerHeartbeat mkHeartbeat(String topologyId, long timeSecs) {
        LSWorkerHeartbeat hb = new LSWorkerHeartbeat();
        hb.set_topology_id(topologyId);
        hb.set_time_secs(timeSecs);
        return hb;
    }

    private ReportWorkerHeartbeats mkReporter() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.SUPERVISOR_WORKER_TIMEOUT_SECS, WORKER_TIMEOUT_SECS);
        Supervisor supervisor = mock(Supervisor.class);
        when(supervisor.getId()).thenReturn(SUPERVISOR_ID);
        return new ReportWorkerHeartbeats(conf, supervisor);
    }

    private static Set<String> reportedTopologies(SupervisorWorkerHeartbeats heartbeats) {
        return heartbeats.get_worker_heartbeats().stream()
            .map(SupervisorWorkerHeartbeat::get_storm_id)
            .collect(Collectors.toSet());
    }

    @Test
    public void freshHeartbeatsAreReportedAndStaleOnesAreFilteredOut() {
        try (Time.SimulatedTime ignored = new Time.SimulatedTime()) {
            Time.advanceTimeSecs(100_000);
            long now = Time.currentTimeSecsLong();

            Map<String, LSWorkerHeartbeat> local = new LinkedHashMap<>();
            // live worker: heartbeat just refreshed
            local.put("w-fresh", mkHeartbeat("topo-fresh", now));
            // exactly at the timeout boundary is still considered alive (age == timeout, not > timeout)
            local.put("w-boundary", mkHeartbeat("topo-boundary", now - WORKER_TIMEOUT_SECS));
            // just past the timeout: the worker is already considered dead
            local.put("w-stale", mkHeartbeat("topo-stale", now - WORKER_TIMEOUT_SECS - 1));
            // orphaned worker directory left behind long ago for a topology that no longer exists
            local.put("w-orphan", mkHeartbeat("topo-orphan", now - 86_400));

            SupervisorWorkerHeartbeats result = mkReporter().getSupervisorWorkerHeartbeatsFromLocal(local);

            assertEquals(SUPERVISOR_ID, result.get_supervisor_id());
            assertEquals(Set.of("topo-fresh", "topo-boundary"), reportedTopologies(result));
        }
    }

    @Test
    public void nullLocalHeartbeatsAreSkipped() {
        try (Time.SimulatedTime ignored = new Time.SimulatedTime()) {
            Time.advanceTimeSecs(100_000);
            long now = Time.currentTimeSecsLong();

            Map<String, LSWorkerHeartbeat> local = new LinkedHashMap<>();
            local.put("w-null", null);
            local.put("w-fresh", mkHeartbeat("topo-fresh", now));

            SupervisorWorkerHeartbeats result = mkReporter().getSupervisorWorkerHeartbeatsFromLocal(local);

            assertEquals(Set.of("topo-fresh"), reportedTopologies(result));
        }
    }
}
