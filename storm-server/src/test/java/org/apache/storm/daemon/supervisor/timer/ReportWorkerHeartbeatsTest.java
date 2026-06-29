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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
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
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ReportWorkerHeartbeatsTest {

    private static final int WORKER_TIMEOUT_SECS = 30;
    private static final int WORKER_MAX_TIMEOUT_SECS = 600;
    private static final String SUPERVISOR_ID = "test-supervisor";

    private ConfigUtils previousConfigUtils;
    private ConfigUtils mockConfigUtils;

    @BeforeEach
    public void setUp() throws Exception {
        mockConfigUtils = mock(ConfigUtils.class);
        previousConfigUtils = ConfigUtils.setInstance(mockConfigUtils);
        // Default: no per-topology conf override, so the reporter uses the global worker timeout.
        when(mockConfigUtils.readSupervisorStormConfImpl(any(), any())).thenReturn(new HashMap<>());
    }

    @AfterEach
    public void tearDown() {
        ConfigUtils.setInstance(previousConfigUtils);
    }

    private static LSWorkerHeartbeat mkHeartbeat(String topologyId, long timeSecs) {
        LSWorkerHeartbeat hb = new LSWorkerHeartbeat();
        hb.set_topology_id(topologyId);
        hb.set_time_secs(timeSecs);
        return hb;
    }

    private static Map<String, Object> topoConf(String key, Object value) {
        Map<String, Object> conf = new HashMap<>();
        conf.put(key, value);
        return conf;
    }

    private ReportWorkerHeartbeats mkReporter() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.SUPERVISOR_WORKER_TIMEOUT_SECS, WORKER_TIMEOUT_SECS);
        conf.put(Config.WORKER_MAX_TIMEOUT_SECS, WORKER_MAX_TIMEOUT_SECS);
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

    @Test
    public void perTopologyTimeoutOverrideExtendsTheStaleThreshold() throws Exception {
        // topo-long raises its own worker timeout well above the global one, mirroring Slot.getHbTimeoutMs.
        when(mockConfigUtils.readSupervisorStormConfImpl(any(), eq("topo-long")))
            .thenReturn(topoConf(Config.TOPOLOGY_WORKER_TIMEOUT_SECS, 300));

        try (Time.SimulatedTime ignored = new Time.SimulatedTime()) {
            Time.advanceTimeSecs(100_000);
            long now = Time.currentTimeSecsLong();

            Map<String, LSWorkerHeartbeat> local = new LinkedHashMap<>();
            // stale under the global 30s timeout, but still alive under the 300s override
            local.put("w-long", mkHeartbeat("topo-long", now - 100));
            // same age, no override: already dead under the global timeout -> filtered out
            local.put("w-default", mkHeartbeat("topo-default", now - 100));

            SupervisorWorkerHeartbeats result = mkReporter().getSupervisorWorkerHeartbeatsFromLocal(local);

            assertEquals(Set.of("topo-long"), reportedTopologies(result));
        }
    }

    @Test
    public void perTopologyTimeoutBelowGlobalKeepsTheGlobalThreshold() throws Exception {
        // An override smaller than the global timeout must not shrink the effective timeout below the
        // global one: effectiveWorkerTimeoutSecs takes max(global, override), matching Slot.getHbTimeoutMs.
        when(mockConfigUtils.readSupervisorStormConfImpl(any(), eq("topo-small")))
            .thenReturn(topoConf(Config.TOPOLOGY_WORKER_TIMEOUT_SECS, 10));

        try (Time.SimulatedTime ignored = new Time.SimulatedTime()) {
            Time.advanceTimeSecs(100_000);
            long now = Time.currentTimeSecsLong();

            Map<String, LSWorkerHeartbeat> local = new LinkedHashMap<>();
            // age 20s: stale under the 10s override, but still alive under the global 30s floor
            local.put("w-small", mkHeartbeat("topo-small", now - 20));

            SupervisorWorkerHeartbeats result = mkReporter().getSupervisorWorkerHeartbeatsFromLocal(local);

            assertEquals(Set.of("topo-small"), reportedTopologies(result));
        }
    }

    @Test
    public void topologyConfIsReadOncePerRoundForMultipleWorkers() throws Exception {
        // Multiple workers of the same topology on this supervisor must share a single conf read per round.
        when(mockConfigUtils.readSupervisorStormConfImpl(any(), eq("topo-shared")))
            .thenReturn(topoConf(Config.TOPOLOGY_WORKER_TIMEOUT_SECS, 300));

        try (Time.SimulatedTime ignored = new Time.SimulatedTime()) {
            Time.advanceTimeSecs(100_000);
            long now = Time.currentTimeSecsLong();

            Map<String, LSWorkerHeartbeat> local = new LinkedHashMap<>();
            local.put("w1", mkHeartbeat("topo-shared", now));
            local.put("w2", mkHeartbeat("topo-shared", now));
            local.put("w3", mkHeartbeat("topo-shared", now));

            mkReporter().getSupervisorWorkerHeartbeatsFromLocal(local);

            verify(mockConfigUtils, times(1)).readSupervisorStormConfImpl(any(), eq("topo-shared"));
        }
    }

    @Test
    public void perTopologyTimeoutIsCappedByWorkerMaxTimeout() throws Exception {
        // An override beyond the cap must clamp to WORKER_MAX_TIMEOUT_SECS, just like Nimbus does at submission.
        when(mockConfigUtils.readSupervisorStormConfImpl(any(), eq("topo-huge-stale")))
            .thenReturn(topoConf(Config.TOPOLOGY_WORKER_TIMEOUT_SECS, WORKER_MAX_TIMEOUT_SECS * 100));
        when(mockConfigUtils.readSupervisorStormConfImpl(any(), eq("topo-huge-fresh")))
            .thenReturn(topoConf(Config.TOPOLOGY_WORKER_TIMEOUT_SECS, WORKER_MAX_TIMEOUT_SECS * 100));

        try (Time.SimulatedTime ignored = new Time.SimulatedTime()) {
            Time.advanceTimeSecs(1_000_000);
            long now = Time.currentTimeSecsLong();

            Map<String, LSWorkerHeartbeat> local = new LinkedHashMap<>();
            // just past the cap: stale despite the huge override
            local.put("w-stale", mkHeartbeat("topo-huge-stale", now - WORKER_MAX_TIMEOUT_SECS - 1));
            // just within the cap: still alive
            local.put("w-fresh", mkHeartbeat("topo-huge-fresh", now - WORKER_MAX_TIMEOUT_SECS + 1));

            SupervisorWorkerHeartbeats result = mkReporter().getSupervisorWorkerHeartbeatsFromLocal(local);

            assertEquals(Set.of("topo-huge-fresh"), reportedTopologies(result));
        }
    }

    @Test
    public void unreadableTopologyConfFallsBackToGlobalTimeout() throws Exception {
        // Orphaned worker dirs often outlive their topology conf; a read failure must fall back to the
        // global timeout rather than reporting a dead worker indefinitely.
        when(mockConfigUtils.readSupervisorStormConfImpl(any(), eq("topo-orphan-fresh")))
            .thenThrow(new IOException("topology conf gone"));
        when(mockConfigUtils.readSupervisorStormConfImpl(any(), eq("topo-orphan-stale")))
            .thenThrow(new IOException("topology conf gone"));

        try (Time.SimulatedTime ignored = new Time.SimulatedTime()) {
            Time.advanceTimeSecs(100_000);
            long now = Time.currentTimeSecsLong();

            Map<String, LSWorkerHeartbeat> local = new LinkedHashMap<>();
            // alive under the fallback global timeout
            local.put("w-fresh", mkHeartbeat("topo-orphan-fresh", now - WORKER_TIMEOUT_SECS + 1));
            // stale under the fallback global timeout -> filtered out
            local.put("w-stale", mkHeartbeat("topo-orphan-stale", now - WORKER_TIMEOUT_SECS - 1));

            SupervisorWorkerHeartbeats result = mkReporter().getSupervisorWorkerHeartbeatsFromLocal(local);

            assertEquals(Set.of("topo-orphan-fresh"), reportedTopologies(result));
        }
    }
}
