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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.SupervisorWorkerHeartbeat;
import org.apache.storm.stats.ClientStatsUtil;
import org.apache.storm.utils.Time;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HeartbeatCacheTest {
    private static final String TOPO_ID = "test-topology-1";
    private static final int TIMEOUT_SECS = 30;

    @Test
    void testExecutorRemainsAliveWhenHeartbeatTimestampDoesNotAdvance() {
        try (Time.SimulatedTime ignored = new Time.SimulatedTime()) {
            HeartbeatCache cache = new HeartbeatCache();
            Set<List<Integer>> allExecutors = Collections.singleton(Arrays.asList(1, 1));
            Assignment assignment = mkAssignment(Time.currentTimeSecs(), 1, 1);

            // First heartbeat at t=0 with TIME_SECS=100
            cache.updateHeartbeat(mkWorkerHeartbeat(TOPO_ID, 100, 1, 1), TIMEOUT_SECS);

            // Advance time to just before timeout
            Time.advanceTimeSecs(TIMEOUT_SECS - 1);

            // Second heartbeat arrives but TIME_SECS is still 100 (same second, stats not rotated)
            cache.updateHeartbeat(mkWorkerHeartbeat(TOPO_ID, 100, 1, 1), TIMEOUT_SECS);

            // Advance just 2 more seconds: now t = TIMEOUT_SECS + 1, which is past the original
            // timeout window (rooted at t=0) but well within the refreshed window (rooted at t=TIMEOUT_SECS-1).
            Time.advanceTimeSecs(2);

            // Simulate the scheduling cycle timeout check
            cache.timeoutOldHeartbeats(TOPO_ID, TIMEOUT_SECS);

            // Executor should still be alive because a fresh heartbeat was received at t=(TIMEOUT_SECS-1)
            Set<List<Integer>> alive = cache.getAliveExecutors(TOPO_ID, allExecutors, assignment, TIMEOUT_SECS);
            assertFalse(alive.isEmpty(), "Executor should be alive after receiving a recent heartbeat even if TIME_SECS did not advance");
        }
    }

    @Test
    void testExecutorTimesOutWhenNoHeartbeatReceived() {
        try (Time.SimulatedTime ignored = new Time.SimulatedTime()) {
            HeartbeatCache cache = new HeartbeatCache();
            Set<List<Integer>> allExecutors = Collections.singleton(Arrays.asList(1, 1));
            Assignment assignment = mkAssignment(Time.currentTimeSecs(), 1, 1);

            // Single heartbeat at t=0
            cache.updateHeartbeat(mkWorkerHeartbeat(TOPO_ID, 100, 1, 1), TIMEOUT_SECS);

            // No more heartbeats — advance time past the timeout
            Time.advanceTimeSecs(TIMEOUT_SECS + 1);

            // Simulate the scheduling cycle timeout check
            cache.timeoutOldHeartbeats(TOPO_ID, TIMEOUT_SECS);

            Set<List<Integer>> alive = cache.getAliveExecutors(TOPO_ID, allExecutors, assignment, TIMEOUT_SECS);
            assertTrue(alive.isEmpty(), "Executor should be timed out after no heartbeat for longer than timeout");
        }
    }

    @Test
    void testExecutorAliveWithRegularHeartbeats() {
        try (Time.SimulatedTime ignored = new Time.SimulatedTime()) {
            HeartbeatCache cache = new HeartbeatCache();
            Set<List<Integer>> allExecutors = Collections.singleton(Arrays.asList(1, 1));
            Assignment assignment = mkAssignment(Time.currentTimeSecs(), 1, 1);

            // Send heartbeats every second for 60 seconds
            for (int t = 0; t < 60; t++) {
                cache.updateHeartbeat(mkWorkerHeartbeat(TOPO_ID, Time.currentTimeSecs(), 1, 1), TIMEOUT_SECS);
                Time.advanceTimeSecs(1);
            }

            // Simulate the scheduling cycle timeout check
            cache.timeoutOldHeartbeats(TOPO_ID, TIMEOUT_SECS);

            Set<List<Integer>> alive = cache.getAliveExecutors(TOPO_ID, allExecutors, assignment, TIMEOUT_SECS);
            assertFalse(alive.isEmpty(), "Executor should be alive when receiving regular heartbeats");
        }
    }

    @Test
    void testZkExecutorTimesOutWhenTimeSecsStopsAdvancing() {
        try (Time.SimulatedTime ignored = new Time.SimulatedTime()) {
            HeartbeatCache cache = new HeartbeatCache();
            Set<List<Integer>> allExecutors = Collections.singleton(Arrays.asList(1, 1));
            Assignment assignment = mkAssignment(Time.currentTimeSecs(), 1, 1);

            // Heartbeats with advancing TIME_SECS — executor is healthy
            for (int t = 0; t < 5; t++) {
                cache.updateFromZkHeartbeat(TOPO_ID, mkZkExecutorBeats(1, 1, t * 10), allExecutors, TIMEOUT_SECS);
                Time.advanceTimeSecs(1);
            }

            // TIME_SECS freezes — zombie executor keeps sending heartbeats but stats are stuck
            int frozenTimeSecs = 40;
            for (int t = 0; t < TIMEOUT_SECS + 1; t++) {
                cache.updateFromZkHeartbeat(TOPO_ID, mkZkExecutorBeats(1, 1, frozenTimeSecs), allExecutors, TIMEOUT_SECS);
                Time.advanceTimeSecs(1);
            }

            cache.timeoutOldHeartbeats(TOPO_ID, TIMEOUT_SECS);

            Set<List<Integer>> alive = cache.getAliveExecutors(TOPO_ID, allExecutors, assignment, TIMEOUT_SECS);
            assertTrue(alive.isEmpty(), "ZK executor should be timed out when TIME_SECS stops advancing (zombie detection)");
        }
    }

    @Test
    void testZkExecutorAliveWhenTimeSecsAdvances() {
        try (Time.SimulatedTime ignored = new Time.SimulatedTime()) {
            HeartbeatCache cache = new HeartbeatCache();
            Set<List<Integer>> allExecutors = Collections.singleton(Arrays.asList(1, 1));
            Assignment assignment = mkAssignment(Time.currentTimeSecs(), 1, 1);

            // Heartbeats with advancing TIME_SECS every second
            for (int t = 0; t < 60; t++) {
                cache.updateFromZkHeartbeat(TOPO_ID, mkZkExecutorBeats(1, 1, t), allExecutors, TIMEOUT_SECS);
                Time.advanceTimeSecs(1);
            }

            cache.timeoutOldHeartbeats(TOPO_ID, TIMEOUT_SECS);

            Set<List<Integer>> alive = cache.getAliveExecutors(TOPO_ID, allExecutors, assignment, TIMEOUT_SECS);
            assertFalse(alive.isEmpty(), "ZK executor should be alive when TIME_SECS advances regularly");
        }
    }

    private SupervisorWorkerHeartbeat mkWorkerHeartbeat(String topoId, int timeSecs, int... executors) {
        SupervisorWorkerHeartbeat hb = new SupervisorWorkerHeartbeat();
        hb.set_storm_id(topoId);
        hb.set_time_secs(timeSecs);
        for (int i = 0; i < executors.length - 1; i += 2) {
            ExecutorInfo info = new ExecutorInfo();
            info.set_task_start(executors[i]);
            info.set_task_end(executors[i + 1]);
            hb.add_to_executors(info);
        }
        return hb;
    }


    private Map<List<Integer>, Map<String, Object>> mkZkExecutorBeats(int taskStart, int taskEnd, int timeSecs) {
        Map<String, Object> beat = new HashMap<>();
        beat.put(ClientStatsUtil.TIME_SECS, timeSecs);
        return Collections.singletonMap(Arrays.asList(taskStart, taskEnd), beat);
    }

    private Assignment mkAssignment(int startTimeSecs, int... executors) {
        Assignment assignment = new Assignment();
        Map<List<Long>, Long> execToStartTime = new HashMap<>();
        Map<List<Long>, NodeInfo> execToNodePort = new HashMap<>();
        NodeInfo nodeInfo = new NodeInfo("node1", Collections.singleton(6700L));
        for (int i = 0; i < executors.length - 1; i += 2) {
            List<Long> exec = Arrays.asList((long) executors[i], (long) executors[i + 1]);
            execToStartTime.put(exec, (long) startTimeSecs);
            execToNodePort.put(exec, nodeInfo);
        }
        assignment.set_executor_start_time_secs(execToStartTime);
        assignment.set_executor_node_port(execToNodePort);
        return assignment;
    }
}
