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
import org.apache.storm.generated.ClusterWorkerHeartbeat;
import org.apache.storm.generated.LSWorkerHeartbeat;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.stats.ClientStatsUtil;
import org.apache.storm.thrift.TDeserializer;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.TSerializer;
import org.apache.storm.thrift.protocol.TBinaryProtocol;
import org.apache.storm.thrift.protocol.TField;
import org.apache.storm.thrift.protocol.TList;
import org.apache.storm.thrift.protocol.TStruct;
import org.apache.storm.thrift.protocol.TType;
import org.apache.storm.thrift.transport.TMemoryBuffer;
import org.apache.storm.thrift.transport.TMemoryInputTransport;
import org.apache.storm.utils.Time;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for the Y2038 heartbeat overflow (STORM issue #7897).
 *
 * <p>Worker heartbeat timestamps were carried as i32 seconds, which overflows on
 * 2038-01-19T03:14:07Z. These tests pin the post-2038 behavior: heartbeat
 * {@code time_secs} must survive serialization as a 64-bit value, and the Nimbus
 * {@link HeartbeatCache} must not flag fresh post-2038 heartbeats as timed out.
 */
class Y2038HeartbeatTest {
    private static final String TOPO_ID = "y2038-topology-1";
    private static final int TIMEOUT_SECS = 30;
    /** 2040-01-01T00:00:00Z — comfortably past the 2038 i32 rollover. */
    private static final long POST_2038_EPOCH_SECS = 2_208_988_800L;
    /** 2023-11-14T22:13:20Z — a plausible pre-2038 heartbeat timestamp. */
    private static final int LEGACY_EPOCH_SECS = 1_700_000_000;

    /**
     * A post-2038 epoch must round-trip through ClusterWorkerHeartbeat serialization
     * without narrowing. Uses the dynamic field API so this test compiles against both
     * the i32 and i64 schema; with the i32 schema the Long value is rejected.
     */
    @Test
    void testCwhTimeSecsSurvivesPost2038RoundTrip() throws Exception {
        ClusterWorkerHeartbeat hb = new ClusterWorkerHeartbeat();
        hb.set_storm_id(TOPO_ID);
        hb.set_executor_stats(new HashMap<>());
        hb.set_uptime_secs(120);
        hb.setFieldValue(ClusterWorkerHeartbeat._Fields.TIME_SECS, POST_2038_EPOCH_SECS);

        TSerializer ser = new TSerializer(new TBinaryProtocol.Factory());
        byte[] bytes = ser.serialize(hb);
        ClusterWorkerHeartbeat read = new ClusterWorkerHeartbeat();
        TDeserializer des = new TDeserializer(new TBinaryProtocol.Factory());
        des.deserialize(read, bytes);

        long timeSecs = ((Number) read.getFieldValue(ClusterWorkerHeartbeat._Fields.TIME_SECS)).longValue();
        assertTrue(timeSecs > Integer.MAX_VALUE, "time_secs must not be narrowed to i32");
        assertEquals(POST_2038_EPOCH_SECS, timeSecs, "time_secs must round-trip unchanged");
    }

    /**
     * A fresh heartbeat received after 2038 must not be flagged as timed out.
     * The beat map carries TIME_SECS as a Long, matching what the post-fix
     * heartbeat writer produces.
     */
    @Test
    void testHeartbeatCacheNoFalseTimeoutPost2038() {
        try (Time.SimulatedTime ignored = new Time.SimulatedTime()) {
            Time.advanceTime(POST_2038_EPOCH_SECS * 1000L);

            HeartbeatCache cache = new HeartbeatCache();
            Set<List<Integer>> allExecutors = Collections.singleton(Arrays.asList(1, 1));
            Assignment assignment = mkAssignment(POST_2038_EPOCH_SECS, 1, 1);

            cache.updateFromZkHeartbeat(TOPO_ID, mkZkExecutorBeats(1, 1, POST_2038_EPOCH_SECS), allExecutors,
                TIMEOUT_SECS);
            Time.advanceTimeSecs(1);
            cache.timeoutOldHeartbeats(TOPO_ID, TIMEOUT_SECS);

            Set<List<Integer>> alive = cache.getAliveExecutors(TOPO_ID, allExecutors, assignment, TIMEOUT_SECS);
            assertFalse(alive.isEmpty(), "A fresh post-2038 heartbeat must not be flagged as timed out");
        }
    }

    /**
     * Documents why the long-based clock path is required: the int-based
     * {@code Time.currentTimeSecs()} overflows past 2038.
     */
    @Test
    void testCurrentTimeSecsIntOverflowsPost2038() {
        try (Time.SimulatedTime ignored = new Time.SimulatedTime()) {
            Time.advanceTime(POST_2038_EPOCH_SECS * 1000L);
            assertTrue(Time.currentTimeSecs() < 0,
                "currentTimeSecs() (int) is expected to overflow past 2038 — long path required");
        }
    }

    /**
     * Wire compatibility: an LSWorkerHeartbeat blob written by the legacy i32 schema
     * must fail loudly (required-field validation) under the i64 schema, not be
     * silently misread. Supervisors treat the entry as stale and rebuild local state.
     */
    @Test
    void testLegacyI32LswhBlobFailsValidationUnderI64Schema() throws Exception {
        byte[] legacyBlob = writeLegacyI32Lswh(LEGACY_EPOCH_SECS, TOPO_ID, 6700);

        LSWorkerHeartbeat read = new LSWorkerHeartbeat();
        assertThrows(TException.class,
            () -> read.read(new TBinaryProtocol(new TMemoryInputTransport(legacyBlob))),
            "A legacy i32 time_secs blob must fail required-field validation under the i64 schema");
    }

    /**
     * Startup guard: an executor whose assignment start time is past 2038 must not be
     * misclassified as dead by getAliveExecutors while inside the launch window.
     */
    @Test
    void testExecutorStartedPost2038NotMisclassifiedDead() {
        try (Time.SimulatedTime ignored = new Time.SimulatedTime()) {
            Time.advanceTime(POST_2038_EPOCH_SECS * 1000L);

            HeartbeatCache cache = new HeartbeatCache();
            Set<List<Integer>> allExecutors = Collections.singleton(Arrays.asList(1, 1));
            Assignment assignment = mkAssignment(POST_2038_EPOCH_SECS, 1, 1);

            // No heartbeat reported yet; the executor is within the task launch window.
            Time.advanceTimeSecs(1);
            Set<List<Integer>> alive = cache.getAliveExecutors(TOPO_ID, allExecutors, assignment, TIMEOUT_SECS);
            assertFalse(alive.isEmpty(),
                "An executor launched post-2038 must stay alive during its launch window");
        }
    }

    /**
     * Emulates the wire bytes an old (pre-fix) writer produced for LSWorkerHeartbeat:
     * field 1 time_secs as i32, field 2 topology_id, field 3 empty executors list,
     * field 4 port. Field ids and types match the legacy schema.
     */
    private byte[] writeLegacyI32Lswh(int timeSecs, String topologyId, int port) throws Exception {
        TMemoryBuffer buffer = new TMemoryBuffer(128);
        TBinaryProtocol prot = new TBinaryProtocol(buffer);
        prot.writeStructBegin(new TStruct("LSWorkerHeartbeat"));
        prot.writeFieldBegin(new TField("time_secs", TType.I32, (short) 1));
        prot.writeI32(timeSecs);
        prot.writeFieldEnd();
        prot.writeFieldBegin(new TField("topology_id", TType.STRING, (short) 2));
        prot.writeString(topologyId);
        prot.writeFieldEnd();
        prot.writeFieldBegin(new TField("executors", TType.LIST, (short) 3));
        prot.writeListBegin(new TList(TType.STRUCT, 0));
        prot.writeListEnd();
        prot.writeFieldEnd();
        prot.writeFieldBegin(new TField("port", TType.I32, (short) 4));
        prot.writeI32(port);
        prot.writeFieldEnd();
        prot.writeFieldStop();
        prot.writeStructEnd();
        return Arrays.copyOf(buffer.getArray(), buffer.length());
    }

    private Map<List<Integer>, Map<String, Object>> mkZkExecutorBeats(int taskStart, int taskEnd, long timeSecs) {
        Map<String, Object> beat = new HashMap<>();
        beat.put(ClientStatsUtil.TIME_SECS, timeSecs);
        return Collections.singletonMap(Arrays.asList(taskStart, taskEnd), beat);
    }

    private Assignment mkAssignment(long startTimeSecs, int... executors) {
        Assignment assignment = new Assignment();
        Map<List<Long>, Long> execToStartTime = new HashMap<>();
        Map<List<Long>, NodeInfo> execToNodePort = new HashMap<>();
        NodeInfo nodeInfo = new NodeInfo("node1", Collections.singleton(6700L));
        for (int i = 0; i < executors.length - 1; i += 2) {
            List<Long> exec = Arrays.asList((long) executors[i], (long) executors[i + 1]);
            execToStartTime.put(exec, startTimeSecs);
            execToNodePort.put(exec, nodeInfo);
        }
        assignment.set_executor_start_time_secs(execToStartTime);
        assignment.set_executor_node_port(execToNodePort);
        return assignment;
    }
}
