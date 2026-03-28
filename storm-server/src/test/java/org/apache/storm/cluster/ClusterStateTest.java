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

package org.apache.storm.cluster;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.storm.Config;
import org.apache.storm.callback.ZKStateChangedCallback;
import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.Credentials;
import org.apache.storm.generated.ErrorInfo;
import org.apache.storm.generated.NimbusSummary;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.SupervisorInfo;
import org.apache.storm.generated.TopologyStatus;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.shade.org.apache.zookeeper.Watcher;
import org.apache.storm.shade.org.apache.zookeeper.ZooDefs;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.testing.InProcessZookeeper;
import org.apache.storm.utils.CuratorUtils;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.ZookeeperAuthInfo;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFrameworkFactory;
import org.junit.jupiter.api.Test;
import org.awaitility.Awaitility;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for IStateStorage (ZKStateStorage) and IStormClusterState (StormClusterStateImpl)
 * using an in-process ZooKeeper.
 *
 * Ported from storm-core/test/clj/org/apache/storm/cluster_test.clj
 */
public class ClusterStateTest {

    private static Map<String, Object> mkConfig(long zkPort) {
        Map<String, Object> conf = Utils.readStormConfig();
        conf.put(Config.STORM_ZOOKEEPER_PORT, zkPort);
        conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("localhost"));
        return conf;
    }

    private static IStateStorage mkState(long zkPort) throws Exception {
        Map<String, Object> conf = mkConfig(zkPort);
        return ClusterUtils.mkStateStorage(conf, conf, new ClusterStateContext());
    }

    private static IStateStorage mkState(long zkPort, ZKStateChangedCallback cb) throws Exception {
        IStateStorage state = mkState(zkPort);
        state.register(cb);
        return state;
    }

    private static IStormClusterState mkStormState(long zkPort) throws Exception {
        Map<String, Object> conf = mkConfig(zkPort);
        return ClusterUtils.mkStormClusterState(conf, new ClusterStateContext());
    }

    private static byte[] barr(int... vals) {
        byte[] result = new byte[vals.length];
        for (int i = 0; i < vals.length; i++) {
            result[i] = (byte) vals[i];
        }
        return result;
    }

    private static List<ACL> OPEN_ACL = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    @Test
    public void testBasics() throws Exception {
        try (InProcessZookeeper zk = new InProcessZookeeper()) {
            IStateStorage state = mkState(zk.getPort());

            state.set_data("/root", barr(1, 2, 3), OPEN_ACL);
            assertArrayEquals(barr(1, 2, 3), state.get_data("/root", false));
            assertNull(state.get_data("/a", false));

            state.set_data("/root/a", barr(1, 2), OPEN_ACL);
            state.set_data("/root", barr(1), OPEN_ACL);
            assertArrayEquals(barr(1), state.get_data("/root", false));
            assertArrayEquals(barr(1, 2), state.get_data("/root/a", false));

            state.set_data("/a/b/c/d", barr(99), OPEN_ACL);
            assertArrayEquals(barr(99), state.get_data("/a/b/c/d", false));

            state.mkdirs("/lalala", OPEN_ACL);
            assertEquals(List.of(), state.get_children("/lalala", false));
            assertEquals(Set.of("root", "a", "lalala"), new HashSet<>(state.get_children("/", false)));

            state.delete_node("/a");
            assertEquals(Set.of("root", "lalala"), new HashSet<>(state.get_children("/", false)));
            assertNull(state.get_data("/a/b/c/d", false));

            state.close();
        }
    }

    @Test
    public void testMultiState() throws Exception {
        try (InProcessZookeeper zk = new InProcessZookeeper()) {
            IStateStorage state1 = mkState(zk.getPort());
            IStateStorage state2 = mkState(zk.getPort());

            state1.set_data("/root", barr(1), OPEN_ACL);
            assertArrayEquals(barr(1), state1.get_data("/root", false));
            assertArrayEquals(barr(1), state2.get_data("/root", false));

            state2.delete_node("/root");
            assertNull(state1.get_data("/root", false));
            assertNull(state2.get_data("/root", false));

            state1.close();
            state2.close();
        }
    }

    @Test
    public void testEphemeral() throws Exception {
        try (InProcessZookeeper zk = new InProcessZookeeper()) {
            IStateStorage state1 = mkState(zk.getPort());
            IStateStorage state2 = mkState(zk.getPort());
            IStateStorage state3 = mkState(zk.getPort());

            state1.set_ephemeral_node("/a", barr(1), OPEN_ACL);
            assertArrayEquals(barr(1), state1.get_data("/a", false));
            assertArrayEquals(barr(1), state2.get_data("/a", false));

            // closing state3 should not affect state1's ephemeral node
            state3.close();
            assertArrayEquals(barr(1), state1.get_data("/a", false));
            assertArrayEquals(barr(1), state2.get_data("/a", false));

            // closing state1 (the creator) should remove the ephemeral node
            state1.close();
            Awaitility.await().atMost(30, TimeUnit.SECONDS)
                .until(() -> state2.get_data("/a", false) == null);

            state2.close();
        }
    }

    /**
     * Helper: creates a ZKStateChangedCallback that stores the last event in an AtomicReference.
     */
    private static class CallbackTester {
        final AtomicReference<Map<String, Object>> lastEvent = new AtomicReference<>();
        final ZKStateChangedCallback callback = (type, path) -> {
            Map<String, Object> event = new HashMap<>();
            event.put("type", type);
            event.put("path", path);
            lastEvent.set(event);
        };

        Map<String, Object> readAndReset() {
            Awaitility.await().atMost(30, TimeUnit.SECONDS)
                .pollInterval(10, TimeUnit.MILLISECONDS)
                .until(() -> lastEvent.get() != null);
            return lastEvent.getAndSet(null);
        }
    }

    private static Map<String, Object> event(Watcher.Event.EventType type, String path) {
        Map<String, Object> m = new HashMap<>();
        m.put("type", type);
        m.put("path", path);
        return m;
    }

    @Test
    public void testCallbacks() throws Exception {
        try (InProcessZookeeper zk = new InProcessZookeeper()) {
            CallbackTester cb1 = new CallbackTester();
            CallbackTester cb2 = new CallbackTester();
            IStateStorage state1 = mkState(zk.getPort(), cb1.callback);
            IStateStorage state2 = mkState(zk.getPort(), cb2.callback);

            state1.set_data("/root", barr(1), OPEN_ACL);
            state2.get_data("/root", true);
            assertNull(cb1.lastEvent.get());
            assertNull(cb2.lastEvent.get());

            state2.set_data("/root", barr(2), OPEN_ACL);
            assertEquals(event(Watcher.Event.EventType.NodeDataChanged, "/root"), cb2.readAndReset());
            assertNull(cb1.lastEvent.get());

            // no watch set, so no callback
            state2.set_data("/root", barr(3), OPEN_ACL);
            assertNull(cb2.lastEvent.get());

            // set watch, then read without watch — watch should still fire
            state2.get_data("/root", true);
            state2.get_data("/root", false);
            state1.delete_node("/root");
            assertEquals(event(Watcher.Event.EventType.NodeDeleted, "/root"), cb2.readAndReset());

            // watch for creation
            state2.get_data("/root", true);
            state1.set_ephemeral_node("/root", barr(1, 2, 3, 4), OPEN_ACL);
            assertEquals(event(Watcher.Event.EventType.NodeCreated, "/root"), cb2.readAndReset());

            // children watch
            state1.get_children("/", true);
            state2.set_data("/a", barr(9), OPEN_ACL);
            assertNull(cb2.lastEvent.get());
            assertEquals(event(Watcher.Event.EventType.NodeChildrenChanged, "/"), cb1.readAndReset());

            // ephemeral node data change
            state2.get_data("/root", true);
            state1.set_ephemeral_node("/root", barr(1, 2), OPEN_ACL);
            assertEquals(event(Watcher.Event.EventType.NodeDataChanged, "/root"), cb2.readAndReset());

            // children + data creation
            state1.mkdirs("/ccc", OPEN_ACL);
            state1.get_children("/ccc", true);
            state2.get_data("/ccc/b", true);
            state2.set_data("/ccc/b", barr(8), OPEN_ACL);
            assertEquals(event(Watcher.Event.EventType.NodeCreated, "/ccc/b"), cb2.readAndReset());
            assertEquals(event(Watcher.Event.EventType.NodeChildrenChanged, "/ccc"), cb1.readAndReset());

            // closing state1 removes its ephemeral nodes
            state2.get_data("/root", true);
            state2.get_data("/root2", true);
            state1.close();
            assertEquals(event(Watcher.Event.EventType.NodeDeleted, "/root"), cb2.readAndReset());

            state2.set_data("/root2", barr(9), OPEN_ACL);
            assertEquals(event(Watcher.Event.EventType.NodeCreated, "/root2"), cb2.readAndReset());
            state2.close();
        }
    }

    private static Assignment mkAssignment(String masterCodeDir, Map<String, String> nodeToHost,
                                           Map<List<Long>, NodeInfo> executorToNodePort,
                                           Map<List<Long>, Long> executorToStartTimeSecs,
                                           Map<NodeInfo, WorkerResources> workerToResources) {
        Assignment assignment = new Assignment();
        assignment.set_executor_node_port(executorToNodePort);
        assignment.set_executor_start_time_secs(executorToStartTimeSecs);
        assignment.set_worker_resources(workerToResources);
        assignment.set_node_host(nodeToHost);
        assignment.set_master_code_dir(masterCodeDir);
        return assignment;
    }

    private static StormBase mkStormBase(String stormName, int launchTimeSecs,
                                         TopologyStatus status, int numWorkers) {
        StormBase base = new StormBase();
        base.set_name(stormName);
        base.set_launch_time_secs(launchTimeSecs);
        base.set_status(status);
        base.set_num_workers(numWorkers);
        return base;
    }

    @Test
    public void testStormClusterStateBasics() throws Exception {
        try (InProcessZookeeper zk = new InProcessZookeeper()) {
            IStormClusterState state = mkStormState(zk.getPort());

            NodeInfo nodeInfo1 = new NodeInfo("1", new HashSet<>(Arrays.asList(1001L, 1L)));
            NodeInfo nodeInfo2 = new NodeInfo("2", new HashSet<>(Arrays.asList(2002L)));

            Assignment assignment1 = mkAssignment("/aaa", Map.of(),
                Map.of(List.of(1L), nodeInfo1), Map.of(), Map.of());
            Assignment assignment2 = mkAssignment("/aaa", Map.of(),
                Map.of(List.of(2L), nodeInfo2), Map.of(), Map.of());

            NimbusInfo nimbusInfo1 = new NimbusInfo("nimbus1", 6667, false);
            NimbusInfo nimbusInfo2 = new NimbusInfo("nimbus2", 6667, false);
            NimbusSummary nimbusSummary1 = new NimbusSummary("nimbus1", 6667, Time.currentTimeSecs(), false, "v1");
            NimbusSummary nimbusSummary2 = new NimbusSummary("nimbus2", 6667, Time.currentTimeSecs(), false, "v2");

            StormBase base1 = mkStormBase("/tmp/storm1", 1, TopologyStatus.ACTIVE, 2);
            StormBase base2 = mkStormBase("/tmp/storm2", 2, TopologyStatus.ACTIVE, 2);

            // assignments
            assertEquals(List.of(), state.assignments(null));
            state.setAssignment("storm1", assignment1, Map.of());
            assertEquals(assignment1, state.assignmentInfo("storm1", null));
            assertNull(state.assignmentInfo("storm3", null));
            state.setAssignment("storm1", assignment2, Map.of());
            state.setAssignment("storm3", assignment1, Map.of());
            assertEquals(Set.of("storm1", "storm3"), new HashSet<>(state.assignments(null)));
            assertEquals(assignment2, state.assignmentInfo("storm1", null));
            assertEquals(assignment1, state.assignmentInfo("storm3", null));

            // storm bases (active storms)
            assertEquals(List.of(), state.activeStorms());
            state.activateStorm("storm1", base1, Map.of());
            assertEquals(List.of("storm1"), state.activeStorms());
            assertEquals(base1, state.stormBase("storm1", null));
            assertNull(state.stormBase("storm2", null));
            state.activateStorm("storm2", base2, Map.of());
            assertEquals(base1, state.stormBase("storm1", null));
            assertEquals(base2, state.stormBase("storm2", null));
            assertEquals(Set.of("storm1", "storm2"), new HashSet<>(state.activeStorms()));
            state.removeStormBase("storm1");
            assertEquals(base2, state.stormBase("storm2", null));
            assertEquals(Set.of("storm2"), new HashSet<>(state.activeStorms()));

            // credentials
            assertNull(state.credentials("storm1", null));
            Credentials creds1 = new Credentials();
            creds1.set_creds(Map.of("a", "a"));
            state.setCredentials("storm1", creds1, Map.of());
            assertEquals(Map.of("a", "a"), state.credentials("storm1", null).get_creds());
            Credentials creds2 = new Credentials();
            creds2.set_creds(Map.of("b", "b"));
            state.setCredentials("storm1", creds2, Map.of());
            assertEquals(Map.of("b", "b"), state.credentials("storm1", null).get_creds());

            // blobstore
            assertEquals(List.of(), state.blobstoreInfo(""));
            state.setupBlob("key1", nimbusInfo1, 1);
            assertEquals(List.of("key1"), state.blobstoreInfo(""));
            assertEquals(List.of(nimbusInfo1.toHostPortString() + "-1"), state.blobstoreInfo("key1"));
            state.setupBlob("key1", nimbusInfo2, 1);
            assertEquals(Set.of(nimbusInfo1.toHostPortString() + "-1", nimbusInfo2.toHostPortString() + "-1"),
                new HashSet<>(state.blobstoreInfo("key1")));
            state.removeBlobstoreKey("key1");
            assertEquals(List.of(), state.blobstoreInfo(""));

            // nimbuses
            assertEquals(List.of(), state.nimbuses());
            state.addNimbusHost("nimbus1:port", nimbusSummary1);
            assertEquals(List.of(nimbusSummary1), state.nimbuses());
            state.addNimbusHost("nimbus2:port", nimbusSummary2);
            assertEquals(Set.of(nimbusSummary1, nimbusSummary2), new HashSet<>(state.nimbuses()));

            state.disconnect();
        }
    }

    @Test
    public void testStormClusterStateErrors() throws Exception {
        try (InProcessZookeeper zk = new InProcessZookeeper()) {
            try (Time.SimulatedTime ignored = new Time.SimulatedTime()) {
                IStormClusterState state = mkStormState(zk.getPort());
                String hostname = Utils.localHostname();

                state.reportError("a", "1", hostname, 6700L, new RuntimeException());
                validateErrors(state, "a", "1", "RuntimeException");

                Time.advanceTimeSecs(1);
                state.reportError("a", "1", hostname, 6700L, new IllegalArgumentException());
                validateErrors(state, "a", "1", "IllegalArgumentException", "RuntimeException");

                for (int i = 0; i < 10; i++) {
                    state.reportError("a", "2", hostname, 6700L, new RuntimeException());
                    Time.advanceTimeSecs(2);
                }
                validateErrors(state, "a", "2",
                    "RuntimeException", "RuntimeException", "RuntimeException", "RuntimeException", "RuntimeException",
                    "RuntimeException", "RuntimeException", "RuntimeException", "RuntimeException", "RuntimeException");

                for (int i = 0; i < 5; i++) {
                    state.reportError("a", "2", hostname, 6700L, new IllegalArgumentException());
                    Time.advanceTimeSecs(2);
                }
                validateErrors(state, "a", "2",
                    "IllegalArgumentException", "IllegalArgumentException", "IllegalArgumentException",
                    "IllegalArgumentException", "IllegalArgumentException",
                    "RuntimeException", "RuntimeException", "RuntimeException", "RuntimeException", "RuntimeException");

                state.disconnect();
            }
        }
    }

    private void validateErrors(IStormClusterState state, String stormId, String component, String... expectedErrors) {
        List<ErrorInfo> errors = state.errors(stormId, component);
        assertEquals(expectedErrors.length, errors.size(),
            "Expected " + expectedErrors.length + " errors but got " + errors.size());
        for (int i = 0; i < expectedErrors.length; i++) {
            assertTrue(errors.get(i).get_error().contains(expectedErrors[i]),
                "Error " + i + " should contain '" + expectedErrors[i] + "' but was: " + errors.get(i).get_error());
        }
    }

    private static SupervisorInfo mkSupervisorInfo(long timeSecs, String hostname, String assignmentId,
                                                    List<Long> usedPorts, List<Long> meta,
                                                    Map<String, String> schedulerMeta,
                                                    long uptimeSecs, String version,
                                                    Map<String, Double> resourcesMap) {
        SupervisorInfo info = new SupervisorInfo();
        info.set_time_secs(timeSecs);
        info.set_hostname(hostname);
        info.set_assignment_id(assignmentId);
        info.set_used_ports(usedPorts);
        info.set_meta(meta);
        info.set_scheduler_meta(schedulerMeta);
        info.set_uptime_secs(uptimeSecs);
        info.set_version(version);
        if (resourcesMap != null) {
            info.set_resources_map(resourcesMap);
        }
        return info;
    }

    @Test
    public void testSupervisorState() throws Exception {
        try (InProcessZookeeper zk = new InProcessZookeeper()) {
            IStormClusterState state1 = mkStormState(zk.getPort());
            IStormClusterState state2 = mkStormState(zk.getPort());

            SupervisorInfo info1 = mkSupervisorInfo(10, "hostname-1", "id1",
                List.of(1L, 2L), List.of(), Map.of(), 1000, "0.9.2", null);
            SupervisorInfo info2 = mkSupervisorInfo(10, "hostname-2", "id2",
                List.of(1L, 2L), List.of(), Map.of(), 1000, "0.9.2", null);

            assertEquals(List.of(), state1.supervisors(null));

            state2.supervisorHeartbeat("2", info2);
            state1.supervisorHeartbeat("1", info1);
            assertEquals(info2, state1.supervisorInfo("2"));
            assertEquals(info1, state1.supervisorInfo("1"));
            assertEquals(Set.of("1", "2"), new HashSet<>(state1.supervisors(null)));
            assertEquals(Set.of("1", "2"), new HashSet<>(state2.supervisors(null)));

            // disconnecting state2 removes its ephemeral supervisor node
            state2.disconnect();
            Awaitility.await().atMost(30, TimeUnit.SECONDS)
                .until(() -> new HashSet<>(state1.supervisors(null)).equals(Set.of("1")));

            state1.disconnect();
        }
    }

    @Test
    public void testClusterAuthentication() throws Exception {
        try (InProcessZookeeper zk = new InProcessZookeeper()) {
            CuratorFrameworkFactory.Builder builder = Mockito.mock(CuratorFrameworkFactory.Builder.class);
            Map<String, Object> conf = mkConfig(zk.getPort());
            conf.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 10);
            conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 10);
            conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 5);
            conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 2);
            conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING, 15);
            conf.put(Config.STORM_ZOOKEEPER_AUTH_SCHEME, "digest");
            conf.put(Config.STORM_ZOOKEEPER_AUTH_PAYLOAD, "storm:thisisapoorpassword");

            Mockito.when(builder.connectString(Mockito.anyString())).thenReturn(builder);
            Mockito.when(builder.connectionTimeoutMs(Mockito.anyInt())).thenReturn(builder);
            Mockito.when(builder.sessionTimeoutMs(Mockito.anyInt())).thenReturn(builder);

            CuratorUtils.testSetupBuilder(builder, String.valueOf(zk.getPort()) + "/", conf,
                new ZookeeperAuthInfo(conf));

            Mockito.verify(builder).authorization(
                "digest",
                ((String) conf.get(Config.STORM_ZOOKEEPER_AUTH_PAYLOAD)).getBytes());
        }
    }

}
