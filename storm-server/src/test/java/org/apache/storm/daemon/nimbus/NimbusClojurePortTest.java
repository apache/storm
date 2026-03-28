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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import javax.security.auth.Subject;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.LocalCluster;
import org.apache.storm.Thrift;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.Credentials;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.LogConfig;
import org.apache.storm.generated.LogLevel;
import org.apache.storm.generated.LogLevelAction;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SubmitOptions;
import org.apache.storm.generated.TopologyInitialStatus;
import org.apache.storm.generated.TopologyStatus;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.testing.TestPlannerBolt;
import org.apache.storm.testing.TestPlannerSpout;
import org.apache.storm.testing.TmpPath;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests ported from storm-core/test/clj/org/apache/storm/nimbus_test.clj.
 * Batch 7a: Simple/unit-like tests.
 */
public class NimbusClojurePortTest {

    @Test
    public void testBogusId() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
                .withSupervisors(4)
                .withDaemonConf(Map.of(
                    DaemonConfig.SUPERVISOR_ENABLE, false,
                    Config.TOPOLOGY_ACKER_EXECUTORS, 0,
                    Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, 0))
                .build()) {
            Nimbus nimbus = cluster.getNimbus();
            assertThrows(NotAliveException.class, () -> nimbus.getTopologyConf("bogus-id"));
            assertThrows(NotAliveException.class, () -> nimbus.getTopology("bogus-id"));
            assertThrows(NotAliveException.class, () -> nimbus.getUserTopology("bogus-id"));
            assertThrows(NotAliveException.class, () -> nimbus.getTopologyInfo("bogus-id"));
            assertThrows(NotAliveException.class, () -> nimbus.uploadNewCredentials("bogus-id", new Credentials()));
        }
    }

    @Test
    public void testNimbusIfaceSubmitTopologyWithOptsChecksAuthorization() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
                .withDaemonConf(Map.of(
                    DaemonConfig.NIMBUS_AUTHORIZER, "org.apache.storm.security.auth.authorizer.DenyAuthorizer",
                    DaemonConfig.SUPERVISOR_AUTHORIZER, "org.apache.storm.security.auth.authorizer.DenyAuthorizer"))
                .build()) {
            StormTopology topology = Thrift.buildTopology(Map.of(), Map.of());
            assertThrows(AuthorizationException.class, () ->
                cluster.submitTopologyWithOpts("mystorm", Map.of(), topology,
                    new SubmitOptions(TopologyInitialStatus.INACTIVE)));
        }
    }

    @Test
    public void testNimbusIfaceMethodsCheckAuthorization() throws Exception {
        IStormClusterState clusterState = Mockito.mock(IStormClusterState.class);
        BlobStore blobStore = Mockito.mock(BlobStore.class);
        TopoCache tc = Mockito.mock(TopoCache.class);

        try (LocalCluster cluster = new LocalCluster.Builder()
                .withClusterState(clusterState)
                .withBlobStore(blobStore)
                .withTopoCache(tc)
                .withDaemonConf(Map.of(
                    DaemonConfig.NIMBUS_AUTHORIZER, "org.apache.storm.security.auth.authorizer.DenyAuthorizer",
                    DaemonConfig.SUPERVISOR_AUTHORIZER, "org.apache.storm.security.auth.authorizer.DenyAuthorizer"))
                .build()) {
            Nimbus nimbus = cluster.getNimbus();
            String topologyName = "test";
            String topologyId = "test-id";

            Mockito.when(clusterState.getTopoId(topologyName)).thenReturn(Optional.of(topologyId));

            assertThrows(AuthorizationException.class, () ->
                nimbus.rebalance(topologyName, new RebalanceOptions()));
            assertThrows(AuthorizationException.class, () ->
                nimbus.activate(topologyName));
            assertThrows(AuthorizationException.class, () ->
                nimbus.deactivate(topologyName));
        }
    }

    @Test
    public void testNimbusIfaceGetTopologyMethodsThrowCorrectly() throws Exception {
        try (LocalCluster cluster = new LocalCluster()) {
            Nimbus nimbus = cluster.getNimbus();
            String id = "bogus ID";

            NotAliveException e1 = assertThrows(NotAliveException.class, () -> nimbus.getTopology(id));
            assertEquals(id, e1.get_msg());

            NotAliveException e2 = assertThrows(NotAliveException.class, () -> nimbus.getTopologyConf(id));
            assertEquals(id, e2.get_msg());

            NotAliveException e3 = assertThrows(NotAliveException.class, () -> nimbus.getTopologyInfo(id));
            assertEquals(id, e3.get_msg());

            NotAliveException e4 = assertThrows(NotAliveException.class, () -> nimbus.getUserTopology(id));
            assertEquals(id, e4.get_msg());
        }
    }

    @Test
    public void testNimbusIfaceGetClusterInfoFiltersToposWithoutBases() throws Exception {
        IStormClusterState clusterState = Mockito.mock(IStormClusterState.class);
        BlobStore blobStore = Mockito.mock(BlobStore.class);
        TopoCache tc = Mockito.mock(TopoCache.class);

        try (LocalCluster cluster = new LocalCluster.Builder()
                .withClusterState(clusterState)
                .withBlobStore(blobStore)
                .withTopoCache(tc)
                .build()) {
            Nimbus nimbus = cluster.getNimbus();

            int bogusSecs = 42;
            TopologyStatus bogusType = TopologyStatus.ACTIVE;

            StormBase base2 = new StormBase();
            base2.set_name("id2-name");
            base2.set_launch_time_secs(bogusSecs);
            base2.set_status(bogusType);

            StormBase base4 = new StormBase();
            base4.set_name("id4-name");
            base4.set_launch_time_secs(bogusSecs);
            base4.set_status(bogusType);

            Map<String, StormBase> bogusBasesMap = new HashMap<>();
            bogusBasesMap.put("1", null);
            bogusBasesMap.put("2", base2);
            bogusBasesMap.put("3", null);
            bogusBasesMap.put("4", base4);

            Map<String, Object> topoConf = new HashMap<>();
            topoConf.put(Config.TOPOLOGY_NAME, "test-topo");
            topoConf.put(Config.TOPOLOGY_WORKERS, 1);
            topoConf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 30);

            StormTopology topology = new StormTopology();
            topology.set_spouts(Map.of());
            topology.set_bolts(Map.of());
            topology.set_state_spouts(Map.of());

            Mockito.when(clusterState.stormBase(Mockito.any(String.class), ArgumentMatchers.any())).thenReturn(new StormBase());
            Mockito.when(clusterState.topologyBases()).thenReturn(bogusBasesMap);
            Mockito.when(tc.readTopoConf(Mockito.any(String.class), Mockito.any(Subject.class))).thenReturn(topoConf);
            Mockito.when(tc.readTopology(Mockito.any(String.class), Mockito.any(Subject.class))).thenReturn(topology);

            List<TopologySummary> topos = cluster.getNimbus().getClusterInfo().get_topologies();
            // Only topologies with non-null bases should be present
            assertEquals(2, topos.size());
            // Each topology should have a valid name
            for (TopologySummary t : topos) {
                assertNotNull(t);
                assertNotNull(t.get_name());
            }
            // The topologies should be those with even IDs (2 and 4)
            for (TopologySummary t : topos) {
                int id = Integer.parseInt(t.get_id());
                assertEquals(0, id % 2);
            }
        }
    }

    @Test
    public void testValidateTopoConfigOnSubmit() throws Exception {
        IStormClusterState clusterState = Mockito.mock(IStormClusterState.class);
        BlobStore blobStore = Mockito.mock(BlobStore.class);
        TopoCache tc = Mockito.mock(TopoCache.class);

        try (LocalCluster cluster = new LocalCluster.Builder()
                .withClusterState(clusterState)
                .withBlobStore(blobStore)
                .withTopoCache(tc)
                .withDaemonConf(Map.of(
                    DaemonConfig.NIMBUS_AUTHORIZER, "org.apache.storm.security.auth.authorizer.NoopAuthorizer",
                    DaemonConfig.SUPERVISOR_AUTHORIZER, "org.apache.storm.security.auth.authorizer.NoopAuthorizer"))
                .build()) {
            Mockito.when(clusterState.getTopoId("test")).thenReturn(Optional.empty());
            StormTopology topology = Thrift.buildTopology(Map.of(), Map.of());
            Map<String, Object> badConfig = new HashMap<>();
            badConfig.put("topology.isolate.machines", "2");
            assertThrows(InvalidTopologyException.class, () ->
                cluster.submitTopologyWithOpts("test", badConfig, topology, new SubmitOptions()));
        }
    }

    @Test
    public void testDebugOnComponent() throws Exception {
        try (LocalCluster cluster = new LocalCluster()) {
            Nimbus nimbus = cluster.getNimbus();
            StormTopology topology = Thrift.buildTopology(
                Map.of("spout", Thrift.prepareSpoutDetails(new TestPlannerSpout(true), 3)),
                Map.of());
            cluster.submitTopology("t1", Map.of(Config.TOPOLOGY_WORKERS, 1), topology);
            nimbus.debug("t1", "spout", true, 100);
        }
    }

    @Test
    public void testDebugOnGlobal() throws Exception {
        try (LocalCluster cluster = new LocalCluster()) {
            Nimbus nimbus = cluster.getNimbus();
            StormTopology topology = Thrift.buildTopology(
                Map.of("spout", Thrift.prepareSpoutDetails(new TestPlannerSpout(true), 3)),
                Map.of());
            cluster.submitTopology("t1", Map.of(Config.TOPOLOGY_WORKERS, 1), topology);
            nimbus.debug("t1", "", true, 100);
        }
    }

    @Test
    public void emptySaveConfigResultsInAllUnchangedActions() throws Exception {
        IStormClusterState clusterState = Mockito.mock(IStormClusterState.class);
        BlobStore blobStore = Mockito.mock(BlobStore.class);
        TopoCache tc = Mockito.mock(TopoCache.class);

        try (LocalCluster cluster = new LocalCluster.Builder()
                .withClusterState(clusterState)
                .withBlobStore(blobStore)
                .withTopoCache(tc)
                .withDaemonConf(Map.of(
                    DaemonConfig.NIMBUS_AUTHORIZER, "org.apache.storm.security.auth.authorizer.NoopAuthorizer",
                    DaemonConfig.SUPERVISOR_AUTHORIZER, "org.apache.storm.security.auth.authorizer.NoopAuthorizer"))
                .build()) {
            Nimbus nimbus = cluster.getNimbus();

            LogConfig previousConfig = new LogConfig();
            LogLevel prevLevel = new LogLevel();
            prevLevel.set_target_log_level("ERROR");
            prevLevel.set_action(LogLevelAction.UPDATE);
            previousConfig.put_to_named_logger_level("test", prevLevel);

            LogConfig expectedConfig = new LogConfig();
            LogLevel expectedLevel = new LogLevel();
            expectedLevel.set_target_log_level("ERROR");
            expectedLevel.set_action(LogLevelAction.UNCHANGED);
            expectedConfig.put_to_named_logger_level("test", expectedLevel);

            Mockito.when(tc.readTopoConf(Mockito.any(String.class), ArgumentMatchers.any())).thenReturn(Map.of());
            Mockito.when(clusterState.topologyLogConfig(Mockito.any(String.class), ArgumentMatchers.any())).thenReturn(previousConfig);

            LogConfig emptyConfig = new LogConfig();
            nimbus.setLogConfig("foo", emptyConfig);

            Mockito.verify(clusterState).setTopologyLogConfig(
                Mockito.any(String.class), Mockito.eq(expectedConfig), Mockito.any(Map.class));
        }
    }

    @Test
    public void logLevelUpdateMergesAndFlagsExistentLogLevel() throws Exception {
        IStormClusterState clusterState = Mockito.mock(IStormClusterState.class);
        BlobStore blobStore = Mockito.mock(BlobStore.class);
        TopoCache tc = Mockito.mock(TopoCache.class);

        try (LocalCluster cluster = new LocalCluster.Builder()
                .withClusterState(clusterState)
                .withBlobStore(blobStore)
                .withTopoCache(tc)
                .withDaemonConf(Map.of(
                    DaemonConfig.NIMBUS_AUTHORIZER, "org.apache.storm.security.auth.authorizer.NoopAuthorizer",
                    DaemonConfig.SUPERVISOR_AUTHORIZER, "org.apache.storm.security.auth.authorizer.NoopAuthorizer"))
                .build()) {
            Nimbus nimbus = cluster.getNimbus();

            LogConfig previousConfig = new LogConfig();
            LogLevel prevLevel1 = new LogLevel();
            prevLevel1.set_target_log_level("ERROR");
            prevLevel1.set_action(LogLevelAction.UPDATE);
            previousConfig.put_to_named_logger_level("test", prevLevel1);

            LogLevel prevLevel2 = new LogLevel();
            prevLevel2.set_target_log_level("DEBUG");
            prevLevel2.set_action(LogLevelAction.UPDATE);
            previousConfig.put_to_named_logger_level("other-test", prevLevel2);

            // only change "test"
            LogConfig mockConfig = new LogConfig();
            LogLevel mockLevel = new LogLevel();
            mockLevel.set_target_log_level("INFO");
            mockLevel.set_action(LogLevelAction.UPDATE);
            mockConfig.put_to_named_logger_level("test", mockLevel);

            LogConfig expectedConfig = new LogConfig();
            LogLevel expectedLevel1 = new LogLevel();
            expectedLevel1.set_target_log_level("INFO");
            expectedLevel1.set_action(LogLevelAction.UPDATE);
            expectedConfig.put_to_named_logger_level("test", expectedLevel1);

            LogLevel expectedLevel2 = new LogLevel();
            expectedLevel2.set_target_log_level("DEBUG");
            expectedLevel2.set_action(LogLevelAction.UNCHANGED);
            expectedConfig.put_to_named_logger_level("other-test", expectedLevel2);

            Mockito.when(tc.readTopoConf(Mockito.any(String.class), ArgumentMatchers.any())).thenReturn(Map.of());
            Mockito.when(clusterState.topologyLogConfig(Mockito.any(String.class), ArgumentMatchers.any())).thenReturn(previousConfig);

            nimbus.setLogConfig("foo", mockConfig);

            Mockito.verify(clusterState).setTopologyLogConfig(
                Mockito.any(String.class), Mockito.eq(expectedConfig), Mockito.any(Map.class));
        }
    }

    @Test
    public void cleanupStormIdsReturnsInactiveTopos() {
        IStormClusterState mockState = mockClusterState(List.of("topo1"), List.of("topo1", "topo2", "topo3"));
        BlobStore store = Mockito.mock(BlobStore.class);
        Mockito.when(store.storedTopoIds()).thenReturn(Set.of());
        Map<String, Object> conf = Map.of(DaemonConfig.NIMBUS_TOPOLOGY_BLOBSTORE_DELETION_DELAY_MS, 0);

        assertEquals(Set.of("topo2", "topo3"), Nimbus.topoIdsToClean(mockState, store, conf));
    }

    @Test
    public void cleanupStormIdsPerformsUnionOfStormIdsWithActiveZnodes() {
        List<String> activeTopos = List.of("hb1", "e2", "bp3");
        List<String> hbTopos = List.of("hb1", "hb2", "hb3");
        List<String> errorTopos = List.of("e1", "e2", "e3");
        List<String> bpTopos = List.of("bp1", "bp2", "bp3");
        IStormClusterState mockState = mockClusterState(activeTopos, hbTopos, errorTopos, bpTopos, null);
        BlobStore store = Mockito.mock(BlobStore.class);
        Mockito.when(store.storedTopoIds()).thenReturn(Set.of());

        assertEquals(Set.of("hb2", "hb3", "e1", "e3", "bp1", "bp2"),
            Nimbus.topoIdsToClean(mockState, store, Map.of(DaemonConfig.NIMBUS_TOPOLOGY_BLOBSTORE_DELETION_DELAY_MS, 0)));
    }

    @Test
    public void cleanupStormIdsReturnsEmptySetWhenAllToposAreActive() {
        List<String> activeTopos = List.of("hb1", "hb2", "hb3", "e1", "e2", "e3", "bp1", "bp2", "bp3");
        List<String> hbTopos = List.of("hb1", "hb2", "hb3");
        List<String> errorTopos = List.of("e1", "e2", "e3");
        List<String> bpTopos = List.of("bp1", "bp2", "bp3");
        IStormClusterState mockState = mockClusterState(activeTopos, hbTopos, errorTopos, bpTopos, null);
        BlobStore store = Mockito.mock(BlobStore.class);
        Mockito.when(store.storedTopoIds()).thenReturn(Set.of());

        assertEquals(Set.of(), Nimbus.topoIdsToClean(mockState, store, new HashMap<>()));
    }

    // --- Batch 7b: Cleanup, supervisor, submit-invalid, clean-inbox tests ---

    @Test
    public void doCleanupRemovesInactiveZnodes() throws Exception {
        IStormClusterState mockState = mockClusterState(null, null);
        BlobStore mockBlobStore = Mockito.mock(BlobStore.class);
        Map<String, Object> conf = new HashMap<>();
        conf.put(DaemonConfig.NIMBUS_MONITOR_FREQ_SECS, 10);
        conf.put(DaemonConfig.NIMBUS_TOPOLOGY_BLOBSTORE_DELETION_DELAY_MS, 0);
        conf.put(Config.NIMBUS_THRIFT_TLS_PORT, 0);

        Nimbus nimbus = Mockito.spy(new Nimbus(conf, null, mockState, null, mockBlobStore, null,
            mockLeaderElector(), null, new StormMetricsRegistry()));
        nimbus.getHeartbeatsCache().addEmptyTopoForTests("topo2");
        nimbus.getHeartbeatsCache().addEmptyTopoForTests("topo3");
        Mockito.when(mockBlobStore.storedTopoIds()).thenReturn(new HashSet<>(List.of("topo2", "topo3")));

        nimbus.doCleanup();

        // removed heartbeats znode
        Mockito.verify(mockState).teardownHeartbeats("topo2");
        Mockito.verify(mockState).teardownHeartbeats("topo3");

        // removed topo errors znode
        Mockito.verify(mockState).teardownTopologyErrors("topo2");
        Mockito.verify(mockState).teardownTopologyErrors("topo3");

        // removed topo directories
        Mockito.verify(nimbus).forceDeleteTopoDistDir("topo2");
        Mockito.verify(nimbus).forceDeleteTopoDistDir("topo3");

        // removed blob store topo keys
        Mockito.verify(nimbus).rmTopologyKeys("topo2");
        Mockito.verify(nimbus).rmTopologyKeys("topo3");

        // removed topology dependencies
        Mockito.verify(nimbus).rmDependencyJarsInTopology("topo2");
        Mockito.verify(nimbus).rmDependencyJarsInTopology("topo3");

        // remove topos from heartbeat cache
        assertEquals(0, nimbus.getHeartbeatsCache().getNumToposCached());
    }

    @Test
    public void doCleanupDoesNotTeardownActiveTopos() throws Exception {
        IStormClusterState mockState = mockClusterState(null, null);
        BlobStore mockBlobStore = Mockito.mock(BlobStore.class);
        Map<String, Object> conf = new HashMap<>();
        conf.put(DaemonConfig.NIMBUS_MONITOR_FREQ_SECS, 10);
        conf.put(Config.NIMBUS_THRIFT_TLS_PORT, 0);

        Nimbus nimbus = Mockito.spy(new Nimbus(conf, null, mockState, null, mockBlobStore, null,
            mockLeaderElector(), null, new StormMetricsRegistry()));
        nimbus.getHeartbeatsCache().addEmptyTopoForTests("topo1");
        nimbus.getHeartbeatsCache().addEmptyTopoForTests("topo2");
        Mockito.when(mockBlobStore.storedTopoIds()).thenReturn(Set.of());

        nimbus.doCleanup();

        Mockito.verify(mockState, Mockito.never()).teardownHeartbeats(Mockito.any());
        Mockito.verify(mockState, Mockito.never()).teardownTopologyErrors(Mockito.any());
        Mockito.verify(nimbus, Mockito.times(0)).forceDeleteTopoDistDir(ArgumentMatchers.any());
        Mockito.verify(nimbus, Mockito.times(0)).rmTopologyKeys(ArgumentMatchers.any());

        assertEquals(2, nimbus.getHeartbeatsCache().getNumToposCached());
        assertTrue(nimbus.getHeartbeatsCache().getTopologyIds().contains("topo1"));
        assertTrue(nimbus.getHeartbeatsCache().getTopologyIds().contains("topo2"));
    }

    @Test
    public void userTopologiesForSupervisor() throws Exception {
        Assignment assignment = new Assignment();
        assignment.set_executor_node_port(Map.of(
            List.of(1L, 1L), new NodeInfo("super1", Set.of(1L)),
            List.of(2L, 2L), new NodeInfo("super2", Set.of(2L))));

        Assignment assignment2 = new Assignment();
        assignment2.set_executor_node_port(Map.of(
            List.of(1L, 1L), new NodeInfo("super2", Set.of(2L)),
            List.of(2L, 2L), new NodeInfo("super2", Set.of(2L))));

        Map<String, Assignment> assignments = Map.of("topo1", assignment, "topo2", assignment2);

        IStormClusterState mockState = mockClusterState(null, null);
        BlobStore mockBlobStore = Mockito.mock(BlobStore.class);
        TopoCache mockTc = Mockito.mock(TopoCache.class);
        Map<String, Object> conf = new HashMap<>();
        conf.put(DaemonConfig.NIMBUS_MONITOR_FREQ_SECS, 10);
        conf.put(Config.NIMBUS_THRIFT_TLS_PORT, 0);
        Nimbus nimbus = new Nimbus(conf, null, mockState, null, mockBlobStore, mockTc,
            mockLeaderElector(), null, new StormMetricsRegistry());

        List<String> super1Topos = Nimbus.topologiesOnSupervisor(assignments, "super1");
        Set<String> user1Topos = nimbus.filterAuthorized("getTopology", super1Topos);
        assertEquals(List.of("topo1"), super1Topos);
        assertEquals(Set.of("topo1"), user1Topos);

        List<String> super2Topos = Nimbus.topologiesOnSupervisor(assignments, "super2");
        Set<String> user2Topos = nimbus.filterAuthorized("getTopology", super2Topos);
        assertEquals(new HashSet<>(List.of("topo1", "topo2")), new HashSet<>(super2Topos));
        assertEquals(Set.of("topo1", "topo2"), user2Topos);
    }

    @Test
    public void userTopologiesForSupervisorWithUnauthorizedUser() throws Exception {
        Assignment assignment = new Assignment();
        assignment.set_executor_node_port(Map.of(
            List.of(1L, 1L), new NodeInfo("super1", Set.of(1L)),
            List.of(2L, 2L), new NodeInfo("super2", Set.of(2L))));

        Assignment assignment2 = new Assignment();
        assignment2.set_executor_node_port(Map.of(
            List.of(1L, 1L), new NodeInfo("super1", Set.of(2L)),
            List.of(2L, 2L), new NodeInfo("super2", Set.of(2L))));

        Map<String, Assignment> assignments = Map.of("topo1", assignment, "authorized", assignment2);

        IStormClusterState mockState = mockClusterState(null, null);
        BlobStore mockBlobStore = Mockito.mock(BlobStore.class);
        TopoCache mockTc = Mockito.mock(TopoCache.class);
        Map<String, Object> conf = new HashMap<>();
        conf.put(DaemonConfig.NIMBUS_MONITOR_FREQ_SECS, 10);
        conf.put(Config.NIMBUS_THRIFT_TLS_PORT, 0);
        Nimbus nimbus = new Nimbus(conf, null, mockState, null, mockBlobStore, mockTc,
            mockLeaderElector(), null, new StormMetricsRegistry());

        Mockito.when(mockTc.readTopoConf(Mockito.eq("authorized"), ArgumentMatchers.any()))
            .thenReturn(Map.of(Config.TOPOLOGY_NAME, "authorized"));
        Mockito.when(mockTc.readTopoConf(Mockito.eq("topo1"), ArgumentMatchers.any()))
            .thenReturn(Map.of(Config.TOPOLOGY_NAME, "topo1"));

        nimbus.setAuthorizationHandler(new IAuthorizer() {
            @Override
            public void prepare(Map<String, Object> conf) {}

            @Override
            public boolean permit(org.apache.storm.security.auth.ReqContext context, String operation, Map<String, Object> topoConf) {
                return "authorized".equals(topoConf.get(Config.TOPOLOGY_NAME));
            }
        });

        List<String> superTopos = Nimbus.topologiesOnSupervisor(assignments, "super1");
        Set<String> userTopos = nimbus.filterAuthorized("getTopology", superTopos);

        assertEquals(new HashSet<>(List.of("topo1", "authorized")), new HashSet<>(superTopos));
        assertEquals(Set.of("authorized"), userTopos);
    }

    @Test
    public void testCleanInbox() throws Exception {
        try (Time.SimulatedTime ignored = new Time.SimulatedTime();
             TmpPath tmpPath = new TmpPath()) {
            String dirLocation = tmpPath.getPath();

            Time.advanceTimeSecs(100);
            createFile(dirLocation, "a.jar", 20);
            createFile(dirLocation, "b.jar", 20);
            createFile(dirLocation, "c.jar", 0);

            assertJarFiles(dirLocation, "a.jar", "b.jar", "c.jar");
            Nimbus.cleanInbox(dirLocation, 10);
            assertJarFiles(dirLocation, "c.jar");

            // Clean again, c.jar should stay
            Time.advanceTimeSecs(5);
            Nimbus.cleanInbox(dirLocation, 10);
            assertJarFiles(dirLocation, "c.jar");

            // Advance time, clean again, c.jar should be deleted
            Time.advanceTimeSecs(5);
            Nimbus.cleanInbox(dirLocation, 10);
            assertJarFiles(dirLocation);
        }
    }

    @Test
    public void testSubmitInvalid() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
                .withSimulatedTime()
                .withDaemonConf(Map.of(
                    DaemonConfig.SUPERVISOR_ENABLE, false,
                    Config.TOPOLOGY_ACKER_EXECUTORS, 0,
                    Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, 0,
                    DaemonConfig.NIMBUS_EXECUTORS_PER_TOPOLOGY, 8,
                    DaemonConfig.NIMBUS_SLOTS_PER_TOPOLOGY, 8))
                .build()) {
            // Invalid topology name with slash
            StormTopology topology = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(new TestPlannerSpout(true), 1, Map.of(Config.TOPOLOGY_TASKS, 1))),
                Map.of());
            assertThrows(InvalidTopologyException.class, () ->
                cluster.submitTopology("test/aaa", Map.of(), topology));

            // Too many executors
            StormTopology topology2 = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(new TestPlannerSpout(true), 16, Map.of(Config.TOPOLOGY_TASKS, 16))),
                Map.of());
            assertThrows(InvalidTopologyException.class, () ->
                cluster.submitTopology("test", Map.of(Config.TOPOLOGY_WORKERS, 3), topology2));

            // Too many workers
            StormTopology topology3 = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(new TestPlannerSpout(true), 5, Map.of(Config.TOPOLOGY_TASKS, 5))),
                Map.of());
            assertThrows(InvalidTopologyException.class, () ->
                cluster.submitTopology("test", Map.of(Config.TOPOLOGY_WORKERS, 16), topology3));
        }
    }

    // --- Helper methods ---

    private static void createFile(String dirLocation, String name, int secondsAgo) throws IOException {
        File f = new File(dirLocation + "/" + name);
        FileUtils.touch(f);
        long t = Time.currentTimeMillis() - (secondsAgo * 1000L);
        f.setLastModified(t);
    }

    private static void assertJarFiles(String dirLocation, String... expectedFiles) {
        File dir = new File(dirLocation);
        Set<String> actual = Arrays.stream(dir.listFiles())
            .map(File::getName)
            .filter(name -> name.endsWith(".jar"))
            .collect(Collectors.toSet());
        assertEquals(Set.of(expectedFiles), actual);
    }

    private static ILeaderElector mockLeaderElector() {
        ILeaderElector elector = Mockito.mock(ILeaderElector.class);
        try {
            Mockito.when(elector.isLeader()).thenReturn(true);
            Mockito.when(elector.getLeader()).thenReturn(new NimbusInfo("test-host", 9999, false));
            Mockito.when(elector.getAllNimbuses()).thenReturn(List.of());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return elector;
    }

    private static IStormClusterState mockClusterState(List<String> activeTopos, List<String> inactiveTopos) {
        return mockClusterState(activeTopos, inactiveTopos, inactiveTopos, inactiveTopos, null);
    }

    private static IStormClusterState mockClusterState(
            List<String> activeTopos, List<String> hbTopos,
            List<String> errorTopos, List<String> bpTopos,
            List<String> wtTopos) {
        IStormClusterState state = Mockito.mock(IStormClusterState.class);
        Mockito.when(state.activeStorms()).thenReturn(activeTopos);
        Mockito.when(state.heartbeatStorms()).thenReturn(hbTopos);
        Mockito.when(state.errorTopologies()).thenReturn(errorTopos);
        Mockito.when(state.backpressureTopologies()).thenReturn(bpTopos);
        Mockito.when(state.idsOfTopologiesWithPrivateWorkerKeys())
            .thenReturn(wtTopos != null ? new HashSet<>(wtTopos) : Set.of());
        return state;
    }
}
