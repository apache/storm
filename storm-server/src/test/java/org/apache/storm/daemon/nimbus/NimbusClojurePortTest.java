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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import org.apache.storm.daemon.StormCommon;
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
import org.apache.storm.generated.OwnerResourceSummary;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SubmitOptions;
import org.apache.storm.generated.TopologyInitialStatus;
import org.apache.storm.generated.TopologyStatus;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.IGroupMappingServiceProvider;
import org.apache.storm.testing.TestPlannerBolt;
import org.apache.storm.testing.TestPlannerSpout;
import org.apache.storm.testing.TmpPath;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;

import net.minidev.json.JSONValue;
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

    // --- Batch 7c: Assignment/scheduling tests ---

    @Test
    public void testAssignment() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
                .withSimulatedTime()
                .withSupervisors(4)
                .withDaemonConf(Map.of(
                    DaemonConfig.SUPERVISOR_ENABLE, false,
                    Config.TOPOLOGY_ACKER_EXECUTORS, 0,
                    Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, 0))
                .build()) {
            IStormClusterState state = cluster.getClusterState();

            StormTopology topology = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(new TestPlannerSpout(false), 3)),
                Map.of("2", Thrift.prepareBoltDetails(
                            Map.of(Utils.getGlobalStreamId("1", null), Thrift.prepareNoneGrouping()),
                            new TestPlannerBolt(), 4),
                       "3", Thrift.prepareBoltDetails(
                            Map.of(Utils.getGlobalStreamId("2", null), Thrift.prepareNoneGrouping()),
                            new TestPlannerBolt())));

            StormTopology topology2 = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(new TestPlannerSpout(true), 12)),
                Map.of("2", Thrift.prepareBoltDetails(
                            Map.of(Utils.getGlobalStreamId("1", null), Thrift.prepareNoneGrouping()),
                            new TestPlannerBolt(), 6),
                       "3", Thrift.prepareBoltDetails(
                            Map.of(Utils.getGlobalStreamId("1", null), Thrift.prepareGlobalGrouping()),
                            new TestPlannerBolt(), 8),
                       "4", Thrift.prepareBoltDetails(
                            Map.of(Utils.getGlobalStreamId("1", null), Thrift.prepareGlobalGrouping(),
                                   Utils.getGlobalStreamId("2", null), Thrift.prepareNoneGrouping()),
                            new TestPlannerBolt(), 4)));

            cluster.submitTopology("mystorm", Map.of(Config.TOPOLOGY_WORKERS, 4), topology);
            cluster.advanceClusterTime(11);

            Map<String, List<Integer>> taskInfo = stormComponentToTaskInfo(cluster, "mystorm");
            checkConsistency(cluster, "mystorm", true);

            assertEquals(1, state.assignments(null).size());
            assertEquals(1, taskInfo.get("1").size());
            assertEquals(4, taskInfo.get("2").size());
            assertEquals(1, taskInfo.get("3").size());
            assertEquals(4, stormNumWorkers(state, "mystorm"));

            cluster.submitTopology("storm2", Map.of(Config.TOPOLOGY_WORKERS, 20), topology2);
            cluster.advanceClusterTime(11);
            checkConsistency(cluster, "storm2", true);

            assertEquals(2, state.assignments(null).size());
            Map<String, List<Integer>> taskInfo2 = stormComponentToTaskInfo(cluster, "storm2");
            assertEquals(12, taskInfo2.get("1").size());
            assertEquals(6, taskInfo2.get("2").size());
            assertEquals(8, taskInfo2.get("3").size());
            assertEquals(4, taskInfo2.get("4").size());
            assertEquals(8, stormNumWorkers(state, "storm2"));
        }
    }

    @Test
    public void testZeroExecutorOrTasks() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
                .withSimulatedTime()
                .withSupervisors(6)
                .withDaemonConf(Map.of(
                    DaemonConfig.SUPERVISOR_ENABLE, false,
                    Config.TOPOLOGY_ACKER_EXECUTORS, 0,
                    Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, 0))
                .build()) {
            IStormClusterState state = cluster.getClusterState();

            StormTopology topology = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(new TestPlannerSpout(false), 3,
                            Map.of(Config.TOPOLOGY_TASKS, 0))),
                Map.of("2", Thrift.prepareBoltDetails(
                            Map.of(Utils.getGlobalStreamId("1", null), Thrift.prepareNoneGrouping()),
                            new TestPlannerBolt(), 1, Map.of(Config.TOPOLOGY_TASKS, 2)),
                       "3", Thrift.prepareBoltDetails(
                            Map.of(Utils.getGlobalStreamId("2", null), Thrift.prepareNoneGrouping()),
                            new TestPlannerBolt(), null, Map.of(Config.TOPOLOGY_TASKS, 5))));

            cluster.submitTopology("mystorm", Map.of(Config.TOPOLOGY_WORKERS, 4), topology);
            cluster.advanceClusterTime(11);

            Map<String, List<Integer>> taskInfo = stormComponentToTaskInfo(cluster, "mystorm");
            checkConsistency(cluster, "mystorm", true);

            assertEquals(0, taskInfo.getOrDefault("1", List.of()).size());
            assertEquals(2, taskInfo.get("2").size());
            assertEquals(5, taskInfo.get("3").size());
            assertEquals(2, stormNumWorkers(state, "mystorm")); // only 2 executors
        }
    }

    @Test
    public void testOverParallelismAssignment() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
                .withSimulatedTime()
                .withSupervisors(2)
                .withPortsPerSupervisor(5)
                .withDaemonConf(Map.of(
                    DaemonConfig.SUPERVISOR_ENABLE, false,
                    Config.TOPOLOGY_ACKER_EXECUTORS, 0,
                    Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, 0))
                .build()) {
            IStormClusterState state = cluster.getClusterState();

            StormTopology topology = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(new TestPlannerSpout(true), 21)),
                Map.of("2", Thrift.prepareBoltDetails(
                            Map.of(Utils.getGlobalStreamId("1", null), Thrift.prepareNoneGrouping()),
                            new TestPlannerBolt(), 9),
                       "3", Thrift.prepareBoltDetails(
                            Map.of(Utils.getGlobalStreamId("1", null), Thrift.prepareNoneGrouping()),
                            new TestPlannerBolt(), 2),
                       "4", Thrift.prepareBoltDetails(
                            Map.of(Utils.getGlobalStreamId("1", null), Thrift.prepareNoneGrouping()),
                            new TestPlannerBolt(), 10)));

            cluster.submitTopology("test", Map.of(Config.TOPOLOGY_WORKERS, 7), topology);
            cluster.advanceClusterTime(11);

            Map<String, List<Integer>> taskInfo = stormComponentToTaskInfo(cluster, "test");
            checkConsistency(cluster, "test", true);

            assertEquals(21, taskInfo.get("1").size());
            assertEquals(9, taskInfo.get("2").size());
            assertEquals(2, taskInfo.get("3").size());
            assertEquals(10, taskInfo.get("4").size());
            assertEquals(7, stormNumWorkers(state, "test"));
        }
    }

    @Test
    public void testGetOwnerResourceSummaries() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
                .withSimulatedTime()
                .withSupervisors(1)
                .withPortsPerSupervisor(12)
                .withDaemonConf(Map.of(
                    DaemonConfig.SUPERVISOR_ENABLE, false,
                    DaemonConfig.NIMBUS_MONITOR_FREQ_SECS, 10,
                    Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 30,
                    Config.TOPOLOGY_ACKER_EXECUTORS, 0,
                    Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, 0))
                .build()) {
            Nimbus nimbus = cluster.getNimbus();

            // test for 0-topology case
            cluster.advanceClusterTime(11);
            List<OwnerResourceSummary> summaries = nimbus.getOwnerResourceSummaries(null);
            assertTrue(summaries.isEmpty());

            // test for 1-topology case
            StormTopology topology = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(new TestPlannerSpout(true), 3)),
                Map.of());
            cluster.submitTopology("test", Map.of(
                Config.TOPOLOGY_WORKERS, 3,
                Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 90), topology);
            cluster.advanceClusterTime(11);

            summaries = nimbus.getOwnerResourceSummaries(null);
            OwnerResourceSummary summary = summaries.get(0);
            assertEquals(3, summary.get_total_workers());
            assertEquals(3, summary.get_total_executors());
            assertEquals(1, summary.get_total_topologies());

            // test for many-topology case
            StormTopology topology2 = Thrift.buildTopology(
                Map.of("2", Thrift.prepareSpoutDetails(new TestPlannerSpout(true), 4)),
                Map.of());
            StormTopology topology3 = Thrift.buildTopology(
                Map.of("3", Thrift.prepareSpoutDetails(new TestPlannerSpout(true), 5)),
                Map.of());

            cluster.submitTopology("test2", Map.of(
                Config.TOPOLOGY_WORKERS, 4, Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 90), topology2);
            cluster.submitTopology("test3", Map.of(
                Config.TOPOLOGY_WORKERS, 3, Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 90), topology3);
            cluster.advanceClusterTime(11);

            summaries = nimbus.getOwnerResourceSummaries(null);
            summary = summaries.get(0);
            assertEquals(10, summary.get_total_workers());
            assertEquals(12, summary.get_total_executors());
            assertEquals(3, summary.get_total_topologies());

            // test for specific owner
            summaries = nimbus.getOwnerResourceSummaries(System.getProperty("user.name"));
            summary = summaries.get(0);
            assertEquals(10, summary.get_total_workers());
            assertEquals(12, summary.get_total_executors());
            assertEquals(3, summary.get_total_topologies());

            // test for other user
            String otherUser = "not-" + System.getProperty("user.name");
            summaries = nimbus.getOwnerResourceSummaries(otherUser);
            summary = summaries.get(0);
            assertEquals(0, summary.get_total_workers());
            assertEquals(0, summary.get_total_executors());
            assertEquals(0, summary.get_total_topologies());
        }
    }

    @Test
    public void testAutoCredentials() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
                .withSimulatedTime()
                .withSupervisors(6)
                .withDaemonConf(Map.of(
                    DaemonConfig.SUPERVISOR_ENABLE, false,
                    Config.TOPOLOGY_ACKER_EXECUTORS, 0,
                    Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, 0,
                    DaemonConfig.NIMBUS_CREDENTIAL_RENEW_FREQ_SECS, 10,
                    Config.NIMBUS_CREDENTIAL_RENEWERS, List.of("org.apache.storm.MockAutoCred"),
                    Config.NIMBUS_AUTO_CRED_PLUGINS, List.of("org.apache.storm.MockAutoCred")))
                .build()) {
            IStormClusterState state = cluster.getClusterState();
            String topologyName = "test-auto-cred-storm";

            SubmitOptions submitOptions = new SubmitOptions(TopologyInitialStatus.INACTIVE);
            submitOptions.set_creds(new Credentials(new HashMap<>()));

            StormTopology topology = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(new TestPlannerSpout(false), 3)),
                Map.of("2", Thrift.prepareBoltDetails(
                            Map.of(Utils.getGlobalStreamId("1", null), Thrift.prepareNoneGrouping()),
                            new TestPlannerBolt(), 4),
                       "3", Thrift.prepareBoltDetails(
                            Map.of(Utils.getGlobalStreamId("2", null), Thrift.prepareNoneGrouping()),
                            new TestPlannerBolt())));

            cluster.submitTopologyWithOpts(topologyName, Map.of(
                Config.TOPOLOGY_WORKERS, 4,
                Config.TOPOLOGY_AUTO_CREDENTIALS, List.of("org.apache.storm.MockAutoCred")),
                topology, submitOptions);

            Map<String, String> credentials = getCredentials(cluster, topologyName);
            // check that the credentials have nimbus auto generated cred
            assertEquals("nimbusTestCred", credentials.get("nimbusCredTestKey"));

            // advance cluster time so the renewers can execute
            cluster.advanceClusterTime(20);

            // check that renewed credentials replace the original credential
            Map<String, String> renewedCreds = getCredentials(cluster, topologyName);
            assertEquals("renewedNimbusTestCred", renewedCreds.get("nimbusCredTestKey"));
            assertEquals("renewedGatewayTestCred", renewedCreds.get("gatewayCredTestKey"));
        }
    }

    // --- Helper methods ---

    // --- Cluster state helpers (ported from Clojure nimbus_test.clj) ---

    @SuppressWarnings("unchecked")
    private static Map<String, Object> fromJson(String str) {
        if (str == null) return null;
        return (Map<String, Object>) JSONValue.parse(str);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, List<Integer>> stormComponentToTaskInfo(LocalCluster cluster, String stormName) throws Exception {
        IStormClusterState state = cluster.getClusterState();
        String stormId = state.getTopoId(stormName).get();
        Nimbus nimbus = cluster.getNimbus();
        StormTopology userTopology = nimbus.getUserTopology(stormId);
        Map<String, Object> topoConf = fromJson(nimbus.getTopologyConf(stormId));
        Map<Integer, String> taskToComponent = StormCommon.stormTaskInfo(userTopology, topoConf);

        // reverse: component -> list of task ids
        Map<String, List<Integer>> result = new HashMap<>();
        for (Map.Entry<Integer, String> entry : taskToComponent.entrySet()) {
            result.computeIfAbsent(entry.getValue(), k -> new java.util.ArrayList<>()).add(entry.getKey());
        }
        return result;
    }

    private static int stormNumWorkers(IStormClusterState state, String stormName) {
        String stormId = state.getTopoId(stormName).get();
        Assignment assignment = state.assignmentInfo(stormId, null);
        return Utils.reverseMap(assignment.get_executor_node_port()).size();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> getCredentials(LocalCluster cluster, String stormName) {
        IStormClusterState state = cluster.getClusterState();
        String stormId = state.getTopoId(stormName).get();
        Credentials creds = state.credentials(stormId, null);
        if (creds == null) return null;
        return new HashMap<>(creds.get_creds());
    }

    private static void checkConsistency(LocalCluster cluster, String stormName, boolean shouldBeAssigned) throws Exception {
        IStormClusterState state = cluster.getClusterState();
        String stormId = state.getTopoId(stormName).get();
        Nimbus nimbus = cluster.getNimbus();

        StormTopology userTopology = nimbus.getUserTopology(stormId);
        Map<String, Object> topoConf = fromJson(nimbus.getTopologyConf(stormId));
        Map<Integer, String> taskToComponent = StormCommon.stormTaskInfo(userTopology, topoConf);
        Set<Integer> taskIds = taskToComponent.keySet();

        Assignment assignment = state.assignmentInfo(stormId, null);
        Map<List<Long>, NodeInfo> executorNodePort = assignment.get_executor_node_port();

        // Collect assigned tasks
        Set<Integer> assignedTasks = new HashSet<>();
        for (List<Long> executor : executorNodePort.keySet()) {
            long start = executor.get(0);
            long end = executor.get(1);
            for (long t = start; t <= end; t++) {
                assignedTasks.add((int) t);
            }
        }

        if (shouldBeAssigned) {
            assertEquals(taskIds, assignedTasks);
            Map<Integer, NodeInfo> taskToNodePort = StormCommon.taskToNodeport(executorNodePort);
            for (int t : taskIds) {
                assertNotNull(taskToNodePort.get(t));
            }
        }

        // All node+ports should be non-null
        for (NodeInfo np : executorNodePort.values()) {
            assertNotNull(np);
        }

        // All nodes should be in node_host
        Set<String> allNodes = new HashSet<>();
        for (NodeInfo np : executorNodePort.values()) {
            allNodes.add(np.get_node());
        }
        assertEquals(allNodes, assignment.get_node_host().keySet());

        // All executors should have start times
        for (List<Long> executor : executorNodePort.keySet()) {
            assertNotNull(assignment.get_executor_start_time_secs().get(executor));
        }
    }

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

    private static ILeaderElector mockLeaderElector(boolean isLeader) {
        ILeaderElector elector = Mockito.mock(ILeaderElector.class);
        try {
            Mockito.when(elector.isLeader()).thenReturn(isLeader);
            Mockito.when(elector.getLeader()).thenReturn(new NimbusInfo("test-host", 9999, false));
            Mockito.when(elector.getAllNimbuses()).thenReturn(List.of());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return elector;
    }

    // --- Additional helpers for batch 7d/7e ---

    private static Set<String> topologyNodes(IStormClusterState state, String stormName) {
        String stormId = state.getTopoId(stormName).get();
        Assignment assignment = state.assignmentInfo(stormId, null);
        Set<String> nodes = new HashSet<>();
        for (NodeInfo np : assignment.get_executor_node_port().values()) {
            nodes.add(np.get_node());
        }
        return nodes;
    }

    private static int topologyNumNodes(IStormClusterState state, String stormName) {
        return topologyNodes(state, stormName).size();
    }

    private static Set<NodeInfo> topologySlots(IStormClusterState state, String stormName) {
        String stormId = state.getTopoId(stormName).get();
        Assignment assignment = state.assignmentInfo(stormId, null);
        return new HashSet<>(assignment.get_executor_node_port().values());
    }

    /** Returns a map like {nodeCount: numberOfNodesWithThatCount}. */
    private static Map<Integer, Integer> topologyNodeDistribution(IStormClusterState state, String stormName) {
        String stormId = state.getTopoId(stormName).get();
        Assignment assignment = state.assignmentInfo(stormId, null);
        Set<NodeInfo> slots = new HashSet<>(assignment.get_executor_node_port().values());
        Map<String, Integer> nodeToSlotCount = new HashMap<>();
        for (NodeInfo np : slots) {
            nodeToSlotCount.merge(np.get_node(), 1, Integer::sum);
        }
        Map<Integer, Integer> distribution = new HashMap<>();
        for (int count : nodeToSlotCount.values()) {
            distribution.merge(count, 1, Integer::sum);
        }
        return distribution;
    }

    private static NodeInfo executorAssignment(LocalCluster cluster, String stormId, List<Long> executorId) {
        IStormClusterState state = cluster.getClusterState();
        Assignment assignment = state.assignmentInfo(stormId, null);
        return assignment.get_executor_node_port().get(executorId);
    }

    private static Map<List<Long>, Long> executorStartTimes(LocalCluster cluster, String stormId) {
        IStormClusterState state = cluster.getClusterState();
        Assignment assignment = state.assignmentInfo(stormId, null);
        return assignment.get_executor_start_time_secs();
    }

    private static List<List<Long>> topologyExecutors(LocalCluster cluster, String stormId) {
        IStormClusterState state = cluster.getClusterState();
        Assignment assignment = state.assignmentInfo(stormId, null);
        return new ArrayList<>(assignment.get_executor_node_port().keySet());
    }

    /** Reverse map: NodeInfo -> List<List<Long>> (slot -> executors). */
    @SuppressWarnings("unchecked")
    private static Map<NodeInfo, List<List<Long>>> slotAssignments(LocalCluster cluster, String stormId) {
        IStormClusterState state = cluster.getClusterState();
        Assignment assignment = state.assignmentInfo(stormId, null);
        Map<NodeInfo, List<List<Long>>> result = new HashMap<>();
        for (Map.Entry<List<Long>, NodeInfo> entry : assignment.get_executor_node_port().entrySet()) {
            result.computeIfAbsent(entry.getValue(), k -> new ArrayList<>()).add(entry.getKey());
        }
        return result;
    }

    private static void doExecutorHeartbeat(LocalCluster cluster, String stormId, List<Long> executor) throws Exception {
        IStormClusterState state = cluster.getClusterState();
        Map<List<Long>, NodeInfo> executorNodePort = state.assignmentInfo(stormId, null).get_executor_node_port();
        NodeInfo np = executorNodePort.get(executor);
        String node = np.get_node();
        long port = np.get_port().iterator().next();

        org.apache.storm.stats.StatsUtil.convertZkWorkerHb(
            state.getWorkerHeartbeat(stormId, node, port));

        Map<List<Integer>, org.apache.storm.generated.ExecutorStats> stats = new HashMap<>();
        stats.put(org.apache.storm.stats.ClientStatsUtil.convertExecutor(executor),
            new org.apache.storm.stats.BoltExecutorStats(20,
                ((Number) ConfigUtils.readStormConfig().getOrDefault(Config.NUM_STAT_BUCKETS, 20)).intValue()
            ).renderStats());

        state.workerHeartbeat(stormId, node, port,
            org.apache.storm.stats.ClientStatsUtil.thriftifyZkWorkerHb(
                org.apache.storm.stats.ClientStatsUtil.mkZkWorkerHb(stormId, stats, 10)));
        cluster.getNimbus().sendSupervisorWorkerHeartbeat(
            org.apache.storm.stats.StatsUtil.thriftifyRpcWorkerHb(stormId, executor));
    }

    private static void checkDistribution(Collection<? extends Collection<?>> items, List<Integer> expectedDistribution) {
        List<Long> counts = new ArrayList<>();
        for (Collection<?> item : items) {
            counts.add((long) item.size());
        }
        List<Long> expected = expectedDistribution.stream().map(Long::valueOf).collect(Collectors.toList());
        java.util.Collections.sort(counts);
        java.util.Collections.sort(expected);
        assertEquals(expected, counts);
    }

    private static void checkExecutorDistribution(Map<NodeInfo, List<List<Long>>> slotExecutors, List<Integer> distribution) {
        checkDistribution(slotExecutors.values(), distribution);
    }

    private static void checkNumNodes(Map<NodeInfo, List<List<Long>>> slotExecutors, int numNodes) {
        Set<String> nodes = new HashSet<>();
        for (NodeInfo np : slotExecutors.keySet()) {
            nodes.add(np.get_node());
        }
        assertEquals(numNodes, nodes.size());
    }

    private static void checkForCollisions(IStormClusterState state) {
        List<String> assignments = state.assignments(null);
        Map<String, Map<String, Set<Long>>> idToNodeToPorts = new HashMap<>();
        for (String id : assignments) {
            Assignment a = state.assignmentInfo(id, null);
            Map<List<Long>, NodeInfo> executorNodePort = a.get_executor_node_port();
            Set<NodeInfo> nodePorts = new HashSet<>(executorNodePort.values());
            Map<String, Set<Long>> nodeToPorts = new HashMap<>();
            for (NodeInfo np : nodePorts) {
                nodeToPorts.computeIfAbsent(np.get_node(), k -> new HashSet<>()).addAll(np.get_port());
            }
            idToNodeToPorts.put(id, nodeToPorts);
        }
        // Check all topologies have disjoint ports on the same nodes
        Map<String, Set<Long>> combined = new HashMap<>();
        for (Map<String, Set<Long>> nodeToPorts : idToNodeToPorts.values()) {
            for (Map.Entry<String, Set<Long>> entry : nodeToPorts.entrySet()) {
                Set<Long> existing = combined.computeIfAbsent(entry.getKey(), k -> new HashSet<>());
                for (Long port : entry.getValue()) {
                    assertTrue(existing.add(port), "Port collision on node " + entry.getKey() + " port " + port);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, List<List<Long>>> stormComponentToExecutorInfo(LocalCluster cluster, String stormName) throws Exception {
        IStormClusterState state = cluster.getClusterState();
        String stormId = state.getTopoId(stormName).get();
        Nimbus nimbus = cluster.getNimbus();
        Map<String, Object> stormConf = fromJson(nimbus.getTopologyConf(stormId));
        StormTopology topology = nimbus.getUserTopology(stormId);
        Map<Integer, String> taskToComponent = StormCommon.stormTaskInfo(topology, stormConf);
        Assignment assignment = state.assignmentInfo(stormId, null);

        // executor -> component (using first task of executor)
        Map<List<Long>, String> executorToComponent = new HashMap<>();
        for (List<Long> executor : assignment.get_executor_node_port().keySet()) {
            int firstTask = executor.get(0).intValue();
            executorToComponent.put(executor, taskToComponent.get(firstTask));
        }

        // reverse: component -> list of executors
        Map<String, List<List<Long>>> result = new HashMap<>();
        for (Map.Entry<List<Long>, String> entry : executorToComponent.entrySet()) {
            result.computeIfAbsent(entry.getValue(), k -> new ArrayList<>()).add(entry.getKey());
        }
        return result;
    }

    private static List<Integer> executorToTasks(List<Long> executorId) {
        return StormCommon.executorIdToTasks(executorId);
    }

    private static Nimbus mkNimbus(Map<String, Object> conf, INimbus inimbus,
            BlobStore blobStore, ILeaderElector leaderElector,
            IGroupMappingServiceProvider groupMapper, IStormClusterState clusterState) throws Exception {
        Map<String, Object> confWithMonitor = new HashMap<>(conf);
        confWithMonitor.putIfAbsent(DaemonConfig.NIMBUS_MONITOR_FREQ_SECS, 10);
        return new Nimbus(confWithMonitor, inimbus, clusterState, null, blobStore, null,
            leaderElector, groupMapper, new StormMetricsRegistry());
    }

    private static void waitForStatus(Nimbus nimbus, String name, String status) throws Exception {
        org.apache.storm.Testing.whileTimeout(5000, () -> {
            try {
                for (TopologySummary topo : nimbus.getClusterInfo().get_topologies()) {
                    if (name.equals(topo.get_name())) {
                        return !status.equals(topo.get_status());
                    }
                }
                return true; // not found yet, keep waiting
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, () -> {
            try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        });
    }

    // --- Batch 7d: Assignment, scheduling, and rebalance tests ---

    @Test
    public void testIsolatedAssignment() throws Exception {
        INimbus standalone = new Nimbus.StandaloneINimbus();
        INimbus isolationNimbus = new INimbus() {
            @Override public void prepare(Map<String, Object> topoConf, String schedulerLocalDir) {
                standalone.prepare(topoConf, schedulerLocalDir);
            }
            @Override public Collection<org.apache.storm.scheduler.WorkerSlot> allSlotsAvailableForScheduling(
                    Collection<org.apache.storm.scheduler.SupervisorDetails> supervisors,
                    org.apache.storm.scheduler.Topologies topologies,
                    Set<String> topologiesMissingAssignments) {
                return standalone.allSlotsAvailableForScheduling(supervisors, topologies, topologiesMissingAssignments);
            }
            @Override public void assignSlots(org.apache.storm.scheduler.Topologies topologies,
                    Map<String, Collection<org.apache.storm.scheduler.WorkerSlot>> newSlotsByTopologyId) {
                standalone.assignSlots(topologies, newSlotsByTopologyId);
            }
            @Override public String getHostName(Map<String, org.apache.storm.scheduler.SupervisorDetails> supervisors,
                    String nodeId) {
                return nodeId;
            }
            @Override public org.apache.storm.scheduler.IScheduler getForcedScheduler() {
                return standalone.getForcedScheduler();
            }
        };

        try (LocalCluster cluster = new LocalCluster.Builder()
                .withSimulatedTime()
                .withSupervisors(6)
                .withINimbus(isolationNimbus)
                .withDaemonConf(Map.of(
                    DaemonConfig.SUPERVISOR_ENABLE, false,
                    Config.TOPOLOGY_ACKER_EXECUTORS, 0,
                    Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, 0,
                    DaemonConfig.STORM_SCHEDULER, "org.apache.storm.scheduler.IsolationScheduler",
                    DaemonConfig.ISOLATION_SCHEDULER_MACHINES, Map.of("tester1", 3, "tester2", 2),
                    DaemonConfig.NIMBUS_MONITOR_FREQ_SECS, 10))
                .build()) {
            IStormClusterState state = cluster.getClusterState();

            StormTopology topology = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(new TestPlannerSpout(false), 3)),
                Map.of("2", Thrift.prepareBoltDetails(
                            Map.of(Utils.getGlobalStreamId("1", null), Thrift.prepareNoneGrouping()),
                            new TestPlannerBolt(), 5),
                       "3", Thrift.prepareBoltDetails(
                            Map.of(Utils.getGlobalStreamId("2", null), Thrift.prepareNoneGrouping()),
                            new TestPlannerBolt(), null)));

            cluster.submitTopology("noniso", Map.of(Config.TOPOLOGY_WORKERS, 4), topology);
            cluster.advanceClusterTime(11);
            assertEquals(4, topologyNumNodes(state, "noniso"));
            assertEquals(4, stormNumWorkers(state, "noniso"));

            cluster.submitTopology("tester1", Map.of(Config.TOPOLOGY_WORKERS, 6), topology);
            cluster.submitTopology("tester2", Map.of(Config.TOPOLOGY_WORKERS, 6), topology);
            cluster.advanceClusterTime(11);

            assertEquals(1, topologyNumNodes(state, "noniso"));
            assertEquals(3, stormNumWorkers(state, "noniso"));

            assertEquals(Map.of(2, 3), topologyNodeDistribution(state, "tester1"));
            assertEquals(Map.of(3, 2), topologyNodeDistribution(state, "tester2"));

            // Check disjoint nodes
            Set<String> allNodesSeen = new HashSet<>();
            for (String name : List.of("noniso", "tester1", "tester2")) {
                Set<String> nodes = topologyNodes(state, name);
                for (String n : nodes) {
                    assertTrue(allNodesSeen.add(n), "Node " + n + " used by multiple topologies");
                }
            }

            checkConsistency(cluster, "tester1", true);
            checkConsistency(cluster, "tester2", true);
            checkConsistency(cluster, "noniso", true);

            // Check that nothing gets reassigned
            Set<NodeInfo> tester1Slots = topologySlots(state, "tester1");
            Set<NodeInfo> tester2Slots = topologySlots(state, "tester2");
            Set<NodeInfo> nonisoSlots = topologySlots(state, "noniso");
            cluster.advanceClusterTime(20);
            assertEquals(tester1Slots, topologySlots(state, "tester1"));
            assertEquals(tester2Slots, topologySlots(state, "tester2"));
            assertEquals(nonisoSlots, topologySlots(state, "noniso"));
        }
    }

    @Test
    public void testExecutorAssignments() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
                .withSimulatedTime()
                .withDaemonConf(Map.of(
                    DaemonConfig.SUPERVISOR_ENABLE, false,
                    Config.TOPOLOGY_ACKER_EXECUTORS, 0,
                    Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, 0))
                .build()) {

            StormTopology topology = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(new TestPlannerSpout(true), 3, Map.of(Config.TOPOLOGY_TASKS, 5))),
                Map.of("2", Thrift.prepareBoltDetails(
                            Map.of(Utils.getGlobalStreamId("1", null), Thrift.prepareNoneGrouping()),
                            new TestPlannerBolt(), 8, Map.of(Config.TOPOLOGY_TASKS, 2)),
                       "3", Thrift.prepareBoltDetails(
                            Map.of(Utils.getGlobalStreamId("2", null), Thrift.prepareNoneGrouping()),
                            new TestPlannerBolt(), 3)));

            cluster.submitTopology("mystorm", Map.of(Config.TOPOLOGY_WORKERS, 4), topology);
            cluster.advanceClusterTime(11);

            Map<String, List<Integer>> taskInfo = stormComponentToTaskInfo(cluster, "mystorm");
            Map<String, List<List<Long>>> executorInfo = stormComponentToExecutorInfo(cluster, "mystorm");

            checkConsistency(cluster, "mystorm", true);

            assertEquals(5, taskInfo.get("1").size());
            // executors for "1": 3 executors, 5 tasks -> distribution [2, 2, 1]
            List<List<Integer>> exec1Tasks = new ArrayList<>();
            for (List<Long> e : executorInfo.get("1")) {
                exec1Tasks.add(executorToTasks(e));
            }
            checkDistribution(exec1Tasks, List.of(2, 2, 1));

            assertEquals(2, taskInfo.get("2").size());
            List<List<Integer>> exec2Tasks = new ArrayList<>();
            for (List<Long> e : executorInfo.get("2")) {
                exec2Tasks.add(executorToTasks(e));
            }
            checkDistribution(exec2Tasks, List.of(1, 1));

            assertEquals(3, taskInfo.get("3").size());
            List<List<Integer>> exec3Tasks = new ArrayList<>();
            for (List<Long> e : executorInfo.get("3")) {
                exec3Tasks.add(executorToTasks(e));
            }
            checkDistribution(exec3Tasks, List.of(1, 1, 1));
        }
    }

    @Test
    public void testTopoHistory() throws Exception {
        IGroupMappingServiceProvider groupMapper = Mockito.mock(IGroupMappingServiceProvider.class);

        try (LocalCluster cluster = new LocalCluster.Builder()
                .withSimulatedTime()
                .withSupervisors(2)
                .withPortsPerSupervisor(5)
                .withGroupMapper(groupMapper)
                .withDaemonConf(Map.of(
                    DaemonConfig.SUPERVISOR_ENABLE, false,
                    Config.NIMBUS_ADMINS, List.of("admin-user"),
                    DaemonConfig.NIMBUS_TASK_TIMEOUT_SECS, 30,
                    DaemonConfig.NIMBUS_MONITOR_FREQ_SECS, 10,
                    Config.TOPOLOGY_ACKER_EXECUTORS, 0))
                .build()) {
            Mockito.when(groupMapper.getGroups(ArgumentMatchers.any())).thenReturn(Set.of("alice-group"));

            IStormClusterState state = cluster.getClusterState();
            Nimbus nimbus = cluster.getNimbus();
            String currentUser = System.getProperty("user.name");

            StormTopology topology = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(new TestPlannerSpout(true), 4)),
                Map.of());

            // No topology history initially
            assertEquals(0, nimbus.getTopologyHistory(currentUser).get_topo_ids().size());

            // Submit and kill "test" (readable by current user)
            cluster.submitTopology("test", Map.of(
                Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 20,
                DaemonConfig.LOGS_USERS, List.of("alice", currentUser)), topology);
            String stormId = state.getTopoId("test").get();
            cluster.advanceClusterTime(5);
            assertNotNull(state.stormBase(stormId, null));
            nimbus.killTopology("test");
            assertEquals(TopologyStatus.KILLED, state.stormBase(stormId, null).get_status());
            cluster.advanceClusterTime(35);

            // Submit and kill "killgrouptest" (readable by alice-group)
            cluster.submitTopology("killgrouptest", Map.of(
                Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 20,
                DaemonConfig.LOGS_GROUPS, List.of("alice-group")), topology);
            String stormIdKillGroup = state.getTopoId("killgrouptest").get();
            cluster.advanceClusterTime(5);
            nimbus.killTopology("killgrouptest");
            cluster.advanceClusterTime(35);

            // Submit and kill "killnoreadtest" (not readable by anyone except owner/admin)
            cluster.submitTopology("killnoreadtest", Map.of(
                Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 20), topology);
            String stormIdKillNoRead = state.getTopoId("killnoreadtest").get();
            cluster.advanceClusterTime(5);
            nimbus.killTopology("killnoreadtest");
            cluster.advanceClusterTime(35);

            // Submit active "2test" (readable by current user)
            cluster.submitTopology("2test", Map.of(
                Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 10,
                DaemonConfig.LOGS_USERS, List.of("alice", currentUser)), topology);
            cluster.advanceClusterTime(11);
            String stormId2 = state.getTopoId("2test").get();

            // Submit active "testnoread" (readable by alice only)
            cluster.submitTopology("testnoread", Map.of(
                Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 10,
                DaemonConfig.LOGS_USERS, List.of("alice")), topology);
            cluster.advanceClusterTime(11);
            String stormId3 = state.getTopoId("testnoread").get();

            // Submit active "testreadgroup" (readable by alice-group)
            cluster.submitTopology("testreadgroup", Map.of(
                Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 10,
                DaemonConfig.LOGS_GROUPS, List.of("alice-group")), topology);
            cluster.advanceClusterTime(11);
            String stormId4 = state.getTopoId("testreadgroup").get();

            // Current user should see 4 topologies
            List<String> histIds = new ArrayList<>(nimbus.getTopologyHistory(currentUser).get_topo_ids());
            java.util.Collections.sort(histIds);
            assertEquals(4, histIds.size());
            assertEquals(stormId2, histIds.get(0));
            assertEquals(stormIdKillGroup, histIds.get(1));
            assertEquals(stormId, histIds.get(2));
            assertEquals(stormId4, histIds.get(3));

            // Alice should see 5 topologies
            List<String> aliceIds = new ArrayList<>(nimbus.getTopologyHistory("alice").get_topo_ids());
            java.util.Collections.sort(aliceIds);
            assertEquals(5, aliceIds.size());

            // Admin should see all 6
            List<String> adminIds = new ArrayList<>(nimbus.getTopologyHistory("admin-user").get_topo_ids());
            java.util.Collections.sort(adminIds);
            assertEquals(6, adminIds.size());

            // Group-only user should see 2
            List<String> groupOnlyIds = new ArrayList<>(nimbus.getTopologyHistory("group-only-user").get_topo_ids());
            java.util.Collections.sort(groupOnlyIds);
            assertEquals(2, groupOnlyIds.size());
        }
    }

    // test-file-bogus-download skipped: beginFileDownload has been removed from the Nimbus API

    @Test
    public void testNimbusCheckAuthorizationParams() throws Exception {
        IStormClusterState clusterState = Mockito.mock(IStormClusterState.class);
        BlobStore blobStore = Mockito.mock(BlobStore.class);
        TopoCache tc = Mockito.mock(TopoCache.class);

        try (LocalCluster cluster = new LocalCluster.Builder()
                .withClusterState(clusterState)
                .withBlobStore(blobStore)
                .withTopoCache(tc)
                .withNimbusWrapper(nimbus -> Mockito.spy(nimbus))
                .withDaemonConf(Map.of(
                    DaemonConfig.NIMBUS_AUTHORIZER, "org.apache.storm.security.auth.authorizer.NoopAuthorizer",
                    DaemonConfig.SUPERVISOR_AUTHORIZER, "org.apache.storm.security.auth.authorizer.NoopAuthorizer"))
                .build()) {
            Nimbus nimbus = cluster.getNimbus();
            String topologyName = "test-nimbus-check-autho-params";
            String topologyId = "fake-id";

            Map<String, Object> expectedConf = new HashMap<>();
            expectedConf.put(Config.TOPOLOGY_NAME, topologyName);
            expectedConf.put("foo", "bar");

            Mockito.when(clusterState.getTopoId(topologyName)).thenReturn(Optional.of(topologyId));
            Mockito.when(tc.readTopoConf(Mockito.any(String.class), ArgumentMatchers.any())).thenReturn(expectedConf);
            Mockito.when(tc.readTopology(Mockito.any(String.class), ArgumentMatchers.any())).thenReturn(null);

            // getTopologyConf calls checkAuthorization with the correct parameters
            try {
                String confJson = nimbus.getTopologyConf(topologyId);
                @SuppressWarnings("unchecked")
                Map<String, Object> parsed = (Map<String, Object>) JSONValue.parse(confJson);
                assertEquals(topologyName, parsed.get(Config.TOPOLOGY_NAME));
            } catch (NotAliveException e) {
                // acceptable
            }
            Mockito.verify(nimbus).checkAuthorization(Mockito.isNull(), Mockito.isNull(), Mockito.eq("getClusterInfo"));
            Mockito.verify(nimbus).checkAuthorization(Mockito.eq(topologyName), Mockito.any(Map.class), Mockito.eq("getTopologyConf"));

            // getTopology calls checkAuthorization with the correct parameters
            StormCommon commonOverride = new StormCommon() {
                @Override
                protected StormTopology systemTopologyImpl(Map<String, Object> topoConf, StormTopology topology) {
                    return null;
                }
            };
            try (org.apache.storm.utils.StormCommonInstaller ignored = new org.apache.storm.utils.StormCommonInstaller(commonOverride)) {
                try {
                    nimbus.getTopology(topologyId);
                } catch (NotAliveException e) {
                    // acceptable
                }
                Mockito.verify(nimbus).checkAuthorization(Mockito.eq(topologyName), Mockito.any(Map.class), Mockito.eq("getTopology"));
            }

            // getUserTopology calls checkAuthorization with the correct parameters
            try {
                nimbus.getUserTopology(topologyId);
            } catch (NotAliveException e) {
                // acceptable
            }
            Mockito.verify(nimbus).checkAuthorization(Mockito.eq(topologyName), Mockito.any(Map.class), Mockito.eq("getUserTopology"));
            Mockito.verify(tc, Mockito.times(2)).readTopology(Mockito.eq(topologyId), ArgumentMatchers.any());
        }
    }

    @Test
    public void testCheckAuthorizationGetSupervisorPageInfo() throws Exception {
        IStormClusterState clusterState = Mockito.mock(IStormClusterState.class);
        BlobStore blobStore = Mockito.mock(BlobStore.class);
        TopoCache tc = Mockito.mock(TopoCache.class);

        try (LocalCluster cluster = new LocalCluster.Builder()
                .withClusterState(clusterState)
                .withBlobStore(blobStore)
                .withTopoCache(tc)
                .withNimbusWrapper(nimbus -> Mockito.spy(nimbus))
                .withDaemonConf(Map.of(
                    DaemonConfig.NIMBUS_AUTHORIZER, "org.apache.storm.security.auth.authorizer.NoopAuthorizer",
                    DaemonConfig.SUPERVISOR_AUTHORIZER, "org.apache.storm.security.auth.authorizer.NoopAuthorizer"))
                .build()) {
            Nimbus nimbus = cluster.getNimbus();
            String expectedName = "test-nimbus-check-autho-params";

            Map<String, Object> expectedConf = new HashMap<>();
            expectedConf.put(Config.TOPOLOGY_NAME, expectedName);
            expectedConf.put(Config.TOPOLOGY_WORKERS, 1);
            expectedConf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 30);
            expectedConf.put("foo", "bar");

            Assignment assignment = new Assignment();
            assignment.set_executor_node_port(Map.of(
                List.of(1L, 1L), new NodeInfo("super1", Set.of(1L)),
                List.of(2L, 2L), new NodeInfo("super2", Set.of(2L))));

            StormTopology topology = new StormTopology();
            topology.set_spouts(Map.of());
            topology.set_bolts(Map.of());
            topology.set_state_spouts(Map.of());

            Map<String, Assignment> topoAssignment = new HashMap<>();
            topoAssignment.put(expectedName, assignment);

            HashMap<String, org.apache.storm.generated.SupervisorInfo> allSupervisors = new HashMap<>();
            org.apache.storm.generated.SupervisorInfo si1 = new org.apache.storm.generated.SupervisorInfo();
            si1.set_hostname("host1");
            si1.set_meta(List.of(1234L));
            si1.set_uptime_secs(123);
            si1.set_used_ports(List.of());
            si1.set_resources_map(Map.of());
            allSupervisors.put("super1", si1);

            org.apache.storm.generated.SupervisorInfo si2 = new org.apache.storm.generated.SupervisorInfo();
            si2.set_hostname("host2");
            si2.set_meta(List.of(1234L));
            si2.set_uptime_secs(123);
            si2.set_used_ports(List.of());
            si2.set_resources_map(Map.of());
            allSupervisors.put("super2", si2);

            Mockito.when(clusterState.allSupervisorInfo()).thenReturn(allSupervisors);
            Mockito.when(tc.readTopoConf(Mockito.any(String.class), Mockito.any(Subject.class))).thenReturn(expectedConf);
            Mockito.when(tc.readTopology(Mockito.any(String.class), Mockito.any(Subject.class))).thenReturn(topology);
            Mockito.when(clusterState.assignmentsInfo()).thenReturn(topoAssignment);

            nimbus.getSupervisorPageInfo("super1", null, true);

            Mockito.verify(nimbus).checkAuthorization(Mockito.eq(expectedName), Mockito.any(Map.class), Mockito.eq("getSupervisorPageInfo"));
            Mockito.verify(nimbus).checkAuthorization(Mockito.isNull(), Mockito.isNull(), Mockito.eq("getClusterInfo"));
            Mockito.verify(nimbus).checkAuthorization(Mockito.eq(expectedName), Mockito.any(Map.class), Mockito.eq("getTopology"));
        }
    }

    @Test
    public void testKillStorm() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
                .withSimulatedTime()
                .withSupervisors(2)
                .withPortsPerSupervisor(5)
                .withDaemonConf(Map.of(
                    DaemonConfig.SUPERVISOR_ENABLE, false,
                    DaemonConfig.NIMBUS_TOPOLOGY_BLOBSTORE_DELETION_DELAY_MS, 0,
                    DaemonConfig.NIMBUS_TASK_TIMEOUT_SECS, 30,
                    DaemonConfig.NIMBUS_MONITOR_FREQ_SECS, 10,
                    Config.TOPOLOGY_ACKER_EXECUTORS, 0,
                    Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, 0))
                .build()) {
            Nimbus nimbus = cluster.getNimbus();
            IStormClusterState state = cluster.getClusterState();

            StormTopology topology = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(new TestPlannerSpout(true), 14)),
                Map.of());

            cluster.submitTopology("test", Map.of(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 20), topology);
            String stormId = state.getTopoId("test").get();
            cluster.advanceClusterTime(15);
            assertNotNull(state.stormBase(stormId, null));
            assertNotNull(state.assignmentInfo(stormId, null));

            nimbus.killTopology("test");
            assertEquals(TopologyStatus.KILLED, state.stormBase(stormId, null).get_status());
            assertNotNull(state.assignmentInfo(stormId, null));

            cluster.advanceClusterTime(18);
            assertEquals(1, state.heartbeatStorms().size());
            cluster.advanceClusterTime(3);
            assertNull(state.stormBase(stormId, null));
            assertNull(state.assignmentInfo(stormId, null));

            // cleanup happens on monitoring thread
            cluster.advanceClusterTime(11);
            assertTrue(state.heartbeatStorms().isEmpty());

            assertThrows(NotAliveException.class, () -> nimbus.killTopology("lalala"));

            cluster.submitTopology("2test", Map.of(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 10), topology);
            cluster.advanceClusterTime(11);
            assertThrows(org.apache.storm.generated.AlreadyAliveException.class, () ->
                cluster.submitTopology("2test", Map.of(), topology));

            cluster.advanceClusterTime(11);
            String stormId2 = state.getTopoId("2test").get();
            assertNotNull(state.stormBase(stormId2, null));
            nimbus.killTopology("2test");
            assertThrows(org.apache.storm.generated.AlreadyAliveException.class, () ->
                cluster.submitTopology("2test", Map.of(), topology));

            cluster.advanceClusterTime(11);
            assertEquals(1, state.heartbeatStorms().size());

            cluster.advanceClusterTime(6);
            assertNull(state.stormBase(stormId2, null));
            assertNull(state.assignmentInfo(stormId2, null));
            cluster.advanceClusterTime(11);
            assertEquals(0, state.heartbeatStorms().size());

            cluster.submitTopology("test3", Map.of(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 5), topology);
            String stormId3 = state.getTopoId("test3").get();
            cluster.advanceClusterTime(11);
            nimbus.killTopology("test3");
            cluster.advanceClusterTime(41);
            assertNull(state.stormBase(stormId3, null));
            assertNull(state.assignmentInfo(stormId3, null));
            assertEquals(0, state.heartbeatStorms().size());

            // Test kill with opts
            Time.advanceTimeSecs(11);
            cluster.waitForIdle();

            cluster.submitTopology("test3", Map.of(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 5), topology);
            stormId3 = state.getTopoId("test3").get();
            cluster.advanceClusterTime(11);

            List<Long> executorId = topologyExecutors(cluster, stormId3).get(0);
            doExecutorHeartbeat(cluster, stormId3, executorId);
            nimbus.killTopology("test3");
            cluster.advanceClusterTime(6);
            assertEquals(1, state.heartbeatStorms().size());
            cluster.advanceClusterTime(5);
            assertEquals(0, state.heartbeatStorms().size());

            // test kill with opts
            cluster.submitTopology("test4", Map.of(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 100), topology);
            cluster.advanceClusterTime(11);
            org.apache.storm.generated.KillOptions killOpts = new org.apache.storm.generated.KillOptions();
            killOpts.set_wait_secs(10);
            nimbus.killTopologyWithOpts("test4", killOpts);
            String stormId4 = state.getTopoId("test4").get();
            cluster.advanceClusterTime(9);
            assertNotNull(state.assignmentInfo(stormId4, null));
            cluster.advanceClusterTime(2);
            assertNull(state.assignmentInfo(stormId4, null));
        }
    }

    @Test
    public void testReassignment() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
                .withSimulatedTime()
                .withSupervisors(2)
                .withPortsPerSupervisor(5)
                .withDaemonConf(Map.of(
                    DaemonConfig.SUPERVISOR_ENABLE, false,
                    DaemonConfig.NIMBUS_TASK_LAUNCH_SECS, 60,
                    DaemonConfig.NIMBUS_TASK_TIMEOUT_SECS, 20,
                    DaemonConfig.NIMBUS_MONITOR_FREQ_SECS, 10,
                    DaemonConfig.NIMBUS_SUPERVISOR_TIMEOUT_SECS, 100,
                    Config.TOPOLOGY_ACKER_EXECUTORS, 0,
                    Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, 0))
                .build()) {
            IStormClusterState state = cluster.getClusterState();

            StormTopology topology = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(new TestPlannerSpout(true), 2)),
                Map.of());

            cluster.submitTopology("test", Map.of(Config.TOPOLOGY_WORKERS, 2), topology);
            cluster.advanceClusterTime(11);
            checkConsistency(cluster, "test", true);

            String stormId = state.getTopoId("test").get();
            List<List<Long>> executors = topologyExecutors(cluster, stormId);
            List<Long> executorId1 = executors.get(0);
            List<Long> executorId2 = executors.get(1);
            NodeInfo ass1 = executorAssignment(cluster, stormId, executorId1);
            NodeInfo ass2 = executorAssignment(cluster, stormId, executorId2);

            cluster.advanceClusterTime(30);
            doExecutorHeartbeat(cluster, stormId, executorId1);
            doExecutorHeartbeat(cluster, stormId, executorId2);

            cluster.advanceClusterTime(13);
            assertEquals(ass1, executorAssignment(cluster, stormId, executorId1));
            assertEquals(ass2, executorAssignment(cluster, stormId, executorId2));
            doExecutorHeartbeat(cluster, stormId, executorId1);

            cluster.advanceClusterTime(11);
            doExecutorHeartbeat(cluster, stormId, executorId1);
            assertEquals(ass1, executorAssignment(cluster, stormId, executorId1));
            checkConsistency(cluster, "test", true);

            // have to wait an extra 10 seconds because nimbus may not
            // resynchronize its heartbeat time till monitor-time secs after
            cluster.advanceClusterTime(11);
            doExecutorHeartbeat(cluster, stormId, executorId1);
            assertEquals(ass1, executorAssignment(cluster, stormId, executorId1));
            checkConsistency(cluster, "test", true);

            cluster.advanceClusterTime(11);
            assertEquals(ass1, executorAssignment(cluster, stormId, executorId1));
            // executor2 should have been reassigned since it didn't heartbeat
            assertFalse(ass2.equals(executorAssignment(cluster, stormId, executorId2)));
            ass2 = executorAssignment(cluster, stormId, executorId2);
            checkConsistency(cluster, "test", true);

            cluster.advanceClusterTime(31);
            // executor1 also timed out (launch timeout 60s)
            assertFalse(ass1.equals(executorAssignment(cluster, stormId, executorId1)));
            assertEquals(ass2, executorAssignment(cluster, stormId, executorId2)); // tests launch timeout
            checkConsistency(cluster, "test", true);

            ass1 = executorAssignment(cluster, stormId, executorId1);
            String activeSupervisor = ass2.get_node();
            cluster.killSupervisor(activeSupervisor);

            for (int i = 0; i < 12; i++) {
                doExecutorHeartbeat(cluster, stormId, executorId1);
                doExecutorHeartbeat(cluster, stormId, executorId2);
                cluster.advanceClusterTime(10);
            }
            // doesn't reassign if heartbeating even if supervisor times out
            assertEquals(ass1, executorAssignment(cluster, stormId, executorId1));
            assertEquals(ass2, executorAssignment(cluster, stormId, executorId2));
            checkConsistency(cluster, "test", true);

            cluster.advanceClusterTime(30);
            ass1 = executorAssignment(cluster, stormId, executorId1);
            ass2 = executorAssignment(cluster, stormId, executorId2);
            assertNotNull(ass1);
            assertNotNull(ass2);
            assertFalse(activeSupervisor.equals(executorAssignment(cluster, stormId, executorId2).get_node()));
            assertFalse(activeSupervisor.equals(executorAssignment(cluster, stormId, executorId1).get_node()));
            checkConsistency(cluster, "test", true);

            // Kill all supervisors
            for (String supervisorId : state.supervisors(null)) {
                cluster.killSupervisor(supervisorId);
            }

            cluster.advanceClusterTime(90);
            assertNull(executorAssignment(cluster, stormId, executorId1));
            assertNull(executorAssignment(cluster, stormId, executorId2));
            checkConsistency(cluster, "test", false);

            cluster.addSupervisor();
            cluster.advanceClusterTime(11);
            checkConsistency(cluster, "test", true);
        }
    }

    @Test
    public void testReassignmentToConstrainedCluster() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
                .withSimulatedTime()
                .withSupervisors(0)
                .withDaemonConf(Map.of(
                    DaemonConfig.SUPERVISOR_ENABLE, false,
                    DaemonConfig.NIMBUS_TASK_LAUNCH_SECS, 60,
                    DaemonConfig.NIMBUS_TASK_TIMEOUT_SECS, 20,
                    DaemonConfig.NIMBUS_MONITOR_FREQ_SECS, 10,
                    DaemonConfig.NIMBUS_SUPERVISOR_TIMEOUT_SECS, 100,
                    Config.TOPOLOGY_ACKER_EXECUTORS, 0,
                    Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, 0))
                .build()) {
            cluster.addSupervisor(1, "a");
            cluster.addSupervisor(1, "b");

            IStormClusterState state = cluster.getClusterState();

            StormTopology topology = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(new TestPlannerSpout(true), 2)),
                Map.of());

            cluster.submitTopology("test", Map.of(Config.TOPOLOGY_WORKERS, 2), topology);
            cluster.advanceClusterTime(11);
            checkConsistency(cluster, "test", true);

            String stormId = state.getTopoId("test").get();
            List<List<Long>> executors = topologyExecutors(cluster, stormId);
            List<Long> executorId1 = executors.get(0);
            List<Long> executorId2 = executors.get(1);
            NodeInfo ass1 = executorAssignment(cluster, stormId, executorId1);
            NodeInfo ass2 = executorAssignment(cluster, stormId, executorId2);

            cluster.advanceClusterTime(30);
            doExecutorHeartbeat(cluster, stormId, executorId1);
            doExecutorHeartbeat(cluster, stormId, executorId2);

            cluster.advanceClusterTime(13);
            assertEquals(ass1, executorAssignment(cluster, stormId, executorId1));
            assertEquals(ass2, executorAssignment(cluster, stormId, executorId2));

            // Kill the supervisor for executor2; only heartbeat executor1
            cluster.killSupervisor(ass2.get_node());
            doExecutorHeartbeat(cluster, stormId, executorId1);

            cluster.advanceClusterTime(11);
            doExecutorHeartbeat(cluster, stormId, executorId1);
            cluster.advanceClusterTime(11);
            doExecutorHeartbeat(cluster, stormId, executorId1);
            cluster.advanceClusterTime(11);
            doExecutorHeartbeat(cluster, stormId, executorId1);
            cluster.advanceClusterTime(11);
            doExecutorHeartbeat(cluster, stormId, executorId1);

            checkConsistency(cluster, "test", true);
            assertEquals(1, stormNumWorkers(state, "test"));
        }
    }

    @Test
    public void testReassignSqueezedTopology() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
                .withSimulatedTime()
                .withSupervisors(1)
                .withPortsPerSupervisor(1)
                .withDaemonConf(Map.of(
                    DaemonConfig.SUPERVISOR_ENABLE, false,
                    DaemonConfig.NIMBUS_TASK_LAUNCH_SECS, 60,
                    DaemonConfig.NIMBUS_TASK_TIMEOUT_SECS, 20,
                    DaemonConfig.NIMBUS_MONITOR_FREQ_SECS, 10,
                    Config.TOPOLOGY_ACKER_EXECUTORS, 0,
                    Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, 0))
                .build()) {
            IStormClusterState state = cluster.getClusterState();

            StormTopology topology = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(new TestPlannerSpout(true), 9)),
                Map.of());

            cluster.submitTopology("test", Map.of(Config.TOPOLOGY_WORKERS, 4), topology);
            cluster.advanceClusterTime(11);
            String stormId = state.getTopoId("test").get();
            Map<NodeInfo, List<List<Long>>> slotExecs = slotAssignments(cluster, stormId);
            checkExecutorDistribution(slotExecs, List.of(9));
            checkConsistency(cluster, "test", true);

            cluster.addSupervisor(2);
            cluster.advanceClusterTime(11);
            slotExecs = slotAssignments(cluster, stormId);
            Map<List<Long>, Long> startTimes = executorStartTimes(cluster, stormId);
            checkExecutorDistribution(slotExecs, List.of(3, 3, 3));
            checkConsistency(cluster, "test", true);

            cluster.addSupervisor(8);
            cluster.advanceClusterTime(11);
            Map<NodeInfo, List<List<Long>>> slotExecs2 = slotAssignments(cluster, stormId);
            Map<List<Long>, Long> startTimes2 = executorStartTimes(cluster, stormId);
            checkExecutorDistribution(slotExecs2, List.of(2, 2, 2, 3));
            checkConsistency(cluster, "test", true);

            // Find the common slot (the one with 3 executors)
            NodeInfo commonSlot = null;
            for (Map.Entry<NodeInfo, List<List<Long>>> entry : slotExecs2.entrySet()) {
                if (entry.getValue().size() == 3) {
                    commonSlot = entry.getKey();
                    break;
                }
            }
            assertNotNull(commonSlot);
            assertEquals(slotExecs.get(commonSlot) != null ? slotExecs.get(commonSlot) : null,
                slotExecs2.get(commonSlot) != null ? slotExecs2.get(commonSlot) : null);

            // Check start times - same executors should keep same start times
            List<List<Long>> sameExecutors = slotExecs2.get(commonSlot);
            List<List<Long>> changedExecutors = new ArrayList<>();
            for (Map.Entry<NodeInfo, List<List<Long>>> entry : slotExecs2.entrySet()) {
                if (!entry.getKey().equals(commonSlot)) {
                    changedExecutors.addAll(entry.getValue());
                }
            }
            for (List<Long> t : sameExecutors) {
                assertEquals(startTimes.get(t), startTimes2.get(t));
            }
            for (List<Long> t : changedExecutors) {
                assertFalse(startTimes.get(t).equals(startTimes2.get(t)));
            }
        }
    }

    @Test
    public void testRebalance() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
                .withSimulatedTime()
                .withSupervisors(1)
                .withDaemonConf(Map.of(
                    DaemonConfig.SUPERVISOR_ENABLE, false,
                    DaemonConfig.NIMBUS_MONITOR_FREQ_SECS, 10,
                    Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 30,
                    Config.TOPOLOGY_ACKER_EXECUTORS, 0,
                    Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, 0))
                .build()) {
            IStormClusterState state = cluster.getClusterState();
            Nimbus nimbus = cluster.getNimbus();

            StormTopology topology = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(new TestPlannerSpout(true), 3)),
                Map.of());

            cluster.submitTopology("test", Map.of(
                Config.TOPOLOGY_WORKERS, 3,
                Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 60), topology);
            cluster.advanceClusterTime(11);
            String stormId = state.getTopoId("test").get();

            cluster.addSupervisor(3);
            cluster.addSupervisor(3);
            cluster.advanceClusterTime(11);

            Map<NodeInfo, List<List<Long>>> slotExecs = slotAssignments(cluster, stormId);
            checkExecutorDistribution(slotExecs, List.of(1, 1, 1));
            checkNumNodes(slotExecs, 1);

            nimbus.rebalance("test", new RebalanceOptions());
            cluster.advanceClusterTime(30);
            // Still on one node during wait
            slotExecs = slotAssignments(cluster, stormId);
            checkExecutorDistribution(slotExecs, List.of(1, 1, 1));
            checkNumNodes(slotExecs, 1);

            cluster.advanceClusterTime(30);
            slotExecs = slotAssignments(cluster, stormId);
            checkExecutorDistribution(slotExecs, List.of(1, 1, 1));
            checkNumNodes(slotExecs, 3);

            // Invalid rebalance
            assertThrows(InvalidTopologyException.class, () -> {
                RebalanceOptions opts = new RebalanceOptions();
                opts.set_num_executors(Map.of("1", 0));
                nimbus.rebalance("test", opts);
            });
        }
    }

    @Test
    public void testRebalanceChangeParallelism() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
                .withSimulatedTime()
                .withSupervisors(4)
                .withDaemonConf(Map.of(
                    DaemonConfig.SUPERVISOR_ENABLE, false,
                    DaemonConfig.NIMBUS_MONITOR_FREQ_SECS, 10,
                    Config.TOPOLOGY_ACKER_EXECUTORS, 0,
                    Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, 0))
                .build()) {
            IStormClusterState state = cluster.getClusterState();
            Nimbus nimbus = cluster.getNimbus();

            StormTopology topology = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(new TestPlannerSpout(true), 6,
                    Map.of(Config.TOPOLOGY_TASKS, 12))),
                Map.of());

            cluster.submitTopology("test", Map.of(
                Config.TOPOLOGY_WORKERS, 3,
                Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 30), topology);
            cluster.advanceClusterTime(11);
            String stormId = state.getTopoId("test").get();

            checkExecutorDistribution(slotAssignments(cluster, stormId), List.of(2, 2, 2));

            RebalanceOptions opts1 = new RebalanceOptions();
            opts1.set_num_workers(6);
            nimbus.rebalance("test", opts1);
            cluster.advanceClusterTime(29);
            checkExecutorDistribution(slotAssignments(cluster, stormId), List.of(2, 2, 2));
            cluster.advanceClusterTime(3);
            checkExecutorDistribution(slotAssignments(cluster, stormId), List.of(1, 1, 1, 1, 1, 1));

            RebalanceOptions opts2 = new RebalanceOptions();
            opts2.set_num_executors(Map.of("1", 1));
            nimbus.rebalance("test", opts2);
            cluster.advanceClusterTime(29);
            checkExecutorDistribution(slotAssignments(cluster, stormId), List.of(1, 1, 1, 1, 1, 1));
            cluster.advanceClusterTime(3);
            checkExecutorDistribution(slotAssignments(cluster, stormId), List.of(1));

            RebalanceOptions opts3 = new RebalanceOptions();
            opts3.set_num_executors(Map.of("1", 8));
            opts3.set_num_workers(4);
            nimbus.rebalance("test", opts3);
            cluster.advanceClusterTime(32);
            checkExecutorDistribution(slotAssignments(cluster, stormId), List.of(2, 2, 2, 2));
            checkConsistency(cluster, "test", true);

            Map<String, List<List<Long>>> executorInfo = stormComponentToExecutorInfo(cluster, "test");
            List<List<Integer>> exec1Tasks = new ArrayList<>();
            for (List<Long> e : executorInfo.get("1")) {
                exec1Tasks.add(executorToTasks(e));
            }
            checkDistribution(exec1Tasks, List.of(2, 2, 2, 2, 1, 1, 1, 1));
        }
    }

    @Test
    public void testRebalanceConstrainedCluster() throws Exception {
        try (LocalCluster cluster = new LocalCluster.Builder()
                .withSimulatedTime()
                .withSupervisors(1)
                .withPortsPerSupervisor(4)
                .withDaemonConf(Map.of(
                    DaemonConfig.SUPERVISOR_ENABLE, false,
                    DaemonConfig.NIMBUS_MONITOR_FREQ_SECS, 10,
                    Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 30,
                    Config.TOPOLOGY_ACKER_EXECUTORS, 0,
                    Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, 0))
                .build()) {
            IStormClusterState state = cluster.getClusterState();
            Nimbus nimbus = cluster.getNimbus();

            StormTopology topology1 = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(new TestPlannerSpout(true), 3)), Map.of());
            StormTopology topology2 = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(new TestPlannerSpout(true), 3)), Map.of());
            StormTopology topology3 = Thrift.buildTopology(
                Map.of("1", Thrift.prepareSpoutDetails(new TestPlannerSpout(true), 3)), Map.of());

            cluster.submitTopology("test", Map.of(
                Config.TOPOLOGY_WORKERS, 3, Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 90), topology1);
            cluster.submitTopology("test2", Map.of(
                Config.TOPOLOGY_WORKERS, 3, Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 90), topology2);
            cluster.submitTopology("test3", Map.of(
                Config.TOPOLOGY_WORKERS, 3, Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 90), topology3);

            cluster.advanceClusterTime(11);
            checkForCollisions(state);

            RebalanceOptions opts = new RebalanceOptions();
            opts.set_num_workers(4);
            opts.set_wait_secs(0);
            nimbus.rebalance("test", opts);

            cluster.advanceClusterTime(11);
            checkForCollisions(state);

            cluster.advanceClusterTime(30);
            checkForCollisions(state);
        }
    }

    // The following 3 tests from nimbus_test.clj are not ported because they require
    // direct Nimbus construction with Zookeeper static mocking (MockedZookeeper/MockLeaderElector
    // from storm-core), which causes JVM crashes (System.exit) from Nimbus timer threads
    // encountering ZK errors. These tests exercise leader election and Nimbus lifecycle
    // recovery, which are better tested via integration tests with a real ZK setup:
    //   - test-leadership: verifies leader vs non-leader Nimbus behavior
    //   - test-stateless-with-scheduled-topology-to-be-killed: regression test for STORM-856
    //   - test-topology-action-notifier: verifies ITopologyActionNotifierPlugin callbacks
}
