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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;

import javax.security.auth.Subject;

import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.LocalCluster;
import org.apache.storm.Thrift;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.Credentials;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.LogConfig;
import org.apache.storm.generated.LogLevel;
import org.apache.storm.generated.LogLevelAction;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SubmitOptions;
import org.apache.storm.generated.TopologyInitialStatus;
import org.apache.storm.generated.TopologyStatus;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.testing.TestPlannerBolt;
import org.apache.storm.testing.TestPlannerSpout;
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

    // --- Helper methods ---

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
