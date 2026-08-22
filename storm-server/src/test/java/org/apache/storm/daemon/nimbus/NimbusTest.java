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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.security.auth.Subject;

import com.codahale.metrics.Meter;
import net.minidev.json.JSONValue;
import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.KeySequenceNumber;
import org.apache.storm.blobstore.LocalFsBlobStore;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.Credentials;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.ListBlobsResult;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.GenericResourceAwareStrategyOld;
import org.apache.storm.scheduler.resource.strategies.scheduling.RoundRobinResourceAwareStrategy;
import org.apache.storm.security.auth.DefaultPrincipalToLocal;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.IGroupMappingServiceProvider;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.security.auth.SingleUserPrincipal;
import org.apache.storm.security.auth.authorizer.DenyAuthorizer;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.WrappedAuthorizationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NimbusTest {
    private static final String BLOB_FILE_KEY = "file-key";
    private static final String TOPO_NAME = "topo";
    private static final String TOPO_ID = "topology1-1-1";

    @Mock
    private StormMetricsRegistry metricRegistry;
    @Mock
    private INimbus iNimbus;
    @Mock
    private IStormClusterState stormClusterState;
    @Mock
    private NimbusInfo nimbusInfo;
    @Mock
    private LocalFsBlobStore localBlobStore;
    @Mock
    private ILeaderElector leaderElector;
    @Mock
    private IGroupMappingServiceProvider groupMapper;
    @Mock
    private TopoCache topoCache;

    private Nimbus nimbus;

    @BeforeEach
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();

        Map<String, Object> conf = Map.of(DaemonConfig.NIMBUS_MONITOR_FREQ_SECS, 10);
        nimbus = new Nimbus(conf, iNimbus, stormClusterState, nimbusInfo, localBlobStore, leaderElector, groupMapper, metricRegistry);
    }

    @AfterEach
    public void tearDown() {
        ReqContext.reset();
    }

    @Test
    public void testMemoryLoadLargerThanMaxHeapSize() {
        // Topology will not be able to be successfully scheduled: Config TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB=128.0 < 129.0,
        // Largest memory requirement of a component in the topology).
        TopologyBuilder builder1 = new TopologyBuilder();
        builder1.setSpout("wordSpout1", new TestWordSpout(), 4);
        StormTopology stormTopology1 = builder1.createTopology();
        Config config1 = new Config();
        config1.put(Config.STORM_NETWORK_TOPOGRAPHY_PLUGIN, "org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping");
        config1.put(DaemonConfig.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, DefaultSchedulingPriorityStrategy.class.getName());

        config1.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 10.0);
        config1.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 0.0);
        config1.put(Config.TOPOLOGY_PRIORITY, 0);
        config1.put(Config.TOPOLOGY_SUBMITTER_USER, "zhuo");
        config1.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 128.0);
        config1.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 129.0);
        Class[] strategyClasses = {
                DefaultResourceAwareStrategy.class,
                RoundRobinResourceAwareStrategy.class,
                GenericResourceAwareStrategyOld.class};
        for (Class strategyClass: strategyClasses) {
            String strategyClassName = strategyClass.getName();
            config1.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, strategyClassName);
            try {
                ServerUtils.validateTopologyWorkerMaxHeapSizeConfigs(config1, stormTopology1, 768.0);
                fail("Expected exception not thrown when using Strategy " + strategyClassName);
            } catch (InvalidTopologyException e) {
                //Expected...
            }
        }
    }

    @Test
    public void uploadedBlobPersistsMinimumTime() {
        Set<String> idleTopologies = new HashSet<>();
        idleTopologies.add("topology1");
        Map<String, Object> conf = new HashMap<>();
        conf.put(DaemonConfig.NIMBUS_TOPOLOGY_BLOBSTORE_DELETION_DELAY_MS, 300000);

        try (Time.SimulatedTime ignored = new Time.SimulatedTime(null)) {
            Set<String> toDelete = Nimbus.getExpiredTopologyIds(idleTopologies, conf);
            assertTrue(toDelete.isEmpty());

            Time.advanceTime(10 * 60 * 1000L);

            toDelete = Nimbus.getExpiredTopologyIds(idleTopologies, conf);
            assertTrue(toDelete.contains("topology1"));
            assertEquals(1, toDelete.size());

        }
    }

    @Test
    public void validateNoTopoConfOverrides() {
        StormTopology topology = new StormTopology();
        topology.set_spouts(new HashMap<>());
        topology.set_bolts(new HashMap<>());
        topology.set_state_spouts(new HashMap<>());

        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_MESSAGING_NETTY_AUTHENTICATION, false);
        conf.put(Config.TOPOLOGY_WORKER_NIMBUS_THRIFT_CLIENT_USE_TLS, false);
        conf.put(Config.STORM_WORKERS_ARTIFACTS_DIR, "a");
        Map<String, Object> topoConf = new HashMap<>();
        topoConf.put(Config.STORM_WORKERS_ARTIFACTS_DIR, "b");
        Map<String, Object> normalized = Nimbus.normalizeConf(conf, topoConf, topology);
        assertNull(normalized.get(Config.STORM_WORKERS_ARTIFACTS_DIR));
    }

    @Test
    void testCreateStateInZookeeper() throws TException {
        nimbus.createStateInZookeeper(BLOB_FILE_KEY);

        verify(stormClusterState).setupBlob(eq(BLOB_FILE_KEY), eq(nimbusInfo), any());
    }

    @Test
    void testCreateStateInZookeeperIsNotAllowedWhenTheAuthorizerDeniesIt() throws Exception {
        IAuthorizer authorizer = mock(IAuthorizer.class);
        when(authorizer.permit(any(), eq("createStateInZookeeper"), any())).thenReturn(false);
        nimbus.setAuthorizationHandler(authorizer);

        assertThrows(AuthorizationException.class, () -> nimbus.createStateInZookeeper(BLOB_FILE_KEY));
        verify(stormClusterState, never()).setupBlob(eq(BLOB_FILE_KEY), eq(nimbusInfo), any());
    }

    @Test
    void testCreateStateInZookeeperIsAllowedWhenTheAuthorizerPermitsIt() throws Exception {
        IAuthorizer authorizer = mock(IAuthorizer.class);
        when(authorizer.permit(any(), eq("createStateInZookeeper"), any())).thenReturn(true);
        nimbus.setAuthorizationHandler(authorizer);

        nimbus.createStateInZookeeper(BLOB_FILE_KEY);

        verify(stormClusterState).setupBlob(eq(BLOB_FILE_KEY), eq(nimbusInfo), any());
    }

    @Test
    void testCreateStateInZookeeperWithoutLocalFsBlobStoreInstanceShouldNotCreate() throws Exception {
        BlobStore blobStore = mock(BlobStore.class);
        Map<String, Object> conf = Map.of(DaemonConfig.NIMBUS_MONITOR_FREQ_SECS, 10);
        nimbus = new Nimbus(conf, iNimbus, stormClusterState, nimbusInfo, blobStore, leaderElector, groupMapper, metricRegistry);

        nimbus.createStateInZookeeper(BLOB_FILE_KEY);

        verify(stormClusterState, never()).setupBlob(eq(BLOB_FILE_KEY), eq(nimbusInfo), any());
    }

    @Test
    void testCreateStateInZookeeperWhenFailToSetupBlobWithRuntimeExceptionThrowsRuntimeException() {
        doThrow(new RuntimeException("Failed to setup blob")).when(stormClusterState).setupBlob(eq(BLOB_FILE_KEY), eq(nimbusInfo), any());

        assertThrows(RuntimeException.class, () -> nimbus.createStateInZookeeper(BLOB_FILE_KEY));
        verify(stormClusterState).setupBlob(eq(BLOB_FILE_KEY), eq(nimbusInfo), any());
    }

    @Test
    void testCreateStateInZookeeperWhenKeyNotFoundHandlesException() throws Exception {
        try (MockedConstruction<KeySequenceNumber> keySequenceNumber = mockConstruction(KeySequenceNumber.class, (mock, context) ->
                when(mock.getKeySequenceNumber(any())).thenThrow(new KeyNotFoundException("Failed to setup blob")))) {
            nimbus.createStateInZookeeper(BLOB_FILE_KEY);

            verify(keySequenceNumber.constructed().get(0)).getKeySequenceNumber(any());
            verify(stormClusterState, never()).setupBlob(eq(BLOB_FILE_KEY), eq(nimbusInfo), any());
        }
    }

    @Test
    void testListBlobsOnlyReturnsKeysTheCallerMayReadTheMetadataOf() throws Exception {
        when(localBlobStore.listKeys()).thenReturn(List.of("readable-key", "other-users-key").iterator());
        when(localBlobStore.getBlobMeta(eq("other-users-key"), any()))
            .thenThrow(new WrappedAuthorizationException("not allowed"));

        ListBlobsResult result = nimbus.listBlobs("");

        assertEquals(List.of("readable-key"), result.get_keys());
    }

    @Test
    void testListBlobsIsAuthorized() throws Exception {
        Map<String, Object> conf = Map.of(DaemonConfig.NIMBUS_MONITOR_FREQ_SECS, 10,
                                          DaemonConfig.NIMBUS_AUTHORIZER, DenyAuthorizer.class.getName());
        nimbus = new Nimbus(conf, iNimbus, stormClusterState, nimbusInfo, localBlobStore, leaderElector, groupMapper, metricRegistry);
        when(localBlobStore.listKeys()).thenReturn(List.of("readable-key").iterator());

        assertThrows(AuthorizationException.class, () -> nimbus.listBlobs(""));
        verify(localBlobStore, never()).listKeys();
    }

    @Test
    void testUploadNewCredentialsRejectsACallerWhoIsNotTheOwner() throws Exception {
        Nimbus nimbus = makeNimbusOwningTopology(TOPO_NAME, TOPO_ID, "alice");
        setCaller("bob");

        // bob claims the topology is owned by alice, which it is, but bob is not alice
        Credentials creds = new Credentials(Map.of("key", "value"));
        creds.set_topoOwner("alice");

        assertThrows(AuthorizationException.class, () -> nimbus.uploadNewCredentials(TOPO_NAME, creds));
        verify(stormClusterState, never()).setCredentials(eq(TOPO_ID), any(), any());
    }

    @Test
    void testUploadNewCredentialsAcceptsTheOwner() throws Exception {
        Nimbus nimbus = makeNimbusOwningTopology(TOPO_NAME, TOPO_ID, "alice");
        setCaller("alice");

        Credentials creds = new Credentials(Map.of("key", "value"));
        creds.set_topoOwner("alice");
        nimbus.uploadNewCredentials(TOPO_NAME, creds);

        verify(stormClusterState).setCredentials(eq(TOPO_ID), eq(creds), any());
    }

    @Test
    void testUploadNewCredentialsRejectsAnOwnerMismatchClaimedByTheOwner() throws Exception {
        Nimbus nimbus = makeNimbusOwningTopology(TOPO_NAME, TOPO_ID, "alice");
        setCaller("alice");

        // alice expects the topology to be owned by bob, so the push must not happen
        Credentials creds = new Credentials(Map.of("key", "value"));
        creds.set_topoOwner("bob");

        assertThrows(AuthorizationException.class, () -> nimbus.uploadNewCredentials(TOPO_NAME, creds));
        verify(stormClusterState, never()).setCredentials(eq(TOPO_ID), any(), any());
    }

    private Nimbus makeNimbusOwningTopology(String topoName, String topoId, String owner) throws Exception {
        Map<String, Object> topoConf = new HashMap<>();
        topoConf.put(Config.TOPOLOGY_SUBMITTER_PRINCIPAL, owner);
        topoConf.put(Config.TOPOLOGY_SUBMITTER_USER, owner);
        when(stormClusterState.getTopoId(topoName)).thenReturn(Optional.of(topoId));
        when(topoCache.readTopoConf(eq(topoId), any())).thenReturn(topoConf);
        when(metricRegistry.registerMeter(anyString())).thenReturn(new Meter());

        Map<String, Object> conf = Map.of(DaemonConfig.NIMBUS_MONITOR_FREQ_SECS, 10,
            Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, DefaultPrincipalToLocal.class.getName());
        return new Nimbus(conf, iNimbus, stormClusterState, nimbusInfo, localBlobStore, topoCache, leaderElector, groupMapper,
            metricRegistry);
    }

    @Test
    void testValidateUploadedJarLocationRejectsLocationsOutsideTheInbox() throws Exception {
        Path inbox = Files.createTempDirectory("nimbus-inbox");
        Path sibling = Paths.get(inbox + "evil");
        try {
            Path jar = Files.write(inbox.resolve("stormjar-cafebabe.jar"), new byte[]{ 1 });
            Path outside = Files.write(Files.createDirectory(sibling).resolve("stormjar-cafebabe.jar"), new byte[]{ 1 });

            // a location handed out by beginFileUpload is accepted, and so is one that only walks inside the inbox
            Nimbus.validateUploadedJarLocation(inbox.toString(), jar.toString());
            Files.createDirectory(inbox.resolve("nested"));
            Nimbus.validateUploadedJarLocation(inbox.toString(), inbox + "/nested/../stormjar-cafebabe.jar");

            // an absolute path elsewhere, a ".." walk out of the inbox, the inbox itself and a sibling directory
            // whose name merely starts with the inbox path are all rejected
            assertThrows(AuthorizationException.class,
                () -> Nimbus.validateUploadedJarLocation(inbox.toString(), "/etc/passwd"));
            assertThrows(AuthorizationException.class,
                () -> Nimbus.validateUploadedJarLocation(inbox.toString(), inbox + "/../../etc/passwd"));
            assertThrows(AuthorizationException.class,
                () -> Nimbus.validateUploadedJarLocation(inbox.toString(), inbox.toString()));
            assertThrows(AuthorizationException.class,
                () -> Nimbus.validateUploadedJarLocation(inbox.toString(), outside.toString()));

            // a symlink inside the inbox pointing back out of it is rejected too
            Path link = Files.createSymbolicLink(inbox.resolve("stormjar-link.jar"), outside);
            assertThrows(AuthorizationException.class,
                () -> Nimbus.validateUploadedJarLocation(inbox.toString(), link.toString()));
        } finally {
            FileUtils.deleteQuietly(inbox.toFile());
            FileUtils.deleteQuietly(sibling.toFile());
        }
    }

    @Test
    void testRebalanceRejectsConfOverridesWithBlobsTheCallerCannotRead() throws Exception {
        final String topoName = "topo-with-blobs";
        final String topoId = "topo-with-blobs-1-1234";
        final String blobKey = "someone-elses-blob";

        TopoCache topoCache = mock(TopoCache.class);
        Map<String, Object> conf = Map.of(DaemonConfig.NIMBUS_MONITOR_FREQ_SECS, 10);
        nimbus = new Nimbus(conf, iNimbus, stormClusterState, nimbusInfo, localBlobStore, topoCache, leaderElector, groupMapper,
            new StormMetricsRegistry());

        StormTopology topology = new StormTopology();
        topology.set_spouts(new HashMap<>());
        topology.set_bolts(new HashMap<>());
        topology.set_state_spouts(new HashMap<>());
        when(stormClusterState.getTopoId(topoName)).thenReturn(Optional.of(topoId));
        when(topoCache.readTopoConf(eq(topoId), any())).thenReturn(new HashMap<>(Map.of(Config.TOPOLOGY_NAME, topoName)));
        when(topoCache.readTopology(eq(topoId), any())).thenReturn(topology);
        doThrow(new AuthorizationException("does not have READ access to " + blobKey))
            .when(localBlobStore).getBlobMeta(eq(blobKey), any());

        RebalanceOptions options = new RebalanceOptions();
        options.set_topology_conf_overrides(
            JSONValue.toJSONString(Map.of(Config.TOPOLOGY_BLOBSTORE_MAP, Map.of(blobKey, new HashMap<>()))));

        Subject caller = new Subject(false, Set.of(new SingleUserPrincipal("alice")), Set.of(), Set.of());
        ReqContext.context().setSubject(caller);
        try {
            ArgumentCaptor<Subject> subjectCaptor = ArgumentCaptor.forClass(Subject.class);
            assertThrows(AuthorizationException.class, () -> nimbus.rebalance(topoName, options));
            verify(localBlobStore).getBlobMeta(eq(blobKey), subjectCaptor.capture());
            //the blobs are looked up as the one asking for the rebalance, not as nimbus
            assertSame(caller, subjectCaptor.getValue());
        } finally {
            ReqContext.reset();
        }
    }

    @Test
    void testGetTopologyHistoryFiltersByTheAuthenticatedCaller() throws Exception {
        Map<String, Object> conf = new HashMap<>();
        conf.put(DaemonConfig.NIMBUS_MONITOR_FREQ_SECS, 10);
        conf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, DefaultPrincipalToLocal.class.getName());
        conf.put(Config.NIMBUS_ADMINS, Collections.singletonList("admin"));
        nimbus = new Nimbus(conf, iNimbus, stormClusterState, nimbusInfo, localBlobStore, leaderElector, groupMapper, metricRegistry);

        Map<String, Object> topoConf = new HashMap<>();
        topoConf.put(Config.TOPOLOGY_NAME, "topology1");
        topoConf.put(Config.TOPOLOGY_USERS, Collections.singletonList("alice"));
        when(stormClusterState.assignments(null)).thenReturn(Collections.singletonList(TOPO_ID));
        when(localBlobStore.readBlob(eq(ConfigUtils.masterStormConfKey(TOPO_ID)), any()))
            .thenReturn(Utils.toCompressedJsonConf(topoConf));
        when(localBlobStore.getBlobMeta(eq(ConfigUtils.masterStormConfKey(TOPO_ID)), any()))
            .thenReturn(new ReadableBlobMeta(new SettableBlobMeta(new ArrayList<>()), 0));

        try {
            setCaller("bob");
            // asking for somebody else's history is only for admins, the ui daemon is expected to be one.
            // a caller that is not an admin gets its own history back rather than an error, so a ui that
            // was left out of nimbus.admins keeps serving the page instead of failing it
            assertTrue(nimbus.getTopologyHistory("alice").get_topo_ids().isEmpty());
            // and no user argument at all is the caller's own history, not everybody's
            assertTrue(nimbus.getTopologyHistory(null).get_topo_ids().isEmpty());

            setCaller("alice");
            assertEquals(Collections.singletonList(TOPO_ID), nimbus.getTopologyHistory(null).get_topo_ids());

            setCaller("admin");
            assertEquals(Collections.singletonList(TOPO_ID), nimbus.getTopologyHistory("alice").get_topo_ids());
            assertTrue(nimbus.getTopologyHistory("bob").get_topo_ids().isEmpty());
        } finally {
            ReqContext.reset();
        }
    }

    @Test
    void testGetTopologyHistoryIsAuthorized() throws Exception {
        Map<String, Object> conf = new HashMap<>();
        conf.put(DaemonConfig.NIMBUS_MONITOR_FREQ_SECS, 10);
        conf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, DefaultPrincipalToLocal.class.getName());
        conf.put(DaemonConfig.NIMBUS_AUTHORIZER, DenyAuthorizer.class.getName());
        nimbus = new Nimbus(conf, iNimbus, stormClusterState, nimbusInfo, localBlobStore, leaderElector, groupMapper, metricRegistry);

        try {
            setCaller("bob");
            assertThrows(AuthorizationException.class, () -> nimbus.getTopologyHistory("bob"));
        } finally {
            ReqContext.reset();
        }
    }

    private static void setCaller(String user) {
        Subject subject = new Subject();
        subject.getPrincipals().add(new SingleUserPrincipal(user));
        ReqContext.context().setSubject(subject);
    }
}
