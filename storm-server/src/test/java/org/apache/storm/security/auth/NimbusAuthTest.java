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

package org.apache.storm.security.auth;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.LocalCluster;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.nimbus.TopoCache;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.SubmitOptions;
import org.apache.storm.generated.TopologyInitialStatus;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.NimbusClient;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Nimbus authentication and authorization with various transport plugins.
 *
 * Ported from storm-core/test/clj/org/apache/storm/security/auth/nimbus_auth_test.clj
 */
public class NimbusAuthTest {

    private static final int NIMBUS_TIMEOUT = 3000;
    private static final String JAAS_CONF = jaasConfPath();

    private static String jaasConfPath() {
        java.net.URL url = NimbusAuthTest.class.getResource("/org/apache/storm/security/auth/jaas_digest.conf");
        if (url == null) {
            throw new RuntimeException("jaas_digest.conf not found on classpath");
        }
        return url.getPath();
    }

    /**
     * Checks if the given exception or any of its causes is an instance of the expected class.
     */
    private static boolean hasCause(Throwable t, Class<? extends Throwable> expected) {
        Throwable current = t;
        while (current != null) {
            if (expected.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static void assertThrowsCause(Class<? extends Throwable> expectedCause,
                                           ThrowingRunnable action) {
        try {
            action.run();
            fail("Expected exception with cause " + expectedCause.getSimpleName() + " but no exception was thrown");
        } catch (Exception e) {
            assertTrue(hasCause(e, expectedCause),
                "Expected cause " + expectedCause.getSimpleName() + " but got: " + e);
        }
    }

    @Test
    public void testSimpleAuthentication() throws Exception {
        Map<String, Object> clusterConf = new HashMap<>();
        clusterConf.put(DaemonConfig.NIMBUS_AUTHORIZER, null);
        clusterConf.put(DaemonConfig.SUPERVISOR_AUTHORIZER,
            "org.apache.storm.security.auth.authorizer.NoopAuthorizer");
        clusterConf.put(Config.NIMBUS_THRIFT_PORT, 0);
        clusterConf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN,
            "org.apache.storm.security.auth.SimpleTransportPlugin");

        try (LocalCluster cluster = new LocalCluster.Builder()
                .withNimbusDaemon()
                .withDaemonConf(clusterConf)
                .withSupervisors(0)
                .withPortsPerSupervisor(0)
                .build()) {

            Map<String, Object> clientConf = new HashMap<>(ConfigUtils.readStormConfig());
            clientConf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN,
                "org.apache.storm.security.auth.SimpleTransportPlugin");
            clientConf.put(Config.STORM_NIMBUS_RETRY_TIMES, 0);

            try (NimbusClient client = new NimbusClient(clientConf, "localhost",
                    cluster.getThriftServerPort(), NIMBUS_TIMEOUT)) {
                Nimbus.Iface nimbusClient = client.getClient();
                assertThrowsCause(NotAliveException.class, () -> nimbusClient.activate("topo-name"));
            }
        }
    }

    @Test
    public void testNoopAuthorizationWithSimpleTransport() throws Exception {
        IStormClusterState clusterState = Mockito.mock(IStormClusterState.class);
        BlobStore blobStore = Mockito.mock(BlobStore.class);
        TopoCache tc = Mockito.mock(TopoCache.class);
        String topoName = "topo-name";

        Mockito.when(clusterState.getTopoId(topoName)).thenReturn(Optional.empty());

        Map<String, Object> clusterConf = new HashMap<>();
        clusterConf.put(DaemonConfig.NIMBUS_AUTHORIZER,
            "org.apache.storm.security.auth.authorizer.NoopAuthorizer");
        clusterConf.put(DaemonConfig.SUPERVISOR_AUTHORIZER,
            "org.apache.storm.security.auth.authorizer.NoopAuthorizer");
        clusterConf.put(Config.NIMBUS_THRIFT_PORT, 0);
        clusterConf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN,
            "org.apache.storm.security.auth.SimpleTransportPlugin");

        try (LocalCluster cluster = new LocalCluster.Builder()
                .withClusterState(clusterState)
                .withBlobStore(blobStore)
                .withTopoCache(tc)
                .withNimbusDaemon()
                .withDaemonConf(clusterConf)
                .build()) {

            Map<String, Object> clientConf = new HashMap<>(ConfigUtils.readStormConfig());
            clientConf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN,
                "org.apache.storm.security.auth.SimpleTransportPlugin");
            clientConf.put(Config.STORM_NIMBUS_RETRY_TIMES, 0);

            try (NimbusClient client = new NimbusClient(clientConf, "localhost",
                    cluster.getThriftServerPort(), NIMBUS_TIMEOUT)) {
                Nimbus.Iface nimbusClient = client.getClient();
                assertThrowsCause(NotAliveException.class, () -> nimbusClient.activate(topoName));
            }
        }
    }

    @Test
    public void testDenyAuthorizationWithSimpleTransport() throws Exception {
        IStormClusterState clusterState = Mockito.mock(IStormClusterState.class);
        BlobStore blobStore = Mockito.mock(BlobStore.class);
        TopoCache tc = Mockito.mock(TopoCache.class);
        String topoName = "topo-name";
        String topoId = "topo-name-1";

        Mockito.when(clusterState.getTopoId(topoName)).thenReturn(Optional.of(topoId));
        Mockito.when(tc.readTopoConf(Mockito.any(String.class), ArgumentMatchers.any()))
            .thenReturn(Map.of());

        Map<String, Object> clusterConf = new HashMap<>();
        clusterConf.put(DaemonConfig.NIMBUS_AUTHORIZER,
            "org.apache.storm.security.auth.authorizer.DenyAuthorizer");
        clusterConf.put(DaemonConfig.SUPERVISOR_AUTHORIZER,
            "org.apache.storm.security.auth.authorizer.DenyAuthorizer");
        clusterConf.put(Config.NIMBUS_THRIFT_PORT, 0);
        clusterConf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN,
            "org.apache.storm.security.auth.SimpleTransportPlugin");

        try (LocalCluster cluster = new LocalCluster.Builder()
                .withClusterState(clusterState)
                .withBlobStore(blobStore)
                .withTopoCache(tc)
                .withNimbusDaemon()
                .withDaemonConf(clusterConf)
                .build()) {

            Map<String, Object> clientConf = new HashMap<>(ConfigUtils.readStormConfig());
            clientConf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN,
                "org.apache.storm.security.auth.SimpleTransportPlugin");
            clientConf.put(Config.NIMBUS_THRIFT_PORT, cluster.getThriftServerPort());
            clientConf.put(Config.STORM_NIMBUS_RETRY_TIMES, 0);

            try (NimbusClient client = new NimbusClient(clientConf, "localhost",
                    cluster.getThriftServerPort(), NIMBUS_TIMEOUT)) {
                Nimbus.Iface nimbusClient = client.getClient();
                SubmitOptions submitOptions = new SubmitOptions(TopologyInitialStatus.findByValue(2));

                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.submitTopology(topoName, null, null, null));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.submitTopologyWithOpts(topoName, null, null, null, submitOptions));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.beginFileUpload());
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.uploadChunk(null, null));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.finishFileUpload(null));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.downloadChunk(null));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.getNimbusConf());
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.getClusterInfo());
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.killTopology(topoName));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.killTopologyWithOpts(topoName, new KillOptions()));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.activate(topoName));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.deactivate(topoName));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.rebalance(topoName, null));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.getTopologyConf(topoId));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.getTopology(topoId));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.getUserTopology(topoId));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.getTopologyInfo(topoId));
            }
        }
    }

    @Test
    public void testNoopAuthorizationWithSaslDigest() throws Exception {
        Map<String, Object> clusterConf = new HashMap<>();
        clusterConf.put(DaemonConfig.NIMBUS_AUTHORIZER,
            "org.apache.storm.security.auth.authorizer.NoopAuthorizer");
        clusterConf.put(DaemonConfig.SUPERVISOR_AUTHORIZER,
            "org.apache.storm.security.auth.authorizer.NoopAuthorizer");
        clusterConf.put(Config.NIMBUS_THRIFT_PORT, 0);
        clusterConf.put("java.security.auth.login.config",
            JAAS_CONF);
        clusterConf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN,
            "org.apache.storm.security.auth.digest.DigestSaslTransportPlugin");

        try (LocalCluster cluster = new LocalCluster.Builder()
                .withNimbusDaemon()
                .withDaemonConf(clusterConf)
                .withSupervisors(0)
                .withPortsPerSupervisor(0)
                .build()) {

            Map<String, Object> clientConf = new HashMap<>(ConfigUtils.readStormConfig());
            clientConf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN,
                "org.apache.storm.security.auth.digest.DigestSaslTransportPlugin");
            clientConf.put("java.security.auth.login.config",
                JAAS_CONF);
            clientConf.put(Config.NIMBUS_THRIFT_PORT, cluster.getThriftServerPort());
            clientConf.put(Config.STORM_NIMBUS_RETRY_TIMES, 0);

            try (NimbusClient client = new NimbusClient(clientConf, "localhost",
                    cluster.getThriftServerPort(), NIMBUS_TIMEOUT)) {
                Nimbus.Iface nimbusClient = client.getClient();
                assertThrowsCause(NotAliveException.class, () -> nimbusClient.activate("topo-name"));
            }
        }
    }

    @Test
    public void testDenyAuthorizationWithSaslDigest() throws Exception {
        IStormClusterState clusterState = Mockito.mock(IStormClusterState.class);
        BlobStore blobStore = Mockito.mock(BlobStore.class);
        TopoCache tc = Mockito.mock(TopoCache.class);
        String topoName = "topo-name";
        String topoId = "topo-name-1";

        Mockito.when(clusterState.getTopoId(topoName)).thenReturn(Optional.of(topoId));
        Mockito.when(tc.readTopoConf(Mockito.any(String.class), ArgumentMatchers.any()))
            .thenReturn(Map.of());

        Map<String, Object> clusterConf = new HashMap<>();
        clusterConf.put(DaemonConfig.NIMBUS_AUTHORIZER,
            "org.apache.storm.security.auth.authorizer.DenyAuthorizer");
        clusterConf.put(DaemonConfig.SUPERVISOR_AUTHORIZER,
            "org.apache.storm.security.auth.authorizer.DenyAuthorizer");
        clusterConf.put(Config.NIMBUS_THRIFT_PORT, 0);
        clusterConf.put("java.security.auth.login.config",
            JAAS_CONF);
        clusterConf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN,
            "org.apache.storm.security.auth.digest.DigestSaslTransportPlugin");

        try (LocalCluster cluster = new LocalCluster.Builder()
                .withClusterState(clusterState)
                .withBlobStore(blobStore)
                .withTopoCache(tc)
                .withNimbusDaemon()
                .withDaemonConf(clusterConf)
                .build()) {

            Map<String, Object> clientConf = new HashMap<>(ConfigUtils.readStormConfig());
            clientConf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN,
                "org.apache.storm.security.auth.digest.DigestSaslTransportPlugin");
            clientConf.put("java.security.auth.login.config",
                JAAS_CONF);
            clientConf.put(Config.NIMBUS_THRIFT_PORT, cluster.getThriftServerPort());
            clientConf.put(Config.STORM_NIMBUS_RETRY_TIMES, 0);

            try (NimbusClient client = new NimbusClient(clientConf, "localhost",
                    cluster.getThriftServerPort(), NIMBUS_TIMEOUT)) {
                Nimbus.Iface nimbusClient = client.getClient();
                SubmitOptions submitOptions = new SubmitOptions(TopologyInitialStatus.findByValue(2));

                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.submitTopology(topoName, null, null, null));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.submitTopologyWithOpts(topoName, null, null, null, submitOptions));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.beginFileUpload());
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.uploadChunk(null, null));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.finishFileUpload(null));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.downloadChunk(null));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.getNimbusConf());
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.getClusterInfo());
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.killTopology(topoName));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.killTopologyWithOpts(topoName, new KillOptions()));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.activate(topoName));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.deactivate(topoName));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.rebalance(topoName, null));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.getTopologyConf(topoId));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.getTopology(topoId));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.getUserTopology(topoId));
                assertThrowsCause(AuthorizationException.class,
                    () -> nimbusClient.getTopologyInfo(topoId));
            }
        }
    }
}
