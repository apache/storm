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
import java.util.Map;
import java.util.Optional;

import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.LocalCluster;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.cluster.IStormClusterState;
import net.minidev.json.JSONValue;

import org.apache.storm.security.serialization.BlowfishTupleSerializer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NimbusGetTopologyConfTest {

    private static final String MASKED = "*****";
    private static final String TOPO_NAME = "test-get-topology-conf-masking";
    private static final String TOPO_ID = "fake-id";

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parse(String json) {
        return (Map<String, Object>) JSONValue.parse(json);
    }

    @Test
    public void getTopologyConfMasksCredentialsAndKeepsOtherValues() throws Exception {
        IStormClusterState clusterState = Mockito.mock(IStormClusterState.class);
        BlobStore blobStore = Mockito.mock(BlobStore.class);
        TopoCache topoCache = Mockito.mock(TopoCache.class);

        Map<String, Object> storedConf = new HashMap<>();
        storedConf.put(Config.TOPOLOGY_NAME, TOPO_NAME);
        storedConf.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD, "topology-zk-secret");
        storedConf.put(BlowfishTupleSerializer.SECRET_KEY, "0123456789abcdef");
        storedConf.put(Config.TOPOLOGY_WORKERS, 3);

        try (LocalCluster cluster = new LocalCluster.Builder()
                .withClusterState(clusterState)
                .withBlobStore(blobStore)
                .withTopoCache(topoCache)
                .withDaemonConf(Map.of(
                    DaemonConfig.NIMBUS_AUTHORIZER, "org.apache.storm.security.auth.authorizer.NoopAuthorizer",
                    DaemonConfig.SUPERVISOR_AUTHORIZER, "org.apache.storm.security.auth.authorizer.NoopAuthorizer"))
                .build()) {
            Nimbus nimbus = cluster.getNimbus();

            Mockito.when(clusterState.getTopoId(TOPO_NAME)).thenReturn(Optional.of(TOPO_ID));
            Mockito.when(topoCache.readTopoConf(Mockito.any(String.class), ArgumentMatchers.any()))
                .thenReturn(storedConf);

            Map<String, Object> served = parse(nimbus.getTopologyConf(TOPO_ID));

            assertEquals(MASKED, served.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD),
                "the ZooKeeper auth payload should be masked");
            assertEquals(MASKED, served.get(BlowfishTupleSerializer.SECRET_KEY),
                "the tuple serializer key should be masked");

            assertEquals(TOPO_NAME, served.get(Config.TOPOLOGY_NAME));
            assertEquals(3, ((Number) served.get(Config.TOPOLOGY_WORKERS)).intValue());

            assertEquals("topology-zk-secret", storedConf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD),
                "the stored conf should keep its own values");
            assertEquals("0123456789abcdef", storedConf.get(BlowfishTupleSerializer.SECRET_KEY),
                "the stored conf should keep its own values");
        }
    }

    @Test
    public void getTopologyConfLeavesCredentialFreeConfAlone() throws Exception {
        IStormClusterState clusterState = Mockito.mock(IStormClusterState.class);
        BlobStore blobStore = Mockito.mock(BlobStore.class);
        TopoCache topoCache = Mockito.mock(TopoCache.class);

        Map<String, Object> storedConf = new HashMap<>();
        storedConf.put(Config.TOPOLOGY_NAME, TOPO_NAME);
        storedConf.put("some.topology.setting", "plain-value");

        try (LocalCluster cluster = new LocalCluster.Builder()
                .withClusterState(clusterState)
                .withBlobStore(blobStore)
                .withTopoCache(topoCache)
                .withDaemonConf(Map.of(
                    DaemonConfig.NIMBUS_AUTHORIZER, "org.apache.storm.security.auth.authorizer.NoopAuthorizer",
                    DaemonConfig.SUPERVISOR_AUTHORIZER, "org.apache.storm.security.auth.authorizer.NoopAuthorizer"))
                .build()) {
            Nimbus nimbus = cluster.getNimbus();

            Mockito.when(clusterState.getTopoId(TOPO_NAME)).thenReturn(Optional.of(TOPO_ID));
            Mockito.when(topoCache.readTopoConf(Mockito.any(String.class), ArgumentMatchers.any()))
                .thenReturn(storedConf);

            Map<String, Object> served = parse(nimbus.getTopologyConf(TOPO_ID));

            assertEquals(storedConf, served);
        }
    }
}
