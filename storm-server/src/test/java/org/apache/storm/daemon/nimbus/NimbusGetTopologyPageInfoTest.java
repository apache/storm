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

import net.minidev.json.JSONValue;

import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.LocalCluster;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.TopologyPageInfo;
import org.apache.storm.generated.TopologyStatus;
import org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.TestSpout;
import org.apache.storm.security.serialization.BlowfishTupleSerializer;
import org.apache.storm.topology.TopologyBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * getTopologyPageInfo serves the daemon configuration merged with the topology configuration, and it is a
 * topology read-only operation, so a principal that is only allowed to look at a topology reaches it. The
 * merged map must therefore not carry credential values off the daemon.
 */
public class NimbusGetTopologyPageInfoTest {

    private static final String MASKED = "*****";
    private static final String TOPO_NAME = "test-get-topology-page-info-masking";
    private static final String TOPO_ID = "fake-id";

    private static final String PLUGIN_SECRET_KEY = "some.plugin.password";
    private static final String NIMBUS_KEYSTORE_PASSWORD_KEY = "nimbus.thrift.tls.keystore.password";

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parse(String json) {
        return (Map<String, Object>) JSONValue.parse(json);
    }

    private static StormTopology userTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        // setNumTasks so the component conf carries topology.tasks, which submit-time normalization
        // would otherwise have filled in before the conf reached the cache this test mocks
        builder.setSpout("spout-1", new TestSpout(), 1).setNumTasks(1);
        return builder.createTopology();
    }

    private static Map<String, Object> storedTopoConf() {
        Map<String, Object> topoConf = new HashMap<>();
        topoConf.put(Config.TOPOLOGY_NAME, TOPO_NAME);
        topoConf.put(Config.TOPOLOGY_WORKERS, 1);
        topoConf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
        topoConf.put(Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, 0);
        topoConf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 30);
        topoConf.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD, "topology-zk-secret");
        topoConf.put(BlowfishTupleSerializer.SECRET_KEY, "0123456789abcdef");
        return topoConf;
    }

    private static StormBase stormBase() {
        StormBase base = new StormBase();
        base.set_name(TOPO_NAME);
        base.set_owner("some-owner");
        base.set_status(TopologyStatus.ACTIVE);
        base.set_num_workers(1);
        base.set_launch_time_secs(1);
        return base;
    }

    @Test
    public void getTopologyPageInfoMasksDaemonAndTopologyCredentials() throws Exception {
        IStormClusterState clusterState = Mockito.mock(IStormClusterState.class);
        BlobStore blobStore = Mockito.mock(BlobStore.class);
        TopoCache topoCache = Mockito.mock(TopoCache.class);

        Map<String, Object> storedConf = storedTopoConf();

        Map<String, Object> daemonConf = new HashMap<>();
        daemonConf.put(DaemonConfig.NIMBUS_AUTHORIZER, "org.apache.storm.security.auth.authorizer.NoopAuthorizer");
        daemonConf.put(DaemonConfig.SUPERVISOR_AUTHORIZER, "org.apache.storm.security.auth.authorizer.NoopAuthorizer");
        // the daemon-side values that the merge pulls in on top of the topology's own conf
        daemonConf.put(Config.STORM_ZOOKEEPER_AUTH_PAYLOAD, "cluster-zk-digest-secret");
        daemonConf.put(NIMBUS_KEYSTORE_PASSWORD_KEY, "keystore-secret");
        daemonConf.put(PLUGIN_SECRET_KEY, "plugin-secret");

        try (LocalCluster cluster = new LocalCluster.Builder()
                .withClusterState(clusterState)
                .withBlobStore(blobStore)
                .withTopoCache(topoCache)
                .withDaemonConf(daemonConf)
                .build()) {
            Nimbus nimbus = cluster.getNimbus();

            Mockito.when(topoCache.readTopoConf(Mockito.any(String.class), ArgumentMatchers.any()))
                .thenReturn(storedConf);
            Mockito.when(topoCache.readTopology(Mockito.any(String.class), ArgumentMatchers.any()))
                .thenReturn(userTopology());
            Mockito.when(clusterState.stormBase(Mockito.eq(TOPO_ID), ArgumentMatchers.any()))
                .thenReturn(stormBase());
            Mockito.when(clusterState.assignmentInfo(Mockito.eq(TOPO_ID), ArgumentMatchers.any()))
                .thenReturn(null);

            TopologyPageInfo pageInfo = nimbus.getTopologyPageInfo(TOPO_ID, ":all-time", false);
            Map<String, Object> served = parse(pageInfo.get_topology_conf());

            // daemon-side credentials, which only this operation merges in
            assertEquals(MASKED, served.get(Config.STORM_ZOOKEEPER_AUTH_PAYLOAD),
                "the cluster ZooKeeper auth payload should be masked");
            assertEquals(MASKED, served.get(NIMBUS_KEYSTORE_PASSWORD_KEY),
                "TLS keystore passwords should be masked");
            assertEquals(MASKED, served.get(PLUGIN_SECRET_KEY),
                "a plugin key whose name denotes a secret should be masked");

            // topology-side credentials, masked on getTopologyConf and equally reachable here
            assertEquals(MASKED, served.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD),
                "the topology ZooKeeper auth payload should be masked");
            assertEquals(MASKED, served.get(BlowfishTupleSerializer.SECRET_KEY),
                "the tuple serializer key should be masked");

            // values that carry no credential are served untouched
            assertEquals(TOPO_NAME, served.get(Config.TOPOLOGY_NAME));
            assertEquals(1, ((Number) served.get(Config.TOPOLOGY_WORKERS)).intValue());
            assertEquals(30, ((Number) served.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)).intValue());

            // masking is applied to the served copy, never to the daemon's own configuration
            assertEquals("cluster-zk-digest-secret", nimbus.getConf().get(Config.STORM_ZOOKEEPER_AUTH_PAYLOAD),
                "the daemon conf should keep its own values");
            assertEquals("topology-zk-secret", storedConf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD),
                "the stored topology conf should keep its own values");
        }
    }
}
