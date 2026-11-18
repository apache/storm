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

package org.apache.storm.zookeeper.curator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.curator.test.TestingServer;
import org.apache.storm.Config;
import org.apache.storm.cluster.ConnectionState;
import org.apache.storm.shade.org.apache.zookeeper.CreateMode;
import org.apache.storm.shade.org.apache.zookeeper.KeeperException;
import org.apache.storm.shade.org.apache.zookeeper.Watcher;
import org.apache.storm.shade.org.apache.zookeeper.ZooDefs;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.apache.zookeeper.data.Stat;
import org.apache.storm.utils.Utils;
import org.apache.storm.zookeeper.ZkClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CuratorZkClientTest {
    /**
     * A default ACL that differs from OPEN_ACL_UNSAFE (no ADMIN permission), so tests can tell the
     * client default apart from Curator's fallback.
     */
    private static final List<ACL> DEFAULT_ACL = Collections.singletonList(
            new ACL(ZooDefs.Perms.ALL & ~ZooDefs.Perms.ADMIN, ZooDefs.Ids.ANYONE_ID_UNSAFE));

    private TestingServer server;
    private ZkClient client;
    private final LinkedBlockingQueue<Watcher.Event.EventType> events = new LinkedBlockingQueue<>();

    @BeforeEach
    public void setUp() throws Exception {
        server = new TestingServer(true);
        Map<String, Object> conf = Utils.readDefaultConfig();
        conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 1);
        client = CuratorUtils.newClient(conf, Collections.singletonList("127.0.0.1"),
                server.getPort(), "", null, DEFAULT_ACL,
            (state, type, path) -> {
                // Session events (type None) are delivered too; only keep node events.
                if (type != Watcher.Event.EventType.None) {
                    events.add(type);
                }
            });
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    public void createWithDefaultDataStoresLocalAddress() throws Exception {
        client.createWithDefaultData("/dflt", CreateMode.PERSISTENT, null, false);
        byte[] expected = InetAddress.getLocalHost().getHostAddress().getBytes();
        assertArrayEquals(expected, client.getData("/dflt", false, null));
    }

    @Test
    public void createWithNullDataStoresNoData() throws Exception {
        client.create("/nulldata", null, CreateMode.PERSISTENT, null, false);
        byte[] data = client.getData("/nulldata", false, null);
        assertTrue(data == null || data.length == 0, "expected no data");
    }

    @Test
    public void createParentsUseClientDefaultAcl() throws Exception {
        String path = client.create("/a/b/c", "x".getBytes(StandardCharsets.UTF_8),
                CreateMode.PERSISTENT, ZooDefs.Ids.OPEN_ACL_UNSAFE, true);
        assertEquals("/a/b/c", path);
        assertEquals(DEFAULT_ACL, client.getAcl("/a"));
        assertEquals(DEFAULT_ACL, client.getAcl("/a/b"));
        assertEquals(ZooDefs.Ids.OPEN_ACL_UNSAFE, client.getAcl("/a/b/c"));

        client.create("/a/dflt", null, CreateMode.PERSISTENT, null, false);
        assertEquals(DEFAULT_ACL, client.getAcl("/a/dflt"));
    }

    @Test
    public void aclRoundTrip() throws Exception {
        client.create("/acl", null, CreateMode.PERSISTENT, ZooDefs.Ids.OPEN_ACL_UNSAFE, false);
        client.setAcl("/acl", ZooDefs.Ids.READ_ACL_UNSAFE);
        assertEquals(ZooDefs.Ids.READ_ACL_UNSAFE, client.getAcl("/acl"));
    }

    @Test
    public void dataStatAndVersions() throws Exception {
        assertNull(client.exists("/v", false));
        client.create("/v", "1".getBytes(StandardCharsets.UTF_8), CreateMode.PERSISTENT, null,
                false);
        Stat stat = client.setData("/v", "2".getBytes(StandardCharsets.UTF_8));
        assertEquals(1, stat.getVersion());
        Stat read = new Stat();
        assertArrayEquals("2".getBytes(StandardCharsets.UTF_8), client.getData("/v", false, read));
        assertEquals(1, read.getVersion());
        assertEquals(1, client.exists("/v", false).getVersion());
    }

    @Test
    public void sequentialCreateReturnsActualPath() throws Exception {
        String path = client.create("/seq-", null, CreateMode.PERSISTENT_SEQUENTIAL, null, false);
        assertTrue(path.startsWith("/seq-") && path.length() > "/seq-".length(), path);
    }

    @Test
    public void watchesReachWatcherCallBack() throws Exception {
        assertNull(client.exists("/w", true));
        client.create("/w", "a".getBytes(StandardCharsets.UTF_8), CreateMode.PERSISTENT, null,
                false);
        assertEquals(Watcher.Event.EventType.NodeCreated, events.poll(30, TimeUnit.SECONDS));

        client.getData("/w", true, null);
        client.setData("/w", "b".getBytes(StandardCharsets.UTF_8));
        assertEquals(Watcher.Event.EventType.NodeDataChanged, events.poll(30, TimeUnit.SECONDS));

        client.getChildren("/w", true);
        client.create("/w/child", null, CreateMode.PERSISTENT, null, false);
        assertEquals(Watcher.Event.EventType.NodeChildrenChanged,
                events.poll(30, TimeUnit.SECONDS));
    }

    @Test
    public void deleteRecursiveAndNonRecursive() throws Exception {
        client.create("/d/e/f", null, CreateMode.PERSISTENT, null, true);
        assertThrows(KeeperException.NotEmptyException.class, () -> client.delete("/d", false));
        client.delete("/d", true);
        assertNull(client.exists("/d", false));
    }

    @Test
    public void keeperExceptionsPropagateUnwrapped() throws Exception {
        assertThrows(KeeperException.NoNodeException.class,
            () -> client.getData("/missing", false, null));
        assertThrows(KeeperException.NoNodeException.class,
            () -> client.setData("/missing", new byte[0]));
        assertThrows(KeeperException.NoNodeException.class,
            () -> client.delete("/missing", false));
        client.create("/dup", null, CreateMode.PERSISTENT, null, false);
        assertThrows(KeeperException.NodeExistsException.class,
            () -> client.create("/dup", null, CreateMode.PERSISTENT, null, false));
    }

    @Test
    public void syncDoesNotFail() throws Exception {
        client.create("/s", null, CreateMode.PERSISTENT, null, false);
        client.sync("/s");
    }

    @Test
    public void unwrapReturnsBackingCuratorAndRejectsOthers() {
        assertNotNull(CuratorZkClient.unwrap(client));
        assertSame(CuratorZkClient.unwrap(client),
                CuratorZkClient.unwrap(CuratorZkClient.wrap(CuratorZkClient.unwrap(client))));
        assertThrows(IllegalArgumentException.class, () -> CuratorZkClient.unwrap(null));
    }

    @Test
    public void connectionStatesMapOneToOne() {
        assertEquals(ConnectionState.CONNECTED, CuratorZkClient.toStorm(
                org.apache.storm.shade.org.apache.curator.framework.state.ConnectionState.CONNECTED));
        assertEquals(ConnectionState.SUSPENDED, CuratorZkClient.toStorm(
                org.apache.storm.shade.org.apache.curator.framework.state.ConnectionState.SUSPENDED));
        assertEquals(ConnectionState.RECONNECTED, CuratorZkClient.toStorm(
                org.apache.storm.shade.org.apache.curator.framework.state.ConnectionState.RECONNECTED));
        assertEquals(ConnectionState.LOST, CuratorZkClient.toStorm(
                org.apache.storm.shade.org.apache.curator.framework.state.ConnectionState.LOST));
        assertEquals(ConnectionState.READ_ONLY, CuratorZkClient.toStorm(
                org.apache.storm.shade.org.apache.curator.framework.state.ConnectionState.READ_ONLY));
        // Every Curator state must be mapped (fails loudly if Curator adds one).
        for (org.apache.storm.shade.org.apache.curator.framework.state.ConnectionState s
                : org.apache.storm.shade.org.apache.curator.framework.state.ConnectionState.values()) {
            assertEquals(s.name(), CuratorZkClient.toStorm(s).name());
        }
    }
}
