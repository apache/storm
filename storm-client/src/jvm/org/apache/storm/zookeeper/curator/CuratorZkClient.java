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

import java.util.List;
import java.util.Objects;
import org.apache.storm.cluster.ClusterStateListener;
import org.apache.storm.cluster.ConnectionState;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.zookeeper.CreateMode;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.apache.zookeeper.data.Stat;
import org.apache.storm.zookeeper.ZkClient;

/**
 * Internal. The Curator-backed {@link ZkClient}. Every method is a one-to-one translation to the
 * corresponding Curator builder chain, so behaviour is identical to calling Curator directly.
 *
 * <p>Not part of Storm's public API; may change or be removed without notice.
 */
public final class CuratorZkClient implements ZkClient {
    private final CuratorFramework curator;

    CuratorZkClient(CuratorFramework curator) {
        this.curator = Objects.requireNonNull(curator, "curator");
    }

    /**
     * Wrap an already started {@link CuratorFramework}. Intended for tests that build their own
     * Curator client.
     *
     * @param curator the started Curator client
     * @return a {@link ZkClient} delegating to {@code curator}
     */
    public static ZkClient wrap(CuratorFramework curator) {
        return new CuratorZkClient(curator);
    }

    /**
     * Return the {@link CuratorFramework} backing a {@link ZkClient}. Only used by code that still
     * needs Curator recipes directly (leader election).
     *
     * @param client a Curator-backed client
     * @return the underlying Curator client
     * @throws IllegalArgumentException if {@code client} is not Curator-backed
     */
    public static CuratorFramework unwrap(ZkClient client) {
        if (client instanceof CuratorZkClient) {
            return ((CuratorZkClient) client).curator;
        }
        throw new IllegalArgumentException("ZkClient is not backed by Curator: "
                + (client == null ? "null" : client.getClass().getName()));
    }

    @Override
    public String create(String path, byte[] data, CreateMode mode, List<ACL> acls,
            boolean createParents) throws Exception {
        if (createParents) {
            return curator.create().creatingParentsIfNeeded().withMode(mode).withACL(acls)
                    .forPath(path, data);
        }
        return curator.create().withMode(mode).withACL(acls).forPath(path, data);
    }

    @Override
    public String createWithDefaultData(String path, CreateMode mode, List<ACL> acls,
            boolean createParents) throws Exception {
        if (createParents) {
            return curator.create().creatingParentsIfNeeded().withMode(mode).withACL(acls)
                    .forPath(path);
        }
        return curator.create().withMode(mode).withACL(acls).forPath(path);
    }

    @Override
    public Stat exists(String path, boolean watch) throws Exception {
        if (watch) {
            return curator.checkExists().watched().forPath(path);
        }
        return curator.checkExists().forPath(path);
    }

    @Override
    public byte[] getData(String path, boolean watch, Stat stat) throws Exception {
        if (stat != null) {
            if (watch) {
                return curator.getData().storingStatIn(stat).watched().forPath(path);
            }
            return curator.getData().storingStatIn(stat).forPath(path);
        }
        if (watch) {
            return curator.getData().watched().forPath(path);
        }
        return curator.getData().forPath(path);
    }

    @Override
    public Stat setData(String path, byte[] data) throws Exception {
        return curator.setData().forPath(path, data);
    }

    @Override
    public List<String> getChildren(String path, boolean watch) throws Exception {
        if (watch) {
            return curator.getChildren().watched().forPath(path);
        }
        return curator.getChildren().forPath(path);
    }

    @Override
    public void delete(String path, boolean recursive) throws Exception {
        if (recursive) {
            curator.delete().deletingChildrenIfNeeded().forPath(path);
        } else {
            curator.delete().forPath(path);
        }
    }

    @Override
    public List<ACL> getAcl(String path) throws Exception {
        return curator.getACL().forPath(path);
    }

    @Override
    public void setAcl(String path, List<ACL> acls) throws Exception {
        curator.setACL().withACL(acls).forPath(path);
    }

    @Override
    public void sync(String path) throws Exception {
        curator.sync().forPath(path);
    }

    @Override
    public void addConnectionStateListener(ClusterStateListener listener) {
        Objects.requireNonNull(listener, "listener");
        curator.getConnectionStateListenable().addListener(
            (unused, newState) -> listener.stateChanged(toStorm(newState)));
    }

    @Override
    public void close() {
        curator.close();
    }

    /**
     * Map a Curator connection state onto Storm's. An explicit switch so that a Curator upgrade
     * adding a state fails loudly instead of being silently mis-mapped.
     */
    static ConnectionState toStorm(
            org.apache.storm.shade.org.apache.curator.framework.state.ConnectionState state) {
        switch (state) {
            case CONNECTED:
                return ConnectionState.CONNECTED;
            case SUSPENDED:
                return ConnectionState.SUSPENDED;
            case RECONNECTED:
                return ConnectionState.RECONNECTED;
            case LOST:
                return ConnectionState.LOST;
            case READ_ONLY:
                return ConnectionState.READ_ONLY;
            default:
                throw new IllegalStateException("Unknown Curator connection state " + state);
        }
    }
}
