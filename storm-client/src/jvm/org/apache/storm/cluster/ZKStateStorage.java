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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.storm.Config;
import org.apache.storm.callback.DefaultWatcherCallBack;
import org.apache.storm.callback.WatcherCallBack;
import org.apache.storm.callback.ZKStateChangedCallback;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.storm.shade.org.apache.zookeeper.CreateMode;
import org.apache.storm.shade.org.apache.zookeeper.KeeperException;
import org.apache.storm.shade.org.apache.zookeeper.Watcher;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.utils.Utils;
import org.apache.storm.zookeeper.ClientZookeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class ZKStateStorage implements IStateStorage {

    private static Logger LOG = LoggerFactory.getLogger(ZKStateStorage.class);

    private ConcurrentHashMap<String, ZKStateChangedCallback> callbacks = new ConcurrentHashMap<String, ZKStateChangedCallback>();
    private CuratorFramework zkWriter;
    private CuratorFramework zkReader;
    private AtomicBoolean active;

    private boolean isNimbus;
    private Map<String, Object> authConf;
    private Map<String, Object> conf;

    public ZKStateStorage(Map<String, Object> conf, Map<String, Object> authConf, ClusterStateContext context) throws Exception {
        this.conf = conf;
        this.authConf = authConf;
        if (context.getDaemonType().equals(DaemonType.NIMBUS)) {
            this.isNimbus = true;
        }

        // just mkdir STORM_ZOOKEEPER_ROOT dir
        CuratorFramework zkTemp = mkZk(context.getDaemonType());
        String rootPath = String.valueOf(conf.get(Config.STORM_ZOOKEEPER_ROOT));
        ClientZookeeper.mkdirs(zkTemp, rootPath, context.getDefaultZkAcls());
        zkTemp.close();

        active = new AtomicBoolean(true);
        zkWriter = mkZk(new ZkWatcherCallBack(), context.getDaemonType());
        if (isNimbus) {
            zkReader = mkZk(new ZkWatcherCallBack(), context.getDaemonType());
        } else {
            zkReader = zkWriter;
        }

    }

    @SuppressWarnings("unchecked")
    private CuratorFramework mkZk(DaemonType type) {
        return ClientZookeeper.mkClient(conf, (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS),
                                        conf.get(Config.STORM_ZOOKEEPER_PORT), "", new DefaultWatcherCallBack(), authConf, type);
    }

    @SuppressWarnings("unchecked")
    private CuratorFramework mkZk(WatcherCallBack watcher, DaemonType type) throws NumberFormatException {
        return ClientZookeeper.mkClient(conf, (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS),
                                        conf.get(Config.STORM_ZOOKEEPER_PORT), String.valueOf(conf.get(Config.STORM_ZOOKEEPER_ROOT)),
                                        watcher, authConf, type);
    }

    @Override
    public void delete_node_blobstore(String path, String nimbusHostPortInfo) {
        ClientZookeeper.deleteNodeBlobstore(zkWriter, path, nimbusHostPortInfo);
    }

    @Override
    public String register(ZKStateChangedCallback callback) {
        String id = UUID.randomUUID().toString();
        this.callbacks.put(id, callback);
        return id;
    }

    @Override
    public void unregister(String id) {
        this.callbacks.remove(id);
    }

    @Override
    public String create_sequential(String path, byte[] data, List<ACL> acls) {
        return ClientZookeeper.createNode(zkWriter, path, data, CreateMode.EPHEMERAL_SEQUENTIAL, acls);
    }

    @Override
    public void mkdirs(String path, List<ACL> acls) {
        ClientZookeeper.mkdirs(zkWriter, path, acls);
    }

    @Override
    public void delete_node(String path) {
        ClientZookeeper.deleteNode(zkWriter, path);
    }

    @Override
    public void set_ephemeral_node(String path, byte[] data, List<ACL> acls) {
        ClientZookeeper.mkdirs(zkWriter, ClientZookeeper.parentPath(path), acls);
        if (ClientZookeeper.exists(zkWriter, path, false)) {
            try {
                ClientZookeeper.setData(zkWriter, path, data);
            } catch (RuntimeException e) {
                if (Utils.exceptionCauseIsInstanceOf(KeeperException.NoNodeException.class, e)) {
                    ClientZookeeper.createNode(zkWriter, path, data, CreateMode.EPHEMERAL, acls);
                } else {
                    throw e;
                }
            }

        } else {
            ClientZookeeper.createNode(zkWriter, path, data, CreateMode.EPHEMERAL, acls);
        }
    }

    @Override
    public Integer get_version(String path, boolean watch) throws Exception {
        Integer ret = ClientZookeeper.getVersion(zkReader, path, watch);
        return ret;
    }

    @Override
    public boolean node_exists(String path, boolean watch) {
        return ClientZookeeper.existsNode(zkReader, path, watch);
    }

    @Override
    public List<String> get_children(String path, boolean watch) {
        return ClientZookeeper.getChildren(zkReader, path, watch);
    }

    @Override
    public void close() {
        this.active.set(false);
        zkWriter.close();
        if (isNimbus) {
            zkReader.close();
        }
    }

    @Override
    public void set_data(String path, byte[] data, List<ACL> acls) {
        if (ClientZookeeper.exists(zkWriter, path, false)) {
            ClientZookeeper.setData(zkWriter, path, data);
        } else {
            ClientZookeeper.mkdirs(zkWriter, ClientZookeeper.parentPath(path), acls);
            try {
                ClientZookeeper.createNode(zkWriter, path, data, CreateMode.PERSISTENT, acls);
            } catch (RuntimeException e) {
                if (Utils.exceptionCauseIsInstanceOf(KeeperException.NodeExistsException.class, e)) {
                    ClientZookeeper.setData(zkWriter, path, data);
                } else {
                    throw e;
                }
            }
        }
    }

    @Override
    public byte[] get_data(String path, boolean watch) {
        byte[] ret = null;

        ret = ClientZookeeper.getData(zkReader, path, watch);

        return ret;
    }

    @Override
    public VersionedData<byte[]> get_data_with_version(String path, boolean watch) {
        return ClientZookeeper.getDataWithVersion(zkReader, path, watch);
    }

    @Override
    public void set_worker_hb(String path, byte[] data, List<ACL> acls) {
        set_data(path, data, acls);
    }

    @Override
    public byte[] get_worker_hb(String path, boolean watch) {
        return ClientZookeeper.getData(zkReader, path, watch);
    }

    @Override
    public List<String> get_worker_hb_children(String path, boolean watch) {
        return get_children(path, watch);
    }

    @Override
    public void delete_worker_hb(String path) {
        delete_node(path);
    }

    @Override
    public void add_listener(final ConnectionStateListener listener) {
        ClientZookeeper.addListener(zkReader,
            (curatorFramework, connectionState) -> listener.stateChanged(curatorFramework, connectionState));
    }

    @Override
    public void sync_path(String path) {
        ClientZookeeper.syncPath(zkWriter, path);
    }

    private class ZkWatcherCallBack implements WatcherCallBack {
        @Override
        public void execute(Watcher.Event.KeeperState state, Watcher.Event.EventType type, String path) {
            if (active.get()) {
                if (!(state.equals(Watcher.Event.KeeperState.SyncConnected))) {
                    LOG.debug("Received event {} : {}: {} with disconnected Zookeeper.", state, type, path);
                } else {
                    LOG.debug("Received event {} : {} : {}", state, type, path);
                }

                if (!type.equals(Watcher.Event.EventType.None)) {
                    for (Map.Entry<String, ZKStateChangedCallback> e : callbacks.entrySet()) {
                        ZKStateChangedCallback fn = e.getValue();
                        fn.changed(type, path);
                    }
                }
            }
        }
    }
}
