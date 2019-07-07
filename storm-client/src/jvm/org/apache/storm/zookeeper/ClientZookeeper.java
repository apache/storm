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

package org.apache.storm.zookeeper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.storm.callback.WatcherCallBack;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.cluster.VersionedData;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.curator.framework.api.CuratorEvent;
import org.apache.storm.shade.org.apache.curator.framework.api.CuratorEventType;
import org.apache.storm.shade.org.apache.curator.framework.api.CuratorListener;
import org.apache.storm.shade.org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.storm.shade.org.apache.zookeeper.CreateMode;
import org.apache.storm.shade.org.apache.zookeeper.KeeperException;
import org.apache.storm.shade.org.apache.zookeeper.WatchedEvent;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.apache.zookeeper.data.Stat;
import org.apache.storm.utils.CuratorUtils;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.ZookeeperAuthInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientZookeeper {

    // A singleton instance allows us to mock delegated static methods in our
    // tests by subclassing.
    private static final ClientZookeeper INSTANCE = new ClientZookeeper();
    private static Logger LOG = LoggerFactory.getLogger(ClientZookeeper.class);
    private static ClientZookeeper _instance = INSTANCE;

    /**
     * Provide an instance of this class for delegates to use.  To mock out delegated methods, provide an instance of a subclass that
     * overrides the implementation of the delegated method.
     *
     * @param u a ClientZookeeper instance
     */
    public static void setInstance(ClientZookeeper u) {
        _instance = u;
    }

    /**
     * Resets the singleton instance to the default. This is helpful to reset the class to its original functionality when mocking is no
     * longer desired.
     */
    public static void resetInstance() {
        _instance = INSTANCE;
    }

    public static void mkdirs(CuratorFramework zk, String path, List<ACL> acls) {
        _instance.mkdirsImpl(zk, path, acls);
    }

    public static CuratorFramework mkClient(Map<String, Object> conf, List<String> servers, Object port,
                                            String root, final WatcherCallBack watcher, Map<String, Object> authConf, DaemonType type) {
        return _instance.mkClientImpl(conf, servers, port, root, watcher, authConf, type);
    }

    // Deletes the state inside the zookeeper for a key, for which the
    // contents of the key starts with nimbus host port information
    public static void deleteNodeBlobstore(CuratorFramework zk, String parentPath, String hostPortInfo) {
        String normalizedParentPath = normalizePath(parentPath);
        List<String> childPathList = null;
        if (existsNode(zk, normalizedParentPath, false)) {
            childPathList = getChildren(zk, normalizedParentPath, false);
            for (String child : childPathList) {
                if (child.startsWith(hostPortInfo)) {
                    LOG.debug("deleteNode child {}", child);
                    deleteNode(zk, normalizedParentPath + "/" + child);
                }
            }
        }
    }

    public static String createNode(CuratorFramework zk, String path, byte[] data, CreateMode mode, List<ACL> acls) {
        String ret = null;
        try {
            String npath = normalizePath(path);
            ret = zk.create().creatingParentsIfNeeded().withMode(mode).withACL(acls).forPath(npath, data);
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
        return ret;
    }

    public static String createNode(CuratorFramework zk, String path, byte[] data, List<ACL> acls) {
        return createNode(zk, path, data, CreateMode.PERSISTENT, acls);
    }

    public static List<String> tokenizePath(String path) {
        String[] toks = path.split("/");
        java.util.ArrayList<String> rtn = new ArrayList<String>();
        for (String str : toks) {
            if (!str.isEmpty()) {
                rtn.add(str);
            }
        }
        return rtn;
    }

    public static String toksToPath(List<String> toks) {
        StringBuffer buff = new StringBuffer();
        buff.append("/");
        int size = toks.size();
        for (int i = 0; i < size; i++) {
            buff.append(toks.get(i));
            if (i < (size - 1)) {
                buff.append("/");
            }
        }
        return buff.toString();
    }

    public static String normalizePath(String path) {
        String rtn = toksToPath(tokenizePath(path));
        return rtn;
    }

    public static boolean existsNode(CuratorFramework zk, String path, boolean watch) {
        Stat stat = null;
        try {
            if (watch) {
                stat = zk.checkExists().watched().forPath(normalizePath(path));
            } else {
                stat = zk.checkExists().forPath(normalizePath(path));
            }
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
        return stat != null;
    }

    public static void deleteNode(CuratorFramework zk, String path) {
        try {
            String npath = normalizePath(path);
            if (existsNode(zk, npath, false)) {
                zk.delete().deletingChildrenIfNeeded().forPath(normalizePath(path));
            }
        } catch (Exception e) {
            if (Utils.exceptionCauseIsInstanceOf(KeeperException.NodeExistsException.class, e)) {
                // do nothing
                LOG.info("delete {} failed.", path, e);
            } else {
                throw Utils.wrapInRuntime(e);
            }
        }
    }

    public static String parentPath(String path) {
        List<String> toks = tokenizePath(path);
        int size = toks.size();
        if (size > 0) {
            toks.remove(size - 1);
        }
        return toksToPath(toks);
    }

    public static boolean exists(CuratorFramework zk, String path, boolean watch) {
        return existsNode(zk, path, watch);
    }

    public static Stat setData(CuratorFramework zk, String path, byte[] data) {
        try {
            String npath = normalizePath(path);
            return zk.setData().forPath(npath, data);
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    public static Integer getVersion(CuratorFramework zk, String path, boolean watch) throws Exception {
        String npath = normalizePath(path);
        Stat stat = null;
        if (existsNode(zk, npath, watch)) {
            if (watch) {
                stat = zk.checkExists().watched().forPath(npath);
            } else {
                stat = zk.checkExists().forPath(npath);
            }
        }
        return stat == null ? null : Integer.valueOf(stat.getVersion());
    }

    public static List<String> getChildren(CuratorFramework zk, String path, boolean watch) {
        try {
            String npath = normalizePath(path);
            if (watch) {
                return zk.getChildren().watched().forPath(npath);
            } else {
                return zk.getChildren().forPath(npath);
            }
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    public static byte[] getData(CuratorFramework zk, String path, boolean watch) {
        try {
            String npath = normalizePath(path);
            if (existsNode(zk, npath, watch)) {
                if (watch) {
                    return zk.getData().watched().forPath(npath);
                } else {
                    return zk.getData().forPath(npath);
                }
            }
        } catch (Exception e) {
            if (Utils.exceptionCauseIsInstanceOf(KeeperException.NoNodeException.class, e)) {
                // this is fine b/c we still have a watch from the successful exists call
            } else {
                throw Utils.wrapInRuntime(e);
            }
        }
        return null;
    }

    /**
     * Get the data along with a version.
     *
     * @param zk    the zk instance to use
     * @param path  the path to get it from
     * @param watch should a watch be enabled
     * @return null if no data is found, else the data with the version.
     */
    public static VersionedData<byte[]> getDataWithVersion(CuratorFramework zk, String path, boolean watch) {
        VersionedData<byte[]> data = null;
        try {
            byte[] bytes = null;
            Stat stats = new Stat();
            String npath = normalizePath(path);
            if (existsNode(zk, npath, watch)) {
                if (watch) {
                    bytes = zk.getData().storingStatIn(stats).watched().forPath(npath);
                } else {
                    bytes = zk.getData().storingStatIn(stats).forPath(npath);
                }
                if (bytes != null) {
                    int version = stats.getVersion();
                    data = new VersionedData<>(version, bytes);
                }
            }
        } catch (Exception e) {
            if (Utils.exceptionCauseIsInstanceOf(KeeperException.NoNodeException.class, e)) {
                // this is fine b/c we still have a watch from the successful exists call
            } else {
                Utils.wrapInRuntime(e);
            }
        }
        return data;
    }

    public static void addListener(CuratorFramework zk, ConnectionStateListener listener) {
        zk.getConnectionStateListenable().addListener(listener);
    }

    public static void syncPath(CuratorFramework zk, String path) {
        try {
            zk.sync().forPath(normalizePath(path));
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    public void mkdirsImpl(CuratorFramework zk, String path, List<ACL> acls) {
        String npath = ClientZookeeper.normalizePath(path);
        if (npath.equals("/")) {
            return;
        }
        if (ClientZookeeper.existsNode(zk, npath, false)) {
            return;
        }
        byte[] byteArray = new byte[1];
        byteArray[0] = (byte) 7;
        try {
            ClientZookeeper.createNode(zk, npath, byteArray, CreateMode.PERSISTENT, acls);
        } catch (Exception e) {
            if (Utils.exceptionCauseIsInstanceOf(KeeperException.NodeExistsException.class, e)) {
                // this can happen when multiple clients doing mkdir at same time
            }
        }
    }

    public CuratorFramework mkClientImpl(Map<String, Object> conf, List<String> servers, Object port, String root,
                                         final WatcherCallBack watcher, Map<String, Object> authConf, DaemonType type) {
        CuratorFramework fk;
        if (authConf != null) {
            fk = CuratorUtils.newCurator(conf, servers, port, root, new ZookeeperAuthInfo(authConf), type.getDefaultZkAcls(conf));
        } else {
            fk = CuratorUtils.newCurator(conf, servers, port, root, null, type.getDefaultZkAcls(conf));
        }

        fk.getCuratorListenable().addListener((unused, e) -> {
            if (e.getType().equals(CuratorEventType.WATCHED)) {
                WatchedEvent event = e.getWatchedEvent();
                watcher.execute(event.getState(), event.getType(), event.getPath());
            }
        });
        LOG.info("Starting ZK Curator");
        fk.start();
        return fk;
    }
}
