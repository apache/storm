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
import org.apache.storm.cluster.ClusterStateListener;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.cluster.VersionedData;
import org.apache.storm.shade.org.apache.zookeeper.CreateMode;
import org.apache.storm.shade.org.apache.zookeeper.KeeperException;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.apache.zookeeper.data.Stat;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.ZookeeperAuthInfo;
import org.apache.storm.zookeeper.curator.CuratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientZookeeper {

    // A singleton instance allows us to mock delegated static methods in our
    // tests by subclassing.
    private static final ClientZookeeper INSTANCE = new ClientZookeeper();
    private static Logger LOG = LoggerFactory.getLogger(ClientZookeeper.class);
    private static ClientZookeeper _instance = INSTANCE;

    /**
     * Provide an instance of this class for delegates to use. To mock out delegated methods,
     * provide an instance of a subclass that
     * overrides the implementation of the delegated method.
     *
     * @param u a ClientZookeeper instance
     */
    public static void setInstance(ClientZookeeper u) {
        _instance = u;
    }

    /**
     * Resets the singleton instance to the default. This is helpful to reset the class to its
     * original functionality when mocking is no
     * longer desired.
     */
    public static void resetInstance() {
        _instance = INSTANCE;
    }

    public static void mkdirs(ZkClient zk, String path, List<ACL> acls) {
        _instance.mkdirsImpl(zk, path, acls);
    }

    /**
     * Create and start a ZooKeeper client.
     *
     * @param conf the Storm configuration (timeouts, retries, TLS)
     * @param servers the ZooKeeper servers
     * @param port the ZooKeeper port
     * @param root the chroot, {@code ""} for none
     * @param watcher receives events of watches set through the client; may be {@code null}, in
     *     which case watch events are dropped
     * @param authConf configuration holding the ZooKeeper authentication, may be {@code null}
     * @param type the daemon type, which determines the client's default ACL
     * @return the started client
     */
    public static ZkClient mkClient(Map<String, Object> conf, List<String> servers,
            Object port,
                                            String root, final WatcherCallBack watcher, Map<String, Object> authConf, DaemonType type) {
        return _instance.mkClientImpl(conf, servers, port, root, watcher, authConf, type);
    }

    // Deletes the state inside the zookeeper for a key, for which the
    // contents of the key starts with nimbus host port information
    public static void deleteNodeBlobstore(ZkClient zk, String parentPath,
            String hostPortInfo) {
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

    public static String createNode(ZkClient zk, String path, byte[] data, CreateMode mode,
            List<ACL> acls) {
        String ret = null;
        try {
            String npath = normalizePath(path);
            ret = zk.create(npath, data, mode, acls, true);
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
        return ret;
    }

    public static String createNode(ZkClient zk, String path, byte[] data, List<ACL> acls) {
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

    public static boolean existsNode(ZkClient zk, String path, boolean watch) {
        Stat stat = null;
        try {
            stat = zk.exists(normalizePath(path), watch);
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
        return stat != null;
    }

    public static void deleteNode(ZkClient zk, String path) {
        try {
            String npath = normalizePath(path);
            if (existsNode(zk, npath, false)) {
                zk.delete(normalizePath(path), true);
            }
        } catch (Exception e) {
            if (Utils.exceptionCauseIsInstanceOf(KeeperException.NoNodeException.class, e)) {
                // Node was deleted concurrently between the exists check and the delete.
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

    public static boolean exists(ZkClient zk, String path, boolean watch) {
        return existsNode(zk, path, watch);
    }

    public static Stat setData(ZkClient zk, String path, byte[] data) {
        try {
            String npath = normalizePath(path);
            return zk.setData(npath, data);
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    public static Integer getVersion(ZkClient zk, String path,
            boolean watch) throws Exception {
        String npath = normalizePath(path);
        // exists returns the Stat directly (null when absent) and still arms the watch when
        // watch is true.
        Stat stat = zk.exists(npath, watch);
        return stat == null ? null : Integer.valueOf(stat.getVersion());
    }

    public static List<String> getChildren(ZkClient zk, String path, boolean watch) {
        try {
            String npath = normalizePath(path);
            return zk.getChildren(npath, watch);
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    public static byte[] getData(ZkClient zk, String path, boolean watch) {
        String npath = normalizePath(path);
        try {
            return zk.getData(npath, watch, null);
        } catch (Exception e) {
            if (Utils.exceptionCauseIsInstanceOf(KeeperException.NoNodeException.class, e)) {
                // Node is absent. A watcher can notify when the node is announced.
                if (watch) {
                    existsNode(zk, npath, true);
                }
                return null;
            }
            throw Utils.wrapInRuntime(e);
        }
    }

    /**
     * Get the data along with a version.
     *
     * @param zk    the zk instance to use
     * @param path  the path to get it from
     * @param watch should a watch be enabled
     * @return null if no data is found, else the data with the version.
     */
    public static VersionedData<byte[]> getDataWithVersion(ZkClient zk, String path,
            boolean watch) {
        VersionedData<byte[]> data = null;
        String npath = normalizePath(path);
        Stat stats = new Stat();
        try {
            byte[] bytes = zk.getData(npath, watch, stats);
            if (bytes != null) {
                data = new VersionedData<>(stats.getVersion(), bytes);
            }
        } catch (Exception e) {
            if (Utils.exceptionCauseIsInstanceOf(KeeperException.NoNodeException.class, e)) {
                // Node is absent. getData does not arm a watch when it fails, so when watching we
                // set an existence watch separately to be notified if the node is created.
                if (watch) {
                    existsNode(zk, npath, true);
                }
            } else {
                throw Utils.wrapInRuntime(e);
            }
        }
        return data;
    }

    public static void addListener(ZkClient zk, ClusterStateListener listener) {
        zk.addConnectionStateListener(listener);
    }

    public static void syncPath(ZkClient zk, String path) {
        try {
            zk.sync(normalizePath(path));
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    public void mkdirsImpl(ZkClient zk, String path, List<ACL> acls) {
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

    public ZkClient mkClientImpl(Map<String, Object> conf, List<String> servers,
            Object port, String root,
                                         final WatcherCallBack watcher, Map<String, Object> authConf, DaemonType type) {
        ZookeeperAuthInfo auth = authConf != null ? new ZookeeperAuthInfo(authConf) : null;
        LOG.info("Starting ZK Curator");
        return CuratorUtils.newClient(conf, servers, port, root, auth, type.getDefaultZkAcls(conf),
                watcher);
    }
}
