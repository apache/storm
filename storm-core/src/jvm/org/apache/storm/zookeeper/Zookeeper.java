/**
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

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.storm.Config;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.callback.DefaultWatcherCallBack;
import org.apache.storm.callback.WatcherCallBack;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.VersionedData;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.ZookeeperAuthInfo;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;


public class Zookeeper {
    private static Logger LOG = LoggerFactory.getLogger(Zookeeper.class);

    // A singleton instance allows us to mock delegated static methods in our
    // tests by subclassing.
    private static final Zookeeper INSTANCE = new Zookeeper();
    private static Zookeeper _instance = INSTANCE;

    /**
     * Provide an instance of this class for delegates to use.  To mock out
     * delegated methods, provide an instance of a subclass that overrides the
     * implementation of the delegated method.
     *
     * @param u a Zookeeper instance
     */
    public static void setInstance(Zookeeper u) {
        _instance = u;
    }

    /**
     * Resets the singleton instance to the default. This is helpful to reset
     * the class to its original functionality when mocking is no longer
     * desired.
     */
    public static void resetInstance() {
        _instance = INSTANCE;
    }

    public  CuratorFramework mkClientImpl(Map conf, List<String> servers, Object port, String root) {
        return mkClientImpl(conf, servers, port, root, new DefaultWatcherCallBack());
    }

    public  CuratorFramework mkClientImpl(Map conf, List<String> servers, Object port, Map authConf) {
        return mkClientImpl(conf, servers, port, "", new DefaultWatcherCallBack(), authConf);
    }

    public  CuratorFramework mkClientImpl(Map conf, List<String> servers, Object port, String root, Map authConf) {
        return mkClientImpl(conf, servers, port, root, new DefaultWatcherCallBack(), authConf);
    }

    public static CuratorFramework mkClient(Map conf, List<String> servers, Object port, String root, final WatcherCallBack watcher, Map authConf) {
        return _instance.mkClientImpl(conf, servers, port, root, watcher, authConf);
    }

    public  CuratorFramework mkClientImpl(Map conf, List<String> servers, Object port, String root, final WatcherCallBack watcher, Map authConf) {
        CuratorFramework fk;
        if (authConf != null) {
            fk = Utils.newCurator(conf, servers, port, root, new ZookeeperAuthInfo(authConf));
        } else {
            fk = Utils.newCurator(conf, servers, port, root);
        }

        fk.getCuratorListenable().addListener(new CuratorListener() {
            @Override
            public void eventReceived(CuratorFramework _fk, CuratorEvent e) throws Exception {
                if (e.getType().equals(CuratorEventType.WATCHED)) {
                    WatchedEvent event = e.getWatchedEvent();
                    watcher.execute(event.getState(), event.getType(), event.getPath());
                }
            }
        });
        LOG.info("Staring ZK Curator");
        fk.start();
        return fk;
    }

    /**
     * connect ZK, register Watch/unhandle Watch
     *
     * @return
     */
    public  CuratorFramework mkClientImpl(Map conf, List<String> servers, Object port, String root, final WatcherCallBack watcher) {
        return mkClientImpl(conf, servers, port, root, watcher, null);
    }

    public static String createNode(CuratorFramework zk, String path, byte[] data, org.apache.zookeeper.CreateMode mode, List<ACL> acls) {
        String ret = null;
        try {
            String npath = normalizePath(path);
            ret = zk.create().creatingParentsIfNeeded().withMode(mode).withACL(acls).forPath(npath, data);
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
        return ret;
    }

    public static String createNode(CuratorFramework zk, String path, byte[] data, List<ACL> acls){
        return createNode(zk, path, data, org.apache.zookeeper.CreateMode.PERSISTENT, acls);
    }

    public static boolean existsNode(CuratorFramework zk, String path, boolean watch){
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

    public static void deleteNode(CuratorFramework zk, String path){
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

    public static void mkdirs(CuratorFramework zk, String path, List<ACL> acls) {
        _instance.mkdirsImpl(zk, path, acls);
    }

    public void mkdirsImpl(CuratorFramework zk, String path, List<ACL> acls) {
        String npath = normalizePath(path);
        if (npath.equals("/")) {
            return;
        }
        if (existsNode(zk, npath, false)) {
            return;
        }
        byte[] byteArray = new byte[1];
        byteArray[0] = (byte) 7;
        try {
            createNode(zk, npath, byteArray, org.apache.zookeeper.CreateMode.PERSISTENT, acls);
        } catch (Exception e) {
            if (Utils.exceptionCauseIsInstanceOf(KeeperException.NodeExistsException.class, e)) {
                // this can happen when multiple clients doing mkdir at same time
            }
        }
    }

    public static void syncPath(CuratorFramework zk, String path){
        try {
            zk.sync().forPath(normalizePath(path));
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    public static void addListener(CuratorFramework zk, ConnectionStateListener listener) {
        zk.getConnectionStateListenable().addListener(listener);
    }

    public static byte[] getData(CuratorFramework zk, String path, boolean watch){
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

    // Deletes the state inside the zookeeper for a key, for which the
    // contents of the key starts with nimbus host port information
    public static void deleteNodeBlobstore(CuratorFramework zk, String parentPath, String hostPortInfo){
        String normalizedPatentPath = normalizePath(parentPath);
        List<String> childPathList = null;
        if (existsNode(zk, normalizedPatentPath, false)) {
            childPathList = getChildren(zk, normalizedPatentPath, false);
            for (String child : childPathList) {
                if (child.startsWith(hostPortInfo)) {
                    LOG.debug("deleteNode child {}", child);
                    deleteNode(zk, normalizedPatentPath + "/" + child);
                }
            }
        }
    }

    public static Stat setData(CuratorFramework zk, String path, byte[] data){
        try {
            String npath = normalizePath(path);
            return zk.setData().forPath(npath, data);
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    public static boolean exists(CuratorFramework zk, String path, boolean watch){
        return existsNode(zk, path, watch);
    }

    public static List mkInprocessZookeeper(String localdir, Integer port) throws Exception {
        File localfile = new File(localdir);
        ZooKeeperServer zk = new ZooKeeperServer(localfile, localfile, 2000);
        NIOServerCnxnFactory factory = null;
        int report = 2000;
        int limitPort = 65535;
        if (port != null) {
            report = port;
            limitPort = port;
        }
        while (true) {
            try {
                factory = new NIOServerCnxnFactory();
                factory.configure(new InetSocketAddress(report), 0);
                break;
            } catch (BindException e) {
                report++;
                if (report > limitPort) {
                    throw new RuntimeException("No port is available to launch an inprocess zookeeper");
                }
            }
        }
        LOG.info("Starting inprocess zookeeper at port {} and dir {}", report, localdir);
        factory.startup(zk);
        return Arrays.asList((Object) new Long(report), (Object) factory);
    }

    public static void shutdownInprocessZookeeper(NIOServerCnxnFactory handle) {
        handle.shutdown();
    }

    public static NimbusInfo toNimbusInfo(Participant participant) {
        String id = participant.getId();
        if (StringUtils.isBlank(id)) {
            throw new RuntimeException("No nimbus leader participant host found, have you started your nimbus hosts?");
        }
        NimbusInfo nimbusInfo = NimbusInfo.parse(id);
        nimbusInfo.setLeader(participant.isLeader());
        return nimbusInfo;
    }

    // Leader latch listener that will be invoked when we either gain or lose leadership
    public static LeaderLatchListener leaderLatchListenerImpl(final Map conf, final CuratorFramework zk, final BlobStore blobStore, final LeaderLatch leaderLatch) throws UnknownHostException {
        final String hostName = InetAddress.getLocalHost().getCanonicalHostName();
        return new LeaderLatchListener() {
            final String STORM_JAR_SUFFIX = "-stormjar.jar";
            final String STORM_CODE_SUFFIX = "-stormcode.ser";
            final String STORM_CONF_SUFFIX = "-stormconf.ser";

            @Override
            public void isLeader() {
                Set<String> activeTopologyIds = new TreeSet<>(Zookeeper.getChildren(zk, conf.get(Config.STORM_ZOOKEEPER_ROOT) + ClusterUtils.STORMS_SUBTREE, false));

                Set<String> activeTopologyBlobKeys = populateTopologyBlobKeys(activeTopologyIds);
                Set<String> activeTopologyCodeKeys = filterTopologyCodeKeys(activeTopologyBlobKeys);
                Set<String> allLocalBlobKeys = Sets.newHashSet(blobStore.listKeys());
                Set<String> allLocalTopologyBlobKeys = filterTopologyBlobKeys(allLocalBlobKeys);

                // this finds all active topologies blob keys from all local topology blob keys
                Sets.SetView<String> diffTopology = Sets.difference(activeTopologyBlobKeys, allLocalTopologyBlobKeys);
                LOG.info("active-topology-blobs [{}] local-topology-blobs [{}] diff-topology-blobs [{}]",
                        generateJoinedString(activeTopologyIds), generateJoinedString(allLocalTopologyBlobKeys),
                        generateJoinedString(diffTopology));

                if (diffTopology.isEmpty()) {
                    Set<String> activeTopologyDependencies = getTopologyDependencyKeys(activeTopologyCodeKeys);

                    // this finds all dependency blob keys from active topologies from all local blob keys
                    Sets.SetView<String> diffDependencies = Sets.difference(activeTopologyDependencies, allLocalBlobKeys);
                    LOG.info("active-topology-dependencies [{}] local-blobs [{}] diff-topology-dependencies [{}]",
                            generateJoinedString(activeTopologyDependencies), generateJoinedString(allLocalBlobKeys),
                            generateJoinedString(diffDependencies));

                    if (diffDependencies.isEmpty()) {
                        LOG.info("Accepting leadership, all active topologies and corresponding dependencies found locally.");
                    } else {
                        LOG.info("Code for all active topologies is available locally, but some dependencies are not found locally, giving up leadership.");
                        closeLatch();
                    }
                } else {
                    LOG.info("code for all active topologies not available locally, giving up leadership.");
                    closeLatch();
                }
            }

            @Override
            public void notLeader() {
                LOG.info("{} lost leadership.", hostName);
            }

            private String generateJoinedString(Set<String> activeTopologyIds) {
                return Joiner.on(",").join(activeTopologyIds);
            }

            private Set<String> populateTopologyBlobKeys(Set<String> activeTopologyIds) {
                Set<String> activeTopologyBlobKeys = new TreeSet<>();
                for (String activeTopologyId : activeTopologyIds) {
                    activeTopologyBlobKeys.add(activeTopologyId + STORM_JAR_SUFFIX);
                    activeTopologyBlobKeys.add(activeTopologyId + STORM_CODE_SUFFIX);
                    activeTopologyBlobKeys.add(activeTopologyId + STORM_CONF_SUFFIX);
                }
                return activeTopologyBlobKeys;
            }

            private Set<String> filterTopologyBlobKeys(Set<String> blobKeys) {
                Set<String> topologyBlobKeys = new HashSet<>();
                for (String blobKey : blobKeys) {
                    if (blobKey.endsWith(STORM_JAR_SUFFIX) || blobKey.endsWith(STORM_CODE_SUFFIX) ||
                            blobKey.endsWith(STORM_CONF_SUFFIX)) {
                        topologyBlobKeys.add(blobKey);
                    }
                }
                return topologyBlobKeys;
            }

            private Set<String> filterTopologyCodeKeys(Set<String> blobKeys) {
                Set<String> topologyCodeKeys = new HashSet<>();
                for (String blobKey : blobKeys) {
                    if (blobKey.endsWith(STORM_CODE_SUFFIX)) {
                        topologyCodeKeys.add(blobKey);
                    }
                }
                return topologyCodeKeys;
            }

            private Set<String> getTopologyDependencyKeys(Set<String> activeTopologyCodeKeys) {
                Set<String> activeTopologyDependencies = new TreeSet<>();
                Subject subject = ReqContext.context().subject();

                for (String activeTopologyCodeKey : activeTopologyCodeKeys) {
                    try {
                        InputStreamWithMeta blob = blobStore.getBlob(activeTopologyCodeKey, subject);
                        byte[] blobContent = IOUtils.readFully(blob, new Long(blob.getFileLength()).intValue());
                        StormTopology stormCode = Utils.deserialize(blobContent, StormTopology.class);
                        if (stormCode.is_set_dependency_jars()) {
                            activeTopologyDependencies.addAll(stormCode.get_dependency_jars());
                        }
                        if (stormCode.is_set_dependency_artifacts()) {
                            activeTopologyDependencies.addAll(stormCode.get_dependency_artifacts());
                        }
                    } catch (AuthorizationException | KeyNotFoundException | IOException e) {
                        LOG.error("Exception occurs while reading blob for key: " + activeTopologyCodeKey + ", exception: " + e, e);
                        throw new RuntimeException("Exception occurs while reading blob for key: " + activeTopologyCodeKey +
                                ", exception: " + e, e);
                    }
                }
                return activeTopologyDependencies;
            }

            private void closeLatch() {
                try {
                    leaderLatch.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public static ILeaderElector zkLeaderElector(Map conf, BlobStore blobStore) throws UnknownHostException {
        return _instance.zkLeaderElectorImpl(conf, blobStore);
    }

    protected ILeaderElector zkLeaderElectorImpl(Map conf, BlobStore blobStore) throws UnknownHostException {
        List<String> servers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        Object port = conf.get(Config.STORM_ZOOKEEPER_PORT);
        CuratorFramework zk = mkClientImpl(conf, servers, port, "", conf);
        String leaderLockPath = conf.get(Config.STORM_ZOOKEEPER_ROOT) + "/leader-lock";
        String id = NimbusInfo.fromConf(conf).toHostPortString();
        AtomicReference<LeaderLatch> leaderLatchAtomicReference = new AtomicReference<>(new LeaderLatch(zk, leaderLockPath, id));
        AtomicReference<LeaderLatchListener> leaderLatchListenerAtomicReference =
                new AtomicReference<>(leaderLatchListenerImpl(conf, zk, blobStore, leaderLatchAtomicReference.get()));
        return new LeaderElectorImp(conf, servers, zk, leaderLockPath, id, leaderLatchAtomicReference,
            leaderLatchListenerAtomicReference, blobStore);
    }

    /**
     * Get the data along with a version
     * @param zk the zk instance to use
     * @param path the path to get it from
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

    public static String parentPath(String path) {
        List<String> toks = Zookeeper.tokenizePath(path);
        int size = toks.size();
        if (size > 0) {
            toks.remove(size - 1);
        }
        return Zookeeper.toksToPath(toks);
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
}
