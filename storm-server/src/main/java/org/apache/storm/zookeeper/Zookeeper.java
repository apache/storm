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

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.storm.Config;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.callback.DefaultWatcherCallBack;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.utils.Utils;
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
    public static LeaderLatchListener leaderLatchListenerImpl(final Map<String, Object> conf, final CuratorFramework zk, final BlobStore blobStore, final LeaderLatch leaderLatch) throws UnknownHostException {
        final String hostName = InetAddress.getLocalHost().getCanonicalHostName();
        return new LeaderLatchListener() {
            final String STORM_JAR_SUFFIX = "-stormjar.jar";
            final String STORM_CODE_SUFFIX = "-stormcode.ser";
            final String STORM_CONF_SUFFIX = "-stormconf.ser";

            @Override
            public void isLeader() {
                Set<String> activeTopologyIds = new TreeSet<>(ClientZookeeper.getChildren(zk, conf.get(Config.STORM_ZOOKEEPER_ROOT) + ClusterUtils.STORMS_SUBTREE, false));

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

    public static ILeaderElector zkLeaderElector(Map<String, Object> conf, BlobStore blobStore) throws UnknownHostException {
        return _instance.zkLeaderElectorImpl(conf, blobStore);
    }

    protected ILeaderElector zkLeaderElectorImpl(Map<String, Object> conf, BlobStore blobStore) throws UnknownHostException {
        List<String> servers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        Object port = conf.get(Config.STORM_ZOOKEEPER_PORT);
        CuratorFramework zk = ClientZookeeper.mkClient(conf, servers, port, "", new DefaultWatcherCallBack(), conf);
        String leaderLockPath = conf.get(Config.STORM_ZOOKEEPER_ROOT) + "/leader-lock";
        String id = NimbusInfo.fromConf(conf).toHostPortString();
        AtomicReference<LeaderLatch> leaderLatchAtomicReference = new AtomicReference<>(new LeaderLatch(zk, leaderLockPath, id));
        AtomicReference<LeaderLatchListener> leaderLatchListenerAtomicReference =
                new AtomicReference<>(leaderLatchListenerImpl(conf, zk, blobStore, leaderLatchAtomicReference.get()));
        return new LeaderElectorImp(conf, servers, zk, leaderLockPath, id, leaderLatchAtomicReference,
            leaderLatchListenerAtomicReference, blobStore);
    }

}
