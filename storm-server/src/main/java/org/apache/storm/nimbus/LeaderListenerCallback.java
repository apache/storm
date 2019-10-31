/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.nimbus;

import com.codahale.metrics.Meter;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import javax.security.auth.Subject;

import org.apache.commons.io.IOUtils;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.nimbus.TopoCache;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.shade.com.google.common.base.Joiner;
import org.apache.storm.shade.com.google.common.collect.Sets;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.storm.shade.org.apache.zookeeper.CreateMode;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Utils;
import org.apache.storm.zookeeper.ClientZookeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A callback function when nimbus gains leadership.
 */
public class LeaderListenerCallback {
    private static final Logger LOG = LoggerFactory.getLogger(LeaderListenerCallback.class);
    private static final String STORM_JAR_SUFFIX = "-stormjar.jar";
    private static final String STORM_CODE_SUFFIX = "-stormcode.ser";
    private static final String STORM_CONF_SUFFIX = "-stormconf.ser";
    
    private final Meter numGainedLeader;
    private final Meter numLostLeader;
    
    private final BlobStore blobStore;
    private final TopoCache tc;
    private final IStormClusterState clusterState;
    private final CuratorFramework zk;
    private final ILeaderElector leaderElector;
    private final Map conf;
    private final List<ACL> acls;
    private final int requeueDelayMs;

    /**
     * Constructor for {@LeaderListenerCallback}.
     * @param conf config
     * @param zk zookeeper CuratorFramework client
     * @param blobStore BlobStore
     * @param leaderElector Leader elector
     * @param tc TopoCache
     * @param clusterState IStormClusterState
     * @param acls zookeeper acls
     */
    public LeaderListenerCallback(Map conf, CuratorFramework zk, BlobStore blobStore, ILeaderElector leaderElector,
                                  TopoCache tc, IStormClusterState clusterState, List<ACL> acls, StormMetricsRegistry metricsRegistry) {
        this.blobStore = blobStore;
        this.tc = tc;
        this.clusterState = clusterState;
        this.zk = zk;
        this.leaderElector = leaderElector;
        this.conf = conf;
        this.acls = acls;
        this.numGainedLeader = metricsRegistry.registerMeter("nimbus:num-gained-leadership");
        this.numLostLeader = metricsRegistry.registerMeter("nimbus:num-lost-leadership");
        //Since we only give up leadership if we're waiting for blobs to sync,
        //it makes sense to wait a full sync cycle before trying for leadership again.
        this.requeueDelayMs = ObjectReader.getInt(conf.get(DaemonConfig.NIMBUS_CODE_SYNC_FREQ_SECS)) * 1000;
    }

    /**
     * Invoke when gains leadership.
     */
    public void leaderCallBack() {
        numGainedLeader.mark();
        //set up nimbus-info to zk
        setUpNimbusInfo(acls);
        //sync zk assignments/id-info to local
        LOG.info("Sync remote assignments and id-info to local");
        clusterState.syncRemoteAssignments(null);
        clusterState.syncRemoteIds(null);
        clusterState.setAssignmentsBackendSynchronized();

        Set<String> activeTopologyIds = new TreeSet<>(ClientZookeeper.getChildren(zk,
                                                                                  ClusterUtils.STORMS_SUBTREE, false));

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
                tc.clear();
            } else {
                LOG.info("Code for all active topologies is available locally, but some dependencies are not found locally, "
                         + "giving up leadership.");
                surrenderLeadership();
            }
        } else {
            LOG.info("code for all active topologies not available locally, giving up leadership.");
            surrenderLeadership();
        }
    }

    /**
     * Invoke when lost leadership.
     */
    public void notLeaderCallback() {
        numLostLeader.mark();
        tc.clear();
    }

    private void setUpNimbusInfo(List<ACL> acls) {
        String leaderInfoPath = conf.get(Config.STORM_ZOOKEEPER_ROOT) + ClusterUtils.LEADERINFO_SUBTREE;
        NimbusInfo nimbusInfo = NimbusInfo.fromConf(conf);
        if (ClientZookeeper.existsNode(zk, leaderInfoPath, false)) {
            ClientZookeeper.setData(zk, leaderInfoPath, Utils.javaSerialize(nimbusInfo));
        } else {
            ClientZookeeper.createNode(zk, leaderInfoPath, Utils.javaSerialize(nimbusInfo), CreateMode.PERSISTENT, acls);
        }
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
            if (blobKey.endsWith(STORM_JAR_SUFFIX)
                || blobKey.endsWith(STORM_CODE_SUFFIX)
                || blobKey.endsWith(STORM_CONF_SUFFIX)) {
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
            try (InputStreamWithMeta blob = blobStore.getBlob(activeTopologyCodeKey, subject)) {
                byte[] blobContent = IOUtils.readFully(blob, new Long(blob.getFileLength()).intValue());
                StormTopology stormCode = Utils.deserialize(blobContent, StormTopology.class);
                if (stormCode.is_set_dependency_jars()) {
                    activeTopologyDependencies.addAll(stormCode.get_dependency_jars());
                }
                if (stormCode.is_set_dependency_artifacts()) {
                    activeTopologyDependencies.addAll(stormCode.get_dependency_artifacts());
                }
            } catch (AuthorizationException | KeyNotFoundException | IOException e) {
                LOG.error("Exception occurs while reading blob for key: "
                          + activeTopologyCodeKey
                          + ", exception: "
                          + e, e);
                throw new RuntimeException("Exception occurs while reading blob for key: "
                                           + activeTopologyCodeKey
                                           + ", exception: " + e, e);
            }
        }
        return activeTopologyDependencies;
    }

    private void surrenderLeadership() {
        try {
            leaderElector.quitElectionFor(requeueDelayMs);
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }

}
