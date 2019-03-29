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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.nimbus.TopoCache;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.nimbus.LeaderListenerCallback;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderListenerCallbackFactory {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderListenerCallbackFactory.class);
    
    private final Map<String, Object> conf;
    private final CuratorFramework zk;
    private final BlobStore blobStore;
    private final TopoCache tc;
    private final IStormClusterState clusterState;
    private final List<ACL> acls;
    private final StormMetricsRegistry metricsRegistry;

    public LeaderListenerCallbackFactory(Map<String, Object> conf, CuratorFramework zk, BlobStore blobStore, TopoCache tc,
        IStormClusterState clusterState, List<ACL> acls, StormMetricsRegistry metricsRegistry) {
        this.conf = conf;
        this.zk = zk;
        this.blobStore = blobStore;
        this.tc = tc;
        this.clusterState = clusterState;
        this.acls = acls;
        this.metricsRegistry = metricsRegistry;
    }
    
    public LeaderLatchListener create(ILeaderElector elector) throws UnknownHostException {
        final LeaderListenerCallback callback = new LeaderListenerCallback(conf, zk, blobStore, elector,
            tc, clusterState, acls, metricsRegistry);
        final String hostName = InetAddress.getLocalHost().getCanonicalHostName();
        return new LeaderLatchListener() {

            @Override
            public void isLeader() {
                callback.leaderCallBack();
                LOG.info("{} gained leadership.", hostName);
            }

            @Override
            public void notLeader() {
                LOG.info("{} lost leadership.", hostName);
                //Just to be sure
                callback.notLeaderCallback();
            }
        };
    }
    
}
