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

import java.io.File;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.nimbus.TopoCache;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.curator.framework.recipes.leader.Participant;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.storm.shade.org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Zookeeper {
    // A singleton instance allows us to mock delegated static methods in our
    // tests by subclassing.
    private static final Zookeeper INSTANCE = new Zookeeper();
    private static Logger LOG = LoggerFactory.getLogger(Zookeeper.class);
    private static Zookeeper instance = INSTANCE;

    /**
     * Provide an instance of this class for delegates to use.  To mock out delegated methods, provide an instance of a subclass that
     * overrides the implementation of the delegated method.
     *
     * @param u a Zookeeper instance
     */
    public static void setInstance(Zookeeper u) {
        instance = u;
    }

    /**
     * Resets the singleton instance to the default. This is helpful to reset the class to its original functionality when mocking is no
     * longer desired.
     */
    public static void resetInstance() {
        instance = INSTANCE;
    }

    public static NIOServerCnxnFactory mkInprocessZookeeper(String localdir, Integer port) throws Exception {
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
        File localfile = new File(localdir);
        ZooKeeperServer zk = new ZooKeeperServer(localfile, localfile, 2000);
        factory.startup(zk);
        return factory;
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

    /**
     * Get master leader elector.
     *
     * @param conf         Config.
     * @param zkClient     ZkClient, the client must have a default Config.STORM_ZOOKEEPER_ROOT as root path.
     * @param blobStore    {@link BlobStore}
     * @param tc           {@link TopoCache}
     * @param clusterState {@link IStormClusterState}
     * @param acls         ACLs
     * @return Instance of {@link ILeaderElector}
     */
    public static ILeaderElector zkLeaderElector(Map<String, Object> conf, CuratorFramework zkClient, BlobStore blobStore,
                                                 final TopoCache tc, IStormClusterState clusterState, List<ACL> acls,
                                                 StormMetricsRegistry metricsRegistry) {
        return instance.zkLeaderElectorImpl(conf, zkClient, blobStore, tc, clusterState, acls, metricsRegistry);
    }

    protected ILeaderElector zkLeaderElectorImpl(Map<String, Object> conf, CuratorFramework zk, BlobStore blobStore,
                                                 final TopoCache tc, IStormClusterState clusterState, List<ACL> acls,
                                                 StormMetricsRegistry metricsRegistry) {
        String id = NimbusInfo.fromConf(conf).toHostPortString();
        return new LeaderElectorImp(zk, id,
            new LeaderListenerCallbackFactory(conf, zk, blobStore, tc, clusterState, acls, metricsRegistry));
    }

}
