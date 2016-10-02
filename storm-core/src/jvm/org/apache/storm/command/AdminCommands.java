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
package org.apache.storm.command;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.storm.Config;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.blobstore.KeyFilter;
import org.apache.storm.blobstore.LocalFsBlobStore;
import org.apache.storm.callback.DefaultWatcherCallBack;
import org.apache.storm.cluster.ClusterStateContext;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.utils.Utils;
import org.apache.storm.zookeeper.Zookeeper;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.utils.ConfigUtils;

import java.util.*;

public class AdminCommands {

    private static final Logger LOG = LoggerFactory.getLogger(AdminCommands.class);
    private static BlobStore nimbusBlobStore;
    private static IStormClusterState stormClusterState;
    private static Map conf;

    public static void main(String [] args) throws Exception {

        if (args.length == 0) {
            throw new IllegalArgumentException("Missing command. Supported command is remove_corrupt_topologies");
        }
        String command = args[0];
        String[] newArgs = Arrays.copyOfRange(args, 1, args.length);
        switch (command) {
            case "remove_corrupt_topologies":
                initialize();
                removeCorruptTopologies();
                break;
            default:
                LOG.error("****** "+ command + " is not supported. Supported Admin command - \"remove_corrupt_topologies\" ******");
                throw new UnsupportedOperationException(command + " is not a supported admin command.");
        }

    }

    private static void initialize() {
        conf = ConfigUtils.readStormConfig();
        nimbusBlobStore = Utils.getNimbusBlobStore (conf, NimbusInfo.fromConf(conf));
        List<String> servers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        Object port = conf.get(Config.STORM_ZOOKEEPER_PORT);
        List<ACL> acls = null;
        if (Utils.isZkAuthenticationConfiguredStormServer(conf)) {
            acls = adminZkAcls();
        }
        try {
            stormClusterState = ClusterUtils.mkStormClusterState(conf, acls, new ClusterStateContext(DaemonType.NIMBUS));
        } catch (Exception e) {
            LOG.error("admin can't create stormClusterState");
            new RuntimeException(e);
        }
        CuratorFramework zk = Zookeeper.mkClient(conf, servers, port, "", new DefaultWatcherCallBack(),conf);
    }

    // we might think of moving this method in Utils class
    private static List<ACL> adminZkAcls() {
        final List<ACL> acls = new ArrayList<>();
        acls.add(ZooDefs.Ids.CREATOR_ALL_ACL.get(0));
        acls.add(new ACL((ZooDefs.Perms.READ ^ ZooDefs.Perms.CREATE), ZooDefs.Ids.ANYONE_ID_UNSAFE));
        return acls;
    }

    private static Set<String> getKeyListFromId( String corruptId) {
        Set<String> keyLists = new HashSet<>();
        keyLists.add(ConfigUtils.masterStormCodeKey(corruptId));
        keyLists.add(ConfigUtils.masterStormConfKey(corruptId));
        if(!ConfigUtils.isLocalMode(conf)) {
            ConfigUtils.masterStormJarKey(corruptId);
        }
        return keyLists;
    }

    private static void removeCorruptTopologies( ) {
        Iterator<String> corruptTopologies = listCorruptTopologies();
        while(corruptTopologies.hasNext()) {
            String corruptId = corruptTopologies.next();
            stormClusterState.removeStorm(corruptId);
            if(nimbusBlobStore instanceof LocalFsBlobStore) {
                Iterator<String> blobKeys = getKeyListFromId(corruptId).iterator();
                while(blobKeys.hasNext()) {
                    stormClusterState.removeBlobstoreKey(blobKeys.next());
                }
            }
        }
    }

    private static Iterator<String> listCorruptTopologies() {
        Set<String> blobStoreTopologyIds = nimbusBlobStore.filterAndListKeys(new KeyFilter<String>() {
            @Override
            public String filter(String key) {
                return ConfigUtils.getIdFromBlobKey(key);
            }
        });
        Set<String> activeTopologyIds = new HashSet<>(stormClusterState.activeStorms());
        Sets.SetView<String> diffTopology = Sets.difference(activeTopologyIds, blobStoreTopologyIds);
        LOG.info("active-topology-ids [{}] blob-topology-ids [{}] diff-topology [{}]",
                activeTopologyIds.toString(), blobStoreTopologyIds.toString(),diffTopology.toString());
        return diffTopology.iterator();
    }
}
