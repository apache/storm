/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.zookeeper;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.security.auth.Subject;
import org.apache.storm.Config;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.callback.DefaultWatcherCallBack;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.WorkerTokenServiceType;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.security.auth.NimbusPrincipal;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.zookeeper.KeeperException;
import org.apache.storm.shade.org.apache.zookeeper.ZooDefs;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.apache.zookeeper.data.Id;
import org.apache.storm.shade.org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is code intended to enforce ZK ACLs.
 */
public class AclEnforcement {
    private static final Logger LOG = LoggerFactory.getLogger(AclEnforcement.class);

    /**
     * Verify the ZK ACLs are correct and optionally fix them if needed.
     * @param conf the cluster config.
     * @param fixUp true if we want to fix the ACLs else false.
     * @throws Exception on any error.
     */
    public static void verifyAcls(Map<String, Object> conf, final boolean fixUp) throws Exception {
        if (!Utils.isZkAuthenticationConfiguredStormServer(conf)) {
            LOG.info("SECURITY IS DISABLED NO FURTHER CHECKS...");
            //There is no security so we are done.
            return;
        }
        ACL superUserAcl = Utils.getSuperUserAcl(conf);
        List<ACL> superAcl = new ArrayList<>(1);
        superAcl.add(superUserAcl);

        List<ACL> drpcFullAcl = new ArrayList<>(2);
        drpcFullAcl.add(superUserAcl);

        String drpcAclString = (String) conf.get(Config.STORM_ZOOKEEPER_DRPC_ACL);
        if (drpcAclString != null) {
            Id drpcAclId = Utils.parseZkId(drpcAclString, Config.STORM_ZOOKEEPER_DRPC_ACL);
            ACL drpcUserAcl = new ACL(ZooDefs.Perms.READ, drpcAclId);
            drpcFullAcl.add(drpcUserAcl);
        }

        List<String> zkServers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        int port = ObjectReader.getInt(conf.get(Config.STORM_ZOOKEEPER_PORT));
        String stormRoot = (String) conf.get(Config.STORM_ZOOKEEPER_ROOT);

        try (CuratorFramework zk = ClientZookeeper.mkClient(conf, zkServers, port, "",
                                                            new DefaultWatcherCallBack(), conf, DaemonType.NIMBUS)) {
            if (zk.checkExists().forPath(stormRoot) != null) {
                //First off we want to verify that ROOT is good
                verifyAclStrict(zk, superAcl, stormRoot, fixUp);
            } else {
                LOG.warn("{} does not exist no need to check any more...", stormRoot);
                return;
            }
        }

        // Now that the root is fine we can start to look at the other paths under it.
        try (CuratorFramework zk = ClientZookeeper.mkClient(conf, zkServers, port, stormRoot,
                                                            new DefaultWatcherCallBack(), conf, DaemonType.NIMBUS)) {
            //Next verify that the blob store is correct before we start it up.
            if (zk.checkExists().forPath(ClusterUtils.BLOBSTORE_SUBTREE) != null) {
                verifyAclStrictRecursive(zk, superAcl, ClusterUtils.BLOBSTORE_SUBTREE, fixUp);
            }

            if (zk.checkExists().forPath(ClusterUtils.BLOBSTORE_MAX_KEY_SEQUENCE_NUMBER_SUBTREE) != null) {
                verifyAclStrict(zk, superAcl, ClusterUtils.BLOBSTORE_MAX_KEY_SEQUENCE_NUMBER_SUBTREE, fixUp);
            }

            //The blobstore is good, now lets get the list of all topo Ids
            Set<String> topoIds = new HashSet<>();
            if (zk.checkExists().forPath(ClusterUtils.STORMS_SUBTREE) != null) {
                topoIds.addAll(zk.getChildren().forPath(ClusterUtils.STORMS_SUBTREE));
            }

            Map<String, Id> topoToZkCreds = new HashMap<>();
            //Now lets get the creds for the topos so we can verify those as well.
            BlobStore bs = ServerUtils.getNimbusBlobStore(conf, NimbusInfo.fromConf(conf), null);
            try {
                Subject nimbusSubject = new Subject();
                nimbusSubject.getPrincipals().add(new NimbusPrincipal());
                for (String topoId : topoIds) {
                    try {
                        String blobKey = topoId + "-stormconf.ser";
                        Map<String, Object> topoConf = Utils.fromCompressedJsonConf(bs.readBlob(blobKey, nimbusSubject));
                        String payload = (String) topoConf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD);
                        try {
                            topoToZkCreds.put(topoId, new Id("digest", DigestAuthenticationProvider.generateDigest(payload)));
                        } catch (NoSuchAlgorithmException e) {
                            throw new RuntimeException(e);
                        }
                    } catch (KeyNotFoundException knf) {
                        LOG.debug("topo removed {}", topoId, knf);
                    }
                }
            } finally {
                if (bs != null) {
                    bs.shutdown();
                }
            }

            verifyParentWithReadOnlyTopoChildren(zk, superUserAcl, ClusterUtils.STORMS_SUBTREE, topoToZkCreds, fixUp);
            verifyParentWithReadOnlyTopoChildren(zk, superUserAcl, ClusterUtils.ASSIGNMENTS_SUBTREE, topoToZkCreds, fixUp);
            //There is a race on credentials where they can be leaked in some versions of storm.
            verifyParentWithReadOnlyTopoChildrenDeleteDead(zk, superUserAcl, ClusterUtils.CREDENTIALS_SUBTREE, topoToZkCreds, fixUp);
            //There is a race on logconfig where they can be leaked in some versions of storm.
            verifyParentWithReadOnlyTopoChildrenDeleteDead(zk, superUserAcl, ClusterUtils.LOGCONFIG_SUBTREE, topoToZkCreds, fixUp);
            //There is a race on backpressure too...
            verifyParentWithReadWriteTopoChildrenDeleteDead(zk, superUserAcl, ClusterUtils.BACKPRESSURE_SUBTREE, topoToZkCreds, fixUp);

            if (zk.checkExists().forPath(ClusterUtils.ERRORS_SUBTREE) != null) {
                //errors is a bit special because in older versions of storm the worker created the parent directories lazily
                // because of this it means we need to auto create at least the topo-id directory for all running topos.
                for (String topoId : topoToZkCreds.keySet()) {
                    String path = ClusterUtils.errorStormRoot(topoId);
                    if (zk.checkExists().forPath(path) == null) {
                        LOG.warn("Creating missing errors location {}", path);
                        zk.create().withACL(getTopoReadWrite(path, topoId, topoToZkCreds, superUserAcl, fixUp)).forPath(path);
                    }
                }
            }
            //Error should not be leaked according to the code, but they are not important enough to fail the build if
            // for some odd reason they are leaked.
            verifyParentWithReadWriteTopoChildrenDeleteDead(zk, superUserAcl, ClusterUtils.ERRORS_SUBTREE, topoToZkCreds, fixUp);

            if (zk.checkExists().forPath(ClusterUtils.SECRET_KEYS_SUBTREE) != null) {
                verifyAclStrict(zk, superAcl, ClusterUtils.SECRET_KEYS_SUBTREE, fixUp);
                verifyAclStrictRecursive(zk, superAcl, ClusterUtils.secretKeysPath(WorkerTokenServiceType.NIMBUS), fixUp);
                verifyAclStrictRecursive(zk, drpcFullAcl, ClusterUtils.secretKeysPath(WorkerTokenServiceType.DRPC), fixUp);
            }

            if (zk.checkExists().forPath(ClusterUtils.NIMBUSES_SUBTREE) != null) {
                verifyAclStrictRecursive(zk, superAcl, ClusterUtils.NIMBUSES_SUBTREE, fixUp);
            }

            if (zk.checkExists().forPath("/leader-lock") != null) {
                verifyAclStrictRecursive(zk, superAcl, "/leader-lock", fixUp);
            }

            if (zk.checkExists().forPath(ClusterUtils.PROFILERCONFIG_SUBTREE) != null) {
                verifyAclStrictRecursive(zk, superAcl, ClusterUtils.PROFILERCONFIG_SUBTREE, fixUp);
            }

            if (zk.checkExists().forPath(ClusterUtils.SUPERVISORS_SUBTREE) != null) {
                verifyAclStrictRecursive(zk, superAcl, ClusterUtils.SUPERVISORS_SUBTREE, fixUp);
            }

            // When moving to pacemaker workerbeats can be leaked too...
            verifyParentWithReadWriteTopoChildrenDeleteDead(zk, superUserAcl, ClusterUtils.WORKERBEATS_SUBTREE, topoToZkCreds, fixUp);
        }
    }

    private static List<ACL> getTopoAcl(String path, String topoId, Map<String, Id> topoToZkCreds, ACL superAcl, boolean fixUp, int perms) {
        Id id = topoToZkCreds.get(topoId);
        if (id == null) {
            String error = "Could not find credentials for topology " + topoId + " at path " + path + ".";
            if (fixUp) {
                error += " Don't know how to fix this automatically. Please add needed ACLs, or delete the path.";
            }
            throw new IllegalStateException(error);
        }
        List<ACL> ret = new ArrayList<>(2);
        ret.add(superAcl);
        ret.add(new ACL(perms, id));
        return ret;
    }

    private static List<ACL> getTopoReadWrite(String path, String topoId, Map<String, Id> topoToZkCreds, ACL superAcl, boolean fixUp) {
        return getTopoAcl(path, topoId, topoToZkCreds, superAcl, fixUp, ZooDefs.Perms.ALL);
    }

    private static void verifyParentWithTopoChildrenDeleteDead(CuratorFramework zk, ACL superUserAcl, String path,
                                                               Map<String, Id> topoToZkCreds, boolean fixUp, int perms) throws Exception {
        if (zk.checkExists().forPath(path) != null) {
            verifyAclStrict(zk, Arrays.asList(superUserAcl), path, fixUp);
            Set<String> possiblyBadIds = new HashSet<>();
            for (String topoId : zk.getChildren().forPath(path)) {
                String childPath = path + ClusterUtils.ZK_SEPERATOR + topoId;
                if (!topoToZkCreds.containsKey(topoId)) {
                    //Save it to try again later...
                    possiblyBadIds.add(topoId);
                } else {
                    List<ACL> rwAcl = getTopoAcl(path, topoId, topoToZkCreds, superUserAcl, fixUp, perms);
                    verifyAclStrictRecursive(zk, rwAcl, childPath, fixUp);
                }
            }

            if (!possiblyBadIds.isEmpty()) {
                //Lets reread the children in STORMS as the source of truth and see if a new one was created in the background
                possiblyBadIds.removeAll(zk.getChildren().forPath(ClusterUtils.STORMS_SUBTREE));
                for (String topoId : possiblyBadIds) {
                    //Now we know for sure that this is a bad id
                    String childPath = path + ClusterUtils.ZK_SEPERATOR + topoId;
                    zk.delete().deletingChildrenIfNeeded().forPath(childPath);
                }
            }
        }
    }

    private static void verifyParentWithReadOnlyTopoChildrenDeleteDead(CuratorFramework zk, ACL superUserAcl, String path,
                                                                       Map<String, Id> topoToZkCreds, boolean fixUp) throws Exception {
        verifyParentWithTopoChildrenDeleteDead(zk, superUserAcl, path, topoToZkCreds, fixUp, ZooDefs.Perms.READ);
    }

    private static void verifyParentWithReadWriteTopoChildrenDeleteDead(CuratorFramework zk, ACL superUserAcl, String path,
                                                                        Map<String, Id> topoToZkCreds, boolean fixUp) throws Exception {
        verifyParentWithTopoChildrenDeleteDead(zk, superUserAcl, path, topoToZkCreds, fixUp, ZooDefs.Perms.ALL);
    }

    private static void verifyParentWithTopoChildren(CuratorFramework zk, ACL superUserAcl, String path,
                                                     Map<String, Id> topoToZkCreds, boolean fixUp, int perms) throws Exception {
        if (zk.checkExists().forPath(path) != null) {
            verifyAclStrict(zk, Arrays.asList(superUserAcl), path, fixUp);
            for (String topoId : zk.getChildren().forPath(path)) {
                String childPath = path + ClusterUtils.ZK_SEPERATOR + topoId;
                List<ACL> rwAcl = getTopoAcl(path, topoId, topoToZkCreds, superUserAcl, fixUp, perms);
                verifyAclStrictRecursive(zk, rwAcl, childPath, fixUp);
            }
        }
    }

    private static void verifyParentWithReadOnlyTopoChildren(CuratorFramework zk, ACL superUserAcl, String path,
                                                             Map<String, Id> topoToZkCreds, boolean fixUp) throws Exception {
        verifyParentWithTopoChildren(zk, superUserAcl, path, topoToZkCreds, fixUp, ZooDefs.Perms.READ);
    }

    private static void verifyParentWithReadWriteTopoChildren(CuratorFramework zk, ACL superUserAcl, String path,
                                                              Map<String, Id> topoToZkCreds, boolean fixUp) throws Exception {
        verifyParentWithTopoChildren(zk, superUserAcl, path, topoToZkCreds, fixUp, ZooDefs.Perms.ALL);
    }

    private static void verifyAclStrictRecursive(CuratorFramework zk, List<ACL> strictAcl, String path, boolean fixUp) throws Exception {
        verifyAclStrict(zk, strictAcl, path, fixUp);
        for (String child : zk.getChildren().forPath(path)) {
            String newPath = path + ClusterUtils.ZK_SEPERATOR + child;
            verifyAclStrictRecursive(zk, strictAcl, newPath, fixUp);
        }
    }

    private static void verifyAclStrict(CuratorFramework zk, List<ACL> strictAcl, String path, boolean fixUp) throws Exception {
        try {
            List<ACL> foundAcl = zk.getACL().forPath(path);
            if (!equivalent(foundAcl, strictAcl)) {
                if (fixUp) {
                    LOG.warn("{} expected to have ACL {}, but has {}.  Fixing...", path, strictAcl, foundAcl);
                    zk.setACL().withACL(strictAcl).forPath(path);
                } else {
                    throw new IllegalStateException(path + " did not have the correct ACL found " + foundAcl + " expected " + strictAcl);
                }
            }
        } catch (KeeperException.NoNodeException ne) {
            LOG.debug("{} removed in the middle of checking it", ne);
        }
    }

    private static boolean equivalent(List<ACL> a, List<ACL> b) {
        if (a.size() == b.size()) {
            for (ACL acl : a) {
                if (!b.contains(acl)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public static void main(String[] args) throws Exception {
        Map<String, Object> conf = ConfigUtils.readStormConfig();
        boolean fixUp = false;
        for (String arg : args) {
            String a = arg.toLowerCase();
            if ("-f".equals(a) || "--fixup".equals(a)) {
                fixUp = true;
            } else {
                throw new IllegalArgumentException("Unsupported argument " + arg + " only -f or --fixup is supported.");
            }
        }
        verifyAcls(conf, fixUp);
    }
}