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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.security.auth.Subject;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.blobstore.AtomicOutputStream;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.shade.org.apache.zookeeper.CreateMode;
import org.apache.storm.shade.org.apache.zookeeper.ZooDefs;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.apache.zookeeper.data.Id;
import org.apache.storm.shade.org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.apache.storm.testing.InProcessZookeeper;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link AclEnforcement} against a real (in-process) ZooKeeper.
 */
public class AclEnforcementTest {
    private static final String LIVE_TOPO = "live-topo-1-1";
    private static final String DEAD_TOPO = "dead-topo-1-1";
    private static final String TOPO_PAYLOAD = "topo:secret";

    /**
     * Minimal blob store that only serves the topology configuration of {@link #LIVE_TOPO}.
     */
    public static class StubBlobStore extends BlobStore {
        @Override
        public void prepare(Map<String, Object> conf, String baseDir, NimbusInfo nimbusInfo,
                            ILeaderElector leaderElector) {
        }

        @Override
        public byte[] readBlob(String key, Subject who) throws KeyNotFoundException {
            if ((LIVE_TOPO + "-stormconf.ser").equals(key)) {
                return Utils.toCompressedJsonConf(
                    Collections.singletonMap(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD, TOPO_PAYLOAD));
            }
            throw new KeyNotFoundException(key);
        }

        @Override
        public AtomicOutputStream createBlob(String key, SettableBlobMeta meta, Subject who) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AtomicOutputStream updateBlob(String key, Subject who) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ReadableBlobMeta getBlobMeta(String key, Subject who) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setLeaderElector(ILeaderElector leaderElector) {
        }

        @Override
        public void setBlobMeta(String key, SettableBlobMeta meta, Subject who) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteBlob(String key, Subject who) {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputStreamWithMeta getBlob(String key, Subject who) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<String> listKeys() {
            return Collections.emptyIterator();
        }

        @Override
        public int getBlobReplication(String key, Subject who) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int updateBlobReplication(String key, int replication, Subject who) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown() {
        }
    }

    @Test
    public void verifyAndFixAcls() throws Exception {
        try (InProcessZookeeper zkServer = new InProcessZookeeper()) {
            Map<String, Object> conf = Utils.readStormConfig();
            conf.put(Config.STORM_ZOOKEEPER_PORT, zkServer.getPort());
            conf.put(Config.STORM_ZOOKEEPER_SERVERS, Collections.singletonList("localhost"));
            conf.put(Config.STORM_ZOOKEEPER_AUTH_SCHEME, "digest");
            conf.put(Config.STORM_ZOOKEEPER_AUTH_PAYLOAD, "storm:password");
            conf.put(Config.STORM_ZOOKEEPER_SUPERACL,
                "digest:" + DigestAuthenticationProvider.generateDigest("storm:password"));
            conf.put(DaemonConfig.NIMBUS_BLOBSTORE, StubBlobStore.class.getName());
            String root = (String) conf.get(Config.STORM_ZOOKEEPER_ROOT);

            ACL superAcl = Utils.getSuperUserAcl(conf);
            Id topoId = new Id("digest", DigestAuthenticationProvider.generateDigest(TOPO_PAYLOAD));
            List<ACL> open = ZooDefs.Ids.OPEN_ACL_UNSAFE;

            try (ZkClient zk = ClientZookeeper.mkClient(conf, Collections.singletonList("localhost"),
                zkServer.getPort(), "", null, conf, DaemonType.UNKNOWN)) {
                for (String path : Arrays.asList(root, root + "/blobstore", root + "/blobstore/key1",
                    root + "/storms", root + "/storms/" + LIVE_TOPO,
                    root + "/credentials", root + "/credentials/" + DEAD_TOPO,
                    root + "/errors")) {
                    zk.create(path, new byte[]{1}, CreateMode.PERSISTENT, open, false);
                }

                // Wrong ACLs and no fixing must fail.
                assertThrows(IllegalStateException.class, () -> AclEnforcement.verifyAcls(conf, false));

                AclEnforcement.verifyAcls(conf, true);

                Set<ACL> superOnly = Collections.singleton(superAcl);
                assertEquals(superOnly, new HashSet<>(zk.getAcl(root)));
                assertEquals(superOnly, new HashSet<>(zk.getAcl(root + "/blobstore")));
                assertEquals(superOnly, new HashSet<>(zk.getAcl(root + "/blobstore/key1")));
                assertEquals(superOnly, new HashSet<>(zk.getAcl(root + "/storms")));
                assertEquals(new HashSet<>(Arrays.asList(superAcl, new ACL(ZooDefs.Perms.READ, topoId))),
                    new HashSet<>(zk.getAcl(root + "/storms/" + LIVE_TOPO)));
                assertEquals(superOnly, new HashSet<>(zk.getAcl(root + "/credentials")));

                // The dead topology's credentials are removed.
                assertNull(zk.exists(root + "/credentials/" + DEAD_TOPO, false));

                // The missing errors node for the live topology is created with its read-write ACL.
                assertNotNull(zk.exists(root + "/errors/" + LIVE_TOPO, false));
                assertEquals(new HashSet<>(Arrays.asList(superAcl, new ACL(ZooDefs.Perms.ALL, topoId))),
                    new HashSet<>(zk.getAcl(root + "/errors/" + LIVE_TOPO)));

                // Everything is now in order.
                assertDoesNotThrow(() -> AclEnforcement.verifyAcls(conf, false));
            }
        }
    }
}
