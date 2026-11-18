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

package org.apache.storm.blobstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.storm.shade.org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.storm.testing.InProcessZookeeper;
import org.apache.storm.zookeeper.curator.CuratorZkClient;
import org.junit.jupiter.api.Test;

class KeySequenceNumberTest {
    private static final String KEY = "dep-lib-11111111-1111-1111-1111-111111111111.jar";
    private static final String KEY_PATH = "/blobstore/" + KEY;
    private static final String MAX_SEQUENCE_PATH = "/blobstoremaxkeysequencenumber/" + KEY;
    private static final NimbusInfo LEADER = new NimbusInfo("nimbus-1", 6627, false);
    private static final NimbusInfo PEER = new NimbusInfo("nimbus-2", 6627, false);

    /**
     * Replays the blob store state changes behind the nimbus logs reported on STORM-3871. A non-leader that downloaded
     * a blob while the leader deleted it registered the key again as new, and every other nimbus, the leader included,
     * then downloaded the blob back from it.
     */
    @Test
    void aNonLeaderCannotRegisterAKeyAgainThatWasDeletedWhileItDownloadedIt() throws Exception {
        try (InProcessZookeeper zk = new InProcessZookeeper();
             CuratorFramework zkClient = newClient(zk)) {
            // the client uploads the dependency: createBlob, then createStateInZookeeper when it closes the stream
            assertEquals(1, register(zkClient, LEADER, true));
            assertEquals(2, register(zkClient, LEADER, true));

            // the topology is cleaned up and the leader deletes the blob, as LocalFsBlobStore#deleteBlob does
            zkClient.delete().deletingChildrenIfNeeded().forPath(KEY_PATH);
            zkClient.delete().deletingChildrenIfNeeded().forPath(MAX_SEQUENCE_PATH);

            // the non-leader finishes its download and would register the key as if it were new
            assertThrows(KeyNotFoundException.class, () -> register(zkClient, PEER, false));
            assertNull(zkClient.checkExists().forPath(KEY_PATH));
            assertNull(zkClient.checkExists().forPath(MAX_SEQUENCE_PATH));
        }
    }

    @Test
    void aNonLeaderStillRegistersItsCopyOfAKeyTheLeaderCreated() throws Exception {
        try (InProcessZookeeper zk = new InProcessZookeeper();
             CuratorFramework zkClient = newClient(zk)) {
            assertEquals(1, register(zkClient, LEADER, true));

            assertEquals(0, register(zkClient, PEER, false));
        }
    }

    private static CuratorFramework newClient(InProcessZookeeper zk) {
        CuratorFramework zkClient = CuratorFrameworkFactory.newClient("localhost:" + zk.getPort(),
            new ExponentialBackoffRetry(1000, 3));
        zkClient.start();
        return zkClient;
    }

    /**
     * Do what IStormClusterState#setupBlob does with the version KeySequenceNumber hands out.
     */
    private static int register(CuratorFramework zkClient, NimbusInfo nimbus, boolean mayCreateKey) throws Exception {
        int version = new KeySequenceNumber(KEY, nimbus).getKeySequenceNumber(CuratorZkClient.wrap(zkClient), mayCreateKey);
        if (zkClient.checkExists().forPath(KEY_PATH) != null) {
            for (String child : zkClient.getChildren().forPath(KEY_PATH)) {
                if (child.startsWith(nimbus.toHostPortString())) {
                    zkClient.delete().forPath(KEY_PATH + "/" + child);
                }
            }
        }
        zkClient.create().creatingParentsIfNeeded().forPath(KEY_PATH + "/" + nimbus.toHostPortString() + "-" + version);
        return version;
    }
}
