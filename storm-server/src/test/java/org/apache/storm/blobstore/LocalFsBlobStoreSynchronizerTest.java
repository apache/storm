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

package org.apache.storm.blobstore;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.storm.Config;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.storm.shade.org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *  Unit tests for most of the testable utility methods
 *  and LocalFsBlobStoreSynchronizer class methods
 */
public class LocalFsBlobStoreSynchronizerTest {
    private static Map<String, Object> conf = new HashMap<>();

    private File baseFile;

    // Method which initializes nimbus admin
    @BeforeAll
    public static void initializeConfigs() {
        conf.put(Config.NIMBUS_ADMINS, Collections.singletonList("admin"));
        conf.put(Config.NIMBUS_SUPERVISOR_USERS, Collections.singletonList("supervisor"));
    }

    @BeforeEach
    public void init() {
        baseFile = new File("target/blob-store-test-" + UUID.randomUUID());
    }

    @AfterEach
    public void cleanUp() throws IOException {
        FileUtils.deleteDirectory(baseFile);
    }

    private LocalFsBlobStore initLocalFs() {
        LocalFsBlobStore store = new LocalFsBlobStore();
        Map<String, Object> conf = new HashMap<>(this.conf);
        conf.putAll(Utils.readStormConfig());
        conf.put(Config.STORM_LOCAL_DIR, baseFile.getAbsolutePath());
        conf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, "org.apache.storm.security.auth.DefaultPrincipalToLocal");
        store.prepare(conf, null, null, null);
        return store;
    }

  @Test
  public void testBlobSynchronizerForKeysToDownload() {
    BlobStore store = initLocalFs();
    LocalFsBlobStoreSynchronizer sync = new LocalFsBlobStoreSynchronizer(store, conf);
    // test for keylist to download
    Set<String> zkSet = new HashSet<>();
    zkSet.add("key1");
    Set<String> blobStoreSet = new HashSet<>();
    blobStoreSet.add("key1");
    Set<String> resultSet = sync.getKeySetToDownload(blobStoreSet, zkSet);
    assertTrue(resultSet.isEmpty(), "Not Empty");
    zkSet.add("key1");
    blobStoreSet.add("key2");
    resultSet =  sync.getKeySetToDownload(blobStoreSet, zkSet);
    assertTrue(resultSet.isEmpty(), "Not Empty");
    blobStoreSet.remove("key1");
    blobStoreSet.remove("key2");
    zkSet.add("key1");
    resultSet =  sync.getKeySetToDownload(blobStoreSet, zkSet);
    assertTrue((resultSet.size() == 1) && (resultSet.contains("key1")), "Unexpected keys to download");
  }

    @Test
    public void testGetLatestSequenceNumber() {
        List<String> stateInfoList = new ArrayList<>();
        stateInfoList.add("nimbus1:8000-2");
        stateInfoList.add("nimbus-1:8000-4");
        assertEquals(4, BlobStoreUtils.getLatestSequenceNumber(stateInfoList), "Failed to get the latest version");
    }

    @Test
    public void testNimbodesWithLatestVersionOfBlob() throws Exception {
        try (TestingServer server = new TestingServer(); CuratorFramework zkClient = CuratorFrameworkFactory
            .newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3))) {
            zkClient.start();
            // Creating nimbus hosts containing the latest version of blob
            zkClient.create().creatingParentContainersIfNeeded().forPath("/blobstore/key1/nimbus1:7800-1");
            zkClient.create().creatingParentContainersIfNeeded().forPath("/blobstore/key1/nimbus2:7800-2");
            Set<NimbusInfo> set = BlobStoreUtils.getNimbodesWithLatestSequenceNumberOfBlob(zkClient, "key1");
            assertEquals("nimbus2", (set.iterator().next()).getHost(), "Failed to get the correct nimbus hosts with latest blob version");
            zkClient.delete().deletingChildrenIfNeeded().forPath("/blobstore/key1/nimbus1:7800-1");
            zkClient.delete().deletingChildrenIfNeeded().forPath("/blobstore/key1/nimbus2:7800-2");
        }
    }

    @Test
    public void testNormalizeVersionInfo() {
        BlobKeySequenceInfo info1 = BlobStoreUtils.normalizeNimbusHostPortSequenceNumberInfo("nimbus1:7800-1");
        assertEquals("nimbus1:7800", info1.getNimbusHostPort());
        assertEquals("1", info1.getSequenceNumber());
        BlobKeySequenceInfo info2 = BlobStoreUtils.normalizeNimbusHostPortSequenceNumberInfo("nimbus-1:7800-1");
        assertEquals("nimbus-1:7800", info2.getNimbusHostPort());
        assertEquals("1", info2.getSequenceNumber());
    }
}
