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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.storm.generated.AccessControl;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyAlreadyExistsException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.utils.NimbusClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ClientBlobStoreTest {

    private ClientBlobStore client;

    @BeforeEach
    public void setUp() throws Exception {

        client = new TestClientBlobStore();
        Map<String, Object> conf = new HashMap<>();
        client.prepare(conf);

    }

    @AfterEach
    public void tearDown() throws Exception {
        client = null;
    }

    @Test
    public void testDuplicateACLsForCreate() {
        assertThrows(AuthorizationException.class, () -> {
            SettableBlobMeta meta = new SettableBlobMeta();
            AccessControl submitterAcl = BlobStoreAclHandler.parseAccessControl("u:tester:rwa");
            meta.add_to_acl(submitterAcl);
            AccessControl duplicateAcl = BlobStoreAclHandler.parseAccessControl("u:tester:r--");
            meta.add_to_acl(duplicateAcl);
            String testKey = "testDuplicateACLsBlobKey";
            client.createBlob(testKey, meta);
        });
    }

    @Test
    public void testGoodACLsForCreate() throws Exception {
        SettableBlobMeta meta = new SettableBlobMeta();
        AccessControl submitterAcl = BlobStoreAclHandler.parseAccessControl("u:tester:rwa");
        meta.add_to_acl(submitterAcl);
        String testKey = "testBlobKey";
        client.createBlob(testKey, meta);
        validatedBlobAcls(testKey);
    }

    @Test
    public void testDuplicateACLsForSetBlobMeta() {
        assertThrows(AuthorizationException.class, () -> {
            String testKey = "testDuplicateACLsBlobKey";
            SettableBlobMeta meta = new SettableBlobMeta();
            createTestBlob(testKey, meta);
            AccessControl duplicateAcl = BlobStoreAclHandler.parseAccessControl("u:tester:r--");
            meta.add_to_acl(duplicateAcl);
            client.setBlobMeta(testKey, meta);
        });
    }

    @Test
    public void testGoodACLsForSetBlobMeta() throws Exception {
        String testKey = "testBlobKey";
        SettableBlobMeta meta = new SettableBlobMeta();
        createTestBlob(testKey, meta);
        meta.add_to_acl(BlobStoreAclHandler.parseAccessControl("u:nextuser:r--"));
        client.setBlobMeta(testKey, meta);
        validatedBlobAcls(testKey);
    }

    @Test
    public void testBloblStoreKeyWithUnicodesValidation() {
        BlobStore.validateKey("msg-kafka-unicodewriter䶵-11-1483434711-stormconf.ser");
        BlobStore.validateKey("msg-kafka-ascii-11-148343436363-stormconf.ser");
    }

    private void createTestBlob(String testKey, SettableBlobMeta meta) throws AuthorizationException, KeyAlreadyExistsException {
        AccessControl submitterAcl = BlobStoreAclHandler.parseAccessControl("u:tester:rwa");
        meta.add_to_acl(submitterAcl);
        client.createBlob(testKey, meta);
    }

    private void validatedBlobAcls(String testKey) throws KeyNotFoundException, AuthorizationException {
        ReadableBlobMeta blobMeta = client.getBlobMeta(testKey);
        assertNotNull(blobMeta, "The blob" + testKey + "does not have any readable blobMeta.");
        SettableBlobMeta settableBlob = blobMeta.get_settable();
        assertNotNull(settableBlob, "The blob" + testKey + "does not have any settable blobMeta.");
    }

    public static class TestClientBlobStore extends ClientBlobStore {

        private Map<String, SettableBlobMeta> allBlobs;

        @Override
        public void prepare(Map<String, Object> conf) {
            allBlobs = new HashMap<>();
        }

        @Override
        protected AtomicOutputStream createBlobToExtend(String key, SettableBlobMeta meta) {
            allBlobs.put(key, meta);
            return null;
        }

        @Override
        public AtomicOutputStream updateBlob(String key) throws AuthorizationException, KeyNotFoundException {
            return null;
        }

        @Override
        public ReadableBlobMeta getBlobMeta(String key) throws AuthorizationException, KeyNotFoundException {
            ReadableBlobMeta reableMeta = null;
            if (allBlobs.containsKey(key)) {
                reableMeta = new ReadableBlobMeta();
                reableMeta.set_settable(allBlobs.get(key));
            }
            return reableMeta;
        }

        @Override
        public boolean isRemoteBlobExists(String blobKey) {
            return allBlobs.containsKey(blobKey);
        }

        @Override
        protected void setBlobMetaToExtend(String key, SettableBlobMeta meta) {
        }

        @Override
        public void deleteBlob(String key) throws AuthorizationException, KeyNotFoundException {
        }

        @Override
        public InputStreamWithMeta getBlob(String key) throws AuthorizationException, KeyNotFoundException {
            return null;
        }

        @Override
        public Iterator<String> listKeys() {
            return null;
        }

        @Override
        public void shutdown() {
        }

        @Override
        public void close() {
        }

        @Override
        public int getBlobReplication(String key) {
            return -1;
        }

        @Override
        public int updateBlobReplication(String key, int replication) {
            return -1;
        }

        @Override
        public boolean setClient(Map<String, Object> conf, NimbusClient client) {
            return false;
        }

        @Override
        public void createStateInZookeeper(String key) {
        }

        @Override
        public long getRemoteBlobstoreUpdateTime() {
            return -1L; // not supported
        }
    }
}
