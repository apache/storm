
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
package org.apache.storm.hdfs.blobstore;

import org.apache.storm.hdfs.testing.MiniDFSClusterExtension;
import org.apache.storm.Config;
import org.apache.storm.blobstore.AtomicOutputStream;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.BlobStoreAclHandler;
import org.apache.storm.generated.AccessControl;
import org.apache.storm.generated.AccessControlType;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.security.auth.FixedGroupsMapping;
import org.apache.storm.security.auth.NimbusPrincipal;
import org.apache.storm.security.auth.SingleUserPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

public class BlobStoreTest {

    @RegisterExtension
    public static final MiniDFSClusterExtension DFS_CLUSTER_EXTENSION = new MiniDFSClusterExtension();

    private static final Logger LOG = LoggerFactory.getLogger(BlobStoreTest.class);
    URI base;
    private static final Map<String, Object> CONF = new HashMap<>();
    public static final int READ = 0x01;
    public static final int WRITE = 0x02;
    public static final int ADMIN = 0x04;

    @BeforeEach
    public void init() {
        initializeConfigs();
    }

    @AfterEach
    public void cleanup()
        throws IOException {
    }

    // Method which initializes nimbus admin
    public static void initializeConfigs() {
        CONF.put(Config.NIMBUS_ADMINS, "admin");
        CONF.put(Config.NIMBUS_ADMINS_GROUPS, "adminsGroup");

        // Construct a groups mapping for the FixedGroupsMapping class
        Map<String, Set<String>> groupsMapping = new HashMap<>();
        Set<String> groupSet = new HashSet<>();
        groupSet.add("adminsGroup");
        groupsMapping.put("adminsGroupsUser", groupSet);

        // Now create a params map to put it in to our conf
        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put(FixedGroupsMapping.STORM_FIXED_GROUP_MAPPING, groupsMapping);
        CONF.put(Config.STORM_GROUP_MAPPING_SERVICE_PROVIDER_PLUGIN, "org.apache.storm.security.auth.FixedGroupsMapping");
        CONF.put(Config.STORM_GROUP_MAPPING_SERVICE_PARAMS, paramMap);
        CONF.put(Config.NIMBUS_SUPERVISOR_USERS, "supervisor");
    }

    //Gets Nimbus Subject with NimbusPrincipal set on it
    public static Subject getNimbusSubject() {
        Subject nimbus = new Subject();
        nimbus.getPrincipals().add(new NimbusPrincipal());
        return nimbus;
    }

    // Overloading the assertStoreHasExactly method accomodate Subject in order to check for authorization
    public static void assertStoreHasExactly(BlobStore store, Subject who, String... keys)
        throws IOException, KeyNotFoundException, AuthorizationException {
        Set<String> expected = new HashSet<>(Arrays.asList(keys));
        Set<String> found = new HashSet<>();
        Iterator<String> c = store.listKeys();
        while (c.hasNext()) {
            String keyName = c.next();
            found.add(keyName);
        }
        Set<String> extra = new HashSet<>(found);
        extra.removeAll(expected);
        assertTrue("Found extra keys in the blob store " + extra, extra.isEmpty());
        Set<String> missing = new HashSet<>(expected);
        missing.removeAll(found);
        assertTrue("Found keys missing from the blob store " + missing, missing.isEmpty());
    }

    public static void assertStoreHasExactly(BlobStore store, String... keys)
        throws IOException, KeyNotFoundException, AuthorizationException {
        assertStoreHasExactly(store, null, keys);
    }

    // Overloading the readInt method accomodate Subject in order to check for authorization (security turned on)
    public static int readInt(BlobStore store, Subject who, String key) throws IOException, KeyNotFoundException, AuthorizationException {
        try (InputStream in = store.getBlob(key, who)) {
            return in.read();
        }
    }

    public static int readInt(BlobStore store, String key)
        throws IOException, KeyNotFoundException, AuthorizationException {
        return readInt(store, null, key);
    }

    public static void readAssertEquals(BlobStore store, String key, int value)
        throws IOException, KeyNotFoundException, AuthorizationException {
        assertEquals(value, readInt(store, key));
    }

    // Checks for assertion when we turn on security
    public void readAssertEqualsWithAuth(BlobStore store, Subject who, String key, int value)
        throws IOException, KeyNotFoundException, AuthorizationException {
        assertEquals(value, readInt(store, who, key));
    }

    private AutoCloseableBlobStoreContainer initHdfs(String dirName) {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.BLOBSTORE_DIR, dirName);
        conf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, "org.apache.storm.security.auth.DefaultPrincipalToLocal");
        conf.put(Config.STORM_BLOBSTORE_REPLICATION_FACTOR, 3);
        HdfsBlobStore store = new HdfsBlobStore();
        store.prepareInternal(conf, null, DFS_CLUSTER_EXTENSION.getDfscluster().getConfiguration(0));
        return new AutoCloseableBlobStoreContainer(store);
    }

    private static class AutoCloseableBlobStoreContainer implements AutoCloseable {

        private final HdfsBlobStore blobStore;

        public AutoCloseableBlobStoreContainer(HdfsBlobStore blobStore) {
            this.blobStore = blobStore;
        }

        @Override
        public void close() throws Exception {
            this.blobStore.shutdown();
        }

    }

    @Test
    public void testHdfsReplication()
        throws Exception {
        try (AutoCloseableBlobStoreContainer container = initHdfs("/storm/blobstoreReplication")) {
            testReplication("/storm/blobstoreReplication/test", container.blobStore);
        }
    }

    @Test
    public void testBasicHdfs()
        throws Exception {
        try (AutoCloseableBlobStoreContainer container = initHdfs("/storm/blobstore1")) {
            testBasic(container.blobStore);
        }
    }

    @Test
    public void testMultipleHdfs()
        throws Exception {
        // use different blobstore dir so it doesn't conflict with other test
        try (AutoCloseableBlobStoreContainer container = initHdfs("/storm/blobstore2")) {
            testMultiple(container.blobStore);
        }
    }

    // Test for replication.
    public void testReplication(String path, BlobStore store)
        throws Exception {
        SettableBlobMeta metadata = new SettableBlobMeta(BlobStoreAclHandler.WORLD_EVERYTHING);
        metadata.set_replication_factor(4);
        try (AtomicOutputStream out = store.createBlob("test", metadata, null)) {
            out.write(1);
        }
        assertStoreHasExactly(store, "test");
        assertEquals("Blobstore replication not matching", store.getBlobReplication("test", null), 4);
        store.deleteBlob("test", null);

        //Test for replication with NIMBUS as user
        Subject admin = getSubject("admin");
        metadata = new SettableBlobMeta(BlobStoreAclHandler.DEFAULT);
        metadata.set_replication_factor(4);
        try (AtomicOutputStream out = store.createBlob("test", metadata, admin)) {
            out.write(1);
        }
        assertStoreHasExactly(store, "test");
        assertEquals("Blobstore replication not matching", store.getBlobReplication("test", admin), 4);
        store.updateBlobReplication("test", 5, admin);
        assertEquals("Blobstore replication not matching", store.getBlobReplication("test", admin), 5);
        store.deleteBlob("test", admin);

        //Test for replication using SUPERVISOR access
        Subject supervisor = getSubject("supervisor");
        metadata = new SettableBlobMeta(BlobStoreAclHandler.DEFAULT);
        metadata.set_replication_factor(4);
        try (AtomicOutputStream out = store.createBlob("test", metadata, supervisor)) {
            out.write(1);
        }
        assertStoreHasExactly(store, "test");
        assertEquals("Blobstore replication not matching", store.getBlobReplication("test", supervisor), 4);
        store.updateBlobReplication("test", 5, supervisor);
        assertEquals("Blobstore replication not matching", store.getBlobReplication("test", supervisor), 5);
        store.deleteBlob("test", supervisor);

        Subject adminsGroupsUser = getSubject("adminsGroupsUser");
        metadata = new SettableBlobMeta(BlobStoreAclHandler.DEFAULT);
        metadata.set_replication_factor(4);
        try (AtomicOutputStream out = store.createBlob("test", metadata, adminsGroupsUser)) {
            out.write(1);
        }
        assertStoreHasExactly(store, "test");
        assertEquals("Blobstore replication not matching", store.getBlobReplication("test", adminsGroupsUser), 4);
        store.updateBlobReplication("test", 5, adminsGroupsUser);
        assertEquals("Blobstore replication not matching", store.getBlobReplication("test", adminsGroupsUser), 5);
        store.deleteBlob("test", adminsGroupsUser);

        //Test for a user having read or write or admin access to read replication for a blob
        String createSubject = "createSubject";
        String writeSubject = "writeSubject";
        String adminSubject = "adminSubject";
        Subject who = getSubject(createSubject);
        AccessControl writeAccess = new AccessControl(AccessControlType.USER, READ);
        AccessControl adminAccess = new AccessControl(AccessControlType.USER, ADMIN);
        writeAccess.set_name(writeSubject);
        adminAccess.set_name(adminSubject);
        List<AccessControl> acl = Arrays.asList(writeAccess, adminAccess);
        metadata = new SettableBlobMeta(acl);
        metadata.set_replication_factor(4);
        try (AtomicOutputStream out = store.createBlob("test", metadata, who)) {
            out.write(1);
        }
        assertStoreHasExactly(store, "test");
        who = getSubject(writeSubject);
        assertEquals("Blobstore replication not matching", store.getBlobReplication("test", who), 4);

        //Test for a user having WRITE or ADMIN privileges to change replication of a blob
        who = getSubject(adminSubject);
        store.updateBlobReplication("test", 5, who);
        assertEquals("Blobstore replication not matching", store.getBlobReplication("test", who), 5);
        store.deleteBlob("test", getSubject(createSubject));
    }

    public static Subject getSubject(String name) {
        Subject subject = new Subject();
        SingleUserPrincipal user = new SingleUserPrincipal(name);
        subject.getPrincipals().add(user);
        return subject;
    }
    
    static enum AuthenticationTestSubject {
        //Nimbus Admin
        ADMIN(getSubject("admin")),
        //Nimbus groups admin
        ADMIN_GROUPS_USER(getSubject("adminGroupsUser")),
        //Supervisor admin
        SUPERVISOR(getSubject("supervisor")),
        //Nimbus itself
        NIMBUS(getNimbusSubject());
        
        private Subject subject;

        private AuthenticationTestSubject(Subject subject) {
            this.subject = subject;
        }
    }
    
    @ParameterizedTest
    @EnumSource(value = AuthenticationTestSubject.class)
    void testWithAuthentication(AuthenticationTestSubject testSubject) throws Exception {
        try (AutoCloseableBlobStoreContainer container = initHdfs("/storm/blobstore-auth-" + testSubject.name())) {
            BlobStore store = container.blobStore;
            assertStoreHasExactly(store);
            SettableBlobMeta metadata = new SettableBlobMeta(BlobStoreAclHandler.DEFAULT);
            try (AtomicOutputStream out = store.createBlob("test", metadata, testSubject.subject)) {
                assertStoreHasExactly(store, "test");
                out.write(1);
            }
            store.deleteBlob("test", testSubject.subject);
        }
    }
    
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testWithAuthenticationDummy(boolean securityEnabled) throws Exception {
        try (AutoCloseableBlobStoreContainer container = initHdfs("/storm/blobstore-auth-dummy-sec-" + securityEnabled)) {
            BlobStore store = container.blobStore;
            Subject who = getSubject("test_subject");
            assertStoreHasExactly(store);

            // Tests for case when subject != null (security turned on) and
            // acls for the blob are set to WORLD_EVERYTHING
            SettableBlobMeta metadata = new SettableBlobMeta(securityEnabled ? BlobStoreAclHandler.DEFAULT : BlobStoreAclHandler.WORLD_EVERYTHING);
            try (AtomicOutputStream out = store.createBlob("test", metadata, who)) {
                out.write(1);
            }
            assertStoreHasExactly(store, "test");
            if (securityEnabled) {
                // Testing whether acls are set to WORLD_EVERYTHING. Here the acl should not contain WORLD_EVERYTHING because
                // the subject is neither null nor empty. The ACL should however contain USER_EVERYTHING as user needs to have
                // complete access to the blob
                assertTrue("ACL contains WORLD_EVERYTHING", !metadata.toString().contains("AccessControl(type:OTHER, access:7)"));
            } else {
                // Testing whether acls are set to WORLD_EVERYTHING
                assertTrue("ACL does not contain WORLD_EVERYTHING", metadata.toString().contains("AccessControl(type:OTHER, access:7)"));
            }
            
            readAssertEqualsWithAuth(store, who, "test", 1);

            LOG.info("Deleting test");
            store.deleteBlob("test", who);
            assertStoreHasExactly(store);
        }
    }
    
    @Test
    void testWithAuthenticationUpdate() throws Exception {
        try (AutoCloseableBlobStoreContainer container = initHdfs("/storm/blobstore-auth-update")) {
            BlobStore store = container.blobStore;
            Subject who = getSubject("test_subject");
            assertStoreHasExactly(store);

            SettableBlobMeta metadata = new SettableBlobMeta(BlobStoreAclHandler.DEFAULT);
            try (AtomicOutputStream out = store.createBlob("test", metadata, who)) {
                out.write(1);
            }
            assertStoreHasExactly(store, "test");
            readAssertEqualsWithAuth(store, who, "test", 1);
            
            try (AtomicOutputStream out = store.updateBlob("test", who)) {
                out.write(2);
            }
            assertStoreHasExactly(store, "test");
            readAssertEqualsWithAuth(store, who, "test", 2);
            
            try (AtomicOutputStream out = store.updateBlob("test", who)) {
                out.write(3);
            }
            assertStoreHasExactly(store, "test");
            readAssertEqualsWithAuth(store, who, "test", 3);

            LOG.info("Deleting test");
            store.deleteBlob("test", who);
            assertStoreHasExactly(store);
        }
    }
    
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testWithAuthenticationNoPrincipal(boolean securityEnabled) throws Exception {
        try (AutoCloseableBlobStoreContainer container = initHdfs("/storm/blobstore-auth-no-principal-sec-" + securityEnabled)) {
            BlobStore store = container.blobStore;
            //Test for subject with no principals
            Subject who = new Subject();
            assertStoreHasExactly(store);

            // Tests for case when subject != null (security turned on) and
            // acls for the blob are set to WORLD_EVERYTHING
            SettableBlobMeta metadata = new SettableBlobMeta(securityEnabled ? BlobStoreAclHandler.DEFAULT : BlobStoreAclHandler.WORLD_EVERYTHING);
            try (AtomicOutputStream out = store.createBlob("test", metadata, who)) {
                out.write(1);
            }
            assertStoreHasExactly(store, "test");
            // With no principals in the subject ACL should always be set to WORLD_EVERYTHING
            assertTrue("ACL does not contain WORLD_EVERYTHING", metadata.toString().contains("AccessControl(type:OTHER, access:7)"));
            
            readAssertEqualsWithAuth(store, who, "test", 1);
        }
    }

    public void testBasic(BlobStore store)
        throws Exception {
        assertStoreHasExactly(store);
        LOG.info("Creating test");
        // Tests for case when subject == null (security turned off) and
        // acls for the blob are set to WORLD_EVERYTHING
        SettableBlobMeta metadata = new SettableBlobMeta(BlobStoreAclHandler.WORLD_EVERYTHING);
        try (AtomicOutputStream out = store.createBlob("test", metadata, null)) {
            out.write(1);
        }
        assertStoreHasExactly(store, "test");
        // Testing whether acls are set to WORLD_EVERYTHING
        assertTrue("ACL does not contain WORLD_EVERYTHING", metadata.toString().contains("AccessControl(type:OTHER, access:7)"));
        readAssertEquals(store, "test", 1);

        LOG.info("Deleting test");
        store.deleteBlob("test", null);
        assertStoreHasExactly(store);

        // The following tests are run for both hdfs and local store to test the
        // update blob interface
        metadata = new SettableBlobMeta(BlobStoreAclHandler.WORLD_EVERYTHING);
        LOG.info("Creating test again");
        try (AtomicOutputStream out = store.createBlob("test", metadata, null)) {
            out.write(2);
        }
        assertStoreHasExactly(store, "test");
        readAssertEquals(store, "test", 2);
        LOG.info("Updating test");
        try (AtomicOutputStream out = store.updateBlob("test", null)) {
            out.write(3);
        }
        assertStoreHasExactly(store, "test");
        readAssertEquals(store, "test", 3);

        LOG.info("Updating test again");
        try (AtomicOutputStream out = store.updateBlob("test", null)) {
            out.write(4);
        }
        LOG.info("SLEEPING");
        Thread.sleep(2);

        if (store instanceof HdfsBlobStore) {
            ((HdfsBlobStore) store).fullCleanup(1);
        } else {
            fail("Error the blobstore is of unknowntype");
        }
    }

    public void testMultiple(BlobStore store)
        throws Exception {
        assertStoreHasExactly(store);
        LOG.info("Creating test");
        try (AtomicOutputStream out = store.createBlob("test", new SettableBlobMeta(BlobStoreAclHandler.WORLD_EVERYTHING), null)) {
            out.write(1);
        }
        assertStoreHasExactly(store, "test");
        readAssertEquals(store, "test", 1);

        LOG.info("Creating other");
        try (AtomicOutputStream out = store.createBlob("other", new SettableBlobMeta(BlobStoreAclHandler.WORLD_EVERYTHING),
            null)) {
            out.write(2);
        }
        assertStoreHasExactly(store, "test", "other");
        readAssertEquals(store, "test", 1);
        readAssertEquals(store, "other", 2);

        LOG.info("Updating other");
        try (AtomicOutputStream out = store.updateBlob("other", null)) {
            out.write(5);
        }
        assertStoreHasExactly(store, "test", "other");
        readAssertEquals(store, "test", 1);
        readAssertEquals(store, "other", 5);

        LOG.info("Deleting test");
        store.deleteBlob("test", null);
        assertStoreHasExactly(store, "other");
        readAssertEquals(store, "other", 5);

        LOG.info("Creating test again");
        try (AtomicOutputStream out = store.createBlob("test", new SettableBlobMeta(BlobStoreAclHandler.WORLD_EVERYTHING),
            null)) {
            out.write(2);
        }
        assertStoreHasExactly(store, "test", "other");
        readAssertEquals(store, "test", 2);
        readAssertEquals(store, "other", 5);

        LOG.info("Updating test");
        try (AtomicOutputStream out = store.updateBlob("test", null)) {
            out.write(3);
        }
        assertStoreHasExactly(store, "test", "other");
        readAssertEquals(store, "test", 3);
        readAssertEquals(store, "other", 5);

        LOG.info("Deleting other");
        store.deleteBlob("other", null);
        assertStoreHasExactly(store, "test");
        readAssertEquals(store, "test", 3);

        LOG.info("Updating test again");
        try (AtomicOutputStream out = store.updateBlob("test", null)) {
            out.write(4);
        }
        LOG.info("SLEEPING");
        Thread.sleep(2);

        if (store instanceof HdfsBlobStore) {
            ((HdfsBlobStore) store).fullCleanup(1);
        } else {
            fail("Error the blobstore is of unknowntype");
        }
        assertStoreHasExactly(store, "test");
        readAssertEquals(store, "test", 4);
    }
}
