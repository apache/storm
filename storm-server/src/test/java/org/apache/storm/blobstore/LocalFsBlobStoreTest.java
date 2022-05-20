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
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.security.auth.Subject;
import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyAlreadyExistsException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.security.auth.NimbusPrincipal;
import org.apache.storm.security.auth.SingleUserPrincipal;
import org.apache.storm.testing.InProcessZookeeper;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.spy;

public class LocalFsBlobStoreTest {
  private static final Logger LOG = LoggerFactory.getLogger(LocalFsBlobStoreTest.class);
  URI base;
  File baseFile;
  private static Map<String, Object> conf = new HashMap<>();
  private InProcessZookeeper zk;

  @BeforeEach
  public void init() {
    initializeConfigs();
    baseFile = new File("target/blob-store-test-"+UUID.randomUUID());
    base = baseFile.toURI();
    try {
      zk = new InProcessZookeeper();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @AfterEach
  public void cleanup() throws IOException {
    FileUtils.deleteDirectory(baseFile);
    try {
      zk.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Method which initializes nimbus admin
  public static void initializeConfigs() {
    conf.put(Config.NIMBUS_ADMINS,"admin");
    conf.put(Config.NIMBUS_SUPERVISOR_USERS,"supervisor");
  }

  private LocalFsBlobStore initLocalFs() {
    LocalFsBlobStore store = new LocalFsBlobStore();
    // Spy object that tries to mock the real object store
    LocalFsBlobStore spy = spy(store);
    Mockito.doNothing().when(spy).checkForBlobUpdate("test");
    Mockito.doNothing().when(spy).checkForBlobUpdate("other");
    Mockito.doNothing().when(spy).checkForBlobUpdate("test-empty-subject-WE");
    Mockito.doNothing().when(spy).checkForBlobUpdate("test-empty-subject-DEF");
    Mockito.doNothing().when(spy).checkForBlobUpdate("test-empty-acls");
    Map<String, Object> conf = Utils.readStormConfig();
    conf.put(Config.STORM_ZOOKEEPER_PORT, zk.getPort());
    conf.put(Config.STORM_LOCAL_DIR, baseFile.getAbsolutePath());
    conf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN,"org.apache.storm.security.auth.DefaultPrincipalToLocal");
    NimbusInfo nimbusInfo = new NimbusInfo("localhost", 0, false);
    spy.prepare(conf, null, nimbusInfo, null);
    return spy;
  }

  @Test
  public void testLocalFsWithAuth() throws Exception {
    testWithAuthentication(initLocalFs());
  }

  @Test
  public void testBasicLocalFs() throws Exception {
    testBasic(initLocalFs());
  }

  @Test
  public void testMultipleLocalFs() throws Exception {
    testMultiple(initLocalFs());
  }

  @Test
  public void testDeleteAfterFailedCreate() throws Exception{
    //Check that a blob can be deleted when a temporary file exists in the blob directory
    LocalFsBlobStore store = initLocalFs();

    String key = "test";
    SettableBlobMeta metadata = new SettableBlobMeta(BlobStoreAclHandler
            .WORLD_EVERYTHING);
    try (AtomicOutputStream out = store.createBlob(key, metadata, null)) {
        out.write(1);
        File blobDir = store.getKeyDataDir(key);
        Files.createFile(blobDir.toPath().resolve("tempFile.tmp"));
    }

    store.deleteBlob("test",null);

  }

  public Subject getSubject(String name) {
    Subject subject = new Subject();
    SingleUserPrincipal user = new SingleUserPrincipal(name);
    subject.getPrincipals().add(user);
    return subject;
  }

    // Gets Nimbus Subject with NimbusPrincipal set on it
    public static Subject getNimbusSubject() {
        Subject nimbus = new Subject();
        nimbus.getPrincipals().add(new NimbusPrincipal());
        return nimbus;
    }

    // Overloading the assertStoreHasExactly method accomodate Subject in order to check for authorization
    public static void assertStoreHasExactly(BlobStore store, Subject who, String... keys) {
        Set<String> expected = new HashSet<>(Arrays.asList(keys));
        Set<String> found = new HashSet<>();
        Iterator<String> c = store.listKeys();
        while (c.hasNext()) {
            String keyName = c.next();
            found.add(keyName);
        }
        Set<String> extra = new HashSet<>(found);
        extra.removeAll(expected);
        assertTrue(extra.isEmpty(), "Found extra keys in the blob store " + extra);
        Set<String> missing = new HashSet<>(expected);
        missing.removeAll(found);
        assertTrue(missing.isEmpty(), "Found keys missing from the blob store " + missing);
    }

    public static void assertStoreHasExactly(BlobStore store, String... keys) {
        assertStoreHasExactly(store, null, keys);
    }

    // Overloading the readInt method accomodate Subject in order to check for authorization (security turned on)
    public static int readInt(BlobStore store, Subject who, String key)
        throws IOException, KeyNotFoundException, AuthorizationException {
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

    // Check for Blobstore with authentication
    public void testWithAuthentication(BlobStore store) throws Exception {
        //Test for Nimbus Admin
        Subject admin = getSubject("admin");
        assertStoreHasExactly(store);
        SettableBlobMeta metadata = new SettableBlobMeta(BlobStoreAclHandler.DEFAULT);
        try (AtomicOutputStream out = store.createBlob("test", metadata, admin)) {
            assertStoreHasExactly(store, "test");
            out.write(1);
        }
        store.deleteBlob("test", admin);

        //Test for Supervisor Admin
        Subject supervisor = getSubject("supervisor");
        assertStoreHasExactly(store);
        metadata = new SettableBlobMeta(BlobStoreAclHandler.DEFAULT);
        try (AtomicOutputStream out = store.createBlob("test", metadata, supervisor)) {
            assertStoreHasExactly(store, "test");
            out.write(1);
        }
        store.deleteBlob("test", supervisor);

        //Test for Nimbus itself as a user
        Subject nimbus = getNimbusSubject();
        assertStoreHasExactly(store);
        metadata = new SettableBlobMeta(BlobStoreAclHandler.DEFAULT);
        try (AtomicOutputStream out = store.createBlob("test", metadata, nimbus)) {
            assertStoreHasExactly(store, "test");
            out.write(1);
        }
        store.deleteBlob("test", nimbus);

        // Test with a dummy test_subject for cases where subject !=null (security turned on)
        Subject who = getSubject("test_subject");
        assertStoreHasExactly(store);

        // Tests for case when subject != null (security turned on) and
        // acls for the blob are set to WORLD_EVERYTHING
        metadata = new SettableBlobMeta(BlobStoreAclHandler.WORLD_EVERYTHING);
        try (AtomicOutputStream out = store.createBlob("test", metadata, who)) {
            out.write(1);
        }
        assertStoreHasExactly(store, "test");
        // Testing whether acls are set to WORLD_EVERYTHING
        assertTrue(metadata.toString().contains("AccessControl(type:OTHER, access:7)"), "ACL does not contain WORLD_EVERYTHING");
        readAssertEqualsWithAuth(store, who, "test", 1);

        LOG.info("Deleting test");
        store.deleteBlob("test", who);
        assertStoreHasExactly(store);

        // Tests for case when subject != null (security turned on) and
        // acls are not set for the blob (DEFAULT)
        LOG.info("Creating test again");
        metadata = new SettableBlobMeta(BlobStoreAclHandler.DEFAULT);
        try (AtomicOutputStream out = store.createBlob("test", metadata, who)) {
            out.write(2);
        }
        assertStoreHasExactly(store, "test");
        // Testing whether acls are set to WORLD_EVERYTHING. Here the acl should not contain WORLD_EVERYTHING because
        // the subject is neither null nor empty. The ACL should however contain USER_EVERYTHING as user needs to have
        // complete access to the blob
        assertTrue(!metadata.toString().contains("AccessControl(type:OTHER, access:7)"), "ACL does not contain WORLD_EVERYTHING");
        readAssertEqualsWithAuth(store, who, "test", 2);

        LOG.info("Updating test");
        try (AtomicOutputStream out = store.updateBlob("test", who)) {
            out.write(3);
        }
        assertStoreHasExactly(store, "test");
        readAssertEqualsWithAuth(store, who, "test", 3);

        LOG.info("Updating test again");
        try (AtomicOutputStream out = store.updateBlob("test", who)) {
            out.write(4);
            out.flush();
            LOG.info("SLEEPING");
            Thread.sleep(2);
            assertStoreHasExactly(store, "test");
            readAssertEqualsWithAuth(store, who, "test", 3);
        }

        // Test for subject with no principals and acls set to WORLD_EVERYTHING
        who = new Subject();
        metadata = new SettableBlobMeta(BlobStoreAclHandler.WORLD_EVERYTHING);
        LOG.info("Creating test");
        try (AtomicOutputStream out = store.createBlob("test-empty-subject-WE", metadata, who)) {
            out.write(2);
        }
        assertStoreHasExactly(store, "test-empty-subject-WE", "test");
        // Testing whether acls are set to WORLD_EVERYTHING
        assertTrue(metadata.toString().contains("AccessControl(type:OTHER, access:7)"), "ACL does not contain WORLD_EVERYTHING");
        readAssertEqualsWithAuth(store, who, "test-empty-subject-WE", 2);

        // Test for subject with no principals and acls set to DEFAULT
        who = new Subject();
        metadata = new SettableBlobMeta(BlobStoreAclHandler.DEFAULT);
        LOG.info("Creating other");

        try (AtomicOutputStream out = store.createBlob("test-empty-subject-DEF", metadata, who)) {
            out.write(2);
        }
        assertStoreHasExactly(store, "test-empty-subject-DEF", "test", "test-empty-subject-WE");
        // Testing whether acls are set to WORLD_EVERYTHING
        assertTrue(metadata.toString().contains("AccessControl(type:OTHER, access:7)"), "ACL does not contain WORLD_EVERYTHING");
        readAssertEqualsWithAuth(store, who, "test-empty-subject-DEF", 2);

        if (store instanceof LocalFsBlobStore) {
            ((LocalFsBlobStore) store).fullCleanup(1);
        } else {
            fail("Error the blobstore is of unknowntype");
        }
    }

    public void testBasic(BlobStore store) throws Exception {
        assertStoreHasExactly(store);
        LOG.info("Creating test");
        // Tests for case when subject == null (security turned off) and
        // acls for the blob are set to WORLD_EVERYTHING
        SettableBlobMeta metadata = new SettableBlobMeta(BlobStoreAclHandler
                                                             .WORLD_EVERYTHING);
        try (AtomicOutputStream out = store.createBlob("test", metadata, null)) {
            out.write(1);
        }
        assertStoreHasExactly(store, "test");
        // Testing whether acls are set to WORLD_EVERYTHING
        assertTrue(metadata.toString().contains("AccessControl(type:OTHER, access:7)"), "ACL does not contain WORLD_EVERYTHING");
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
        if (store instanceof LocalFsBlobStore) {
            assertTrue(metadata.toString().contains("AccessControl(type:OTHER, access:7)"), "ACL does not contain WORLD_EVERYTHING");
        }
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
            out.flush();
            LOG.info("SLEEPING");
            Thread.sleep(2);
        }

        // Tests for case when subject == null (security turned off) and
        // acls for the blob are set to DEFAULT (Empty ACL List) only for LocalFsBlobstore
        if (store instanceof LocalFsBlobStore) {
            metadata = new SettableBlobMeta(BlobStoreAclHandler.DEFAULT);
            LOG.info("Creating test for empty acls when security is off");
            try (AtomicOutputStream out = store.createBlob("test-empty-acls", metadata, null)) {
                LOG.info("metadata {}", metadata);
                out.write(2);
            }
            assertStoreHasExactly(store, "test-empty-acls", "test");
            // Testing whether acls are set to WORLD_EVERYTHING, Here we are testing only for LocalFsBlobstore
            // as the HdfsBlobstore gets the subject information of the local system user and behaves as it is
            // always authenticated.
            assertTrue(metadata.get_acl().toString().contains("OTHER"), "ACL does not contain WORLD_EVERYTHING");

            LOG.info("Deleting test-empty-acls");
            store.deleteBlob("test-empty-acls", null);
        }

        if (store instanceof LocalFsBlobStore) {
            ((LocalFsBlobStore) store).fullCleanup(1);
        } else {
            fail("Error the blobstore is of unknowntype");
        }
    }


    public void testMultiple(BlobStore store) throws Exception {

        assertStoreHasExactly(store);
        LOG.info("Creating test");
        try (AtomicOutputStream out = store.createBlob("test", new SettableBlobMeta(BlobStoreAclHandler
                                                                                        .WORLD_EVERYTHING), null)) {
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

        // intended to not guarding with try-with-resource since otherwise test will fail
        AtomicOutputStream out = store.updateBlob("test", null);
        out.write(4);
        out.flush();
        LOG.info("SLEEPING");
        Thread.sleep(2);

        if (store instanceof LocalFsBlobStore) {
            ((LocalFsBlobStore) store).fullCleanup(1);
        } else {
            fail("Error the blobstore is of unknowntype");
        }
        assertStoreHasExactly(store, "test");
        readAssertEquals(store, "test", 3);
        try {
            out.close();
        } catch (IOException e) {
            // This is likely to happen when we try to commit something that
            // was cleaned up.  This is expected and acceptable.
        }
    }

    @Test
    public void testGetFileLength()
        throws AuthorizationException, KeyNotFoundException, KeyAlreadyExistsException, IOException {
        LocalFsBlobStore store = initLocalFs();
        try (AtomicOutputStream out = store.createBlob("test", new SettableBlobMeta(BlobStoreAclHandler
                                                                                        .WORLD_EVERYTHING), null)) {
            out.write(1);
        }
        try (InputStreamWithMeta blobInputStream = store.getBlob("test", null)) {
            assertEquals(1, blobInputStream.getFileLength());
        }
    }
}
