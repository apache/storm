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

package org.apache.storm.localizer;

import static org.apache.storm.blobstore.BlobStoreAclHandler.WORLD_EVERYTHING;
import static org.apache.storm.localizer.AsyncLocalizer.USERCACHE;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.endsWith;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Joiner;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.storm.DaemonConfig;
import org.apache.storm.blobstore.BlobStoreAclHandler;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.blobstore.LocalFsBlobStore;
import org.apache.storm.daemon.supervisor.AdvancedFSOps;
import org.apache.storm.generated.AccessControl;
import org.apache.storm.generated.AccessControlType;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.ReflectionUtils;
import org.apache.storm.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.storm.Config;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.security.auth.DefaultPrincipalToLocal;
import org.mockito.Mockito;

public class AsyncLocalizerTest {

    private static String getTestLocalizerRoot() {
        File f = new File("./target/" + Thread.currentThread().getStackTrace()[2].getMethodName() + "/localizer/");
        f.deleteOnExit();
        return f.getPath();
    }

    private class MockInputStreamWithMeta extends InputStreamWithMeta {
        private int at = 0;
        private final int len;
        private final int version;

        public MockInputStreamWithMeta(int len, int version) {
            this.len = len;
            this.version = version;
        }

        @Override
        public long getVersion() throws IOException {
            return version;
        }

        @Override
        public long getFileLength() throws IOException {
            return len;
        }

        @Override
        public int read() throws IOException {
            at++;
            if (at > len) {
                return -1;
            }
            return 0;
        }
    }

    @Test
    public void testRequestDownloadBaseTopologyBlobs() throws Exception {
        final String topoId = "TOPO";
        final String user = "user";
        LocalAssignment la = new LocalAssignment();
        la.set_topology_id(topoId);
        la.set_owner(user);
        ExecutorInfo ei = new ExecutorInfo();
        ei.set_task_start(1);
        ei.set_task_end(1);
        la.add_to_executors(ei);
        final int port = 8080;
        final String stormLocal = "./target/DOWNLOAD-TEST/storm-local/";
        ClientBlobStore blobStore = mock(ClientBlobStore.class);
        Map<String, Object> conf = new HashMap<>();
        conf.put(DaemonConfig.SUPERVISOR_BLOBSTORE, ClientBlobStore.class.getName());
        conf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, DefaultPrincipalToLocal.class.getName());
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");
        conf.put(Config.STORM_LOCAL_DIR, stormLocal);
        AdvancedFSOps ops = mock(AdvancedFSOps.class);
        ReflectionUtils mockedRU = mock(ReflectionUtils.class);
        ServerUtils mockedU = mock(ServerUtils.class);

        AsyncLocalizer bl = spy(new AsyncLocalizer(conf, ops, getTestLocalizerRoot(), new AtomicReference<>(new HashMap<>()), null));
        LocallyCachedTopologyBlob jarBlob = mock(LocallyCachedTopologyBlob.class);
        doReturn(jarBlob).when(bl).getTopoJar(topoId);
        when(jarBlob.getLocalVersion()).thenReturn(-1L);
        when(jarBlob.getRemoteVersion(any())).thenReturn(100L);
        when(jarBlob.downloadToTempLocation(any())).thenReturn(100L);

        LocallyCachedTopologyBlob codeBlob = mock(LocallyCachedTopologyBlob.class);
        doReturn(codeBlob).when(bl).getTopoCode(topoId);
        when(codeBlob.getLocalVersion()).thenReturn(-1L);
        when(codeBlob.getRemoteVersion(any())).thenReturn(200L);
        when(codeBlob.downloadToTempLocation(any())).thenReturn(200L);

        LocallyCachedTopologyBlob confBlob = mock(LocallyCachedTopologyBlob.class);
        doReturn(confBlob).when(bl).getTopoConf(topoId);
        when(confBlob.getLocalVersion()).thenReturn(-1L);
        when(confBlob.getRemoteVersion(any())).thenReturn(300L);
        when(confBlob.downloadToTempLocation(any())).thenReturn(300L);

        ReflectionUtils origRU = ReflectionUtils.setInstance(mockedRU);
        ServerUtils origUtils = ServerUtils.setInstance(mockedU);
        try {
            when(mockedRU.newInstanceImpl(ClientBlobStore.class)).thenReturn(blobStore);

            Future<Void> f = bl.requestDownloadBaseTopologyBlobs(la, port, null);
            f.get(20, TimeUnit.SECONDS);

            verify(jarBlob).downloadToTempLocation(any());
            verify(jarBlob).informAllOfChangeAndWaitForConsensus();
            verify(jarBlob).commitNewVersion(100L);
            verify(jarBlob).informAllChangeComplete();
            verify(jarBlob).cleanupOrphanedData();

            verify(codeBlob).downloadToTempLocation(any());
            verify(codeBlob).informAllOfChangeAndWaitForConsensus();
            verify(codeBlob).commitNewVersion(200L);
            verify(codeBlob).informAllChangeComplete();
            verify(codeBlob).cleanupOrphanedData();

            verify(confBlob).downloadToTempLocation(any());
            verify(confBlob).informAllOfChangeAndWaitForConsensus();
            verify(confBlob).commitNewVersion(300L);
            verify(confBlob).informAllChangeComplete();
            verify(confBlob).cleanupOrphanedData();
        } finally {
            bl.close();
            ReflectionUtils.setInstance(origRU);
            ServerUtils.setInstance(origUtils);
        }
    }

    @Test
    public void testRequestDownloadTopologyBlobs() throws Exception {
        final String topoId = "TOPO-12345";
        final String user = "user";
        LocalAssignment la = new LocalAssignment();
        la.set_topology_id(topoId);
        la.set_owner(user);
        ExecutorInfo ei = new ExecutorInfo();
        ei.set_task_start(1);
        ei.set_task_end(1);
        la.add_to_executors(ei);
        final String topoName = "TOPO";
        final int port = 8080;
        final String simpleLocalName = "simple.txt";
        final String simpleKey = "simple";
        
        final String stormLocal = "/tmp/storm-local/";
        final File userDir = new File(stormLocal, user);
        final String stormRoot = stormLocal+topoId+"/";
        
        final String localizerRoot = getTestLocalizerRoot();
        final String simpleLocalFile = localizerRoot + user + "/simple";
        final String simpleCurrentLocalFile = localizerRoot + user + "/simple.current";
       
        final StormTopology st = new StormTopology();
        st.set_spouts(new HashMap<>());
        st.set_bolts(new HashMap<>());
        st.set_state_spouts(new HashMap<>());
 
        Map<String, Map<String, Object>> topoBlobMap = new HashMap<>();
        Map<String, Object> simple = new HashMap<>();
        simple.put("localname", simpleLocalName);
        simple.put("uncompress", false);
        topoBlobMap.put(simpleKey, simple);
        
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_LOCAL_DIR, stormLocal);
        AdvancedFSOps ops = mock(AdvancedFSOps.class);
        ConfigUtils mockedCU = mock(ConfigUtils.class);

        Map<String, Object> topoConf = new HashMap<>(conf);
        topoConf.put(Config.TOPOLOGY_BLOBSTORE_MAP, topoBlobMap);
        topoConf.put(Config.TOPOLOGY_NAME, topoName);
        
        List<LocalizedResource> localizedList = new ArrayList<>();
        LocalizedResource simpleLocal = new LocalizedResource(simpleKey, simpleLocalFile, false);
        localizedList.add(simpleLocal);

        AsyncLocalizer bl = spy(new AsyncLocalizer(conf, ops, localizerRoot, new AtomicReference<>(new HashMap<>()), null));
        ConfigUtils orig = ConfigUtils.setInstance(mockedCU);
        try {
            when(mockedCU.supervisorStormDistRootImpl(conf, topoId)).thenReturn(stormRoot);
            when(mockedCU.readSupervisorStormConfImpl(conf, topoId)).thenReturn(topoConf);
            when(mockedCU.readSupervisorTopologyImpl(conf, topoId, ops)).thenReturn(st);

            //Write the mocking backwards so the actual method is not called on the spy object
            doReturn(CompletableFuture.supplyAsync(() -> null)).when(bl)
                .requestDownloadBaseTopologyBlobs(la, port, null);
            doReturn(userDir).when(bl).getLocalUserFileCacheDir(user);
            doReturn(localizedList).when(bl).getBlobs(any(List.class), eq(user), eq(topoName), eq(userDir));

            Future<Void> f = bl.requestDownloadTopologyBlobs(la, port, null);
            f.get(20, TimeUnit.SECONDS);
            // We should be done now...

            verify(bl).getLocalUserFileCacheDir(user);

            verify(ops).fileExists(userDir);
            verify(ops).forceMkdir(userDir);

            verify(bl).getBlobs(any(List.class), eq(user), eq(topoName), eq(userDir));

            verify(ops).createSymlink(new File(stormRoot, simpleLocalName), new File(simpleCurrentLocalFile));
        } finally {
            bl.close();
            ConfigUtils.setInstance(orig);
        }
    }


    //From LocalizerTest
    private File baseDir;

    private final String user1 = "user1";
    private final String user2 = "user2";
    private final String user3 = "user3";

    private ClientBlobStore mockblobstore = mock(ClientBlobStore.class);


    class TestLocalizer extends AsyncLocalizer {

        TestLocalizer(Map<String, Object> conf, String baseDir) throws IOException {
            super(conf, AdvancedFSOps.make(conf), baseDir, new AtomicReference<>(new HashMap<>()), null);
        }

        @Override
        protected ClientBlobStore getClientBlobStore() {
            return mockblobstore;
        }
    }

    class TestInputStreamWithMeta extends InputStreamWithMeta {
        private InputStream iostream;

        public TestInputStreamWithMeta() {
            iostream = IOUtils.toInputStream("some test data for my input stream");
        }

        public TestInputStreamWithMeta(InputStream istream) {
            iostream = istream;
        }

        @Override
        public long getVersion() throws IOException {
            return 1;
        }

        @Override
        public synchronized int read() {
            return 0;
        }

        @Override
        public synchronized int read(byte[] b)
            throws IOException {
            int length = iostream.read(b);
            if (length == 0) {
                return -1;
            }
            return length;
        }

        @Override
        public long getFileLength() {
            return 0;
        }
    };

    @Before
    public void setUp() throws Exception {
        baseDir = new File(System.getProperty("java.io.tmpdir") + "/blob-store-localizer-test-"+ UUID.randomUUID());
        if (!baseDir.mkdir()) {
            throw new IOException("failed to create base directory");
        }
    }

    @After
    public void tearDown() throws Exception {
        try {
            FileUtils.deleteDirectory(baseDir);
        } catch (IOException ignore) {}
    }

    protected String joinPath(String... pathList) {
        return Joiner.on(File.separator).join(pathList);
    }

    public String constructUserCacheDir(String base, String user) {
        return joinPath(base, USERCACHE, user);
    }

    public String constructExpectedFilesDir(String base, String user) {
        return joinPath(constructUserCacheDir(base, user), AsyncLocalizer.FILECACHE, AsyncLocalizer.FILESDIR);
    }

    public String constructExpectedArchivesDir(String base, String user) {
        return joinPath(constructUserCacheDir(base, user), AsyncLocalizer.FILECACHE, AsyncLocalizer.ARCHIVESDIR);
    }

    @Test
    public void testDirPaths() throws Exception {
        Map<String, Object> conf = new HashMap();
        AsyncLocalizer localizer = new TestLocalizer(conf, baseDir.toString());

        String expectedDir = constructUserCacheDir(baseDir.toString(), user1);
        assertEquals("get local user dir doesn't return right value",
            expectedDir, localizer.getLocalUserDir(user1).toString());

        String expectedFileDir = joinPath(expectedDir, AsyncLocalizer.FILECACHE);
        assertEquals("get local user file dir doesn't return right value",
            expectedFileDir, localizer.getLocalUserFileCacheDir(user1).toString());
    }

    @Test
    public void testReconstruct() throws Exception {
        Map<String, Object> conf = new HashMap();

        String expectedFileDir1 = constructExpectedFilesDir(baseDir.toString(), user1);
        String expectedArchiveDir1 = constructExpectedArchivesDir(baseDir.toString(), user1);
        String expectedFileDir2 = constructExpectedFilesDir(baseDir.toString(), user2);
        String expectedArchiveDir2 = constructExpectedArchivesDir(baseDir.toString(), user2);

        String key1 = "testfile1.txt";
        String key2 = "testfile2.txt";
        String key3 = "testfile3.txt";
        String key4 = "testfile4.txt";

        String archive1 = "archive1";
        String archive2 = "archive2";

        File user1file1 = new File(expectedFileDir1, key1 + ServerUtils.DEFAULT_CURRENT_BLOB_SUFFIX);
        File user1file2 = new File(expectedFileDir1, key2 + ServerUtils.DEFAULT_CURRENT_BLOB_SUFFIX);
        File user2file3 = new File(expectedFileDir2, key3 + ServerUtils.DEFAULT_CURRENT_BLOB_SUFFIX);
        File user2file4 = new File(expectedFileDir2, key4 + ServerUtils.DEFAULT_CURRENT_BLOB_SUFFIX);

        File user1archive1 = new File(expectedArchiveDir1, archive1 + ServerUtils.DEFAULT_CURRENT_BLOB_SUFFIX);
        File user2archive2 = new File(expectedArchiveDir2, archive2 + ServerUtils.DEFAULT_CURRENT_BLOB_SUFFIX);
        File user1archive1file = new File(user1archive1, "file1");
        File user2archive2file = new File(user2archive2, "file2");

        // setup some files/dirs to emulate supervisor restart
        assertTrue("Failed setup filecache dir1", new File(expectedFileDir1).mkdirs());
        assertTrue("Failed setup filecache dir2", new File(expectedFileDir2).mkdirs());
        assertTrue("Failed setup file1", user1file1.createNewFile());
        assertTrue("Failed setup file2", user1file2.createNewFile());
        assertTrue("Failed setup file3", user2file3.createNewFile());
        assertTrue("Failed setup file4", user2file4.createNewFile());
        assertTrue("Failed setup archive dir1", user1archive1.mkdirs());
        assertTrue("Failed setup archive dir2", user2archive2.mkdirs());
        assertTrue("Failed setup file in archivedir1", user1archive1file.createNewFile());
        assertTrue("Failed setup file in archivedir2", user2archive2file.createNewFile());

        AsyncLocalizer localizer = new TestLocalizer(conf, baseDir.toString());

        ArrayList<LocalResource> arrUser1Keys = new ArrayList<LocalResource>();
        arrUser1Keys.add(new LocalResource(key1, false));
        arrUser1Keys.add(new LocalResource(archive1, true));
        localizer.addReferences(arrUser1Keys, user1, "topo1");

        LocalizedResourceSet lrsrcSet = localizer.getUserResources().get(user1);
        assertEquals("local resource set size wrong", 3, lrsrcSet.getSize());
        assertEquals("user doesn't match", user1, lrsrcSet.getUser());
        LocalizedResource key1rsrc = lrsrcSet.get(key1, false);
        assertNotNull("Local resource doesn't exist but should", key1rsrc);
        assertEquals("key doesn't match", key1, key1rsrc.getKey());
        assertEquals("refcount doesn't match", 1, key1rsrc.getRefCount());
        LocalizedResource key2rsrc = lrsrcSet.get(key2, false);
        assertNotNull("Local resource doesn't exist but should", key2rsrc);
        assertEquals("key doesn't match", key2, key2rsrc.getKey());
        assertEquals("refcount doesn't match", 0, key2rsrc.getRefCount());
        LocalizedResource archive1rsrc = lrsrcSet.get(archive1, true);
        assertNotNull("Local resource doesn't exist but should", archive1rsrc);
        assertEquals("key doesn't match", archive1, archive1rsrc.getKey());
        assertEquals("refcount doesn't match", 1, archive1rsrc.getRefCount());

        LocalizedResourceSet lrsrcSet2 = localizer.getUserResources().get(user2);
        assertEquals("local resource set size wrong", 3, lrsrcSet2.getSize());
        assertEquals("user doesn't match", user2, lrsrcSet2.getUser());
        LocalizedResource key3rsrc = lrsrcSet2.get(key3, false);
        assertNotNull("Local resource doesn't exist but should", key3rsrc);
        assertEquals("key doesn't match", key3, key3rsrc.getKey());
        assertEquals("refcount doesn't match", 0, key3rsrc.getRefCount());
        LocalizedResource key4rsrc = lrsrcSet2.get(key4, false);
        assertNotNull("Local resource doesn't exist but should", key4rsrc);
        assertEquals("key doesn't match", key4, key4rsrc.getKey());
        assertEquals("refcount doesn't match", 0, key4rsrc.getRefCount());
        LocalizedResource archive2rsrc = lrsrcSet2.get(archive2, true);
        assertNotNull("Local resource doesn't exist but should", archive2rsrc);
        assertEquals("key doesn't match", archive2, archive2rsrc.getKey());
        assertEquals("refcount doesn't match", 0, archive2rsrc.getRefCount());
    }

    @Test
    public void testArchivesTgz() throws Exception {
        testArchives(getFileFromResource(joinPath("localizer", "localtestwithsymlink.tgz")), true, 21344);
    }

    @Test
    public void testArchivesZip() throws Exception {
        testArchives(getFileFromResource(joinPath("localizer", "localtest.zip")), false, 21348);
    }

    @Test
    public void testArchivesTarGz() throws Exception {
        testArchives(getFileFromResource(joinPath("localizer", "localtestwithsymlink.tar.gz")), true, 21344);
    }

    @Test
    public void testArchivesTar() throws Exception {
        testArchives(getFileFromResource(joinPath("localizer", "localtestwithsymlink.tar")), true, 21344);
    }

    @Test
    public void testArchivesJar() throws Exception {
        testArchives(getFileFromResource(joinPath("localizer", "localtestwithsymlink.jar")), false, 21416);
    }

    private File getFileFromResource(String archivePath) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(archivePath).getFile());
    }

    // archive passed in must contain symlink named tmptestsymlink if not a zip file
    public void testArchives(File archiveFile, boolean supportSymlinks, int size) throws Exception {
        if (Utils.isOnWindows()) {
            // Windows should set this to false cause symlink in compressed file doesn't work properly.
            supportSymlinks = false;
        }

        Map<String, Object> conf = new HashMap();
        // set clean time really high so doesn't kick in
        conf.put(DaemonConfig.SUPERVISOR_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS, 60*60*1000);

        String key1 = archiveFile.getName();
        String topo1 = "topo1";
        AsyncLocalizer localizer = new TestLocalizer(conf, baseDir.toString());
        // set really small so will do cleanup
        localizer.setTargetCacheSize(1);

        ReadableBlobMeta rbm = new ReadableBlobMeta();
        rbm.set_settable(new SettableBlobMeta(WORLD_EVERYTHING));
        when(mockblobstore.getBlobMeta(key1)).thenReturn(rbm);

        when(mockblobstore.getBlob(key1)).thenReturn(new TestInputStreamWithMeta(new
            FileInputStream(archiveFile.getAbsolutePath())));

        long timeBefore = System.nanoTime();
        File user1Dir = localizer.getLocalUserFileCacheDir(user1);
        assertTrue("failed to create user dir", user1Dir.mkdirs());
        LocalizedResource lrsrc = localizer.getBlob(new LocalResource(key1, true), user1, topo1,
            user1Dir);
        long timeAfter = System.nanoTime();

        String expectedUserDir = joinPath(baseDir.toString(), USERCACHE, user1);
        String expectedFileDir = joinPath(expectedUserDir, AsyncLocalizer.FILECACHE, AsyncLocalizer.ARCHIVESDIR);
        assertTrue("user filecache dir not created", new File(expectedFileDir).exists());
        File keyFile = new File(expectedFileDir, key1 + ".0");
        assertTrue("blob not created", keyFile.exists());
        assertTrue("blob is not uncompressed", keyFile.isDirectory());
        File symlinkFile = new File(keyFile, "tmptestsymlink");

        if (supportSymlinks) {
            assertTrue("blob uncompressed doesn't contain symlink", Files.isSymbolicLink(
                symlinkFile.toPath()));
        } else {
            assertTrue("blob symlink file doesn't exist", symlinkFile.exists());
        }

        LocalizedResourceSet lrsrcSet = localizer.getUserResources().get(user1);
        assertEquals("local resource set size wrong", 1, lrsrcSet.getSize());
        assertEquals("user doesn't match", user1, lrsrcSet.getUser());
        LocalizedResource key1rsrc = lrsrcSet.get(key1, true);
        assertNotNull("Local resource doesn't exist but should", key1rsrc);
        assertEquals("key doesn't match", key1, key1rsrc.getKey());
        assertEquals("refcount doesn't match", 1, key1rsrc.getRefCount());
        assertEquals("file path doesn't match", keyFile.toString(), key1rsrc.getFilePathWithVersion());
        assertEquals("size doesn't match", size, key1rsrc.getSize());
        assertTrue("timestamp not within range", (key1rsrc.getLastAccessTime() >= timeBefore && key1rsrc
            .getLastAccessTime() <= timeAfter));

        timeBefore = System.nanoTime();
        localizer.removeBlobReference(lrsrc.getKey(), user1, topo1, true);
        timeAfter = System.nanoTime();

        lrsrcSet = localizer.getUserResources().get(user1);
        assertEquals("local resource set size wrong", 1, lrsrcSet.getSize());
        key1rsrc = lrsrcSet.get(key1, true);
        assertNotNull("Local resource doesn't exist but should", key1rsrc);
        assertEquals("refcount doesn't match", 0, key1rsrc.getRefCount());
        assertTrue("timestamp not within range", (key1rsrc.getLastAccessTime() >= timeBefore && key1rsrc
            .getLastAccessTime() <= timeAfter));

        // should remove the blob since cache size set really small
        localizer.cleanup();

        lrsrcSet = localizer.getUserResources().get(user1);
        assertFalse("blob contents not deleted", symlinkFile.exists());
        assertFalse("blob not deleted", keyFile.exists());
        assertFalse("blob file dir not deleted", new File(expectedFileDir).exists());
        assertFalse("blob dir not deleted", new File(expectedUserDir).exists());
        assertNull("user set should be null", lrsrcSet);

    }

    @Test
    public void testBasic() throws Exception {
        Map<String, Object> conf = new HashMap();
        // set clean time really high so doesn't kick in
        conf.put(DaemonConfig.SUPERVISOR_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS, 60*60*1000);

        String key1 = "key1";
        String topo1 = "topo1";
        AsyncLocalizer localizer = new TestLocalizer(conf, baseDir.toString());
        // set really small so will do cleanup
        localizer.setTargetCacheSize(1);

        ReadableBlobMeta rbm = new ReadableBlobMeta();
        rbm.set_settable(new SettableBlobMeta(WORLD_EVERYTHING));
        when(mockblobstore.getBlobMeta(key1)).thenReturn(rbm);

        when(mockblobstore.getBlob(key1)).thenReturn(new TestInputStreamWithMeta());

        long timeBefore = System.nanoTime();
        File user1Dir = localizer.getLocalUserFileCacheDir(user1);
        assertTrue("failed to create user dir", user1Dir.mkdirs());
        LocalizedResource lrsrc = localizer.getBlob(new LocalResource(key1, false), user1, topo1,
            user1Dir);
        long timeAfter = System.nanoTime();

        String expectedUserDir = joinPath(baseDir.toString(), USERCACHE, user1);
        String expectedFileDir = joinPath(expectedUserDir, AsyncLocalizer.FILECACHE, AsyncLocalizer.FILESDIR);
        assertTrue("user filecache dir not created", new File(expectedFileDir).exists());
        File keyFile = new File(expectedFileDir, key1);
        File keyFileCurrentSymlink = new File(expectedFileDir, key1 + ServerUtils.DEFAULT_CURRENT_BLOB_SUFFIX);

        assertTrue("blob not created", keyFileCurrentSymlink.exists());

        LocalizedResourceSet lrsrcSet = localizer.getUserResources().get(user1);
        assertEquals("local resource set size wrong", 1, lrsrcSet.getSize());
        assertEquals("user doesn't match", user1, lrsrcSet.getUser());
        LocalizedResource key1rsrc = lrsrcSet.get(key1, false);
        assertNotNull("Local resource doesn't exist but should", key1rsrc);
        assertEquals("key doesn't match", key1, key1rsrc.getKey());
        assertEquals("refcount doesn't match", 1, key1rsrc.getRefCount());
        assertEquals("file path doesn't match", keyFile.toString(), key1rsrc.getFilePath());
        assertEquals("size doesn't match", 34, key1rsrc.getSize());
        assertTrue("timestamp not within range", (key1rsrc.getLastAccessTime() >= timeBefore && key1rsrc
            .getLastAccessTime() <= timeAfter));

        timeBefore = System.nanoTime();
        localizer.removeBlobReference(lrsrc.getKey(), user1, topo1, false);
        timeAfter = System.nanoTime();

        lrsrcSet = localizer.getUserResources().get(user1);
        assertEquals("local resource set size wrong", 1, lrsrcSet.getSize());
        key1rsrc = lrsrcSet.get(key1, false);
        assertNotNull("Local resource doesn't exist but should", key1rsrc);
        assertEquals("refcount doesn't match", 0, key1rsrc.getRefCount());
        assertTrue("timestamp not within range", (key1rsrc.getLastAccessTime() >= timeBefore && key1rsrc
            .getLastAccessTime() <= timeAfter));

        // should remove the blob since cache size set really small
        localizer.cleanup();

        lrsrcSet = localizer.getUserResources().get(user1);
        assertNull("user set should be null", lrsrcSet);
        assertFalse("blob not deleted", keyFile.exists());
        assertFalse("blob dir not deleted", new File(expectedFileDir).exists());
        assertFalse("blob dir not deleted", new File(expectedUserDir).exists());
    }

    @Test
    public void testMultipleKeysOneUser() throws Exception {
        Map<String, Object> conf = new HashMap();
        // set clean time really high so doesn't kick in
        conf.put(DaemonConfig.SUPERVISOR_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS, 60*60*1000);

        String key1 = "key1";
        String topo1 = "topo1";
        String key2 = "key2";
        String key3 = "key3";
        AsyncLocalizer localizer = new TestLocalizer(conf, baseDir.toString());
        // set to keep 2 blobs (each of size 34)
        localizer.setTargetCacheSize(68);

        ReadableBlobMeta rbm = new ReadableBlobMeta();
        rbm.set_settable(new SettableBlobMeta(WORLD_EVERYTHING));
        when(mockblobstore.getBlobMeta(anyString())).thenReturn(rbm);
        when(mockblobstore.getBlob(key1)).thenReturn(new TestInputStreamWithMeta());
        when(mockblobstore.getBlob(key2)).thenReturn(new TestInputStreamWithMeta());
        when(mockblobstore.getBlob(key3)).thenReturn(new TestInputStreamWithMeta());

        List<LocalResource> keys = Arrays.asList(new LocalResource[]{new LocalResource(key1, false),
            new LocalResource(key2, false), new LocalResource(key3, false)});
        File user1Dir = localizer.getLocalUserFileCacheDir(user1);
        assertTrue("failed to create user dir", user1Dir.mkdirs());

        List<LocalizedResource> lrsrcs = localizer.getBlobs(keys, user1, topo1, user1Dir);
        LocalizedResource lrsrc = lrsrcs.get(0);
        LocalizedResource lrsrc2 = lrsrcs.get(1);
        LocalizedResource lrsrc3 = lrsrcs.get(2);

        String expectedFileDir = joinPath(baseDir.toString(), USERCACHE, user1,
            AsyncLocalizer.FILECACHE, AsyncLocalizer.FILESDIR);
        assertTrue("user filecache dir not created", new File(expectedFileDir).exists());
        File keyFile = new File(expectedFileDir, key1 + ServerUtils.DEFAULT_CURRENT_BLOB_SUFFIX);
        File keyFile2 = new File(expectedFileDir, key2 + ServerUtils.DEFAULT_CURRENT_BLOB_SUFFIX);
        File keyFile3 = new File(expectedFileDir, key3 + ServerUtils.DEFAULT_CURRENT_BLOB_SUFFIX);

        assertTrue("blob not created", keyFile.exists());
        assertTrue("blob not created", keyFile2.exists());
        assertTrue("blob not created", keyFile3.exists());
        assertEquals("size doesn't match", 34, keyFile.length());
        assertEquals("size doesn't match", 34, keyFile2.length());
        assertEquals("size doesn't match", 34, keyFile3.length());
        assertEquals("size doesn't match", 34, lrsrc.getSize());
        assertEquals("size doesn't match", 34, lrsrc3.getSize());
        assertEquals("size doesn't match", 34, lrsrc2.getSize());

        LocalizedResourceSet lrsrcSet = localizer.getUserResources().get(user1);
        assertEquals("local resource set size wrong", 3, lrsrcSet.getSize());
        assertEquals("user doesn't match", user1, lrsrcSet.getUser());

        long timeBefore = System.nanoTime();
        localizer.removeBlobReference(lrsrc.getKey(), user1, topo1, false);
        localizer.removeBlobReference(lrsrc2.getKey(), user1, topo1, false);
        localizer.removeBlobReference(lrsrc3.getKey(), user1, topo1, false);
        long timeAfter = System.nanoTime();

        // add reference to one and then remove reference again so it has newer timestamp
        lrsrc = localizer.getBlob(new LocalResource(key1, false), user1, topo1, user1Dir);
        assertTrue("timestamp not within range", (lrsrc.getLastAccessTime() >= timeBefore && lrsrc
            .getLastAccessTime() <= timeAfter));
        localizer.removeBlobReference(lrsrc.getKey(), user1, topo1, false);

        // should remove the second blob first
        localizer.cleanup();

        lrsrcSet = localizer.getUserResources().get(user1);
        assertEquals("local resource set size wrong", 2, lrsrcSet.getSize());
        long end = System.currentTimeMillis() + 100;
        while ((end - System.currentTimeMillis()) >= 0 && keyFile2.exists()) {
            Thread.sleep(1);
        }
        assertFalse("blob not deleted", keyFile2.exists());
        assertTrue("blob deleted", keyFile.exists());
        assertTrue("blob deleted", keyFile3.exists());

        // set size to cleanup another one
        localizer.setTargetCacheSize(34);

        // should remove the third blob
        localizer.cleanup();

        lrsrcSet = localizer.getUserResources().get(user1);
        assertEquals("local resource set size wrong", 1, lrsrcSet.getSize());
        assertTrue("blob deleted", keyFile.exists());
        assertFalse("blob not deleted", keyFile3.exists());
    }

    @Test(expected = AuthorizationException.class)
    public void testFailAcls() throws Exception {
        Map<String, Object> conf = new HashMap();
        // set clean time really high so doesn't kick in
        conf.put(DaemonConfig.SUPERVISOR_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS, 60 * 60 * 1000);
        // enable blobstore acl validation
        conf.put(Config.STORM_BLOBSTORE_ACL_VALIDATION_ENABLED, true);

        String topo1 = "topo1";
        String key1 = "key1";
        AsyncLocalizer localizer = new TestLocalizer(conf, baseDir.toString());

        ReadableBlobMeta rbm = new ReadableBlobMeta();
        // set acl so user doesn't have read access
        AccessControl acl = new AccessControl(AccessControlType.USER, BlobStoreAclHandler.ADMIN);
        acl.set_name(user1);
        rbm.set_settable(new SettableBlobMeta(Arrays.asList(acl)));
        when(mockblobstore.getBlobMeta(anyString())).thenReturn(rbm);
        when(mockblobstore.getBlob(key1)).thenReturn(new TestInputStreamWithMeta());
        File user1Dir = localizer.getLocalUserFileCacheDir(user1);
        assertTrue("failed to create user dir", user1Dir.mkdirs());

        // This should throw AuthorizationException because auth failed
        localizer.getBlob(new LocalResource(key1, false), user1, topo1, user1Dir);
    }

    @Test(expected = KeyNotFoundException.class)
    public void testKeyNotFoundException() throws Exception {
        Map<String, Object> conf = Utils.readStormConfig();
        String key1 = "key1";
        conf.put(Config.STORM_LOCAL_DIR, "target");
        LocalFsBlobStore bs = new LocalFsBlobStore();
        LocalFsBlobStore spy = spy(bs);
        Mockito.doReturn(true).when(spy).checkForBlobOrDownload(key1);
        Mockito.doNothing().when(spy).checkForBlobUpdate(key1);
        spy.prepare(conf,null,null);
        spy.getBlob(key1, null);
    }

    @Test
    public void testMultipleUsers() throws Exception {
        Map<String, Object> conf = new HashMap();
        // set clean time really high so doesn't kick in
        conf.put(DaemonConfig.SUPERVISOR_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS, 60*60*1000);

        String topo1 = "topo1";
        String topo2 = "topo2";
        String topo3 = "topo3";
        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";
        AsyncLocalizer localizer = new TestLocalizer(conf, baseDir.toString());
        // set to keep 2 blobs (each of size 34)
        localizer.setTargetCacheSize(68);

        ReadableBlobMeta rbm = new ReadableBlobMeta();
        rbm.set_settable(new SettableBlobMeta(WORLD_EVERYTHING));
        when(mockblobstore.getBlobMeta(anyString())).thenReturn(rbm);
        when(mockblobstore.getBlob(key1)).thenReturn(new TestInputStreamWithMeta());
        when(mockblobstore.getBlob(key2)).thenReturn(new TestInputStreamWithMeta());
        when(mockblobstore.getBlob(key3)).thenReturn(new TestInputStreamWithMeta());

        File user1Dir = localizer.getLocalUserFileCacheDir(user1);
        assertTrue("failed to create user dir", user1Dir.mkdirs());
        File user2Dir = localizer.getLocalUserFileCacheDir(user2);
        assertTrue("failed to create user dir", user2Dir.mkdirs());
        File user3Dir = localizer.getLocalUserFileCacheDir(user3);
        assertTrue("failed to create user dir", user3Dir.mkdirs());

        LocalizedResource lrsrc = localizer.getBlob(new LocalResource(key1, false), user1, topo1,
            user1Dir);
        LocalizedResource lrsrc2 = localizer.getBlob(new LocalResource(key2, false), user2, topo2,
            user2Dir);
        LocalizedResource lrsrc3 = localizer.getBlob(new LocalResource(key3, false), user3, topo3,
            user3Dir);
        // make sure we support different user reading same blob
        LocalizedResource lrsrc1_user3 = localizer.getBlob(new LocalResource(key1, false), user3,
            topo3, user3Dir);

        String expectedUserDir1 = joinPath(baseDir.toString(), USERCACHE, user1);
        String expectedFileDirUser1 = joinPath(expectedUserDir1, AsyncLocalizer.FILECACHE, AsyncLocalizer.FILESDIR);
        String expectedFileDirUser2 = joinPath(baseDir.toString(), USERCACHE, user2,
            AsyncLocalizer.FILECACHE, AsyncLocalizer.FILESDIR);
        String expectedFileDirUser3 = joinPath(baseDir.toString(), USERCACHE, user3,
            AsyncLocalizer.FILECACHE, AsyncLocalizer.FILESDIR);
        assertTrue("user filecache dir user1 not created", new File(expectedFileDirUser1).exists());
        assertTrue("user filecache dir user2 not created", new File(expectedFileDirUser2).exists());
        assertTrue("user filecache dir user3 not created", new File(expectedFileDirUser3).exists());

        File keyFile = new File(expectedFileDirUser1, key1 + ServerUtils.DEFAULT_CURRENT_BLOB_SUFFIX);
        File keyFile2 = new File(expectedFileDirUser2, key2 + ServerUtils.DEFAULT_CURRENT_BLOB_SUFFIX);
        File keyFile3 = new File(expectedFileDirUser3, key3 + ServerUtils.DEFAULT_CURRENT_BLOB_SUFFIX);
        File keyFile1user3 = new File(expectedFileDirUser3, key1 + ServerUtils.DEFAULT_CURRENT_BLOB_SUFFIX);

        assertTrue("blob not created", keyFile.exists());
        assertTrue("blob not created", keyFile2.exists());
        assertTrue("blob not created", keyFile3.exists());
        assertTrue("blob not created", keyFile1user3.exists());

        LocalizedResourceSet lrsrcSet = localizer.getUserResources().get(user1);
        assertEquals("local resource set size wrong", 1, lrsrcSet.getSize());
        LocalizedResourceSet lrsrcSet2 = localizer.getUserResources().get(user2);
        assertEquals("local resource set size wrong", 1, lrsrcSet2.getSize());
        LocalizedResourceSet lrsrcSet3 = localizer.getUserResources().get(user3);
        assertEquals("local resource set size wrong", 2, lrsrcSet3.getSize());

        localizer.removeBlobReference(lrsrc.getKey(), user1, topo1, false);
        // should remove key1
        localizer.cleanup();

        lrsrcSet = localizer.getUserResources().get(user1);
        lrsrcSet3 = localizer.getUserResources().get(user3);
        assertNull("user set should be null", lrsrcSet);
        assertFalse("blob dir not deleted", new File(expectedFileDirUser1).exists());
        assertFalse("blob dir not deleted", new File(expectedUserDir1).exists());
        assertEquals("local resource set size wrong", 2, lrsrcSet3.getSize());

        assertTrue("blob deleted", keyFile2.exists());
        assertFalse("blob not deleted", keyFile.exists());
        assertTrue("blob deleted", keyFile3.exists());
        assertTrue("blob deleted", keyFile1user3.exists());
    }

    @Test
    public void testUpdate() throws Exception {
        Map<String, Object> conf = new HashMap();
        // set clean time really high so doesn't kick in
        conf.put(DaemonConfig.SUPERVISOR_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS, 60*60*1000);

        String key1 = "key1";
        String topo1 = "topo1";
        String topo2 = "topo2";
        AsyncLocalizer localizer = new TestLocalizer(conf, baseDir.toString());

        ReadableBlobMeta rbm = new ReadableBlobMeta();
        rbm.set_version(1);
        rbm.set_settable(new SettableBlobMeta(WORLD_EVERYTHING));
        when(mockblobstore.getBlobMeta(key1)).thenReturn(rbm);
        when(mockblobstore.getBlob(key1)).thenReturn(new TestInputStreamWithMeta());

        File user1Dir = localizer.getLocalUserFileCacheDir(user1);
        assertTrue("failed to create user dir", user1Dir.mkdirs());
        LocalizedResource lrsrc = localizer.getBlob(new LocalResource(key1, false), user1, topo1,
            user1Dir);

        String expectedUserDir = joinPath(baseDir.toString(), USERCACHE, user1);
        String expectedFileDir = joinPath(expectedUserDir, AsyncLocalizer.FILECACHE, AsyncLocalizer.FILESDIR);
        assertTrue("user filecache dir not created", new File(expectedFileDir).exists());
        File keyFile = new File(expectedFileDir, key1);
        File keyFileCurrentSymlink = new File(expectedFileDir, key1 + ServerUtils.DEFAULT_CURRENT_BLOB_SUFFIX);
        assertTrue("blob not created", keyFileCurrentSymlink.exists());
        File versionFile = new File(expectedFileDir, key1 + ServerUtils.DEFAULT_BLOB_VERSION_SUFFIX);
        assertTrue("blob version file not created", versionFile.exists());
        assertEquals("blob version not correct", 1, ServerUtils.localVersionOfBlob(keyFile.toString()));

        LocalizedResourceSet lrsrcSet = localizer.getUserResources().get(user1);
        assertEquals("local resource set size wrong", 1, lrsrcSet.getSize());

        // test another topology getting blob with updated version - it should update version now
        rbm.set_version(2);

        localizer.getBlob(new LocalResource(key1, false), user1, topo2, user1Dir);
        assertTrue("blob version file not created", versionFile.exists());
        assertEquals("blob version not correct", 2, ServerUtils.localVersionOfBlob(keyFile.toString()));
        assertTrue("blob file with version 2 not created", new File(keyFile + ".2").exists());

        // now test regular updateBlob
        rbm.set_version(3);

        ArrayList<LocalResource> arr = new ArrayList<LocalResource>();
        arr.add(new LocalResource(key1, false));
        localizer.updateBlobs(arr, user1);
        assertTrue("blob version file not created", versionFile.exists());
        assertEquals("blob version not correct", 3, ServerUtils.localVersionOfBlob(keyFile.toString()));
        assertTrue("blob file with version 3 not created", new File(keyFile + ".3").exists());
    }
}
