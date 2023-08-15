
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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.blobstore.BlobStoreFile;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.hdfs.testing.MiniDFSClusterExtensionClassLevel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class HdfsBlobStoreImplTest {

    @RegisterExtension
    public static final MiniDFSClusterExtensionClassLevel DFS_CLUSTER_EXTENSION = new MiniDFSClusterExtensionClassLevel();

    private static final Logger LOG = LoggerFactory.getLogger(HdfsBlobStoreImplTest.class);
    public static final String CONCURRENT_TEST_KEY_PREFIX = "concurrent-test-key";

    // key dir needs to be number 0 to number of buckets, choose one so we know where to look
    private static final String KEYDIR = "0";
    private final Path blobDir = new Path("/storm/blobstore1");
    private final Path fullKeyDir = new Path(blobDir, KEYDIR);
    private final String BLOBSTORE_DATA = "data";
    // for concurrent test
    private Path concurrentTestBlobDir = new Path("/storm/blobstore2");
    private Path concurrentTestFullKeyDir = new Path(concurrentTestBlobDir, KEYDIR);

    public class TestHdfsBlobStoreImpl extends HdfsBlobStoreImpl implements AutoCloseable {

        Path basePath;
        public TestHdfsBlobStoreImpl(Path path, Map<String, Object> conf) throws IOException {
            super(path, conf);
            basePath = path;
        }

        public TestHdfsBlobStoreImpl(Path path, Map<String, Object> conf,
            Configuration hconf) throws IOException {
            super(path, conf, hconf);
            basePath = path;
        }

        @Override
        protected Path getKeyDir(String key) {
            return new Path(new Path(basePath, KEYDIR), key);
        }

        @Override
        public void close() {
            this.shutdown();
        }
    }

    // Be careful about adding additional tests as the dfscluster will be shared
    @Test
    public void testMultiple() throws Exception {
        String testString = "testingblob";
        String validKey = "validkeyBasic";

        //Will be closed automatically when shutting down the DFS cluster
        FileSystem fs = DFS_CLUSTER_EXTENSION.getDfscluster().getFileSystem();
        Map<String, Object> conf = new HashMap<>();

        try (TestHdfsBlobStoreImpl hbs = new TestHdfsBlobStoreImpl(blobDir, conf, DFS_CLUSTER_EXTENSION.getHadoopConf())) {
            // should have created blobDir
            assertTrue(fs.exists(blobDir), "BlobStore dir wasn't created");
            assertEquals(HdfsBlobStoreImpl.BLOBSTORE_DIR_PERMISSION, fs.getFileStatus(blobDir).getPermission(),
                "BlobStore dir was created with wrong permissions");

            // test exist with non-existent key
            assertFalse(hbs.exists("bogus"), "file exists but shouldn't");

            // test write
            BlobStoreFile pfile = hbs.write(validKey, false);
            // Adding metadata to avoid null pointer exception
            SettableBlobMeta meta = new SettableBlobMeta();
            meta.set_replication_factor(1);
            pfile.setMetadata(meta);
            try (OutputStream ios = pfile.getOutputStream()) {
                ios.write(testString.getBytes(StandardCharsets.UTF_8));
            }

            // test modTime can change
            long initialModTime = pfile.getModTime();
            try (OutputStream ios = pfile.getOutputStream()) {
                ios.write(testString.getBytes(StandardCharsets.UTF_8));
            }
            long nextModTime = pfile.getModTime();
            assertTrue(nextModTime > initialModTime);

            // test commit creates properly
            assertTrue(fs.exists(fullKeyDir), "BlobStore key dir wasn't created");
            pfile.commit();
            Path dataFile = new Path(new Path(fullKeyDir, validKey), BLOBSTORE_DATA);
            assertTrue(fs.exists(dataFile), "blob data not committed");
            assertEquals(HdfsBlobStoreFile.BLOBSTORE_FILE_PERMISSION, fs.getFileStatus(dataFile).getPermission(),
                "BlobStore dir was created with wrong permissions");
            assertTrue(hbs.exists(validKey), "key doesn't exist but should");

            // test read
            BlobStoreFile readpFile = hbs.read(validKey);
            try (InputStream inStream = readpFile.getInputStream()) {
                String readString = IOUtils.toString(inStream, StandardCharsets.UTF_8);
                assertEquals(testString, readString, "string read from blob doesn't match");
            }

            // test listkeys
            Iterator<String> keys = hbs.listKeys();
            assertTrue(keys.hasNext(), "blob has one key");
            assertEquals(validKey, keys.next(), "one key in blobstore");

            // delete
            hbs.deleteKey(validKey);
            assertFalse(fs.exists(dataFile), "key not deleted");
            assertFalse(hbs.exists(validKey), "key not deleted");

            // Now do multiple
            String testString2 = "testingblob2";
            String validKey2 = "validkey2";

            // test write
            pfile = hbs.write(validKey, false);
            pfile.setMetadata(meta);
            try (OutputStream ios = pfile.getOutputStream()) {
                ios.write(testString.getBytes(StandardCharsets.UTF_8));
            }

            // test commit creates properly
            assertTrue(fs.exists(fullKeyDir), "BlobStore key dir wasn't created");
            pfile.commit();
            assertTrue(fs.exists(dataFile), "blob data not committed");
            assertEquals(HdfsBlobStoreFile.BLOBSTORE_FILE_PERMISSION, fs.getFileStatus(dataFile).getPermission(),
                "BlobStore dir was created with wrong permissions");
            assertTrue(hbs.exists(validKey), "key doesn't exist but should");

            // test write again
            pfile = hbs.write(validKey2, false);
            pfile.setMetadata(meta);
            try (OutputStream ios2 = pfile.getOutputStream()) {
                ios2.write(testString2.getBytes(StandardCharsets.UTF_8));
            }

            // test commit second creates properly
            pfile.commit();
            Path dataFile2 = new Path(new Path(fullKeyDir, validKey2), BLOBSTORE_DATA);
            assertTrue(fs.exists(dataFile2), "blob data not committed");
            assertEquals(HdfsBlobStoreFile.BLOBSTORE_FILE_PERMISSION, fs.getFileStatus(dataFile2).getPermission(),
                "BlobStore dir was created with wrong permissions");
            assertTrue(hbs.exists(validKey2), "key doesn't exist but should");

            // test listkeys
            keys = hbs.listKeys();
            int total = 0;
            boolean key1Found = false;
            boolean key2Found = false;
            while (keys.hasNext()) {
                total++;
                String key = keys.next();
                if (key.equals(validKey)) {
                    key1Found = true;
                } else if (key.equals(validKey2)) {
                    key2Found = true;
                } else {
                    fail("Found key that wasn't expected: " + key);
                }
            }
            assertEquals(2, total, "number of keys is wrong");
            assertTrue(key1Found, "blobstore missing key1");
            assertTrue(key2Found, "blobstore missing key2");

            // test read
            readpFile = hbs.read(validKey);
            try (InputStream inStream = readpFile.getInputStream()) {
                String readString = IOUtils.toString(inStream, StandardCharsets.UTF_8);
                assertEquals(testString, readString, "string read from blob doesn't match");
            }

            // test read
            readpFile = hbs.read(validKey2);
            try (InputStream inStream = readpFile.getInputStream()) {
                String readString = IOUtils.toString(inStream, StandardCharsets.UTF_8);
                assertEquals(testString2, readString, "string read from blob doesn't match");
            }

            hbs.deleteKey(validKey);
            assertFalse(hbs.exists(validKey), "key not deleted");
            hbs.deleteKey(validKey2);
            assertFalse(hbs.exists(validKey2), "key not deleted");
        }
    }

    @Test
    public void testGetFileLength() throws Exception {
        Map<String, Object> conf = new HashMap<>();
        String validKey = "validkeyBasic";
        String testString = "testingblob";
        try (TestHdfsBlobStoreImpl hbs = new TestHdfsBlobStoreImpl(blobDir, conf, DFS_CLUSTER_EXTENSION.getHadoopConf())) {
            BlobStoreFile pfile = hbs.write(validKey, false);
            // Adding metadata to avoid null pointer exception
            SettableBlobMeta meta = new SettableBlobMeta();
            meta.set_replication_factor(1);
            pfile.setMetadata(meta);
            try (OutputStream ios = pfile.getOutputStream()) {
                ios.write(testString.getBytes(StandardCharsets.UTF_8));
            }
            assertEquals(testString.getBytes(StandardCharsets.UTF_8).length, pfile.getFileLength());
        }
    }

    /**
     * Test by listing keys {@link HdfsBlobStoreImpl#listKeys()} in multiple concurrent threads and then ensure that
     * same keys are retrived in all the threads without any exceptions.
     */
    @Test
    public void testConcurrentIteration() throws Exception {
        int concurrency = 100;
        int keyCount = 10;

        class ConcurrentListerRunnable implements Runnable {
            TestHdfsBlobStoreImpl hbs;
            int instanceNum;
            List<String> keys = new ArrayList<>();

            public ConcurrentListerRunnable(TestHdfsBlobStoreImpl hbs, int instanceNum) {
                this.hbs = hbs;
                this.instanceNum = instanceNum;
            }

            @Override
            public void run() {
                try {
                    Iterator<String> iterator = hbs.listKeys(concurrentTestFullKeyDir);
                    while (iterator.hasNext()) {
                        keys.add(iterator.next());
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }

        Map<String, Object> conf = new HashMap<>();
        try (TestHdfsBlobStoreImpl hbs = new TestHdfsBlobStoreImpl(concurrentTestBlobDir, conf, DFS_CLUSTER_EXTENSION.getHadoopConf())) {
            // test write again
            for (int i = 0 ; i < keyCount ; i++) {
                String key = CONCURRENT_TEST_KEY_PREFIX + i;
                String val = "This is string " + i;
                BlobStoreFile pfile = hbs.write(key, false);
                SettableBlobMeta meta = new SettableBlobMeta();
                meta.set_replication_factor(1);
                pfile.setMetadata(meta);
                try (OutputStream ios = pfile.getOutputStream()) {
                    ios.write(val.getBytes(StandardCharsets.UTF_8));
                }
            }


            ConcurrentListerRunnable[] runnables = new ConcurrentListerRunnable[concurrency];
            Thread[] threads = new Thread[concurrency];
            for (int i = 0 ; i < concurrency ; i++) {
                runnables[i] = new ConcurrentListerRunnable(hbs, i);
                threads[i] = new Thread(runnables[i]);
            }
            for (int i = 0 ; i < concurrency ; i++) {
                threads[i].start();
            }
            for (int i = 0 ; i < concurrency ; i++) {
                threads[i].join();
            }
            List<String> keys = runnables[0].keys;
            assertEquals(keyCount, keys.size(), "Number of keys (values=" + keys + ")");
            for (int i = 1 ; i < concurrency ; i++) {
                ConcurrentListerRunnable otherRunnable = runnables[i];
                assertEquals(keys, otherRunnable.keys);
            }
            for (int i = 0 ; i < keyCount ; i++) {
                String key = CONCURRENT_TEST_KEY_PREFIX + i;
                hbs.deleteKey(key);
            }
           LOG.info("All %d threads have %d keys=[%s]\n", concurrency, keys.size(), String.join(",", keys));
        }
    }
}
