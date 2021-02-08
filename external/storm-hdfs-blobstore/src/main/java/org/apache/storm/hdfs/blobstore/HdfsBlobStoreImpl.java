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

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.storm.Config;
import org.apache.storm.blobstore.BlobStoreFile;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * HDFS blob store impl.
 */
public class HdfsBlobStoreImpl {

    // blobstore directory is private!
    public static final FsPermission BLOBSTORE_DIR_PERMISSION =
            FsPermission.createImmutable((short) 0700); // rwx--------
    private static final String BLOBSTORE_UPDATE_TIME_FILE = "lastUpdatedBlobTime";

    private static final Logger LOG = LoggerFactory.getLogger(HdfsBlobStoreImpl.class);

    private static final long FULL_CLEANUP_FREQ = 60 * 60 * 1000L;
    private static final int BUCKETS = 1024;
    private static final String BLOBSTORE_DATA = "data";
    
    private Timer timer;

    private Path fullPath;
    private FileSystem fileSystem;
    private Configuration hadoopConf;

    public class KeyInHashDirIterator implements Iterator<String> {
        private int currentBucket = 0;
        private Iterator<String> it = null;
        private String next = null;

        public KeyInHashDirIterator() throws IOException {
            primeNext();
        }

        private void primeNext() throws IOException {
            while (it == null && currentBucket < BUCKETS) {
                String name = String.valueOf(currentBucket);
                Path dir = new Path(fullPath, name);
                try {
                    it = listKeys(dir);
                } catch (FileNotFoundException e) {
                    it = null;
                }
                if (it == null || !it.hasNext()) {
                    it = null;
                    currentBucket++;
                } else {
                    next = it.next();
                }
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public String next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            String current = next;
            next = null;
            if (it != null) {
                if (!it.hasNext()) {
                    it = null;
                    currentBucket++;
                    try {
                        primeNext();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    next = it.next();
                }
            }
            return current;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Delete Not Supported");
        }
    }

    public HdfsBlobStoreImpl(Path path, Map<String, Object> conf) throws IOException {
        this(path, conf, new Configuration());
    }

    public HdfsBlobStoreImpl(Path path, Map<String, Object> conf,
                             Configuration hconf) throws IOException {
        LOG.debug("Blob store based in {}", path);
        fullPath = path;
        hadoopConf = hconf;
        fileSystem = path.getFileSystem(hadoopConf);

        if (!fileSystem.exists(fullPath)) {
            FsPermission perms = new FsPermission(BLOBSTORE_DIR_PERMISSION);
            boolean success = fileSystem.mkdirs(fullPath, perms);
            if (!success) {
                throw new IOException("Error creating blobstore directory: " + fullPath);
            }
        }

        Object shouldCleanup = conf.get(Config.BLOBSTORE_CLEANUP_ENABLE);
        if (ObjectReader.getBoolean(shouldCleanup, false)) {
            LOG.debug("Starting hdfs blobstore cleaner");
            TimerTask cleanup = new TimerTask() {
                @Override
                public void run() {
                    try {
                        fullCleanup(FULL_CLEANUP_FREQ);
                    } catch (IOException e) {
                        LOG.error("Error trying to cleanup", e);
                    }
                }
            };
            timer = new Timer("HdfsBlobStore cleanup thread", true);
            timer.scheduleAtFixedRate(cleanup, 0, FULL_CLEANUP_FREQ);
        }
    }

    /**
     * List relevant keys.
     *
     * @return all keys that are available for reading
     * @throws IOException on any error
     */
    public Iterator<String> listKeys() throws IOException {
        return new KeyInHashDirIterator();
    }

    protected Iterator<String> listKeys(Path path) throws IOException {
        ArrayList<String> ret = new ArrayList<String>();
        FileStatus[] files = fileSystem.listStatus(new Path[]{path});
        if (files != null) {
            for (FileStatus sub : files) {
                try {
                    ret.add(sub.getPath().getName().toString());
                } catch (IllegalArgumentException e) {
                    //Ignored the file did not match
                    LOG.debug("Found an unexpected file in {} {}", path, sub.getPath().getName());
                }
            }
        }
        return ret.iterator();
    }

    /**
     * Get an input stream for reading a part.
     *
     * @param key the key of the part to read
     * @return the where to read the data from
     * @throws IOException on any error
     */
    public BlobStoreFile read(String key) throws IOException {
        return new HdfsBlobStoreFile(getKeyDir(key), BLOBSTORE_DATA, hadoopConf);
    }

    /**
     * Get an object tied to writing the data.
     *
     * @param key the key of the part to write to.
     * @param create whether the file needs to be new or not.
     * @return an object that can be used to both write to, but also commit/cancel the operation.
     * @throws IOException on any error
     */
    public BlobStoreFile write(String key, boolean create) throws IOException {
        return new HdfsBlobStoreFile(getKeyDir(key), true, create, hadoopConf);
    }

    /**
     * Check if the key exists in the blob store.
     *
     * @param key the key to check for
     * @return true if it exists else false.
     */
    public boolean exists(String key) {
        Path dir = getKeyDir(key);
        boolean res = false;
        try {
            fileSystem = dir.getFileSystem(hadoopConf);
            res = fileSystem.exists(dir);
        } catch (IOException e) {
            LOG.warn("Exception checking for exists on: " + key);
        }
        return res;
    }

    /**
     * Delete a key from the blob store.
     *
     * @param key the key to delete
     * @throws IOException on any error
     */
    public void deleteKey(String key) throws IOException {
        Path keyDir = getKeyDir(key);
        HdfsBlobStoreFile pf = new HdfsBlobStoreFile(keyDir, BLOBSTORE_DATA,
                hadoopConf);
        pf.delete();
        delete(keyDir);
    }

    protected Path getKeyDir(String key) {
        String hash = String.valueOf(Math.abs((long) key.hashCode()) % BUCKETS);
        Path hashDir = new Path(fullPath, hash);

        Path ret = new Path(hashDir, key);
        LOG.debug("{} Looking for {} in {}", new Object[]{fullPath, key, hash});
        return ret;
    }

    public void fullCleanup(long age) throws IOException {
        long cleanUpIfBefore = System.currentTimeMillis() - age;
        Iterator<String> keys = new KeyInHashDirIterator();
        while (keys.hasNext()) {
            String key = keys.next();
            Path keyDir = getKeyDir(key);
            Iterator<BlobStoreFile> i = listBlobStoreFiles(keyDir);
            if (!i.hasNext()) {
                //The dir is empty, so try to delete it, may fail, but that is OK
                try {
                    fileSystem.delete(keyDir, true);
                } catch (Exception e) {
                    LOG.warn("Could not delete " + keyDir + " will try again later");
                }
            }
            while (i.hasNext()) {
                BlobStoreFile f = i.next();
                if (f.isTmp()) {
                    if (f.getModTime() <= cleanUpIfBefore) {
                        f.delete();
                    }
                }
            }
        }
    }

    protected Iterator<BlobStoreFile> listBlobStoreFiles(Path path) throws IOException {
        ArrayList<BlobStoreFile> ret = new ArrayList<BlobStoreFile>();
        FileStatus[] files = fileSystem.listStatus(new Path[]{path});
        if (files != null) {
            for (FileStatus sub : files) {
                try {
                    ret.add(new HdfsBlobStoreFile(sub.getPath().getParent(), sub.getPath().getName(),
                            hadoopConf));
                } catch (IllegalArgumentException e) {
                    //Ignored the file did not match
                    LOG.warn("Found an unexpected file in {} {}", path, sub.getPath().getName());
                }
            }
        }
        return ret.iterator();
    }

    protected int getBlobReplication(String key) throws IOException {
        Path path = getKeyDir(key);
        Path dest = new Path(path, BLOBSTORE_DATA);
        return fileSystem.getFileStatus(dest).getReplication();
    }

    protected int updateBlobReplication(String key, int replication) throws IOException {
        Path path = getKeyDir(key);
        Path dest = new Path(path, BLOBSTORE_DATA);
        fileSystem.setReplication(dest, (short) replication);
        return fileSystem.getFileStatus(dest).getReplication();
    }

    protected void delete(Path path) throws IOException {
        fileSystem.delete(path, true);
    }

    public void shutdown() {
        if (timer != null) {
            timer.cancel();
        }
    }

    /**
     * Get the last update time of any blob.
     *
     * @return the last updated time of blobs within the blobstore.
     * @throws IOException on any error
     */
    public long getLastBlobUpdateTime() throws IOException {
        Path updateTimeFile = new Path(fullPath, BLOBSTORE_UPDATE_TIME_FILE);
        if (!fileSystem.exists(updateTimeFile)) {
            return -1L;
        }
        FSDataInputStream inputStream = fileSystem.open(updateTimeFile);
        String timestamp = IOUtils.toString(inputStream, "UTF-8");
        inputStream.close();
        try {
            long updateTime = Long.parseLong(timestamp);
            return updateTime;
        } catch (NumberFormatException e) {
            LOG.error("Invalid blobstore update time {} in file {}", timestamp, updateTimeFile);
            return -1L;
        }
    }

    /**
     * Updates the last updated time of existing blobstores to the current time.
     *
     * @throws IOException on any error
     */
    public synchronized void updateLastBlobUpdateTime() throws IOException {
        Long timestamp = Time.currentTimeMillis();
        Path updateTimeFile = new Path(fullPath, BLOBSTORE_UPDATE_TIME_FILE);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(updateTimeFile, true);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
        bufferedWriter.write(timestamp.toString());
        bufferedWriter.close();
        LOG.debug("Updated blobstore update time of {} to {}", updateTimeFile, timestamp);
    }

    /**
     * Validates that the last updated blob time of the blobstore is up to date with the current existing blobs.
     *
     * @throws IOException on any error
     */
    public void validateBlobUpdateTime() throws IOException {
        int currentBucket = 0;
        long baseModTime = 0;
        while (currentBucket < BUCKETS) {
            String name = String.valueOf(currentBucket);
            Path bucketDir = new Path(fullPath, name);

            // only consider bucket dirs that exist with files in them
            if (fileSystem.exists(bucketDir) && fileSystem.listStatus(bucketDir).length > 0) {
                long modtime = fileSystem.getFileStatus(bucketDir).getModificationTime();
                if (modtime > baseModTime) {
                    baseModTime = modtime;
                }
            }

            currentBucket++;
        }
        if (baseModTime > 0 && baseModTime > getLastBlobUpdateTime()) {
            LOG.info("Blobstore update time requires an update to at least {}", baseModTime);
            updateLastBlobUpdateTime();
        }
    }
}
