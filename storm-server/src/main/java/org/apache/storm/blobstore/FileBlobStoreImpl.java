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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.storm.Config;
import org.apache.storm.daemon.supervisor.DirectoryDeleteVisitor;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.storm.utils.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Very basic blob store impl with no ACL handling.
 */
public class FileBlobStoreImpl {
    private static final long FULL_CLEANUP_FREQ = 60 * 60 * 1000l;
    private static final int BUCKETS = 1024;
    private static final Logger LOG = LoggerFactory.getLogger(FileBlobStoreImpl.class);
    private static final Timer timer = new Timer("FileBlobStore cleanup thread", true);
    private Path fullPath;
    private TimerTask cleanup = null;
    public FileBlobStoreImpl(Path path, Map<String, Object> conf) throws IOException {
        LOG.info("Creating new blob store based in {}", path);
        fullPath = path;
        Files.createDirectories(fullPath);
        Object shouldCleanup = conf.get(Config.BLOBSTORE_CLEANUP_ENABLE);
        if (ObjectReader.getBoolean(shouldCleanup, false)) {
            LOG.debug("Starting File blobstore cleaner");
            cleanup = new TimerTask() {
                @Override
                public void run() {
                    try {
                        fullCleanup(FULL_CLEANUP_FREQ);
                    } catch (IOException e) {
                        LOG.error("Error trying to cleanup", e);
                    }
                }
            };
            timer.scheduleAtFixedRate(cleanup, 0, FULL_CLEANUP_FREQ);
        }
    }

    /**
     * @return all keys that are available for reading.
     * @throws IOException on any error.
     */
    public Iterator<String> listKeys() throws IOException {
        return new KeyInHashDirIterator();
    }

    /**
     * Get an input stream for reading a part.
     * @param key the key of the part to read.
     * @return the where to read the data from.
     * @throws IOException on any error
     */
    public LocalFsBlobStoreFile read(String key) throws IOException {
        return new LocalFsBlobStoreFile(getKeyDir(key), BlobStoreFile.BLOBSTORE_DATA_FILE);
    }

    /**
     * Get an object tied to writing the data.
     * @param key the key of the part to write to.
     * @return an object that can be used to both write to, but also commit/cancel the operation.
     * @throws IOException on any error
     */
    public LocalFsBlobStoreFile write(String key, boolean create) throws IOException {
        return new LocalFsBlobStoreFile(getKeyDir(key), true, create);
    }

    /**
     * Check if the key exists in the blob store.
     * @param key the key to check for
     * @return true if it exists else false.
     */
    public boolean exists(String key) {
        return getKeyDir(key).toFile().exists();
    }

    /**
     * Delete a key from the blob store
     * @param key the key to delete
     * @throws IOException on any error
     */
    public void deleteKey(String key) throws IOException {
        Path keyDir = getKeyDir(key);
        LocalFsBlobStoreFile pf = new LocalFsBlobStoreFile(keyDir, BlobStoreFile.BLOBSTORE_DATA_FILE);
        pf.delete();
        delete(keyDir);
    }

    @VisibleForTesting
    Path getKeyDir(String key) {
        String hash = String.valueOf(Math.abs((long) key.hashCode()) % BUCKETS);
        Path ret = fullPath.resolve(hash).resolve(key);
        LOG.debug("{} Looking for {} in {}", new Object[]{ fullPath, key, hash });
        return ret;
    }

    public void fullCleanup(long age) throws IOException {
        long cleanUpIfBefore = System.currentTimeMillis() - age;
        Iterator<String> keys = new KeyInHashDirIterator();
        while (keys.hasNext()) {
            String key = keys.next();
            Path keyDir = getKeyDir(key);
            try (Stream<LocalFsBlobStoreFile> blobs = listBlobStoreFiles(keyDir)) {
                Iterator<LocalFsBlobStoreFile> i = blobs.iterator();
                if (!i.hasNext()) {
                    //The dir is empty, so try to delete it, may fail, but that is OK
                    try {
                        Files.delete(keyDir);
                    } catch (Exception e) {
                        LOG.warn("Could not delete " + keyDir + " will try again later");
                    }
                }
                while (i.hasNext()) {
                    LocalFsBlobStoreFile f = i.next();
                    if (f.isTmp()) {
                        if (f.getModTime() <= cleanUpIfBefore) {
                            f.delete();
                        }
                    }
                }
            }
        }
    }

    protected Stream<LocalFsBlobStoreFile> listBlobStoreFiles(Path path) throws IOException {
        return Files.list(path)
            .map(sub -> {
                try {
                    return new LocalFsBlobStoreFile(sub.getParent(), sub.getFileName().toString());
                } catch (IllegalArgumentException e) {
                    //Ignored the file did not match
                    LOG.warn("Found an unexpected file in {} {}", path, sub.getFileName());
                    return null;
                }
            })
            .filter(sub -> sub != null);
    }

    protected void delete(Path path) throws IOException {
        if (path.toFile().exists()) {
            Files.walkFileTree(path, new DirectoryDeleteVisitor());
        }
    }

    public void shutdown() {
        if (cleanup != null) {
            cleanup.cancel();
            cleanup = null;
        }
    }

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
                Path dir = fullPath.resolve(name);
                try (Stream<Path> fileList = Files.list(dir)) {
                    it = fileList
                        .map(p -> p.getFileName().toString())
                        .collect(Collectors.toList())
                        .iterator();
                } catch (NoSuchFileException e) {
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
}
