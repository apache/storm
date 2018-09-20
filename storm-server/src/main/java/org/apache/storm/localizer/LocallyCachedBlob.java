/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.localizer;

import com.codahale.metrics.Histogram;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a blob that is cached locally on disk by the supervisor.
 */
public abstract class LocallyCachedBlob {
    public static final long NOT_DOWNLOADED_VERSION = -1;
    private static final Logger LOG = LoggerFactory.getLogger(LocallyCachedBlob.class);
    // A callback that does nothing.
    private static final BlobChangingCallback NOOP_CB = (assignment, port, resource, go) -> {
    };
    private final Map<PortAndAssignment, BlobChangingCallback> references = new HashMap<>();
    private final String blobDescription;
    private final String blobKey;
    private long lastUsed = Time.currentTimeMillis();
    private CompletableFuture<Void> doneUpdating = null;

    private final Histogram fetchingRate;

    /**
     * Create a new LocallyCachedBlob.
     *
     * @param blobDescription a description of the blob this represents.  Typically it should at least be the blob key, but ideally also
     *     include if it is an archive or not, what user or topology it is for, or if it is a storm.jar etc.
     */
    protected LocallyCachedBlob(String blobDescription, String blobKey, StormMetricsRegistry metricsRegistry) {
        this.blobDescription = blobDescription;
        this.blobKey = blobKey;
        this.fetchingRate = metricsRegistry.registerHistogram("supervisor:blob-fetching-rate-MB/s");
    }

    /**
     * Helper function to download blob from blob store.
     * @param store Blob store to fetch blobs from
     * @param key Key to retrieve blobs
     * @param pathSupplier A function that supplies the download destination of a blob. It guarantees the validity
     *                     of path or throws {@link IOException}
     * @param outStreamSupplier A function that supplies the {@link OutputStream} object
     * @return The metadata of the download session, including blob's version and download destination
     * @throws KeyNotFoundException Thrown if key to retrieve blob is invalid
     * @throws AuthorizationException Thrown if the retrieval is not under security authorization
     * @throws IOException Thrown if any IO error occurs
     */
    protected DownloadMeta fetch(ClientBlobStore store, String key,
                                 IOFunction<Long, Path> pathSupplier,
                                 IOFunction<File, OutputStream> outStreamSupplier)
            throws KeyNotFoundException, AuthorizationException, IOException {

        try (InputStreamWithMeta in = store.getBlob(key)) {
            long newVersion = in.getVersion();
            long currentVersion = getLocalVersion();
            if (newVersion == currentVersion) {
                LOG.warn("The version did not change, but going to download again {} {}", currentVersion, key);
            }

            //Make sure the parent directory is there and ready to go
            Path downloadPath = pathSupplier.apply(newVersion);
            LOG.debug("Downloading {} to {}", key, downloadPath);

            long duration;
            long totalRead = 0;
            try (OutputStream out = outStreamSupplier.apply(downloadPath.toFile())) {
                long startTime = Time.nanoTime();

                byte[] buffer = new byte[4096];
                int read;
                while ((read = in.read(buffer)) >= 0) {
                    out.write(buffer, 0, read);
                    totalRead += read;
                }

                duration = Time.nanoTime() - startTime;
            }

            long expectedSize = in.getFileLength();
            if (totalRead != expectedSize) {
                throw new IOException("We expected to download " + expectedSize + " bytes but found we got " + totalRead);
            } else {
                double downloadRate = ((double) totalRead * 1e3) / duration;
                fetchingRate.update(Math.round(downloadRate));
            }
            return new DownloadMeta(downloadPath, newVersion);
        }
    }

    /**
     * Get the version of the blob cached locally.  If the version is unknown or it has not been downloaded NOT_DOWNLOADED_VERSION should be
     * returned. PRECONDITION: this can only be called with a lock on this instance held.
     */
    public abstract long getLocalVersion();

    /**
     * Get the version of the blob in the blob store. PRECONDITION: this can only be called with a lock on this instance held.
     */
    public abstract long getRemoteVersion(ClientBlobStore store) throws KeyNotFoundException, AuthorizationException;

    /**
     * Download the latest version to a temp location. This may also include unzipping some or all of the data to a temp location.
     * PRECONDITION: this can only be called with a lock on this instance held.
     *
     * @param store the store to us to download the data.
     * @return the version that was downloaded.
     */
    public abstract long fetchUnzipToTemp(ClientBlobStore store) throws IOException, KeyNotFoundException, AuthorizationException;

    /**
     * Commit the new version and make it available for the end user.
     * PRECONDITION: uncompressToTempLocationIfNeeded will have been called.
     * PRECONDITION: this can only be called with a lock on this instance held.
     * @param version the version of the blob to commit.
     */
    public abstract void commitNewVersion(long version) throws IOException;

    /**
     * Clean up any temporary files.  This will be called after updating a blob, either successfully or if an error has occured.
     * The goal is to find any files that may be left over and remove them so space is not leaked.
     * PRECONDITION: this can only be called with a lock on this instance held.
     */
    public abstract void cleanupOrphanedData() throws IOException;

    /**
     * Completely remove anything that is cached locally for this blob and all tracking files also stored for it.
     * This will be called after the blob was determined to no longer be needed in the cache.
     * PRECONDITION: this can only be called with a lock on this instance held.
     */
    public abstract void completelyRemove() throws IOException;

    /**
     * Get the amount of disk space that is used by this blob.  If the blob is uncompressed it should be the sum of the space used by all
     * of the uncompressed files.  In general this will not be called with any locks held so it is a good idea to cache it and updated it
     * when committing a new version.
     */
    public abstract long getSizeOnDisk();

    /**
     * Get the size of p in bytes.
     * @param p the path to read.
     * @return the size of p in bytes.
     */
    protected static long getSizeOnDisk(Path p) throws IOException {
        if (!Files.exists(p)) {
            return 0;
        } else if (Files.isRegularFile(p)) {
            return Files.size(p);
        } else {
            //We will not follow sym links
            return Files.walk(p)
                    .filter((subp) -> Files.isRegularFile(subp, LinkOption.NOFOLLOW_LINKS))
                    .mapToLong((subp) -> {
                        try {
                            return Files.size(subp);
                        } catch (IOException e) {
                            LOG.warn("Could not get the size of {}", subp);
                        }
                        return 0;
                    }).sum();
        }
    }

    /**
     * Updates the last updated time.  This should be called when references are added or removed.
     */
    protected synchronized void touch() {
        lastUsed = Time.currentTimeMillis();
        LOG.debug("Setting {} ts to {}", blobKey, lastUsed);
    }

    /**
     * Get the last time that this used for LRU calculations.
     */
    public synchronized long getLastUsed() {
        return lastUsed;
    }

    /**
     * Return true if this blob is actively being used, else false (meaning it can be deleted, but might not be).
     */
    public synchronized boolean isUsed() {
        return !references.isEmpty();
    }

    /**
     * Mark that a given port and assignment are using this.
     * @param pna the slot and assignment that are using this blob.
     * @param cb an optional callback indicating that they want to know/synchronize when a blob is updated.
     */
    public synchronized void addReference(final PortAndAssignment pna, BlobChangingCallback cb) {
        LOG.debug("Adding reference {}", pna);
        if (cb == null) {
            cb = NOOP_CB;
        }
        if (references.put(pna, cb) != null) {
            LOG.warn("{} already has a reservation for {}", pna, blobDescription);
        }
    }

    /**
     * Removes a reservation for this blob from a given slot and assignemnt.
     * @param pna the slot + assignment that no longer needs this blob.
     */
    public synchronized void removeReference(final PortAndAssignment pna) {
        LOG.debug("Removing reference {}", pna);
        if (references.remove(pna) == null) {
            LOG.warn("{} had no reservation for {}", pna, blobDescription);
        }
        touch();
    }

    /**
     * Inform all of the callbacks that a change is going to happen and then wait for
     * them to all get back that it is OK to make that change.
     */
    public synchronized void informAllOfChangeAndWaitForConsensus() {
        CountDownLatch cdl = new CountDownLatch(references.size());
        doneUpdating = new CompletableFuture<>();
        for (Map.Entry<PortAndAssignment, BlobChangingCallback> entry : references.entrySet()) {
            GoodToGo gtg = new GoodToGo(cdl, doneUpdating);
            try {
                PortAndAssignment pna = entry.getKey();
                BlobChangingCallback cb = entry.getValue();
                cb.blobChanging(pna.getAssignment(), pna.getPort(), this, gtg);
            } finally {
                gtg.countDownIfLatchWasNotGotten();
            }
        }
        try {
            cdl.await(3, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            //Interrupted is thrown when we are shutting down.
            // So just ignore it for now...
        }
    }

    /**
     * Inform all of the callbacks that the change to the blob is complete.
     */
    public synchronized void informAllChangeComplete() {
        doneUpdating.complete(null);
    }

    /**
     * Get the key for this blob.
     */
    public String getKey() {
        return blobKey;
    }


    public Collection<PortAndAssignment> getDependencies() {
        return references.keySet();
    }

    public abstract boolean isFullyDownloaded();

    static class DownloadMeta {
        private final Path downloadPath;
        private final long version;

        public DownloadMeta(Path downloadPath, long version) {
            this.downloadPath = downloadPath;
            this.version = version;
        }

        public Path getDownloadPath() {
            return downloadPath;
        }

        public long getVersion() {
            return version;
        }
    }

}
