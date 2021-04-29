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

package org.apache.storm.localizer;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.daemon.supervisor.AdvancedFSOps;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.storm.shade.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.NimbusLeaderNotFoundException;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ServerConfigUtils;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.WrappedKeyNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads and caches blobs locally.
 */
public class AsyncLocalizer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncLocalizer.class);

    private static final CompletableFuture<Void> ALL_DONE_FUTURE = CompletableFuture.completedFuture(null);
    private static final int ATTEMPTS_INTERVAL_TIME = 100;

    private final Timer blobCacheUpdateDuration;
    private final Timer blobLocalizationDuration;
    private final Meter localResourceFileNotFoundWhenReleasingSlot;
    private final Meter updateBlobExceptions;

    // track resources - user to resourceSet
    //ConcurrentHashMap is explicitly used everywhere in this class because it uses locks to guarantee atomicity for compute and
    // computeIfAbsent where as ConcurrentMap allows for a retry of the function passed in, and would require the function to have
    // no side effects.
    protected final ConcurrentHashMap<String, ConcurrentHashMap<String, LocalizedResource>> userFiles = new ConcurrentHashMap<>();
    protected final ConcurrentHashMap<String, ConcurrentHashMap<String, LocalizedResource>> userArchives = new ConcurrentHashMap<>();
    private final boolean isLocalMode;
    // topology to tracking of topology dir and resources
    private final ConcurrentHashMap<String, CompletableFuture<Void>> blobPending;
    private final Map<String, Object> conf;
    private final AdvancedFSOps fsOps;
    private final boolean symlinksDisabled;
    private final ConcurrentHashMap<String, LocallyCachedBlob> topologyBlobs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<Void>> topologyBasicDownloaded = new ConcurrentHashMap<>();
    private final Path localBaseDir;
    private final int blobDownloadRetries;
    private final ScheduledExecutorService downloadExecService;
    private final ScheduledExecutorService taskExecService;
    private final long cacheCleanupPeriod;
    private final int updateBlobPeriod;
    private final StormMetricsRegistry metricsRegistry;
    // cleanup
    @VisibleForTesting
    protected long cacheTargetSize;

    @VisibleForTesting
    AsyncLocalizer(Map<String, Object> conf, AdvancedFSOps ops, String baseDir, StormMetricsRegistry metricsRegistry) throws IOException {
        this.conf = conf;
        this.blobCacheUpdateDuration = metricsRegistry.registerTimer("supervisor:blob-cache-update-duration");
        this.blobLocalizationDuration = metricsRegistry.registerTimer("supervisor:blob-localization-duration");
        this.localResourceFileNotFoundWhenReleasingSlot
                = metricsRegistry.registerMeter("supervisor:local-resource-file-not-found-when-releasing-slot");
        this.updateBlobExceptions = metricsRegistry.registerMeter("supervisor:update-blob-exceptions");
        this.metricsRegistry = metricsRegistry;
        isLocalMode = ConfigUtils.isLocalMode(conf);
        fsOps = ops;
        localBaseDir = Paths.get(baseDir);
        // default cache size 10GB, converted to Bytes
        cacheTargetSize = ObjectReader.getInt(conf.get(DaemonConfig.SUPERVISOR_LOCALIZER_CACHE_TARGET_SIZE_MB),
                                              10 * 1024).longValue() << 20;
        // default 30 seconds. (we cache the size so it is cheap to do)
        cacheCleanupPeriod = ObjectReader.getInt(conf.get(
            DaemonConfig.SUPERVISOR_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS), 30 * 1000).longValue();

        updateBlobPeriod = ServerConfigUtils.getLocalizerUpdateBlobInterval(conf);

        blobDownloadRetries = ObjectReader.getInt(conf.get(
            DaemonConfig.SUPERVISOR_BLOBSTORE_DOWNLOAD_MAX_RETRIES), 3);

        int downloadThreadPoolSize = ObjectReader.getInt(conf.get(DaemonConfig.SUPERVISOR_BLOBSTORE_DOWNLOAD_THREAD_COUNT), 5);
        downloadExecService = Executors.newScheduledThreadPool(downloadThreadPoolSize,
                new ThreadFactoryBuilder().setNameFormat("AsyncLocalizer Download Executor - %d").build());
        taskExecService = Executors.newScheduledThreadPool(3,
                new ThreadFactoryBuilder().setNameFormat("AsyncLocalizer Task Executor - %d").build());
        reconstructLocalizedResources();

        symlinksDisabled = (boolean) conf.getOrDefault(Config.DISABLE_SYMLINKS, false);
        blobPending = new ConcurrentHashMap<>();
    }

    public AsyncLocalizer(Map<String, Object> conf, StormMetricsRegistry metricsRegistry) throws IOException {
        this(conf, AdvancedFSOps.make(conf), ConfigUtils.supervisorLocalDir(conf), metricsRegistry);
    }

    @VisibleForTesting
    LocallyCachedBlob getTopoJar(final String topologyId, String owner) {
        return topologyBlobs.computeIfAbsent(ConfigUtils.masterStormJarKey(topologyId),
            (tjk) -> {
                try {
                    return new LocallyCachedTopologyBlob(topologyId, isLocalMode, conf, fsOps,
                                                      LocallyCachedTopologyBlob.TopologyBlobType
                                                          .TOPO_JAR, owner, metricsRegistry);
                } catch (IOException e) {
                    String message = "Failed getTopoJar for " + topologyId;
                    LOG.error(message, e);
                    throw new RuntimeException(message, e);
                }
            });
    }

    @VisibleForTesting
    LocallyCachedBlob getTopoCode(final String topologyId, String owner) {
        return topologyBlobs.computeIfAbsent(ConfigUtils.masterStormCodeKey(topologyId),
            (tck) -> {
                try {
                    return new LocallyCachedTopologyBlob(topologyId, isLocalMode, conf, fsOps,
                                                      LocallyCachedTopologyBlob.TopologyBlobType
                                                          .TOPO_CODE, owner, metricsRegistry);
                } catch (IOException e) {
                    String message = "Failed getTopoCode for " + topologyId;
                    LOG.error(message, e);
                    throw new RuntimeException(message, e);
                }
            });
    }

    @VisibleForTesting
    LocallyCachedBlob getTopoConf(final String topologyId, String owner) {
        return topologyBlobs.computeIfAbsent(ConfigUtils.masterStormConfKey(topologyId),
            (tck) -> {
                try {
                    return new LocallyCachedTopologyBlob(topologyId, isLocalMode, conf, fsOps,
                                                      LocallyCachedTopologyBlob.TopologyBlobType
                                                          .TOPO_CONF, owner, metricsRegistry);
                } catch (IOException e) {
                    String message = "Failed getTopoConf for " + topologyId;
                    LOG.error(message, e);
                    throw new RuntimeException(message, e);
                }
            });
    }

    private LocalizedResource getUserArchive(String user, String key) {
        assert user != null : "All user archives require a user present";
        ConcurrentMap<String, LocalizedResource> keyToResource = userArchives.computeIfAbsent(user, (u) -> new ConcurrentHashMap<>());
        return keyToResource.computeIfAbsent(key, 
            (k) -> new LocalizedResource(key, localBaseDir, true, fsOps, conf, user, metricsRegistry));
    }

    private LocalizedResource getUserFile(String user, String key) {
        assert user != null : "All user archives require a user present";
        ConcurrentMap<String, LocalizedResource> keyToResource = userFiles.computeIfAbsent(user, (u) -> new ConcurrentHashMap<>());
        return keyToResource.computeIfAbsent(key, 
            (k) -> new LocalizedResource(key, localBaseDir, false, fsOps, conf, user, metricsRegistry));
    }

    /**
     * Request that all of the blobs necessary for this topology be downloaded.  Note that this adds references to
     * blobs asynchronously in background threads.
     *
     * @param assignment the assignment that needs the blobs
     * @param port       the port the assignment is a part of
     * @param cb         a callback for when the blobs change.  This is only for blobs that are tied to the lifetime of the worker.
     * @return a Future that indicates when they are all downloaded.
     *
     * @throws IOException if there was an error while trying doing it.
     */
    public CompletableFuture<Void> requestDownloadTopologyBlobs(final LocalAssignment assignment, final int port,
                                                                final BlobChangingCallback cb) throws IOException {
        final PortAndAssignment pna = new TimePortAndAssignment(new PortAndAssignmentImpl(port, assignment), blobLocalizationDuration);
        final String topologyId = pna.getToplogyId();

        LOG.info("requestDownloadTopologyBlobs for {}", pna);

        CompletableFuture<Void> baseBlobs = requestDownloadBaseTopologyBlobs(pna, cb);
        return baseBlobs.thenComposeAsync((v) ->
            blobPending.compute(topologyId, (tid, old) -> {
                CompletableFuture<Void> ret = old;
                if (ret == null) {
                    ret = CompletableFuture.supplyAsync(new DownloadBlobs(pna, cb), taskExecService);
                } else {
                    try {
                        addReferencesToBlobs(pna, cb);
                    } catch (Exception e) {
                        LOG.error("Failed adding references to blobs for " + pna, e);
                        throw new RuntimeException(e);
                    } finally {
                        pna.complete();
                    }
                }
                LOG.debug("Reserved blobs {} {}", topologyId, ret);
                return ret;
            }));
    }

    @VisibleForTesting
    CompletableFuture<Void> requestDownloadBaseTopologyBlobs(PortAndAssignment pna, BlobChangingCallback cb) {
        final String topologyId = pna.getToplogyId();

        final LocallyCachedBlob topoJar = getTopoJar(topologyId, pna.getAssignment().get_owner());
        topoJar.addReference(pna, cb);

        final LocallyCachedBlob topoCode = getTopoCode(topologyId, pna.getAssignment().get_owner());
        topoCode.addReference(pna, cb);

        final LocallyCachedBlob topoConf = getTopoConf(topologyId, pna.getAssignment().get_owner());
        topoConf.addReference(pna, cb);

        return topologyBasicDownloaded.computeIfAbsent(topologyId,
            (tid) -> downloadOrUpdate(topoJar, topoCode, topoConf));
    }

    private CompletableFuture<Void> downloadOrUpdate(LocallyCachedBlob... blobs) {
        return downloadOrUpdate(Arrays.asList(blobs));
    }

    private CompletableFuture<Void> downloadOrUpdate(Collection<? extends LocallyCachedBlob> blobs) {

        final long remoteBlobstoreUpdateTime = getRemoteBlobstoreUpdateTime();

        CompletableFuture<Void>[] all = new CompletableFuture[blobs.size()];
        int i = 0;
        for (final LocallyCachedBlob blob : blobs) {
            all[i] = CompletableFuture.runAsync(() -> {
                LOG.debug("STARTING download of {}", blob);
                try (ClientBlobStore blobStore = getClientBlobStore()) {
                    boolean done = false;
                    long failures = 0;
                    while (!done) {
                        try {
                            blob.update(blobStore, remoteBlobstoreUpdateTime);
                            done = true;
                        } catch (Exception e) {
                            failures++;
                            if (failures > blobDownloadRetries) {
                                throw new RuntimeException("Could not download...", e);
                            }
                            LOG.warn("Failed to download blob {} will try again in {} ms", blob, ATTEMPTS_INTERVAL_TIME, e);
                            Utils.sleep(ATTEMPTS_INTERVAL_TIME);
                        }
                    }
                }
                LOG.debug("FINISHED download of {}", blob);
            }, downloadExecService);
            i++;
        }
        return CompletableFuture.allOf(all);
    }

    private long getRemoteBlobstoreUpdateTime() {
        try (ClientBlobStore blobStore = getClientBlobStore()) {
            try {
                return blobStore.getRemoteBlobstoreUpdateTime();
            } catch (IOException e) {
                LOG.error("Failed to get remote blobstore update time", e);
                return -1L;
            }
        }
    }

    /**
     * Downloads all blobs listed in the topology configuration for all topologies assigned to this supervisor, and creates version files
     * with a suffix. The runnable is intended to be run periodically by a timer, created elsewhere.
     */
    @VisibleForTesting
    void updateBlobs() {
        try (Timer.Context t = blobCacheUpdateDuration.time()) {
            List<CompletableFuture<?>> futures = new ArrayList<>();
            futures.add(downloadOrUpdate(topologyBlobs.values()));
            if (symlinksDisabled) {
                LOG.warn("symlinks are disabled so blobs cannot be downloaded.");
            } else {
                for (ConcurrentMap<String, LocalizedResource> map : userArchives.values()) {
                    futures.add(downloadOrUpdate(map.values()));
                }

                for (ConcurrentMap<String, LocalizedResource> map : userFiles.values()) {
                    futures.add(downloadOrUpdate(map.values()));
                }
            }
            for (CompletableFuture<?> f : futures) {
                try {
                    f.get();
                } catch (Exception e) {
                    updateBlobExceptions.mark();
                    if (Utils.exceptionCauseIsInstanceOf(TTransportException.class, e)) {
                        LOG.error("Network error while updating blobs, will retry again later", e);
                    } else if (Utils.exceptionCauseIsInstanceOf(NimbusLeaderNotFoundException.class, e)) {
                        LOG.error("Nimbus unavailable to update blobs, will retry again later", e);
                    } else {
                        LOG.error("Could not update blob, will retry again later", e);
                    }
                }
            }
        }
    }

    /**
     * Start any background threads needed.  This includes updating blobs and cleaning up unused blobs over the configured size limit.
     */
    public void start() {
        LOG.debug("Scheduling updateBlobs every {} seconds", updateBlobPeriod);
        taskExecService.scheduleWithFixedDelay(this::updateBlobs, updateBlobPeriod, updateBlobPeriod, TimeUnit.SECONDS);
        LOG.debug("Scheduling cleanup every {} millis", cacheCleanupPeriod);
        taskExecService.scheduleAtFixedRate(this::cleanup, cacheCleanupPeriod, cacheCleanupPeriod, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws InterruptedException {
        downloadExecService.shutdown();
        taskExecService.shutdown();
    }

    private List<LocalResource> getLocalResources(PortAndAssignment pna) throws IOException {
        String topologyId = pna.getToplogyId();
        Map<String, Object> topoConf = ConfigUtils.readSupervisorStormConf(conf, topologyId);

        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> blobstoreMap = (Map<String, Map<String, Object>>) topoConf.get(Config.TOPOLOGY_BLOBSTORE_MAP);

        List<LocalResource> ret = new ArrayList<>();
        if (blobstoreMap != null) {
            List<LocalResource> tmp = SupervisorUtils.blobstoreMapToLocalresources(blobstoreMap);
            if (tmp != null) {
                ret.addAll(tmp);
            }
        }

        StormTopology stormCode = ConfigUtils.readSupervisorTopology(conf, topologyId, fsOps);
        List<String> dependencies = new ArrayList<>();
        if (stormCode.is_set_dependency_jars()) {
            dependencies.addAll(stormCode.get_dependency_jars());
        }
        if (stormCode.is_set_dependency_artifacts()) {
            dependencies.addAll(stormCode.get_dependency_artifacts());
        }
        for (String dependency : dependencies) {
            ret.add(new LocalResource(dependency, false, true));
        }
        return ret;
    }

    @VisibleForTesting
    void addReferencesToBlobs(PortAndAssignment pna, BlobChangingCallback cb)
        throws IOException, KeyNotFoundException, AuthorizationException {
        List<LocalResource> localResourceList = getLocalResources(pna);
        if (!localResourceList.isEmpty()) {
            getBlobs(localResourceList, pna, cb);
        }
    }

    /**
     * Do everything needed to recover the state in the AsyncLocalizer for a running topology.
     *
     * @param currentAssignment the assignment for the topology.
     * @param port              the port the assignment is on.
     * @param cb                a callback for when the blobs are updated.  This will only be for blobs that indicate that if they change
     *                          the worker should be restarted.
     * @throws IOException on any error trying to recover the state.
     */
    public void recoverRunningTopology(final LocalAssignment currentAssignment, final int port,
                                       final BlobChangingCallback cb) throws IOException {
        final PortAndAssignment pna = new PortAndAssignmentImpl(port, currentAssignment);
        final String topologyId = pna.getToplogyId();
        LOG.info("recoverRunningTopology for {}", pna);

        LocallyCachedBlob topoJar = getTopoJar(topologyId, pna.getAssignment().get_owner());
        topoJar.addReference(pna, cb);

        LocallyCachedBlob topoCode = getTopoCode(topologyId, pna.getAssignment().get_owner());
        topoCode.addReference(pna, cb);

        LocallyCachedBlob topoConf = getTopoConf(topologyId, pna.getAssignment().get_owner());
        topoConf.addReference(pna, cb);

        CompletableFuture<Void> localResource = blobPending.computeIfAbsent(topologyId, (tid) -> ALL_DONE_FUTURE);

        try {
            addReferencesToBlobs(pna, cb);
        } catch (KeyNotFoundException | AuthorizationException e) {
            LOG.error("Could not recover all blob references for {}", pna);
        }

        LOG.debug("Recovered blobs {} {}", topologyId, localResource);
    }

    /**
     * Remove this assignment/port as blocking resources from being cleaned up.
     *
     * @param assignment the assignment the resources are for
     * @param port       the port the topology is running on
     * @throws IOException on any error
     */
    public void releaseSlotFor(LocalAssignment assignment, int port) throws IOException {

        PortAndAssignment pna = new PortAndAssignmentImpl(port, assignment);
        final String topologyId = assignment.get_topology_id();
        LOG.info("Releasing slot for {} {}", topologyId, port);

        String topoJarKey = ConfigUtils.masterStormJarKey(topologyId);
        String topoCodeKey = ConfigUtils.masterStormCodeKey(topologyId);
        String topoConfKey = ConfigUtils.masterStormConfKey(topologyId);

        LocallyCachedBlob topoJar = topologyBlobs.get(topoJarKey);
        if (topoJar != null) {
            topoJar.removeReference(pna);
        }

        LocallyCachedBlob topoCode = topologyBlobs.get(topoCodeKey);
        if (topoCode != null) {
            topoCode.removeReference(pna);
        }

        LocallyCachedBlob topoConfBlob = topologyBlobs.get(topoConfKey);
        if (topoConfBlob != null) {
            topoConfBlob.removeReference(pna);
        }

        List<LocalResource> localResources;
        try {
            // Precondition1: Base blob stormconf.ser and stormcode.ser are available
            // Precondition2: Both files have proper permission
            localResources = getLocalResources(pna);
        } catch (IOException e) {
            LOG.info("Port and assignment info: {}", pna);
            if (e instanceof FileNotFoundException) {
                localResourceFileNotFoundWhenReleasingSlot.mark();
                LOG.warn("Local base blobs are not available. ", e);
                return;
            } else {
                LOG.error("Unable to read local file. ", e);
                throw e;
            }
        }

        for (LocalResource lr : localResources) {
            removeBlobReference(lr.getBlobName(), pna, lr.shouldUncompress());
        }
    }

    // baseDir/supervisor/usercache/user1/
    @VisibleForTesting
    File getLocalUserDir(String userName) {
        return LocalizedResource.getLocalUserDir(localBaseDir, userName).toFile();
    }

    // baseDir/supervisor/usercache/user1/filecache
    @VisibleForTesting
    File getLocalUserFileCacheDir(String userName) {
        return LocalizedResource.getLocalUserFileCacheDir(localBaseDir, userName).toFile();
    }

    private void recoverLocalizedArchivesForUser(String user) throws IOException {
        for (String key : LocalizedResource.getLocalizedArchiveKeys(localBaseDir, user)) {
            getUserArchive(user, key);
        }
    }

    private void recoverLocalizedFilesForUser(String user) throws IOException {
        for (String key : LocalizedResource.getLocalizedFileKeys(localBaseDir, user)) {
            getUserFile(user, key);
        }
    }

    // Check to see if there are any existing files already localized.
    private void reconstructLocalizedResources() {
        try {
            LOG.info("Reconstruct localized resources");
            Collection<String> users = LocalizedResource.getLocalizedUsers(localBaseDir);
            if (!(users == null || users.isEmpty())) {
                for (String user : users) {
                    LOG.debug("reconstructing resources owned by {}", user);
                    recoverLocalizedFilesForUser(user);
                    recoverLocalizedArchivesForUser(user);
                }
            } else {
                LOG.debug("No left over resources found for any user");
            }
        } catch (Exception e) {
            LOG.error("ERROR reconstructing localized resources", e);
        }
    }

    // ignores invalid user/topo/key
    void removeBlobReference(String key, PortAndAssignment pna, boolean uncompress) {
        String user = pna.getOwner();
        String topo = pna.getToplogyId();
        ConcurrentMap<String, LocalizedResource> lrsrcSet = uncompress ? userArchives.get(user) : userFiles.get(user);
        if (lrsrcSet != null) {
            LocalizedResource lrsrc = lrsrcSet.get(key);
            if (lrsrc != null) {
                LOG.debug("removing blob reference to: {} for topo: {}", key, topo);
                lrsrc.removeReference(pna);
            } else {
                LOG.warn("trying to remove non-existent blob, key: " + key + " for user: " + user
                         + " topo: " + topo);
            }
        } else {
            LOG.warn("trying to remove blob for non-existent resource set for user: " + user + " key: "
                     + key + " topo: " + topo);
        }
    }

    protected ClientBlobStore getClientBlobStore() {
        return ServerUtils.getClientBlobStoreForSupervisor(conf);
    }

    /**
     * This function either returns the blobs in the existing cache or if they don't exist in the cache, it downloads them in parallel (up
     * to SUPERVISOR_BLOBSTORE_DOWNLOAD_THREAD_COUNT) and will block until all of them have been downloaded.
     */
    List<LocalizedResource> getBlobs(List<LocalResource> localResources, PortAndAssignment pna, BlobChangingCallback cb)
        throws AuthorizationException, KeyNotFoundException, IOException {
        if ((boolean) conf.getOrDefault(Config.DISABLE_SYMLINKS, false)) {
            throw new WrappedKeyNotFoundException("symlinks are disabled so blobs cannot be downloaded.");
        }
        String user = pna.getOwner();
        ArrayList<LocalizedResource> results = new ArrayList<>();
        List<CompletableFuture<?>> futures = new ArrayList<>();

        try {
            for (LocalResource localResource : localResources) {
                String key = localResource.getBlobName();
                boolean uncompress = localResource.shouldUncompress();
                LocalizedResource lrsrc = uncompress ? getUserArchive(user, key) : getUserFile(user, key);

                // go off to blobstore and get it
                // assume dir passed in exists and has correct permission
                LOG.debug("fetching blob: {}", key);
                lrsrc.addReference(pna, localResource.needsCallback() ? cb : null);
                futures.add(downloadOrUpdate(lrsrc));
                results.add(lrsrc);
            }

            for (CompletableFuture<?> futureRsrc : futures) {
                futureRsrc.get();
            }
        } catch (ExecutionException e) {
            Utils.unwrapAndThrow(AuthorizationException.class, e);
            Utils.unwrapAndThrow(KeyNotFoundException.class, e);
            throw new IOException("Error getting blobs", e);
        } catch (RejectedExecutionException re) {
            throw new IOException("RejectedExecutionException: ", re);
        } catch (InterruptedException ie) {
            throw new IOException("Interrupted Exception", ie);
        }
        return results;
    }

    private void forEachTopologyDistDir(ConsumePathAndId consumer) throws IOException {
        Path stormCodeRoot = Paths.get(ConfigUtils.supervisorStormDistRoot(conf));
        if (Files.exists(stormCodeRoot) && Files.isDirectory(stormCodeRoot)) {
            try (DirectoryStream<Path> children = Files.newDirectoryStream(stormCodeRoot)) {
                for (Path child : children) {
                    if (Files.isDirectory(child)) {
                        String topologyId = child.getFileName().toString();
                        consumer.accept(child, topologyId);
                    }
                }
            }
        }
    }

    @VisibleForTesting
    void cleanup() {
        try {
            LOG.info("Starting cleanup");
            LocalizedResourceRetentionSet toClean = new LocalizedResourceRetentionSet(cacheTargetSize);
            // need one large set of all and then clean via LRU
            for (Map.Entry<String, ConcurrentHashMap<String, LocalizedResource>> t : userArchives.entrySet()) {
                toClean.addResources(t.getValue());
                LOG.debug("Resources to be cleaned after adding {} archives : {}", t.getKey(), toClean);
            }

            for (Map.Entry<String, ConcurrentHashMap<String, LocalizedResource>> t : userFiles.entrySet()) {
                toClean.addResources(t.getValue());
                LOG.debug("Resources to be cleaned after adding {} files : {}", t.getKey(), toClean);
            }

            toClean.addResources(topologyBlobs);
            Set<String> topologiesWithDeletes = new HashSet<>();
            try (ClientBlobStore store = getClientBlobStore()) {
                Set<LocallyCachedBlob> deletedBlobs = toClean.cleanup(store);
                for (LocallyCachedBlob deletedBlob : deletedBlobs) {
                    String topologyId = ConfigUtils.getIdFromBlobKey(deletedBlob.getKey());
                    if (topologyId != null) {
                        topologiesWithDeletes.add(topologyId);
                    }
                }
            }

            HashSet<String> safeTopologyIds = new HashSet<>();
            for (String blobKey : topologyBlobs.keySet()) {
                safeTopologyIds.add(ConfigUtils.getIdFromBlobKey(blobKey));
            }
            LOG.debug("Topologies {} can no longer be considered fully downloaded", topologiesWithDeletes);
            safeTopologyIds.removeAll(topologiesWithDeletes);

            //Deleting this early does not hurt anything
            topologyBasicDownloaded.keySet().removeIf(topoId -> !safeTopologyIds.contains(topoId));
            blobPending.keySet().removeIf(topoId -> !safeTopologyIds.contains(topoId));

            try {
                forEachTopologyDistDir((p, topologyId) -> {
                    String topoJarKey = ConfigUtils.masterStormJarKey(topologyId);
                    String topoCodeKey = ConfigUtils.masterStormCodeKey(topologyId);
                    String topoConfKey = ConfigUtils.masterStormConfKey(topologyId);
                    if (!topologyBlobs.containsKey(topoJarKey)
                        && !topologyBlobs.containsKey(topoCodeKey)
                        && !topologyBlobs.containsKey(topoConfKey)) {
                        fsOps.deleteIfExists(p.toFile());
                    }
                });
            } catch (Exception e) {
                LOG.error("Could not read topology directories for cleanup", e);
            }

            LOG.debug("Resource cleanup: {}", toClean);
            Set<String> allUsers = new HashSet<>(userArchives.keySet());
            allUsers.addAll(userFiles.keySet());
            for (String user : allUsers) {
                ConcurrentMap<String, LocalizedResource> filesForUser = userFiles.get(user);
                ConcurrentMap<String, LocalizedResource> archivesForUser = userArchives.get(user);
                if ((filesForUser == null || filesForUser.size() == 0)
                        && (archivesForUser == null || archivesForUser.size() == 0)) {

                    LOG.debug("removing empty set: {}", user);
                    try {
                        LocalizedResource.completelyRemoveUnusedUser(localBaseDir, user);
                        userFiles.remove(user);
                        userArchives.remove(user);
                    } catch (IOException e) {
                        LOG.error("Error trying to delete cached user files", e);
                    }
                }
            }
        } catch (Exception ex) {
            LOG.error("AsyncLocalizer cleanup failure", ex);
        } catch (Error error) {
            LOG.error("AsyncLocalizer cleanup failure", error);
            Utils.exitProcess(20, "AsyncLocalizer cleanup failure");
        } finally {
            LOG.info("Finish cleanup");
        }
    }

    private interface ConsumePathAndId {
        void accept(Path path, String topologyId) throws IOException;
    }

    private class DownloadBlobs implements Supplier<Void> {
        private final PortAndAssignment pna;
        private final BlobChangingCallback cb;

        DownloadBlobs(PortAndAssignment pna, BlobChangingCallback cb) {
            this.pna = pna;
            this.cb = cb;
        }

        @Override
        public Void get() {
            try {
                String topologyId = pna.getToplogyId();
                String topoOwner = pna.getOwner();
                String stormroot = ConfigUtils.supervisorStormDistRoot(conf, topologyId);
                Map<String, Object> topoConf = ConfigUtils.readSupervisorStormConf(conf, topologyId);

                @SuppressWarnings("unchecked")
                Map<String, Map<String, Object>> blobstoreMap =
                    (Map<String, Map<String, Object>>) topoConf.get(Config.TOPOLOGY_BLOBSTORE_MAP);

                List<LocalResource> localResourceList = getLocalResources(pna);
                if (!localResourceList.isEmpty()) {
                    File userDir = getLocalUserFileCacheDir(topoOwner);
                    if (!fsOps.fileExists(userDir)) {
                        fsOps.forceMkdir(userDir);
                    }
                    List<LocalizedResource> localizedResources = getBlobs(localResourceList, pna, cb);
                    fsOps.setupBlobPermissions(userDir, topoOwner);
                    if (!symlinksDisabled) {
                        for (LocalizedResource localizedResource : localizedResources) {
                            String keyName = localizedResource.getKey();
                            //The sym link we are pointing to
                            File rsrcFilePath = localizedResource.getCurrentSymlinkPath().toFile();

                            String symlinkName = null;
                            if (blobstoreMap != null) {
                                Map<String, Object> blobInfo = blobstoreMap.get(keyName);
                                if (blobInfo != null && blobInfo.containsKey("localname")) {
                                    symlinkName = (String) blobInfo.get("localname");
                                } else {
                                    symlinkName = keyName;
                                }
                            } else {
                                // all things are from dependencies
                                symlinkName = keyName;
                            }
                            fsOps.createSymlink(new File(stormroot, symlinkName), rsrcFilePath);
                        }
                    }
                }

                pna.complete();
                return null;
            } catch (Exception e) {
                LOG.warn("Caught Exception While Downloading (rethrowing)... ", e);
                throw new RuntimeException(e);
            }
        }
    }
}
