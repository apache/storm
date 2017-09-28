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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.daemon.supervisor.AdvancedFSOps;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.streams.Pair;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.NimbusLeaderNotFoundException;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ServerConfigUtils;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.ShellUtils;
import org.apache.storm.utils.Utils;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

/**
 * Downloads and caches blobs locally.
 */
public class AsyncLocalizer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncLocalizer.class);
    public static final String FILECACHE = "filecache";
    public static final String USERCACHE = "usercache";
    // sub directories to store either files or uncompressed archives respectively
    public static final String FILESDIR = "files";
    public static final String ARCHIVESDIR = "archives";

    private static final String TO_UNCOMPRESS = "_tmp_";
    private static final CompletableFuture<Void> ALL_DONE_FUTURE = new CompletableFuture<>();
    static {
        ALL_DONE_FUTURE.complete(null);
    }

    private static Set<String> readDownloadedTopologyIds(Map<String, Object> conf) throws IOException {
        Set<String> stormIds = new HashSet<>();
        String path = ConfigUtils.supervisorStormDistRoot(conf);
        Collection<String> rets = ConfigUtils.readDirContents(path);
        for (String ret : rets) {
            stormIds.add(URLDecoder.decode(ret, "UTF-8"));
        }
        return stormIds;
    }

    private final AtomicReference<Map<Long, LocalAssignment>> currAssignment;
    private final boolean isLocalMode;
    private final Map<String, LocalDownloadedResource> basicPending;
    private final Map<String, LocalDownloadedResource> blobPending;
    private final Map<String, Object> conf;
    private final AdvancedFSOps fsOps;
    private final boolean symlinksDisabled;

    // track resources - user to resourceSet
    private final ConcurrentMap<String, LocalizedResourceSet> userRsrc = new ConcurrentHashMap<>();

    private final String localBaseDir;

    private final int blobDownloadRetries;
    private final ScheduledExecutorService execService;

    // cleanup
    private long cacheTargetSize;
    private long cacheCleanupPeriod;


    public AsyncLocalizer(Map<String, Object> conf, AtomicReference<Map<Long, LocalAssignment>> currAssignment,
                          Map<Integer, LocalAssignment> portToAssignments) throws IOException {
        this(conf, ConfigUtils.supervisorLocalDir(conf), AdvancedFSOps.make(conf), currAssignment, portToAssignments);
    }
    
    @VisibleForTesting
    AsyncLocalizer(Map<String, Object> conf, String baseDir, AdvancedFSOps ops,
                   AtomicReference<Map<Long, LocalAssignment>> currAssignment,
                   Map<Integer, LocalAssignment> portToAssignments) throws IOException {

        this.conf = conf;
        isLocalMode = ConfigUtils.isLocalMode(conf);
        fsOps = ops;
        localBaseDir = baseDir;
        // default cache size 10GB, converted to Bytes
        cacheTargetSize = ObjectReader.getInt(conf.get(DaemonConfig.SUPERVISOR_LOCALIZER_CACHE_TARGET_SIZE_MB),
            10 * 1024).longValue() << 20;
        // default 30 seconds.
        cacheCleanupPeriod = ObjectReader.getInt(conf.get(
            DaemonConfig.SUPERVISOR_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS), 30 * 1000).longValue();

        // if we needed we could make config for update thread pool size
        int threadPoolSize = ObjectReader.getInt(conf.get(DaemonConfig.SUPERVISOR_BLOBSTORE_DOWNLOAD_THREAD_COUNT), 5);
        blobDownloadRetries = ObjectReader.getInt(conf.get(
            DaemonConfig.SUPERVISOR_BLOBSTORE_DOWNLOAD_MAX_RETRIES), 3);

        execService = Executors.newScheduledThreadPool(threadPoolSize,
            new ThreadFactoryBuilder().setNameFormat("AsyncLocalizer Executor").build());
        reconstructLocalizedResources();

        symlinksDisabled = (boolean)conf.getOrDefault(Config.DISABLE_SYMLINKS, false);
        basicPending = new HashMap<>();
        blobPending = new HashMap<>();
        this.currAssignment = currAssignment;

        recoverBlobReferences(portToAssignments);
    }

    /**
     * For each of the downloaded topologies, adds references to the blobs that the topologies are using. This is used to reconstruct the
     * cache on restart.
     * @param topoId the topology id
     * @param user the User that owns this topology
     */
    private void addBlobReferences(String topoId, String user) throws IOException {
        Map<String, Object> topoConf = ConfigUtils.readSupervisorStormConf(conf, topoId);
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> blobstoreMap = (Map<String, Map<String, Object>>) topoConf.get(Config.TOPOLOGY_BLOBSTORE_MAP);
        String topoName = (String) topoConf.get(Config.TOPOLOGY_NAME);
        List<LocalResource> localresources = SupervisorUtils.blobstoreMapToLocalresources(blobstoreMap);
        if (blobstoreMap != null) {
            addReferences(localresources, user, topoName);
        }
    }

    /**
     * Pick up where we left off last time.
     * @param portToAssignments the current set of assignments for the supervisor.
     */
    private void recoverBlobReferences(Map<Integer, LocalAssignment> portToAssignments) throws IOException {
        Set<String> downloadedTopoIds = readDownloadedTopologyIds(conf);
        if (portToAssignments != null) {
            Map<String, LocalAssignment> assignments = new HashMap<>();
            for (LocalAssignment la : portToAssignments.values()) {
                assignments.put(la.get_topology_id(), la);
            }
            for (String topoId : downloadedTopoIds) {
                LocalAssignment la = assignments.get(topoId);
                if (la != null) {
                    addBlobReferences(topoId, la.get_owner());
                } else {
                    LOG.warn("Could not find an owner for topo {}", topoId);
                }
            }
        }
    }

    /**
     * Downloads all blobs listed in the topology configuration for all topologies assigned to this supervisor, and creates version files
     * with a suffix. The runnable is intended to be run periodically by a timer, created elsewhere.
     */
    private void updateBlobs() {
        try {
            Map<String, String> topoIdToOwner = currAssignment.get().values().stream()
                .map((la) -> Pair.of(la.get_topology_id(), la.get_owner()))
                .distinct()
                .collect(Collectors.toMap((p) -> p.getFirst(), (p) -> p.getSecond()));
            for (String topoId : readDownloadedTopologyIds(conf)) {
                String owner = topoIdToOwner.get(topoId);
                if (owner == null) {
                    //We got a case where the local assignment is not up to date, no point in going on...
                    LOG.warn("The blobs will not be updated for {} until the local assignment is updated...", topoId);
                } else {
                    String stormRoot = ConfigUtils.supervisorStormDistRoot(conf, topoId);
                    LOG.debug("Checking Blob updates for storm topology id {} With target_dir: {}", topoId, stormRoot);
                    updateBlobsForTopology(conf, topoId, owner);
                }
            }
        } catch (Exception e) {
            if (Utils.exceptionCauseIsInstanceOf(TTransportException.class, e)) {
                LOG.error("Network error while updating blobs, will retry again later", e);
            } else if (Utils.exceptionCauseIsInstanceOf(NimbusLeaderNotFoundException.class, e)) {
                LOG.error("Nimbus unavailable to update blobs, will retry again later", e);
            } else {
                throw Utils.wrapInRuntime(e);
            }
        }
    }

    /**
     * Update each blob listed in the topology configuration if the latest version of the blob has not been downloaded.
     */
    private void updateBlobsForTopology(Map<String, Object> conf, String stormId, String user)
        throws IOException {
        Map<String, Object> topoConf = ConfigUtils.readSupervisorStormConf(conf, stormId);
        Map<String, Map<String, Object>> blobstoreMap = (Map<String, Map<String, Object>>) topoConf.get(Config.TOPOLOGY_BLOBSTORE_MAP);
        List<LocalResource> localresources = SupervisorUtils.blobstoreMapToLocalresources(blobstoreMap);
        try {
            updateBlobs(localresources, user);
        } catch (AuthorizationException authExp) {
            LOG.error("AuthorizationException error", authExp);
        } catch (KeyNotFoundException knf) {
            LOG.error("KeyNotFoundException error", knf);
        }
    }

    /**
     * Start any background threads needed.  This includes updating blobs and cleaning up
     * unused blobs over the configured size limit.
     */
    public void start() {
        execService.scheduleWithFixedDelay(this::updateBlobs, 30, 30, TimeUnit.SECONDS);
        execService.scheduleAtFixedRate(this::cleanup, cacheCleanupPeriod, cacheCleanupPeriod, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws InterruptedException {
        if (execService != null) {
            execService.shutdown();
        }
    }

    //ILocalizer
    private class DownloadBaseBlobsDistributed implements Supplier<Void> {
        protected final String topologyId;
        protected final File stormRoot;
        protected final LocalAssignment assignment;
        protected final String owner;

        public DownloadBaseBlobsDistributed(String topologyId, LocalAssignment assignment) throws IOException {
            this.topologyId = topologyId;
            stormRoot = new File(ConfigUtils.supervisorStormDistRoot(conf, this.topologyId));
            this.assignment = assignment;
            owner = assignment.get_owner();
        }

        protected void downloadBaseBlobs(File tmproot) throws Exception {
            String stormJarKey = ConfigUtils.masterStormJarKey(topologyId);
            String stormCodeKey = ConfigUtils.masterStormCodeKey(topologyId);
            String topoConfKey = ConfigUtils.masterStormConfKey(topologyId);
            String jarPath = ConfigUtils.supervisorStormJarPath(tmproot.getAbsolutePath());
            String codePath = ConfigUtils.supervisorStormCodePath(tmproot.getAbsolutePath());
            String confPath = ConfigUtils.supervisorStormConfPath(tmproot.getAbsolutePath());
            fsOps.forceMkdir(tmproot);
            fsOps.restrictDirectoryPermissions(tmproot);
            ClientBlobStore blobStore = ServerUtils.getClientBlobStoreForSupervisor(conf);
            try {
                ServerUtils.downloadResourcesAsSupervisor(stormJarKey, jarPath, blobStore);
                ServerUtils.downloadResourcesAsSupervisor(stormCodeKey, codePath, blobStore);
                ServerUtils.downloadResourcesAsSupervisor(topoConfKey, confPath, blobStore);
            } finally {
                blobStore.shutdown();
            }
            ServerUtils.extractDirFromJar(jarPath, ServerConfigUtils.RESOURCES_SUBDIR, tmproot);
        }

        @Override
        public Void get() {
            try {
                if (fsOps.fileExists(stormRoot)) {
                    if (!fsOps.supportsAtomicDirectoryMove()) {
                        LOG.warn("{} may have partially downloaded blobs, recovering", topologyId);
                        fsOps.deleteIfExists(stormRoot);
                    } else {
                        LOG.warn("{} already downloaded blobs, skipping", topologyId);
                        return null;
                    }
                }
                boolean deleteAll = true;
                String tmproot = ServerConfigUtils.supervisorTmpDir(conf) + Utils.FILE_PATH_SEPARATOR + Utils.uuid();
                File tr = new File(tmproot);
                try {
                    downloadBaseBlobs(tr);
                    if (assignment.is_set_total_node_shared()) {
                        File sharedMemoryDirTmpLocation = new File(tr, "shared_by_topology");
                        //We need to create a directory for shared memory to write to (we should not encourage this though)
                        Path path = sharedMemoryDirTmpLocation.toPath();
                        Files.createDirectories(path);
                    }
                    fsOps.moveDirectoryPreferAtomic(tr, stormRoot);
                    fsOps.setupStormCodeDir(owner, stormRoot);
                    if (assignment.is_set_total_node_shared()) {
                        File sharedMemoryDir = new File(stormRoot, "shared_by_topology");
                        fsOps.setupWorkerArtifactsDir(owner, sharedMemoryDir);
                    }
                    deleteAll = false;
                } finally {
                    if (deleteAll) {
                        LOG.warn("Failed to download basic resources for topology-id {}", topologyId);
                        fsOps.deleteIfExists(tr);
                        fsOps.deleteIfExists(stormRoot);
                    }
                }
                return null;
            } catch (Exception e) {
                LOG.warn("Caught Exception While Downloading (rethrowing)... ", e);
                throw new RuntimeException(e);
            }
        }
    }

    private class DownloadBaseBlobsLocal extends DownloadBaseBlobsDistributed {

        public DownloadBaseBlobsLocal(String topologyId, LocalAssignment assignment) throws IOException {
            super(topologyId, assignment);
        }

        @Override
        protected void downloadBaseBlobs(File tmproot) throws Exception {
            fsOps.forceMkdir(tmproot);
            String stormCodeKey = ConfigUtils.masterStormCodeKey(topologyId);
            String topoConfKey = ConfigUtils.masterStormConfKey(topologyId);
            File codePath = new File(ConfigUtils.supervisorStormCodePath(tmproot.getAbsolutePath()));
            File confPath = new File(ConfigUtils.supervisorStormConfPath(tmproot.getAbsolutePath()));
            BlobStore blobStore = ServerUtils.getNimbusBlobStore(conf, null);
            try {
                try (OutputStream codeOutStream = fsOps.getOutputStream(codePath)){
                    blobStore.readBlobTo(stormCodeKey, codeOutStream, null);
                }
                try (OutputStream confOutStream = fsOps.getOutputStream(confPath)) {
                    blobStore.readBlobTo(topoConfKey, confOutStream, null);
                }
            } finally {
                blobStore.shutdown();
            }

            ClassLoader classloader = Thread.currentThread().getContextClassLoader();
            String resourcesJar = resourcesJar();
            URL url = classloader.getResource(ServerConfigUtils.RESOURCES_SUBDIR);

            String targetDir = tmproot + Utils.FILE_PATH_SEPARATOR;
            if (resourcesJar != null) {
                LOG.info("Extracting resources from jar at {} to {}", resourcesJar, targetDir);
                ServerUtils.extractDirFromJar(resourcesJar, ServerConfigUtils.RESOURCES_SUBDIR, new File(targetDir));
            } else if (url != null) {
                LOG.info("Copying resources at {} to {}", url, targetDir);
                if ("jar".equals(url.getProtocol())) {
                    JarURLConnection urlConnection = (JarURLConnection) url.openConnection();
                    ServerUtils.extractDirFromJar(urlConnection.getJarFileURL().getFile(), ServerConfigUtils.RESOURCES_SUBDIR, new File(targetDir));
                } else {
                    fsOps.copyDirectory(new File(url.getFile()), new File(targetDir, ConfigUtils.RESOURCES_SUBDIR));
                }
            }
        }
    }

    private class DownloadBlobs implements Supplier<Void> {
        private final String topologyId;
        private final String topoOwner;

        public DownloadBlobs(String topologyId, String topoOwner) {
            this.topologyId = topologyId;
            this.topoOwner = topoOwner;
        }

        @Override
        public Void get() {
            try {
                String stormroot = ConfigUtils.supervisorStormDistRoot(conf, topologyId);
                Map<String, Object> topoConf = ConfigUtils.readSupervisorStormConf(conf, topologyId);

                @SuppressWarnings("unchecked")
                Map<String, Map<String, Object>> blobstoreMap = (Map<String, Map<String, Object>>) topoConf.get(Config.TOPOLOGY_BLOBSTORE_MAP);
                String topoName = (String) topoConf.get(Config.TOPOLOGY_NAME);

                List<LocalResource> localResourceList = new ArrayList<>();
                if (blobstoreMap != null) {
                    List<LocalResource> tmp = SupervisorUtils.blobstoreMapToLocalresources(blobstoreMap);
                    if (tmp != null) {
                        localResourceList.addAll(tmp);
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
                    localResourceList.add(new LocalResource(dependency, false));
                }

                if (!localResourceList.isEmpty()) {
                    File userDir = getLocalUserFileCacheDir(topoOwner);
                    if (!fsOps.fileExists(userDir)) {
                        fsOps.forceMkdir(userDir);
                    }
                    List<LocalizedResource> localizedResources = getBlobs(localResourceList, topoOwner, topoName, userDir);
                    fsOps.setupBlobPermissions(userDir, topoOwner);
                    if (!symlinksDisabled) {
                        for (LocalizedResource localizedResource : localizedResources) {
                            String keyName = localizedResource.getKey();
                            //The sym link we are pointing to
                            File rsrcFilePath = new File(localizedResource.getCurrentSymlinkPath());

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

                return null;
            } catch (Exception e) {
                LOG.warn("Caught Exception While Downloading (rethrowing)... ", e);
                throw new RuntimeException(e);
            }
        }
    }

    public synchronized CompletableFuture<Void> requestDownloadBaseTopologyBlobs(final LocalAssignment assignment, final int port) throws IOException {
        final String topologyId = assignment.get_topology_id();
        LocalDownloadedResource localResource = basicPending.get(topologyId);
        if (localResource == null) {
            Supplier<Void> supplier;
            if (isLocalMode) {
                supplier = new DownloadBaseBlobsLocal(topologyId, assignment);
            } else {
                supplier = new DownloadBaseBlobsDistributed(topologyId, assignment);
            }
            localResource = new LocalDownloadedResource(CompletableFuture.supplyAsync(supplier, execService));
            basicPending.put(topologyId, localResource);
        }
        CompletableFuture<Void> ret = localResource.reserve(port, assignment);
        LOG.debug("Reserved basic {} {}", topologyId, localResource);
        return ret;
    }

    private static String resourcesJar() throws IOException {
        String path = ServerUtils.currentClasspath();
        if (path == null) {
            return null;
        }

        for (String jpath : path.split(File.pathSeparator)) {
            if (jpath.endsWith(".jar")) {
                if (ServerUtils.zipDoesContainDir(jpath, ServerConfigUtils.RESOURCES_SUBDIR)) {
                    return jpath;
                }
            }
        }
        return null;
    }

    public synchronized void recoverRunningTopology(LocalAssignment assignment, int port) {
        final String topologyId = assignment.get_topology_id();
        LocalDownloadedResource localResource = basicPending.get(topologyId);
        if (localResource == null) {
            localResource = new LocalDownloadedResource(ALL_DONE_FUTURE);
            basicPending.put(topologyId, localResource);
        }
        localResource.reserve(port, assignment);
        LOG.debug("Recovered basic {} {}", topologyId, localResource);

        localResource = blobPending.get(topologyId);
        if (localResource == null) {
            localResource = new LocalDownloadedResource(ALL_DONE_FUTURE);
            blobPending.put(topologyId, localResource);
        }
        localResource.reserve(port, assignment);
        LOG.debug("Recovered blobs {} {}", topologyId, localResource);
    }

    public synchronized CompletableFuture<Void> requestDownloadTopologyBlobs(LocalAssignment assignment, int port) {
        final String topologyId = assignment.get_topology_id();
        LocalDownloadedResource localResource = blobPending.get(topologyId);
        if (localResource == null) {
            Supplier<Void> supplier = new DownloadBlobs(topologyId, assignment.get_owner());
            localResource = new LocalDownloadedResource(CompletableFuture.supplyAsync(supplier, execService));
            blobPending.put(topologyId, localResource);
        }
        CompletableFuture<Void> ret = localResource.reserve(port, assignment);
        LOG.debug("Reserved blobs {} {}", topologyId, localResource);
        return ret;
    }

    /**
     * Remove this assignment/port as blocking resources from being cleaned up.
     *
     * @param assignment the assignment the resources are for
     * @param port the port the topology is running on
     * @throws IOException on any error
     */
    public synchronized void releaseSlotFor(LocalAssignment assignment, int port) throws IOException {
        final String topologyId = assignment.get_topology_id();
        LOG.debug("Releasing slot for {} {}", topologyId, port);
        LocalDownloadedResource localResource = blobPending.get(topologyId);
        if (localResource == null || !localResource.release(port, assignment)) {
            LOG.warn("Released blob reference {} {} for something that we didn't have {}", topologyId, port, localResource);
        } else if (localResource.isDone()){
            LOG.info("Released blob reference {} {} Cleaning up BLOB references...", topologyId, port);
            blobPending.remove(topologyId);
            Map<String, Object> topoConf = ConfigUtils.readSupervisorStormConf(conf, topologyId);
            @SuppressWarnings("unchecked")
            Map<String, Map<String, Object>> blobstoreMap = (Map<String, Map<String, Object>>) topoConf.get(Config.TOPOLOGY_BLOBSTORE_MAP);
            if (blobstoreMap != null) {
                String user = assignment.get_owner();
                String topoName = (String) topoConf.get(Config.TOPOLOGY_NAME);

                for (Map.Entry<String, Map<String, Object>> entry : blobstoreMap.entrySet()) {
                    String key = entry.getKey();
                    Map<String, Object> blobInfo = entry.getValue();
                    try {
                        removeBlobReference(key, user, topoName, SupervisorUtils.shouldUncompressBlob(blobInfo));
                    } catch (Exception e) {
                        throw new IOException(e);
                    }
                }
            }
        } else {
            LOG.debug("Released blob reference {} {} still waiting on {}", topologyId, port, localResource);
        }

        localResource = basicPending.get(topologyId);
        if (localResource == null || !localResource.release(port, assignment)) {
            LOG.warn("Released basic reference {} {} for something that we didn't have {}", topologyId, port, localResource);
        } else if (localResource.isDone()){
            LOG.info("Released blob reference {} {} Cleaning up basic files...", topologyId, port);
            basicPending.remove(topologyId);
            String path = ConfigUtils.supervisorStormDistRoot(conf, topologyId);
            fsOps.deleteIfExists(new File(path), null, "rmr "+topologyId);
        } else {
            LOG.debug("Released basic reference {} {} still waiting on {}", topologyId, port, localResource);
        }
    }

    public synchronized void cleanupUnusedTopologies() throws IOException {
        File distRoot = new File(ConfigUtils.supervisorStormDistRoot(conf));
        LOG.info("Cleaning up unused topologies in {}", distRoot);
        File[] children = distRoot.listFiles();
        if (children != null) {
            for (File topoDir : children) {
                String topoId = URLDecoder.decode(topoDir.getName(), "UTF-8");
                if (basicPending.get(topoId) == null && blobPending.get(topoId) == null) {
                    fsOps.deleteIfExists(topoDir, null, "rmr " + topoId);
                }
            }
        }
    }

    //From Localizer

    // For testing, it allows setting size in bytes
    protected void setTargetCacheSize(long size) {
        cacheTargetSize = size;
    }

    // For testing, be careful as it doesn't clone
    ConcurrentMap<String, LocalizedResourceSet> getUserResources() {
        return userRsrc;
    }

    // baseDir/supervisor/usercache/
    protected File getUserCacheDir() {
        return new File(localBaseDir, USERCACHE);
    }

    // baseDir/supervisor/usercache/user1/
    protected File getLocalUserDir(String userName) {
        return new File(getUserCacheDir(), userName);
    }

    // baseDir/supervisor/usercache/user1/filecache
    public File getLocalUserFileCacheDir(String userName) {
        return new File(getLocalUserDir(userName), FILECACHE);
    }

    // baseDir/supervisor/usercache/user1/filecache/files
    protected File getCacheDirForFiles(File dir) {
        return new File(dir, FILESDIR);
    }

    // get the directory to put uncompressed archives in
    // baseDir/supervisor/usercache/user1/filecache/archives
    protected File getCacheDirForArchives(File dir) {
        return new File(dir, ARCHIVESDIR);
    }

    protected void addLocalizedResourceInDir(String dir, LocalizedResourceSet lrsrcSet,
                                             boolean uncompress) {
        File[] lrsrcs = readCurrentBlobs(dir);

        if (lrsrcs != null) {
            for (File rsrc : lrsrcs) {
                LOG.info("add localized in dir found: " + rsrc);
                /// strip off .suffix
                String path = rsrc.getPath();
                int p = path.lastIndexOf('.');
                if (p > 0) {
                    path = path.substring(0, p);
                }
                LOG.debug("local file is: {} path is: {}", rsrc.getPath(), path);
                LocalizedResource lrsrc = new LocalizedResource(new File(path).getName(), path,
                    uncompress);
                lrsrcSet.add(lrsrc.getKey(), lrsrc, uncompress);
            }
        }
    }

    // Looks for files in the directory with .current suffix
    protected File[] readCurrentBlobs(String location) {
        File dir = new File(location);
        File[] files = null;
        if (dir.exists()) {
            files = dir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.toLowerCase().endsWith(ServerUtils.DEFAULT_CURRENT_BLOB_SUFFIX);
                }
            });
        }
        return files;
    }

    // Check to see if there are any existing files already localized.
    protected void reconstructLocalizedResources() {
        try {
            LOG.info("Reconstruct localized resource: " + getUserCacheDir().getPath());
            Collection<File> users = ConfigUtils.readDirFiles(getUserCacheDir().getPath());
            if (!(users == null || users.isEmpty())) {
                for (File userDir : users) {
                    String user = userDir.getName();
                    LOG.debug("looking in: {} for user: {}", userDir.getPath(), user);
                    LocalizedResourceSet newSet = new LocalizedResourceSet(user);
                    LocalizedResourceSet lrsrcSet = userRsrc.putIfAbsent(user, newSet);
                    if (lrsrcSet == null) {
                        lrsrcSet = newSet;
                    }
                    addLocalizedResourceInDir(getCacheDirForFiles(getLocalUserFileCacheDir(user)).getPath(),
                        lrsrcSet, false);
                    addLocalizedResourceInDir(
                        getCacheDirForArchives(getLocalUserFileCacheDir(user)).getPath(),
                        lrsrcSet, true);
                }
            } else {
                LOG.warn("No left over resources found for any user during reconstructing of local resources at: {}", getUserCacheDir().getPath());
            }
        } catch (Exception e) {
            LOG.error("ERROR reconstructing localized resources", e);
        }
    }

    // ignores invalid user/topo/key
    public synchronized void removeBlobReference(String key, String user, String topo,
                                                 boolean uncompress) throws AuthorizationException, KeyNotFoundException {
        LocalizedResourceSet lrsrcSet = userRsrc.get(user);
        if (lrsrcSet != null) {
            LocalizedResource lrsrc = lrsrcSet.get(key, uncompress);
            if (lrsrc != null) {
                LOG.debug("removing blob reference to: {} for topo: {}", key, topo);
                lrsrc.removeReference(topo);
            } else {
                LOG.warn("trying to remove non-existent blob, key: " + key + " for user: " + user +
                    " topo: " + topo);
            }
        } else {
            LOG.warn("trying to remove blob for non-existent resource set for user: " + user + " key: "
                + key + " topo: " + topo);
        }
    }

    public synchronized void addReferences(List<LocalResource> localresource, String user,
                                           String topo) {
        LocalizedResourceSet lrsrcSet = userRsrc.get(user);
        if (lrsrcSet != null) {
            for (LocalResource blob : localresource) {
                LocalizedResource lrsrc = lrsrcSet.get(blob.getBlobName(), blob.shouldUncompress());
                if (lrsrc != null) {
                    lrsrc.addReference(topo);
                    LOG.debug("added reference for topo: {} key: {}", topo, blob);
                } else {
                    LOG.warn("trying to add reference to non-existent blob, key: " + blob + " topo: " + topo);
                }
            }
        } else {
            LOG.warn("trying to add reference to non-existent local resource set, " +
                "user: " + user + " topo: " + topo);
        }
    }

    /**
     * This function either returns the blob in the existing cache or if it doesn't exist in the
     * cache, it will download the blob and will block until the download is complete.
     */
    public LocalizedResource getBlob(LocalResource localResource, String user, String topo,
                                     File userFileDir) throws AuthorizationException, KeyNotFoundException, IOException {
        ArrayList<LocalResource> arr = new ArrayList<LocalResource>();
        arr.add(localResource);
        List<LocalizedResource> results = getBlobs(arr, user, topo, userFileDir);
        if (results.isEmpty() || results.size() != 1) {
            throw new IOException("Unknown error getting blob: " + localResource + ", for user: " + user +
                ", topo: " + topo);
        }
        return results.get(0);
    }

    protected boolean isLocalizedResourceDownloaded(LocalizedResource lrsrc) {
        File rsrcFileCurrent = new File(lrsrc.getCurrentSymlinkPath());
        File rsrcFileWithVersion = new File(lrsrc.getFilePathWithVersion());
        File versionFile = new File(lrsrc.getVersionFilePath());
        return (rsrcFileWithVersion.exists() && rsrcFileCurrent.exists() && versionFile.exists());
    }

    protected boolean isLocalizedResourceUpToDate(LocalizedResource lrsrc,
                                                  ClientBlobStore blobstore) throws AuthorizationException, KeyNotFoundException {
        String localFile = lrsrc.getFilePath();
        long nimbusBlobVersion = ServerUtils.nimbusVersionOfBlob(lrsrc.getKey(), blobstore);
        long currentBlobVersion = ServerUtils.localVersionOfBlob(localFile);
        return (nimbusBlobVersion == currentBlobVersion);
    }

    protected ClientBlobStore getClientBlobStore() {
        return ServerUtils.getClientBlobStoreForSupervisor(conf);
    }

    /**
     * This function updates blobs on the supervisor. It uses a separate thread pool and runs
     * asynchronously of the download and delete.
     */
    public List<LocalizedResource> updateBlobs(List<LocalResource> localResources,
                                               String user) throws AuthorizationException, KeyNotFoundException, IOException {
        LocalizedResourceSet lrsrcSet = userRsrc.get(user);
        ArrayList<LocalizedResource> results = new ArrayList<>();
        ArrayList<Callable<LocalizedResource>> updates = new ArrayList<>();

        if (lrsrcSet == null) {
            // resource set must have been removed
            return results;
        }
        ClientBlobStore blobstore = null;
        try {
            blobstore = getClientBlobStore();
            for (LocalResource localResource: localResources) {
                String key = localResource.getBlobName();
                LocalizedResource lrsrc = lrsrcSet.get(key, localResource.shouldUncompress());
                if (lrsrc == null) {
                    LOG.warn("blob requested for update doesn't exist: {}", key);
                    continue;
                } else if ((boolean) conf.getOrDefault(Config.DISABLE_SYMLINKS, false)) {
                    LOG.warn("symlinks are disabled so blobs cannot be downloaded.");
                    continue;
                } else {
                    // update it if either the version isn't the latest or if any local blob files are missing
                    if (!isLocalizedResourceUpToDate(lrsrc, blobstore) ||
                        !isLocalizedResourceDownloaded(lrsrc)) {
                        LOG.debug("updating blob: {}", key);
                        updates.add(new DownloadBlob(this, conf, key, new File(lrsrc.getFilePath()), user,
                            lrsrc.isUncompressed(), true));
                    }
                }
            }
        } finally {
            if(blobstore != null) {
                blobstore.shutdown();
            }
        }
        try {
            List<Future<LocalizedResource>> futures = execService.invokeAll(updates);
            for (Future<LocalizedResource> futureRsrc : futures) {
                try {
                    LocalizedResource lrsrc = futureRsrc.get();
                    // put the resource just in case it was removed at same time by the cleaner
                    LocalizedResourceSet newSet = new LocalizedResourceSet(user);
                    LocalizedResourceSet newlrsrcSet = userRsrc.putIfAbsent(user, newSet);
                    if (newlrsrcSet == null) {
                        newlrsrcSet = newSet;
                    }
                    newlrsrcSet.putIfAbsent(lrsrc.getKey(), lrsrc, lrsrc.isUncompressed());
                    results.add(lrsrc);
                }
                catch (ExecutionException e) {
                    LOG.error("Error updating blob: ", e);
                    if (e.getCause() instanceof AuthorizationException) {
                        throw (AuthorizationException)e.getCause();
                    }
                    if (e.getCause() instanceof KeyNotFoundException) {
                        throw (KeyNotFoundException)e.getCause();
                    }
                }
            }
        } catch (RejectedExecutionException re) {
            LOG.error("Error updating blobs : ", re);
        } catch (InterruptedException ie) {
            throw new IOException("Interrupted Exception", ie);
        }
        return results;
    }

    /**
     * This function either returns the blobs in the existing cache or if they don't exist in the
     * cache, it downloads them in parallel (up to SUPERVISOR_BLOBSTORE_DOWNLOAD_THREAD_COUNT)
     * and will block until all of them have been downloaded
     */
    public synchronized List<LocalizedResource> getBlobs(List<LocalResource> localResources,
                                                         String user, String topo, File userFileDir)
        throws AuthorizationException, KeyNotFoundException, IOException {
        if ((boolean) conf.getOrDefault(Config.DISABLE_SYMLINKS, false)) {
            throw new KeyNotFoundException("symlinks are disabled so blobs cannot be downloaded.");
        }
        LocalizedResourceSet newSet = new LocalizedResourceSet(user);
        LocalizedResourceSet lrsrcSet = userRsrc.putIfAbsent(user, newSet);
        if (lrsrcSet == null) {
            lrsrcSet = newSet;
        }
        ArrayList<LocalizedResource> results = new ArrayList<>();
        ArrayList<Callable<LocalizedResource>> downloads = new ArrayList<>();

        ClientBlobStore blobstore = null;
        try {
            blobstore = getClientBlobStore();
            for (LocalResource localResource: localResources) {
                String key = localResource.getBlobName();
                boolean uncompress = localResource.shouldUncompress();
                LocalizedResource lrsrc = lrsrcSet.get(key, localResource.shouldUncompress());
                boolean isUpdate = false;
                if ((lrsrc != null) && (lrsrc.isUncompressed() == localResource.shouldUncompress()) &&
                    (isLocalizedResourceDownloaded(lrsrc))) {
                    if (isLocalizedResourceUpToDate(lrsrc, blobstore)) {
                        LOG.debug("blob already exists: {}", key);
                        lrsrc.addReference(topo);
                        results.add(lrsrc);
                        continue;
                    }
                    LOG.debug("blob exists but isn't up to date: {}", key);
                    isUpdate = true;
                }

                // go off to blobstore and get it
                // assume dir passed in exists and has correct permission
                LOG.debug("fetching blob: {}", key);
                File downloadDir = getCacheDirForFiles(userFileDir);
                File localFile = new File(downloadDir, key);
                if (uncompress) {
                    // for compressed file, download to archives dir
                    downloadDir = getCacheDirForArchives(userFileDir);
                    localFile = new File(downloadDir, key);
                }
                downloadDir.mkdir();
                downloads.add(new DownloadBlob(this, conf, key, localFile, user, uncompress,
                    isUpdate));
            }
        } finally {
            if(blobstore !=null) {
                blobstore.shutdown();
            }
        }
        try {
            List<Future<LocalizedResource>> futures = execService.invokeAll(downloads);
            for (Future<LocalizedResource> futureRsrc: futures) {
                LocalizedResource lrsrc = futureRsrc.get();
                lrsrc.addReference(topo);
                lrsrcSet.add(lrsrc.getKey(), lrsrc, lrsrc.isUncompressed());
                results.add(lrsrc);
            }
        } catch (ExecutionException e) {
            if (e.getCause() instanceof AuthorizationException)
                throw (AuthorizationException)e.getCause();
            else if (e.getCause() instanceof KeyNotFoundException) {
                throw (KeyNotFoundException)e.getCause();
            } else {
                throw new IOException("Error getting blobs", e);
            }
        } catch (RejectedExecutionException re) {
            throw new IOException("RejectedExecutionException: ", re);
        } catch (InterruptedException ie) {
            throw new IOException("Interrupted Exception", ie);
        }
        return results;
    }

    static class DownloadBlob implements Callable<LocalizedResource> {

        private AsyncLocalizer localizer;
        private Map conf;
        private String key;
        private File localFile;
        private String user;
        private boolean uncompress;
        private boolean isUpdate;

        public DownloadBlob(AsyncLocalizer localizer, Map<String, Object> conf, String key, File localFile,
                            String user, boolean uncompress, boolean update) {
            this.localizer = localizer;
            this.conf = conf;
            this.key = key;
            this.localFile = localFile;
            this.user = user;
            this.uncompress = uncompress;
            isUpdate = update;
        }

        @Override
        public LocalizedResource call()
            throws AuthorizationException, KeyNotFoundException, IOException  {
            return localizer.downloadBlob(conf, key, localFile, user, uncompress,
                isUpdate);
        }
    }

    private LocalizedResource downloadBlob(Map<String, Object> conf, String key, File localFile,
                                           String user, boolean uncompress, boolean isUpdate)
        throws AuthorizationException, KeyNotFoundException, IOException {
        ClientBlobStore blobstore = null;
        try {
            blobstore = getClientBlobStore();
            long nimbusBlobVersion = ServerUtils.nimbusVersionOfBlob(key, blobstore);
            long oldVersion = ServerUtils.localVersionOfBlob(localFile.toString());
            FileOutputStream out = null;
            PrintWriter writer = null;
            int numTries = 0;
            String localizedPath = localFile.toString();
            String localFileWithVersion = ServerUtils.constructBlobWithVersionFileName(localFile.toString(),
                nimbusBlobVersion);
            String localVersionFile = ServerUtils.constructVersionFileName(localFile.toString());
            String downloadFile = localFileWithVersion;
            if (uncompress) {
                // we need to download to temp file and then unpack into the one requested
                downloadFile = new File(localFile.getParent(), TO_UNCOMPRESS + localFile.getName()).toString();
            }
            while (numTries < blobDownloadRetries) {
                out = new FileOutputStream(downloadFile);
                numTries++;
                try {
                    if (!ServerUtils.canUserReadBlob(blobstore.getBlobMeta(key), user, conf)) {
                        throw new AuthorizationException(user + " does not have READ access to " + key);
                    }
                    InputStreamWithMeta in = blobstore.getBlob(key);
                    byte[] buffer = new byte[1024];
                    int len;
                    while ((len = in.read(buffer)) >= 0) {
                        out.write(buffer, 0, len);
                    }
                    out.close();
                    in.close();
                    if (uncompress) {
                        ServerUtils.unpack(new File(downloadFile), new File(localFileWithVersion));
                        LOG.debug("uncompressed " + downloadFile + " to: " + localFileWithVersion);
                    }

                    // Next write the version.
                    LOG.info("Blob: " + key + " updated with new Nimbus-provided version: " +
                        nimbusBlobVersion + " local version was: " + oldVersion);
                    // The false parameter ensures overwriting the version file, not appending
                    writer = new PrintWriter(
                        new BufferedWriter(new FileWriter(localVersionFile, false)));
                    writer.println(nimbusBlobVersion);
                    writer.close();

                    try {
                        setBlobPermissions(conf, user, localFileWithVersion);
                        setBlobPermissions(conf, user, localVersionFile);

                        // Update the key.current symlink. First create tmp symlink and do
                        // move of tmp to current so that the operation is atomic.
                        String tmp_uuid_local = java.util.UUID.randomUUID().toString();
                        LOG.debug("Creating a symlink @" + localFile + "." + tmp_uuid_local + " , " +
                            "linking to: " + localFile + "." + nimbusBlobVersion);
                        File uuid_symlink = new File(localFile + "." + tmp_uuid_local);

                        Files.createSymbolicLink(uuid_symlink.toPath(),
                            Paths.get(ServerUtils.constructBlobWithVersionFileName(localFile.toString(),
                                nimbusBlobVersion)));
                        File current_symlink = new File(ServerUtils.constructBlobCurrentSymlinkName(
                            localFile.toString()));
                        Files.move(uuid_symlink.toPath(), current_symlink.toPath(), ATOMIC_MOVE);
                    } catch (IOException e) {
                        // if we fail after writing the version file but before we move current link we need to
                        // restore the old version to the file
                        try {
                            PrintWriter restoreWriter = new PrintWriter(
                                new BufferedWriter(new FileWriter(localVersionFile, false)));
                            restoreWriter.println(oldVersion);
                            restoreWriter.close();
                        } catch (IOException ignore) {}
                        throw e;
                    }

                    String oldBlobFile = localFile + "." + oldVersion;
                    try {
                        // Remove the old version. Note that if a number of processes have that file open,
                        // the OS will keep the old blob file around until they all close the handle and only
                        // then deletes it. No new process will open the old blob, since the users will open the
                        // blob through the "blob.current" symlink, which always points to the latest version of
                        // a blob. Remove the old version after the current symlink is updated as to not affect
                        // anyone trying to read it.
                        if ((oldVersion != -1) && (oldVersion != nimbusBlobVersion)) {
                            LOG.info("Removing an old blob file:" + oldBlobFile);
                            Files.delete(Paths.get(oldBlobFile));
                        }
                    } catch (IOException e) {
                        // At this point we have downloaded everything and moved symlinks.  If the remove of
                        // old fails just log an error
                        LOG.error("Exception removing old blob version: " + oldBlobFile);
                    }

                    break;
                } catch (AuthorizationException ae) {
                    // we consider this non-retriable exceptions
                    if (out != null) {
                        out.close();
                    }
                    new File(downloadFile).delete();
                    throw ae;
                } catch (IOException | KeyNotFoundException e) {
                    if (out != null) {
                        out.close();
                    }
                    if (writer != null) {
                        writer.close();
                    }
                    new File(downloadFile).delete();
                    if (uncompress) {
                        try {
                            FileUtils.deleteDirectory(new File(localFileWithVersion));
                        } catch (IOException ignore) {}
                    }
                    if (!isUpdate) {
                        // don't want to remove existing version file if its an update
                        new File(localVersionFile).delete();
                    }

                    if (numTries < blobDownloadRetries) {
                        LOG.error("Failed to download blob, retrying", e);
                    } else {
                        throw e;
                    }
                }
            }
            return new LocalizedResource(key, localizedPath, uncompress);
        } finally {
            if(blobstore != null) {
                blobstore.shutdown();
            }
        }
    }

    public void setBlobPermissions(Map<String, Object> conf, String user, String path)
        throws IOException {

        if (!ObjectReader.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false)) {
            return;
        }
        String wlCommand = ObjectReader.getString(conf.get(Config.SUPERVISOR_WORKER_LAUNCHER), "");
        if (wlCommand.isEmpty()) {
            String stormHome = System.getProperty("storm.home");
            wlCommand = stormHome + "/bin/worker-launcher";
        }
        List<String> command = new ArrayList<String>(Arrays.asList(wlCommand, user, "blob", path));

        String[] commandArray = command.toArray(new String[command.size()]);
        ShellUtils.ShellCommandExecutor shExec = new ShellUtils.ShellCommandExecutor(commandArray);
        LOG.info("Setting blob permissions, command: {}", Arrays.toString(commandArray));

        try {
            shExec.execute();
            LOG.debug("output: {}", shExec.getOutput());
        } catch (ShellUtils.ExitCodeException e) {
            int exitCode = shExec.getExitCode();
            LOG.warn("Exit code from worker-launcher is : " + exitCode, e);
            LOG.debug("output: {}", shExec.getOutput());
            throw new IOException("Setting blob permissions failed" +
                " (exitCode=" + exitCode + ") with output: " + shExec.getOutput(), e);
        }
    }


    public synchronized void cleanup() {
        LocalizedResourceRetentionSet toClean = new LocalizedResourceRetentionSet(cacheTargetSize);
        // need one large set of all and then clean via LRU
        for (LocalizedResourceSet t : userRsrc.values()) {
            toClean.addResources(t);
            LOG.debug("Resources to be cleaned after adding {} : {}", t.getUser(), toClean);
        }
        toClean.cleanup();
        LOG.debug("Resource cleanup: {}", toClean);
        for (LocalizedResourceSet t : userRsrc.values()) {
            if (t.getSize() == 0) {
                String user = t.getUser();

                LOG.debug("removing empty set: {}", user);
                File userFileCacheDir = getLocalUserFileCacheDir(user);
                getCacheDirForFiles(userFileCacheDir).delete();
                getCacheDirForArchives(userFileCacheDir).delete();
                getLocalUserFileCacheDir(user).delete();
                boolean dirsRemoved = getLocalUserDir(user).delete();
                // to catch race with update thread
                if (dirsRemoved) {
                    userRsrc.remove(user);
                }
            }
        }
    }

}
