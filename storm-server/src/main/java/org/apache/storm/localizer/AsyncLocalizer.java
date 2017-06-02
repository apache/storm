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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.utils.ServerConfigUtils;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.daemon.Shutdownable;
import org.apache.storm.daemon.supervisor.AdvancedFSOps;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * This is a wrapper around the Localizer class that provides the desired
 * async interface to Slot.
 */
public class AsyncLocalizer implements ILocalizer, Shutdownable {
    /**
     * A future that has already completed.
     */
    private static class AllDoneFuture implements Future<Void> {

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public Void get() {
            return null;
        }

        @Override
        public Void get(long timeout, TimeUnit unit) {
            return null;
        }

    }

    private static final Logger LOG = LoggerFactory.getLogger(AsyncLocalizer.class);

    private final Localizer _localizer;
    private final ExecutorService _execService;
    private final boolean _isLocalMode;
    private final Map<String, Object> _conf;
    private final Map<String, LocalDownloadedResource> _basicPending;
    private final Map<String, LocalDownloadedResource> _blobPending;
    private final AdvancedFSOps _fsOps;
    private final boolean _symlinksDisabled;

    private class DownloadBaseBlobsDistributed implements Callable<Void> {
        protected final String _topologyId;
        protected final File _stormRoot;
        protected final LocalAssignment _assignment;
        protected final String owner;
        
        public DownloadBaseBlobsDistributed(String topologyId, LocalAssignment assignment) throws IOException {
            _topologyId = topologyId;
            _stormRoot = new File(ConfigUtils.supervisorStormDistRoot(_conf, _topologyId));
            _assignment = assignment;
	    owner = assignment.get_owner();
        }
        
        protected void downloadBaseBlobs(File tmproot) throws Exception {
            String stormJarKey = ConfigUtils.masterStormJarKey(_topologyId);
            String stormCodeKey = ConfigUtils.masterStormCodeKey(_topologyId);
            String topoConfKey = ConfigUtils.masterStormConfKey(_topologyId);
            String jarPath = ConfigUtils.supervisorStormJarPath(tmproot.getAbsolutePath());
            String codePath = ConfigUtils.supervisorStormCodePath(tmproot.getAbsolutePath());
            String confPath = ConfigUtils.supervisorStormConfPath(tmproot.getAbsolutePath());
            _fsOps.forceMkdir(tmproot);
            _fsOps.restrictDirectoryPermissions(tmproot);
            ClientBlobStore blobStore = ServerUtils.getClientBlobStoreForSupervisor(_conf);
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
        public Void call() throws Exception {
            try {
                if (_fsOps.fileExists(_stormRoot)) {
                    if (!_fsOps.supportsAtomicDirectoryMove()) {
                        LOG.warn("{} may have partially downloaded blobs, recovering", _topologyId);
                        _fsOps.deleteIfExists(_stormRoot);
                    } else {
                        LOG.warn("{} already downloaded blobs, skipping", _topologyId);
                        return null;
                    }
                }
                boolean deleteAll = true;
                String tmproot = ServerConfigUtils.supervisorTmpDir(_conf) + Utils.FILE_PATH_SEPARATOR + Utils.uuid();
                File tr = new File(tmproot);
                try {
                    downloadBaseBlobs(tr);
                    if (_assignment.is_set_total_node_shared()) {
                        File sharedMemoryDirTmpLocation = new File(tr, "shared_by_topology");
                        //We need to create a directory for shared memory to write to (we should not encourage this though)
                        Path path = sharedMemoryDirTmpLocation.toPath();
                        Files.createDirectories(path);
                    }
                    _fsOps.moveDirectoryPreferAtomic(tr, _stormRoot);
                    _fsOps.setupStormCodeDir(owner, _stormRoot);
                    if (_assignment.is_set_total_node_shared()) {
                        File sharedMemoryDir = new File(_stormRoot, "shared_by_topology");
                        _fsOps.setupWorkerArtifactsDir(owner, sharedMemoryDir);
                    }
                    deleteAll = false;
                } finally {
                    if (deleteAll) {
                        LOG.warn("Failed to download basic resources for topology-id {}", _topologyId);
                        _fsOps.deleteIfExists(tr);
                        _fsOps.deleteIfExists(_stormRoot);
                    }
                }
                return null;
            } catch (Exception e) {
                LOG.warn("Caught Exception While Downloading (rethrowing)... ", e);
                throw e;
            }
        }
    }
    
    private class DownloadBaseBlobsLocal extends DownloadBaseBlobsDistributed {

        public DownloadBaseBlobsLocal(String topologyId, LocalAssignment assignment) throws IOException {
            super(topologyId, assignment);
        }
        
        @Override
        protected void downloadBaseBlobs(File tmproot) throws Exception {
            _fsOps.forceMkdir(tmproot);
            String stormCodeKey = ConfigUtils.masterStormCodeKey(_topologyId);
            String topoConfKey = ConfigUtils.masterStormConfKey(_topologyId);
            File codePath = new File(ConfigUtils.supervisorStormCodePath(tmproot.getAbsolutePath()));
            File confPath = new File(ConfigUtils.supervisorStormConfPath(tmproot.getAbsolutePath()));
            BlobStore blobStore = ServerUtils.getNimbusBlobStore(_conf, null);
            try {
                try (OutputStream codeOutStream = _fsOps.getOutputStream(codePath)){
                    blobStore.readBlobTo(stormCodeKey, codeOutStream, null);
                }
                try (OutputStream confOutStream = _fsOps.getOutputStream(confPath)) {
                    blobStore.readBlobTo(topoConfKey, confOutStream, null);
                }
            } finally {
                blobStore.shutdown();
            }

            ClassLoader classloader = Thread.currentThread().getContextClassLoader();
            String resourcesJar = AsyncLocalizer.resourcesJar();
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
                    _fsOps.copyDirectory(new File(url.getFile()), new File(targetDir, ConfigUtils.RESOURCES_SUBDIR));
                }
            }
        }
    }
    
    private class DownloadBlobs implements Callable<Void> {
        private final String _topologyId;
        private final String topoOwner;

        public DownloadBlobs(String topologyId, String topoOwner) {
            _topologyId = topologyId;
            this.topoOwner = topoOwner;
        }

        @Override
        public Void call() throws Exception {
            try {
                String stormroot = ConfigUtils.supervisorStormDistRoot(_conf, _topologyId);
                Map<String, Object> topoConf = ConfigUtils.readSupervisorStormConf(_conf, _topologyId);

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

                StormTopology stormCode = ConfigUtils.readSupervisorTopology(_conf, _topologyId, _fsOps);
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
                    File userDir = _localizer.getLocalUserFileCacheDir(topoOwner);
                    if (!_fsOps.fileExists(userDir)) {
                        _fsOps.forceMkdir(userDir);
                    }
                    List<LocalizedResource> localizedResources = _localizer.getBlobs(localResourceList, topoOwner, topoName, userDir);
                    _fsOps.setupBlobPermissions(userDir, topoOwner);
                    if (!_symlinksDisabled) {
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
                            _fsOps.createSymlink(new File(stormroot, symlinkName), rsrcFilePath);
                        }
                    }
                }

                return null;
            } catch (Exception e) {
                LOG.warn("Caught Exception While Downloading (rethrowing)... ", e);
                throw e;
            }
        }
    }
    
    //Visible for testing
    AsyncLocalizer(Map<String, Object> conf, Localizer localizer, AdvancedFSOps ops) {
        _conf = conf;
        _symlinksDisabled = (boolean)conf.getOrDefault(Config.DISABLE_SYMLINKS, false);
        _isLocalMode = ConfigUtils.isLocalMode(conf);
        _localizer = localizer;
        _execService = Executors.newFixedThreadPool(1,  
                new ThreadFactoryBuilder()
                .setNameFormat("Async Localizer")
                .build());
        _basicPending = new HashMap<>();
        _blobPending = new HashMap<>();
        _fsOps = ops;
    }
    
    public AsyncLocalizer(Map<String, Object> conf, Localizer localizer) {
        this(conf, localizer, AdvancedFSOps.make(conf));
    }

    @Override
    public synchronized Future<Void> requestDownloadBaseTopologyBlobs(final LocalAssignment assignment, final int port) throws IOException {
        final String topologyId = assignment.get_topology_id();
        LocalDownloadedResource localResource = _basicPending.get(topologyId);
        if (localResource == null) {
            Callable<Void> c;
            if (_isLocalMode) {
                c = new DownloadBaseBlobsLocal(topologyId, assignment);
            } else {
                c = new DownloadBaseBlobsDistributed(topologyId, assignment);
            }
            localResource = new LocalDownloadedResource(_execService.submit(c));
            _basicPending.put(topologyId, localResource);
        }
        Future<Void> ret = localResource.reserve(port, assignment);
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
    
    @Override
    public synchronized void recoverRunningTopology(LocalAssignment assignment, int port) {
        final String topologyId = assignment.get_topology_id();
        LocalDownloadedResource localResource = _basicPending.get(topologyId);
        if (localResource == null) {
            localResource = new LocalDownloadedResource(new AllDoneFuture());
            _basicPending.put(topologyId, localResource);
        }
        localResource.reserve(port, assignment);
        LOG.debug("Recovered basic {} {}", topologyId, localResource);
        
        localResource = _blobPending.get(topologyId);
        if (localResource == null) {
            localResource = new LocalDownloadedResource(new AllDoneFuture());
            _blobPending.put(topologyId, localResource);
        }
        localResource.reserve(port, assignment);
        LOG.debug("Recovered blobs {} {}", topologyId, localResource);
    }
    
    @Override
    public synchronized Future<Void> requestDownloadTopologyBlobs(LocalAssignment assignment, int port) {
        final String topologyId = assignment.get_topology_id();
        LocalDownloadedResource localResource = _blobPending.get(topologyId);
        if (localResource == null) {
            Callable<Void> c = new DownloadBlobs(topologyId, assignment.get_owner());
            localResource = new LocalDownloadedResource(_execService.submit(c));
            _blobPending.put(topologyId, localResource);
        }
        Future<Void> ret = localResource.reserve(port, assignment);
        LOG.debug("Reserved blobs {} {}", topologyId, localResource);
        return ret;
    }

    @Override
    public synchronized void releaseSlotFor(LocalAssignment assignment, int port) throws IOException {
        final String topologyId = assignment.get_topology_id();
        LOG.debug("Releasing slot for {} {}", topologyId, port);
        LocalDownloadedResource localResource = _blobPending.get(topologyId);
        if (localResource == null || !localResource.release(port, assignment)) {
            LOG.warn("Released blob reference {} {} for something that we didn't have {}", topologyId, port, localResource);
        } else if (localResource.isDone()){
            LOG.info("Released blob reference {} {} Cleaning up BLOB references...", topologyId, port);
            _blobPending.remove(topologyId);
            Map<String, Object> topoConf = ConfigUtils.readSupervisorStormConf(_conf, topologyId);
            @SuppressWarnings("unchecked")
            Map<String, Map<String, Object>> blobstoreMap = (Map<String, Map<String, Object>>) topoConf.get(Config.TOPOLOGY_BLOBSTORE_MAP);
            if (blobstoreMap != null) {
                String user = assignment.get_owner();
                String topoName = (String) topoConf.get(Config.TOPOLOGY_NAME);
                
                for (Map.Entry<String, Map<String, Object>> entry : blobstoreMap.entrySet()) {
                    String key = entry.getKey();
                    Map<String, Object> blobInfo = entry.getValue();
                    try {
                        _localizer.removeBlobReference(key, user, topoName, SupervisorUtils.shouldUncompressBlob(blobInfo));
                    } catch (Exception e) {
                        throw new IOException(e);
                    }
                }
            }
        } else {
            LOG.debug("Released blob reference {} {} still waiting on {}", topologyId, port, localResource);
        }
        
        localResource = _basicPending.get(topologyId);
        if (localResource == null || !localResource.release(port, assignment)) {
            LOG.warn("Released basic reference {} {} for something that we didn't have {}", topologyId, port, localResource);
        } else if (localResource.isDone()){
            LOG.info("Released blob reference {} {} Cleaning up basic files...", topologyId, port);
            _basicPending.remove(topologyId);
            String path = ConfigUtils.supervisorStormDistRoot(_conf, topologyId);
            _fsOps.deleteIfExists(new File(path), null, "rmr "+topologyId);
        } else {
            LOG.debug("Released basic reference {} {} still waiting on {}", topologyId, port, localResource);
        }
    }

    @Override
    public synchronized void cleanupUnusedTopologies() throws IOException {
        File distRoot = new File(ConfigUtils.supervisorStormDistRoot(_conf));
        LOG.info("Cleaning up unused topologies in {}", distRoot);
        File[] children = distRoot.listFiles();
        if (children != null) {
            for (File topoDir : children) {
                String topoId = URLDecoder.decode(topoDir.getName(), "UTF-8");
                if (_basicPending.get(topoId) == null && _blobPending.get(topoId) == null) {
                    _fsOps.deleteIfExists(topoDir, null, "rmr " + topoId);
                }
            }
        }
    }

    @Override
    public void shutdown() {
        _execService.shutdown();
    }
}
