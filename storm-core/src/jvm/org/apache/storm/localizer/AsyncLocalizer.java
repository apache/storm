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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.daemon.Shutdownable;
import org.apache.storm.daemon.supervisor.AdvancedFSOps;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * This is a wrapper around the Localizer class that provides the desired
 * async interface to Slot.
 * TODO once we have replaced the original supervisor merge this with
 * Localizer and optimize them
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

    private class DownloadBaseBlobsDistributed implements Callable<Void> {
        private final String _topologyId;
        
        public DownloadBaseBlobsDistributed(String topologyId) {
            this._topologyId = topologyId;
        }
        
        @Override
        public Void call() throws Exception {
            String stormroot = ConfigUtils.supervisorStormDistRoot(_conf, _topologyId);
            File sr = new File(stormroot);
            if (sr.exists()) {
                if (!_fsOps.supportsAtomicDirectoryMove()) {
                    LOG.warn("{} may have partially downloaded blobs, recovering", _topologyId);
                    Utils.forceDelete(stormroot);
                } else {
                    LOG.warn("{} already downloaded blobs, skipping", _topologyId);
                    return null;
                }
            }
            boolean deleteAll = true;
            String tmproot = ConfigUtils.supervisorTmpDir(_conf) + Utils.FILE_PATH_SEPARATOR + Utils.uuid();
            try {
                String stormJarKey = ConfigUtils.masterStormJarKey(_topologyId);
                String stormCodeKey = ConfigUtils.masterStormCodeKey(_topologyId);
                String stormConfKey = ConfigUtils.masterStormConfKey(_topologyId);
                String jarPath = ConfigUtils.supervisorStormJarPath(tmproot);
                String codePath = ConfigUtils.supervisorStormCodePath(tmproot);
                String confPath = ConfigUtils.supervisorStormConfPath(tmproot);
                FileUtils.forceMkdir(new File(tmproot));
                _fsOps.restrictDirectoryPermissions(tmproot);
                ClientBlobStore blobStore = Utils.getClientBlobStoreForSupervisor(_conf);
                try {
                    Utils.downloadResourcesAsSupervisor(stormJarKey, jarPath, blobStore);
                    Utils.downloadResourcesAsSupervisor(stormCodeKey, codePath, blobStore);
                    Utils.downloadResourcesAsSupervisor(stormConfKey, confPath, blobStore);
                } finally {
                    blobStore.shutdown();
                }
                Utils.extractDirFromJar(jarPath, ConfigUtils.RESOURCES_SUBDIR, tmproot);
                _fsOps.moveDriectoryPreferAtomic(new File(tmproot), new File(stormroot));
                deleteAll = false;
            } finally {
                if (deleteAll) {
                    LOG.info("Failed to download basic resources for topology-id {}", _topologyId);
                    Utils.forceDelete(tmproot);
                    Utils.forceDelete(stormroot);
                }
            }
            return null;
        }
    }
    
    private class DownloadBaseBlobsLocal implements Callable<Void> {
        private final String _topologyId;
        
        public DownloadBaseBlobsLocal(String topologyId) {
            this._topologyId = topologyId;
        }
        
        @Override
        public Void call() throws Exception {
            String stormroot = ConfigUtils.supervisorStormDistRoot(_conf, _topologyId);
            File sr = new File(stormroot);
            if (sr.exists()) {
                if (!_fsOps.supportsAtomicDirectoryMove()) {
                    LOG.warn("{} may have partially downloaded blobs, recovering", _topologyId);
                    Utils.forceDelete(stormroot);
                } else {
                    LOG.warn("{} already downloaded blobs, skipping", _topologyId);
                    return null;
                }
            }
            boolean deleteAll = true;
            String tmproot = ConfigUtils.supervisorTmpDir(_conf) + Utils.FILE_PATH_SEPARATOR + Utils.uuid();
            try {
                BlobStore blobStore = Utils.getNimbusBlobStore(_conf, null, null);
                FileOutputStream codeOutStream = null;
                FileOutputStream confOutStream = null;
                try {
                    FileUtils.forceMkdir(new File(tmproot));
                    String stormCodeKey = ConfigUtils.masterStormCodeKey(_topologyId);
                    String stormConfKey = ConfigUtils.masterStormConfKey(_topologyId);
                    String codePath = ConfigUtils.supervisorStormCodePath(tmproot);
                    String confPath = ConfigUtils.supervisorStormConfPath(tmproot);
                    codeOutStream = new FileOutputStream(codePath);
                    blobStore.readBlobTo(stormCodeKey, codeOutStream, null);
                    confOutStream = new FileOutputStream(confPath);
                    blobStore.readBlobTo(stormConfKey, confOutStream, null);
                } finally {
                    if (codeOutStream != null)
                        codeOutStream.close();
                    if (confOutStream != null)
                        codeOutStream.close();
                    blobStore.shutdown();
                }

                ClassLoader classloader = Thread.currentThread().getContextClassLoader();
                String resourcesJar = AsyncLocalizer.resourcesJar();
                URL url = classloader.getResource(ConfigUtils.RESOURCES_SUBDIR);

                String targetDir = tmproot + Utils.FILE_PATH_SEPARATOR + ConfigUtils.RESOURCES_SUBDIR;

                if (resourcesJar != null) {
                    LOG.info("Extracting resources from jar at {} to {}", resourcesJar, targetDir);
                    Utils.extractDirFromJar(resourcesJar, ConfigUtils.RESOURCES_SUBDIR, stormroot);
                } else if (url != null) {
                    LOG.info("Copying resources at {} to {} ", url.toString(), targetDir);
                    if (url.getProtocol() == "jar") {
                        JarURLConnection urlConnection = (JarURLConnection) url.openConnection();
                        Utils.extractDirFromJar(urlConnection.getJarFileURL().getFile(), ConfigUtils.RESOURCES_SUBDIR, stormroot);
                    } else {
                        FileUtils.copyDirectory(new File(url.getFile()), (new File(targetDir)));
                    }
                }
                _fsOps.moveDriectoryPreferAtomic(new File(tmproot), new File(stormroot));
                SupervisorUtils.setupStormCodeDir(_conf, ConfigUtils.readSupervisorStormConf(_conf, _topologyId), stormroot);
                deleteAll = false;
            } finally {
                if (deleteAll) {
                    LOG.info("Failed to download basic resources for topology-id {}", _topologyId);
                    Utils.forceDelete(tmproot);
                    Utils.forceDelete(stormroot);
                }
            }
            return null;
        }
    }
    
    private class DownloadBlobs implements Callable<Void> {
        private final String _topologyId;

        public DownloadBlobs(String topologyId) {
            _topologyId = topologyId;
        }

        @Override
        public Void call() throws Exception {
            String stormroot = ConfigUtils.supervisorStormDistRoot(_conf, _topologyId);
            Map<String, Object> stormConf = ConfigUtils.readSupervisorStormConf(_conf, _topologyId);
                
            @SuppressWarnings("unchecked")
            Map<String, Map<String, Object>> blobstoreMap = (Map<String, Map<String, Object>>) stormConf.get(Config.TOPOLOGY_BLOBSTORE_MAP);
            String user = (String) stormConf.get(Config.TOPOLOGY_SUBMITTER_USER);
            String topoName = (String) stormConf.get(Config.TOPOLOGY_NAME);

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
                File userDir = _localizer.getLocalUserFileCacheDir(user);
                if (!userDir.exists()) {
                    FileUtils.forceMkdir(userDir);
                }
                List<LocalizedResource> localizedResources = _localizer.getBlobs(localResourceList, user, topoName, userDir);
                _fsOps.setupBlobPermissions(userDir, user);
                for (LocalizedResource localizedResource : localizedResources) {
                    File rsrcFilePath = new File(localizedResource.getFilePath());
                    String keyName = rsrcFilePath.getName();
                    String blobSymlinkTargetName = new File(localizedResource.getCurrentSymlinkPath()).getName();

                    String symlinkName = null;
                    Map<String, Object> blobInfo = blobstoreMap.get(keyName);
                    if (blobInfo != null && blobInfo.containsKey("localname")) {
                        symlinkName = (String) blobInfo.get("localname");
                    } else {
                        symlinkName = keyName;
                    }
                    Utils.forceCreateSymlink(stormroot, rsrcFilePath.getParent(), symlinkName, blobSymlinkTargetName);
                }
            }

            return null;
        }
    }
    
    public AsyncLocalizer(Map<String, Object> conf, Localizer localizer) {
        _conf = conf;
        _isLocalMode = ConfigUtils.isLocalMode(conf);
        _localizer = localizer;
        _execService = Executors.newFixedThreadPool(1,  
                new ThreadFactoryBuilder()
                .setNameFormat("Async Localizer")
                .build());
        _basicPending = new HashMap<>();
        _blobPending = new HashMap<>();
        _fsOps = AdvancedFSOps.mk(_conf);
    }

    @Override
    public synchronized Future<Void> requestDownloadBaseTopologyBlobs(final String topologyId, final int port) {
        LocalDownloadedResource localResource = _basicPending.get(topologyId);
        if (localResource == null) {
            Callable<Void> c;
            if (_isLocalMode) {
                c = new DownloadBaseBlobsLocal(topologyId);
            } else {
                c = new DownloadBaseBlobsDistributed(topologyId);
            }
            localResource = new LocalDownloadedResource(_execService.submit(c));
            _basicPending.put(topologyId, localResource);
        }
        return localResource.reserve(port);
    }

    private static String resourcesJar() throws IOException {
        String path = Utils.currentClasspath();
        if (path == null) {
            return null;
        }
        String[] paths = path.split(File.pathSeparator);
        List<String> jarPaths = new ArrayList<String>();
        for (String s : paths) {
            if (s.endsWith(".jar")) {
                jarPaths.add(s);
            }
        }

        List<String> rtn = new ArrayList<String>();
        int size = jarPaths.size();
        for (int i = 0; i < size; i++) {
            if (Utils.zipDoesContainDir(jarPaths.get(i), ConfigUtils.RESOURCES_SUBDIR)) {
                rtn.add(jarPaths.get(i));
            }
        }
        if (rtn.size() == 0)
            return null;

        return rtn.get(0);
    }
    
    @Override
    public synchronized void recoverRunningTopology(String topologyId, int port) {
        LocalDownloadedResource localResource = _basicPending.get(topologyId);
        if (localResource == null) {
            localResource = new LocalDownloadedResource(new AllDoneFuture());
            _basicPending.put(topologyId, localResource);
        }
        localResource = _blobPending.get(topologyId);
        if (localResource == null) {
            localResource = new LocalDownloadedResource(new AllDoneFuture());
            _blobPending.put(topologyId, localResource);
        }
    }
    
    @Override
    public synchronized Future<Void> requestDownloadTopologyBlobs(String topologyId, int port) {
        LocalDownloadedResource localResource = _blobPending.get(topologyId);
        if (localResource == null) {
            Callable<Void> c = new DownloadBlobs(topologyId);
            localResource = new LocalDownloadedResource(_execService.submit(c));
            _blobPending.put(topologyId, localResource);
        }
        return localResource.reserve(port);
    }

    @Override
    public synchronized void releaseSlotFor(String topologyId, int port) throws IOException {
        LOG.warn("Releaseing slot for {} {}", topologyId, port);
        LocalDownloadedResource localResource = _blobPending.get(topologyId);
        if (localResource == null || !localResource.release(port)) {
            LOG.warn("Released blob reference {} {} for something that we didn't have {}", topologyId, port, localResource.getPorts());
        } else if (localResource.isDone()){
            LOG.warn("Released blob reference {} {} Cleaning up BLOB references...", topologyId, port);
            _blobPending.remove(topologyId);
            Map<String, Object> topoConf = ConfigUtils.readSupervisorStormConf(_conf, topologyId);
            @SuppressWarnings("unchecked")
            Map<String, Map<String, Object>> blobstoreMap = (Map<String, Map<String, Object>>) topoConf.get(Config.TOPOLOGY_BLOBSTORE_MAP);
            if (blobstoreMap != null) {
                String user = (String) topoConf.get(Config.TOPOLOGY_SUBMITTER_USER);
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
            LOG.warn("Released blob reference {} {} still waiting on {}", topologyId, port, localResource.getPorts());
        }
        
        localResource = _basicPending.get(topologyId);
        if (localResource == null || !localResource.release(port)) {
            LOG.warn("Released basic reference {} {} for something that we didn't have {}", topologyId, port, localResource.getPorts());
        } else if (localResource.isDone()){
            LOG.warn("Released blob reference {} {} Cleaning up basic files...", topologyId, port);
            _basicPending.remove(topologyId);
            String path = ConfigUtils.supervisorStormDistRoot(_conf, topologyId);
            _fsOps.deleteIfExists(new File(path), null, "rmr "+topologyId);
        } else {
            LOG.warn("Released basic reference {} {} still waiting on {}", topologyId, port, localResource.getPorts());
        }
    }

    @Override
    public void shutdown() {
        _execService.shutdown();
    }
}
