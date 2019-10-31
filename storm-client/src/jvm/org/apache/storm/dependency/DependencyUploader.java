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

package org.apache.storm.dependency;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.blobstore.AtomicOutputStream;
import org.apache.storm.blobstore.BlobStoreAclHandler;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.generated.AccessControl;
import org.apache.storm.generated.AccessControlType;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyAlreadyExistsException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.storm.shade.org.apache.commons.io.IOUtils;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DependencyUploader {
    public static final Logger LOG = LoggerFactory.getLogger(DependencyUploader.class);

    private final Map<String, Object> conf;
    private ClientBlobStore blobStore;
    private final int uploadChunkSize;

    public DependencyUploader() {
        conf = Utils.readStormConfig();
        this.uploadChunkSize = ObjectReader.getInt(conf.get(Config.STORM_BLOBSTORE_DEPENDENCY_JAR_UPLOAD_CHUNK_SIZE_BYTES), 1024 * 1024);
    }

    public void init() {
        //NOOP
    }

    public void shutdown() {
        if (blobStore != null) {
            blobStore.shutdown();
        }
    }

    private synchronized ClientBlobStore getBlobStore() {
        if (blobStore == null) {
            blobStore = Utils.getClientBlobStore(conf);
        }
        return blobStore;
    }

    @VisibleForTesting
    void setBlobStore(ClientBlobStore blobStore) {
        this.blobStore = blobStore;
    }

    public List<String> uploadFiles(List<File> dependencies, boolean cleanupIfFails) throws IOException, AuthorizationException {
        checkFilesExist(dependencies);

        List<String> keys = new ArrayList<>(dependencies.size());
        try {
            for (File dependency : dependencies) {
                String fileName = dependency.getName();
                String key = DependencyBlobStoreUtils.generateDependencyBlobKey(DependencyBlobStoreUtils.applyUUIDToFileName(fileName));

                try {
                    uploadDependencyToBlobStore(key, dependency);
                } catch (KeyAlreadyExistsException e) {
                    // it should never happened since we apply UUID
                    throw new RuntimeException(e);
                }

                keys.add(key);
            }
        } catch (Throwable e) {
            if (getBlobStore() != null && cleanupIfFails) {
                deleteBlobs(keys);
            }
            throw new RuntimeException(e);
        }

        return keys;
    }

    public List<String> uploadArtifacts(Map<String, File> artifacts) {
        checkFilesExist(artifacts.values());

        List<String> keys = new ArrayList<>(artifacts.size());
        try {
            for (Map.Entry<String, File> artifactToFile : artifacts.entrySet()) {
                String artifact = artifactToFile.getKey();
                File dependency = artifactToFile.getValue();

                String key = DependencyBlobStoreUtils.generateDependencyBlobKey(convertArtifactToJarFileName(artifact));
                try {
                    uploadDependencyToBlobStore(key, dependency);
                } catch (KeyAlreadyExistsException e) {
                    // we lose the race, but it doesn't matter
                }

                keys.add(key);
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }

        return keys;
    }

    public void deleteBlobs(List<String> keys) {
        for (String key : keys) {
            try {
                getBlobStore().deleteBlob(key);
            } catch (Throwable e) {
                LOG.warn("blob delete failed - key: {} continue...", key);
            }
        }
    }

    private String convertArtifactToJarFileName(String artifact) {
        return artifact.replace(":", "-") + ".jar";
    }

    private boolean uploadDependencyToBlobStore(String key, File dependency)
        throws KeyAlreadyExistsException, AuthorizationException, IOException {

        boolean uploadNew = false;
        try {
            // FIXME: we can filter by listKeys() with local blobstore when STORM-1986 is going to be resolved
            // as a workaround, we call getBlobMeta() for all keys
            getBlobStore().getBlobMeta(key);
        } catch (KeyNotFoundException e) {
            // set acl to below so that it can be shared by other users as well, but allows only read
            List<AccessControl> acls = new ArrayList<>();
            acls.add(new AccessControl(AccessControlType.USER,
                                       BlobStoreAclHandler.READ | BlobStoreAclHandler.WRITE | BlobStoreAclHandler.ADMIN));
            acls.add(new AccessControl(AccessControlType.OTHER,
                                       BlobStoreAclHandler.READ));

            AtomicOutputStream blob = null;
            try {
                blob = getBlobStore().createBlob(key, new SettableBlobMeta(acls));
                try (InputStream in = Files.newInputStream(dependency.toPath())) {
                    IOUtils.copy(in, blob, this.uploadChunkSize);
                }
                blob.close();
                blob = null;

                uploadNew = true;
            } finally {
                try {
                    if (blob != null) {
                        blob.cancel();
                    }
                } catch (IOException throwaway) {
                    // Ignore.
                }
            }
        }

        return uploadNew;
    }

    private void checkFilesExist(Collection<File> dependencies) {
        for (File dependency : dependencies) {
            if (!dependency.isFile() || !dependency.exists()) {
                throw new FileNotAvailableException(dependency.getAbsolutePath());
            }
        }
    }
}
