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
import java.util.Iterator;
import java.util.Map;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyAlreadyExistsException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.utils.NimbusClient;

/**
 * A Client blob store for LocalMode.
 */
public class LocalModeClientBlobStore extends ClientBlobStore {
    private final BlobStore wrapped;

    public LocalModeClientBlobStore(BlobStore wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public void shutdown() {
        wrapped.shutdown();
    }

    @Override
    public void prepare(Map<String, Object> conf) {
        //NOOP prepare should have already been called
    }

    @Override
    protected AtomicOutputStream createBlobToExtend(String key, SettableBlobMeta meta) throws AuthorizationException,
        KeyAlreadyExistsException {
        return wrapped.createBlob(key, meta, null);
    }

    @Override
    public AtomicOutputStream updateBlob(String key) throws AuthorizationException, KeyNotFoundException {
        return wrapped.updateBlob(key, null);
    }

    @Override
    public ReadableBlobMeta getBlobMeta(String key) throws AuthorizationException, KeyNotFoundException {
        return wrapped.getBlobMeta(key, null);
    }

    @Override
    public boolean isRemoteBlobExists(String blobKey) throws AuthorizationException {
        try {
            wrapped.getBlob(blobKey, null);
        } catch (KeyNotFoundException e) {
            return false;
        }
        return true;
    }

    @Override
    protected void setBlobMetaToExtend(String key, SettableBlobMeta meta) throws AuthorizationException, KeyNotFoundException {
        wrapped.setBlobMeta(key, meta, null);
    }

    @Override
    public void deleteBlob(String key) throws AuthorizationException, KeyNotFoundException {
        wrapped.deleteBlob(key, null);
    }

    @Override
    public InputStreamWithMeta getBlob(String key) throws AuthorizationException, KeyNotFoundException {
        return wrapped.getBlob(key, null);
    }

    @Override
    public Iterator<String> listKeys() {
        return wrapped.listKeys();
    }

    @Override
    public int getBlobReplication(String key) throws AuthorizationException, KeyNotFoundException {
        try {
            return wrapped.getBlobReplication(key, null);
        } catch (AuthorizationException | KeyNotFoundException rethrow) {
            throw rethrow;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int updateBlobReplication(String key, int replication) throws AuthorizationException, KeyNotFoundException {
        try {
            return wrapped.updateBlobReplication(key, replication, null);
        } catch (AuthorizationException | KeyNotFoundException rethrow) {
            throw rethrow;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean setClient(Map<String, Object> conf, NimbusClient client) {
        return true;
    }

    @Override
    public void createStateInZookeeper(String key) {
        //NOOP
    }

    @Override
    public void close() {
        wrapped.shutdown();
    }

    @Override
    public long getRemoteBlobstoreUpdateTime() throws IOException {
        return -1L; // not supported
    }
}