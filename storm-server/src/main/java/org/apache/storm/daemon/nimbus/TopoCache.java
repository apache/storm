/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.daemon.nimbus;

import static org.apache.storm.blobstore.BlobStoreAclHandler.READ;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.security.auth.Subject;

import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.BlobStoreAclHandler;
import org.apache.storm.generated.AccessControl;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyAlreadyExistsException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache topologies and topology confs from the blob store.
 * Makes reading this faster because it can skip
 * deserialization in many cases.
 */
public class TopoCache {
    public static final Logger LOG = LoggerFactory.getLogger(TopoCache.class);

    private static final class WithAcl<T> {
        public final List<AccessControl> acl;
        public final T data;

        public WithAcl(List<AccessControl> acl, T data) {
            this.acl = acl;
            this.data = data;
        }
    }

    private final BlobStore store;
    private final BlobStoreAclHandler aclHandler;
    private final ConcurrentHashMap<String, WithAcl<StormTopology>> topos = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, WithAcl<Map<String, Object>>> confs = new ConcurrentHashMap<>();

    public TopoCache(BlobStore store, Map<String, Object> conf) {
        this.store = store;
        aclHandler = new BlobStoreAclHandler(conf);
    }

    /**
     * Read a topology.
     * @param topoId the id of the topology to read
     * @param who who to read it as
     * @return the deserialized topology.
     * @throws IOException on any error while reading the blob.
     * @throws AuthorizationException if who is not allowed to read the blob
     * @throws KeyNotFoundException if the blob could not be found
     */
    public StormTopology readTopology(final String topoId, final Subject who)
        throws KeyNotFoundException, AuthorizationException, IOException {
        final String key = ConfigUtils.masterStormCodeKey(topoId);
        WithAcl<StormTopology> cached = topos.get(topoId);
        if (cached == null) {
            //We need to read a new one
            StormTopology topo = Utils.deserialize(store.readBlob(key, who), StormTopology.class);
            ReadableBlobMeta meta = store.getBlobMeta(key, who);
            cached = new WithAcl<>(meta.get_settable().get_acl(), topo);
            WithAcl<StormTopology> previous = topos.putIfAbsent(topoId, cached);
            if (previous != null) {
                cached = previous;
            }
        } else {
            //Check if the user is allowed to read this
            aclHandler.hasPermissions(cached.acl, READ, who, key);
        }
        return cached.data;
    }

    /**
     * Delete a topology when we are done.
     * @param topoId the id of the topology
     * @param who who is deleting it
     * @throws AuthorizationException if who is not allowed to delete the blob
     * @throws KeyNotFoundException if the blob could not be found
     */
    public void deleteTopology(final String topoId, final Subject who) throws AuthorizationException, KeyNotFoundException {
        final String key = ConfigUtils.masterStormCodeKey(topoId);
        store.deleteBlob(key, who);
        topos.remove(topoId);
    }

    /**
     * Add a new topology.
     * @param topoId the id of the topology
     * @param who who is doing it
     * @param topo the topology itself
     * @throws AuthorizationException if who is not allowed to add a topology
     * @throws KeyAlreadyExistsException if the topology already exists
     * @throws IOException on any error interacting with the blob store
     */
    public void addTopology(final String topoId, final Subject who, final StormTopology topo)
        throws AuthorizationException, KeyAlreadyExistsException, IOException {
        final String key = ConfigUtils.masterStormCodeKey(topoId);
        final List<AccessControl> acl = BlobStoreAclHandler.DEFAULT;
        store.createBlob(key, Utils.serialize(topo), new SettableBlobMeta(acl), who);
        topos.put(topoId, new WithAcl<>(acl, topo));
    }

    /**
     * Update an existing topology .
     * @param topoId the id of the topology
     * @param who who is doing it
     * @param topo the new topology to save
     * @throws AuthorizationException if who is not allowed to update a topology
     * @throws KeyNotFoundException if the topology is not found in the blob store
     * @throws IOException on any error interacting with the blob store
     */
    public void updateTopology(final String topoId, final Subject who, final StormTopology topo)
        throws AuthorizationException, KeyNotFoundException, IOException {
        final String key = ConfigUtils.masterStormCodeKey(topoId);
        store.updateBlob(key, Utils.serialize(topo), who);
        List<AccessControl> acl = BlobStoreAclHandler.DEFAULT;
        WithAcl<StormTopology> old = topos.get(topoId);
        if (old != null) {
            acl = old.acl;
        } else {
            acl = store.getBlobMeta(key, who).get_settable().get_acl();
        }
        topos.put(topoId, new WithAcl<>(acl, topo));
    }

    /**
     * Read a topology conf.
     * @param topoId the id of the topology to read the conf for
     * @param who who to read it as
     * @return the deserialized config.
     * @throws IOException on any error while reading the blob.
     * @throws AuthorizationException if who is not allowed to read the blob
     * @throws KeyNotFoundException if the blob could not be found
     */
    public Map<String, Object> readTopoConf(final String topoId, final Subject who)
        throws KeyNotFoundException, AuthorizationException, IOException {
        final String key = ConfigUtils.masterStormConfKey(topoId);
        WithAcl<Map<String, Object>> cached = confs.get(topoId);
        if (cached == null) {
            //We need to read a new one
            Map<String, Object> topoConf = Utils.fromCompressedJsonConf(store.readBlob(key, who));
            ReadableBlobMeta meta = store.getBlobMeta(key, who);
            cached = new WithAcl<>(meta.get_settable().get_acl(), topoConf);
            WithAcl<Map<String, Object>> previous = confs.putIfAbsent(topoId, cached);
            if (previous != null) {
                cached = previous;
            }
        } else {
            //Check if the user is allowed to read this
            aclHandler.hasPermissions(cached.acl, READ, who, key);
        }
        return cached.data;
    }

    /**
     * Delete a topology conf when we are done.
     * @param topoId the id of the topology
     * @param who who is deleting it
     * @throws AuthorizationException if who is not allowed to delete the topo conf
     * @throws KeyNotFoundException if the topo conf is not found in the blob store
     */
    public void deleteTopoConf(final String topoId, final Subject who) throws AuthorizationException, KeyNotFoundException {
        final String key = ConfigUtils.masterStormConfKey(topoId);
        store.deleteBlob(key, who);
        confs.remove(topoId);
    }

    /**
     * Add a new topology config.
     * @param topoId the id of the topology
     * @param who who is doing it
     * @param topoConf the topology conf itself
     * @throws AuthorizationException if who is not allowed to add a topology conf
     * @throws KeyAlreadyExistsException if the toplogy conf already exists in the blob store
     * @throws IOException on any error interacting with the blob store.
     */
    public void addTopoConf(final String topoId, final Subject who, final Map<String, Object> topoConf)
        throws AuthorizationException, KeyAlreadyExistsException, IOException {
        final String key = ConfigUtils.masterStormConfKey(topoId);
        final List<AccessControl> acl = BlobStoreAclHandler.DEFAULT;
        store.createBlob(key, Utils.toCompressedJsonConf(topoConf), new SettableBlobMeta(acl), who);
        confs.put(topoId, new WithAcl<>(acl, topoConf));
    }

    /**
     * Update an existing topology conf.
     * @param topoId the id of the topology
     * @param who who is doing it
     * @param topoConf the new topology conf to save
     * @throws AuthorizationException if who is not allowed to update the topology conf
     * @throws KeyNotFoundException if the topology conf is not found in the blob store
     * @throws IOException on any error interacting with the blob store.
     */
    public void updateTopoConf(final String topoId, final Subject who, final Map<String, Object> topoConf)
        throws AuthorizationException, KeyNotFoundException, IOException {
        final String key = ConfigUtils.masterStormConfKey(topoId);
        store.updateBlob(key, Utils.toCompressedJsonConf(topoConf), who);
        List<AccessControl> acl = BlobStoreAclHandler.DEFAULT;
        WithAcl<Map<String, Object>> old = confs.get(topoId);
        if (old != null) {
            acl = old.acl;
        } else {
            acl = store.getBlobMeta(key, who).get_settable().get_acl();
        }
        confs.put(topoId, new WithAcl<>(acl, topoConf));
    }

    /**
     * Clear all entries from the Cache. This typically happens right after becoming a leader, just to be sure
     * nothing has changed while we were not the leader.
     */
    public void clear() {
        confs.clear();
        topos.clear();
    }
}