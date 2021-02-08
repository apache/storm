/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.storm.blobstore;

import static org.apache.storm.blobstore.BlobStoreAclHandler.ADMIN;
import static org.apache.storm.blobstore.BlobStoreAclHandler.READ;
import static org.apache.storm.blobstore.BlobStoreAclHandler.WRITE;
import static org.apache.storm.daemon.nimbus.Nimbus.NIMBUS_SUBJECT;
import static org.apache.storm.daemon.nimbus.Nimbus.getVersionForKey;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import javax.security.auth.Subject;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.cluster.ClusterStateContext;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyAlreadyExistsException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.zookeeper.KeeperException;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.WrappedKeyAlreadyExistsException;
import org.apache.storm.utils.WrappedKeyNotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a local file system backed blob store implementation for Nimbus.
 *
 * <p>For a local blob store the user and the supervisor use NimbusBlobStore Client API in order to talk to nimbus through thrift.
 * The authentication and authorization here is based on the subject.
 * We currently have NIMBUS_ADMINS and SUPERVISOR_ADMINS configuration. NIMBUS_ADMINS are given READ, WRITE and ADMIN
 * access whereas the SUPERVISOR_ADMINS are given READ access in order to read and download the blobs form the nimbus.
 *
 * <p>The ACLs for the blob store are validated against whether the subject is a NIMBUS_ADMIN, SUPERVISOR_ADMIN or USER
 * who has read, write or admin privileges in order to perform respective operations on the blob.
 *
 * <p>For local blob store
 * 1. The USER interacts with nimbus to upload and access blobs through NimbusBlobStore Client API.
 * 2. The USER sets the ACLs, and the blob access is validated against these ACLs.
 * 3. The SUPERVISOR interacts with nimbus through the NimbusBlobStore Client API to download the blobs.
 * The supervisors principal should match the set of users configured into SUPERVISOR_ADMINS.
 * Here, the PrincipalToLocalPlugin takes care of mapping the principal to user name before the ACL validation.
 */
public class LocalFsBlobStore extends BlobStore {
    public static final Logger LOG = LoggerFactory.getLogger(LocalFsBlobStore.class);
    private static final String DATA_PREFIX = "data_";
    private static final String META_PREFIX = "meta_";
    private static final String BLOBSTORE_SUBTREE = "/blobstore/";
    private final int allPermissions = READ | WRITE | ADMIN;
    protected BlobStoreAclHandler aclHandler;
    private NimbusInfo nimbusInfo;
    private FileBlobStoreImpl fbs;
    private Map<String, Object> conf;
    private CuratorFramework zkClient;
    private IStormClusterState stormClusterState;
    private Timer timer;
    private ILeaderElector leaderElector;

    @Override
    public void prepare(Map<String, Object> conf, String overrideBase, NimbusInfo nimbusInfo, ILeaderElector leaderElector) {
        this.conf = conf;
        this.nimbusInfo = nimbusInfo;
        zkClient = BlobStoreUtils.createZKClient(conf, DaemonType.NIMBUS);
        if (overrideBase == null) {
            overrideBase = ConfigUtils.absoluteStormBlobStoreDir(conf);
        }
        File baseDir = new File(overrideBase, BASE_BLOBS_DIR_NAME);
        try {
            fbs = new FileBlobStoreImpl(baseDir, conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        aclHandler = new BlobStoreAclHandler(conf);
        try {
            this.stormClusterState = ClusterUtils.mkStormClusterState(conf, new ClusterStateContext(DaemonType.NIMBUS, conf));
        } catch (Exception e) {
            e.printStackTrace();
        }
        timer = new Timer("BLOB-STORE-TIMER", true);
        this.leaderElector = leaderElector;
    }

    /**
     * Sets up blobstore state for all current keys.
     */
    private void setupBlobstore() throws AuthorizationException, KeyNotFoundException {
        IStormClusterState state = stormClusterState;
        BlobStore store = this;
        Set<String> localKeys = new HashSet<>();
        for (Iterator<String> it = store.listKeys(); it.hasNext();) {
            localKeys.add(it.next());
        }
        Set<String> activeKeys = new HashSet<>(state.activeKeys());
        Set<String> activeLocalKeys = new HashSet<>(localKeys);
        activeLocalKeys.retainAll(activeKeys);
        Set<String> keysToDelete = new HashSet<>(localKeys);
        keysToDelete.removeAll(activeKeys);
        NimbusInfo nimbusInfo = this.nimbusInfo;
        LOG.debug("Deleting keys not on the zookeeper {}", keysToDelete);
        for (String toDelete: keysToDelete) {
            store.deleteBlob(toDelete, NIMBUS_SUBJECT);
        }
        LOG.debug("Creating list of key entries for blobstore inside zookeeper {} local {}", activeKeys, activeLocalKeys);
        for (String key: activeLocalKeys) {
            try {
                state.setupBlob(key, nimbusInfo, getVersionForKey(key, nimbusInfo, zkClient));
            } catch (KeyNotFoundException e) {
                // invalid key, remove it from blobstore
                store.deleteBlob(key, NIMBUS_SUBJECT);
            }
        }
    }


    private void blobSync() throws Exception {
        if ("distributed".equals(conf.get(Config.STORM_CLUSTER_MODE))) {
            if (!this.leaderElector.isLeader()) {
                IStormClusterState state = stormClusterState;
                NimbusInfo nimbusInfo = this.nimbusInfo;
                BlobStore store = this;
                Set<String> allKeys = new HashSet<>();
                for (Iterator<String> it = store.listKeys(); it.hasNext();) {
                    allKeys.add(it.next());
                }
                Set<String> zkKeys = new HashSet<>(state.blobstore(() -> {
                    try {
                        this.blobSync();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
                LOG.debug("blob-sync blob-store-keys {} zookeeper-keys {}", allKeys, zkKeys);
                LocalFsBlobStoreSynchronizer sync = new LocalFsBlobStoreSynchronizer(store, conf);
                sync.setNimbusInfo(nimbusInfo);
                sync.setBlobStoreKeySet(allKeys);
                sync.setZookeeperKeySet(zkKeys);
                sync.setZkClient(zkClient);
                sync.syncBlobs();
            } //else leader (NOOP)
        } //else local (NOOP)
    }


    @Override
    public  void startSyncBlobs() throws KeyNotFoundException, AuthorizationException {
        //register call back for blob-store
        this.stormClusterState.blobstore(() -> {
            try {
                blobSync();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        setupBlobstore();

        //Schedule nimbus code sync thread to sync code from other nimbuses.
        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    blobSync();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }, 0, ObjectReader.getInt(conf.get(DaemonConfig.NIMBUS_CODE_SYNC_FREQ_SECS)) * 1000);

    }

    @Override
    public AtomicOutputStream createBlob(String key, SettableBlobMeta meta, Subject who) throws AuthorizationException,
        KeyAlreadyExistsException {
        LOG.debug("Creating Blob for key {}", key);
        validateKey(key);
        aclHandler.normalizeSettableBlobMeta(key, meta, who, allPermissions);
        BlobStoreAclHandler.validateSettableACLs(key, meta.get_acl());
        aclHandler.hasPermissions(meta.get_acl(), allPermissions, who, key);
        if (fbs.exists(DATA_PREFIX + key)) {
            throw new WrappedKeyAlreadyExistsException(key);
        }
        BlobStoreFileOutputStream outputStream = null;
        try {
            outputStream = new BlobStoreFileOutputStream(fbs.write(META_PREFIX + key, true));
            outputStream.write(Utils.thriftSerialize(meta));
            outputStream.close();
            outputStream = null;
            this.stormClusterState.setupBlob(key, this.nimbusInfo, getVersionForKey(key, this.nimbusInfo, zkClient));
            return new BlobStoreFileOutputStream(fbs.write(DATA_PREFIX + key, true));
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (KeyNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            if (outputStream != null) {
                try {
                    outputStream.cancel();
                } catch (IOException e) {
                    //Ignored
                }
            }
        }
    }

    @Override
    public AtomicOutputStream updateBlob(String key, Subject who) throws AuthorizationException, KeyNotFoundException {
        validateKey(key);
        checkPermission(key, who, WRITE);
        try {
            return new BlobStoreFileOutputStream(fbs.write(DATA_PREFIX + key, false));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private SettableBlobMeta getStoredBlobMeta(String key) throws KeyNotFoundException {
        InputStream in = null;
        try {
            LocalFsBlobStoreFile pf = fbs.read(META_PREFIX + key);
            try {
                in = pf.getInputStream();
            } catch (FileNotFoundException fnf) {
                throw new WrappedKeyNotFoundException(key);
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[2048];
            int len;
            while ((len = in.read(buffer)) > 0) {
                out.write(buffer, 0, len);
            }
            in.close();
            in = null;
            return Utils.thriftDeserialize(SettableBlobMeta.class, out.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    //Ignored
                }
            }
        }
    }

    @Override
    public ReadableBlobMeta getBlobMeta(String key, Subject who) throws AuthorizationException, KeyNotFoundException {
        validateKey(key);
        if (!checkForBlobOrDownload(key)) {
            checkForBlobUpdate(key);
        }
        SettableBlobMeta meta = getStoredBlobMeta(key);
        aclHandler.validateUserCanReadMeta(meta.get_acl(), who, key);
        ReadableBlobMeta rbm = new ReadableBlobMeta();
        rbm.set_settable(meta);
        try {
            LocalFsBlobStoreFile pf = fbs.read(DATA_PREFIX + key);
            rbm.set_version(pf.getModTime());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return rbm;
    }

    /**
     * Sets leader elector (only used by LocalFsBlobStore to help sync blobs between Nimbi.
     */
    @Override
    public void setLeaderElector(ILeaderElector leaderElector) {
        this.leaderElector = leaderElector;
    }

    @Override
    public void setBlobMeta(String key, SettableBlobMeta meta, Subject who) throws AuthorizationException, KeyNotFoundException {
        validateKey(key);
        checkForBlobOrDownload(key);
        aclHandler.normalizeSettableBlobMeta(key, meta, who, ADMIN);
        BlobStoreAclHandler.validateSettableACLs(key, meta.get_acl());
        SettableBlobMeta orig = getStoredBlobMeta(key);
        aclHandler.hasPermissions(orig.get_acl(), ADMIN, who, key);
        BlobStoreFileOutputStream outputStream = null;
        try {
            outputStream = new BlobStoreFileOutputStream(fbs.write(META_PREFIX + key, false));
            outputStream.write(Utils.thriftSerialize(meta));
            outputStream.close();
            outputStream = null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (outputStream != null) {
                try {
                    outputStream.cancel();
                } catch (IOException e) {
                    //Ignored
                }
            }
        }
    }

    @Override
    public void deleteBlob(String key, Subject who) throws AuthorizationException, KeyNotFoundException {
        validateKey(key);

        if (!aclHandler.checkForValidUsers(who, WRITE)) {
            // need to get ACL from meta
            LOG.debug("Retrieving meta to get ACL info... key: {} subject: {}", key, who);

            try {
                checkPermission(key, who, WRITE);
            } catch (KeyNotFoundException e) {
                LOG.error("Error while retrieving meta from ZK or local... key: {} subject: {}", key, who);
                throw e;
            }
        } else {
            // able to delete the blob without checking meta's ACL
            // skip checking everything and continue deleting local files
            LOG.debug("Given subject is eligible to delete key without checking ACL, skipping... key: {} subject: {}",
                      key, who);
        }

        try {
            deleteKeyIgnoringFileNotFound(DATA_PREFIX + key);
            deleteKeyIgnoringFileNotFound(META_PREFIX + key);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.stormClusterState.removeBlobstoreKey(key);
        this.stormClusterState.removeKeyVersion(key);
    }

    private void checkPermission(String key, Subject who, int mask) throws KeyNotFoundException, AuthorizationException {
        checkForBlobOrDownload(key);
        SettableBlobMeta meta = getStoredBlobMeta(key);
        aclHandler.hasPermissions(meta.get_acl(), mask, who, key);
    }

    private void deleteKeyIgnoringFileNotFound(String key) throws IOException {
        try {
            fbs.deleteKey(key);
        } catch (IOException e) {
            if (e instanceof FileNotFoundException || e instanceof NoSuchFileException) {
                LOG.debug("Ignoring FileNotFoundException since we're about to delete such key... key: {}", key);
            } else {
                throw e;
            }
        }
    }

    @Override
    public InputStreamWithMeta getBlob(String key, Subject who) throws AuthorizationException, KeyNotFoundException {
        validateKey(key);
        if (!checkForBlobOrDownload(key)) {
            checkForBlobUpdate(key);
        }
        SettableBlobMeta meta = getStoredBlobMeta(key);
        aclHandler.hasPermissions(meta.get_acl(), READ, who, key);
        try {
            return new BlobStoreFileInputStream(fbs.read(DATA_PREFIX + key));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<String> listKeys() {
        try {
            return new KeyTranslationIterator(fbs.listKeys(), DATA_PREFIX);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void shutdown() {
        if (zkClient != null) {
            zkClient.close();
        }
        if (timer != null) {
            timer.cancel();;
        }
        stormClusterState.disconnect();
    }

    @Override
    public int getBlobReplication(String key, Subject who) throws Exception {
        int replicationCount = 0;
        validateKey(key);
        SettableBlobMeta meta = getStoredBlobMeta(key);
        aclHandler.hasPermissions(meta.get_acl(), READ, who, key);
        if (zkClient.checkExists().forPath(BLOBSTORE_SUBTREE + key) == null) {
            return 0;
        }
        try {
            replicationCount = zkClient.getChildren().forPath(BLOBSTORE_SUBTREE + key).size();
        } catch (KeeperException.NoNodeException e) {
            //Race with delete
            //If it is not here the replication is 0 
        }
        return replicationCount;
    }

    @Override
    public int updateBlobReplication(String key, int replication, Subject who) throws AuthorizationException, KeyNotFoundException {
        throw new UnsupportedOperationException("For local file system blob store the update blobs function does not work. "
                                                + "Please use HDFS blob store to make this feature available.");
    }

    //This additional check and download is for nimbus high availability in case you have more than one nimbus
    public synchronized boolean checkForBlobOrDownload(String key) throws KeyNotFoundException {
        boolean checkBlobDownload = false;
        try {
            List<String> keyList = BlobStoreUtils.getKeyListFromBlobStore(this);
            if (!keyList.contains(key)) {
                if (zkClient.checkExists().forPath(BLOBSTORE_SUBTREE + key) != null) {
                    Set<NimbusInfo> nimbusSet = BlobStoreUtils.getNimbodesWithLatestSequenceNumberOfBlob(zkClient, key);
                    nimbusSet.remove(this.nimbusInfo);
                    if (BlobStoreUtils.downloadMissingBlob(conf, this, key, nimbusSet)) {
                        LOG.debug("Updating blobs state");
                        BlobStoreUtils.createStateInZookeeper(conf, key, nimbusInfo);
                        checkBlobDownload = true;
                    }
                }
            }
        } catch (KeyNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return checkBlobDownload;
    }

    public synchronized void checkForBlobUpdate(String key) {
        BlobStoreUtils.updateKeyForBlobStore(conf, this, zkClient, key, nimbusInfo);
    }

    public void fullCleanup(long age) throws IOException {
        fbs.fullCleanup(age);
    }

    @VisibleForTesting
    File getKeyDataDir(String key) {
        return fbs.getKeyDir(DATA_PREFIX + key);
    }
}
