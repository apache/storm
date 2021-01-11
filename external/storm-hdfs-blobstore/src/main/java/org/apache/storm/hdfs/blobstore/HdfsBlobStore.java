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

package org.apache.storm.hdfs.blobstore;

import static org.apache.storm.blobstore.BlobStoreAclHandler.ADMIN;
import static org.apache.storm.blobstore.BlobStoreAclHandler.READ;
import static org.apache.storm.blobstore.BlobStoreAclHandler.WRITE;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.storm.Config;
import org.apache.storm.blobstore.AtomicOutputStream;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.BlobStoreAclHandler;
import org.apache.storm.blobstore.BlobStoreFile;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyAlreadyExistsException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.utils.HadoopLoginUtil;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.WrappedKeyAlreadyExistsException;
import org.apache.storm.utils.WrappedKeyNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a HDFS file system backed blob store implementation.
 * Note that this provides an api for having HDFS be the backing store for the blobstore,
 * it is not a service/daemon.
 *
 * <p>We currently have NIMBUS_ADMINS and SUPERVISOR_ADMINS configuration. NIMBUS_ADMINS are given READ, WRITE and ADMIN
 * access whereas the SUPERVISOR_ADMINS are given READ access in order to read and download the blobs form the nimbus.
 *
 * <p>The ACLs for the blob store are validated against whether the subject is a NIMBUS_ADMIN, SUPERVISOR_ADMIN or USER
 * who has read, write or admin privileges in order to perform respective operations on the blob.
 *
 * <p>For hdfs blob store
 * 1. The USER interacts with nimbus to upload and access blobs through NimbusBlobStore Client API. Here, unlike
 * local blob store which stores the blobs locally, the nimbus talks to HDFS to upload the blobs.
 * 2. The USER sets the ACLs, and the blob access is validated against these ACLs.
 * 3. The SUPERVISOR interacts with nimbus through HdfsClientBlobStore to download the blobs. Here, unlike local
 * blob store the supervisor interacts with HDFS directly to download the blobs. The call to HdfsBlobStore is made as a "null"
 * subject. The blobstore gets the hadoop user and validates permissions for the supervisor.
 */
public class HdfsBlobStore extends BlobStore {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsBlobStore.class);
    private static final String DATA_PREFIX = "data_";
    private static final String META_PREFIX = "meta_";

    private BlobStoreAclHandler aclHandler;
    private HdfsBlobStoreImpl hbs;
    private Subject localSubject;
    private Map<String, Object> conf;
    private Cache<String, SettableBlobMeta> cacheMetas = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build();
    private Cache<String, Integer> cachedReplicationCount = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build();

    /**
     * If who is null then we want to use the user hadoop says we are.
     * Required for the supervisor to call these routines as its not
     * logged in as anyone.
     */
    private Subject checkAndGetSubject(Subject who) {
        if (who == null) {
            return localSubject;
        }
        return who;
    }

    @Override
    public void prepare(Map<String, Object> conf, String overrideBase, NimbusInfo nimbusInfo, ILeaderElector leaderElector) {
        this.conf = conf;
        prepareInternal(conf, overrideBase, null);
    }

    /**
     * Allow a Hadoop Configuration to be passed for testing. If it's null then the hadoop configs
     * must be in your classpath.
     */
    protected void prepareInternal(Map<String, Object> conf, String overrideBase, Configuration hadoopConf) {
        this.conf = conf;
        if (overrideBase == null) {
            overrideBase = (String) conf.get(Config.BLOBSTORE_DIR);
        }
        if (overrideBase == null) {
            throw new RuntimeException("You must specify a blobstore directory for HDFS to use!");
        }
        LOG.debug("directory is: {}", overrideBase);

        //Login to hdfs
        localSubject = HadoopLoginUtil.loginHadoop(conf);

        aclHandler = new BlobStoreAclHandler(conf);
        Path baseDir = new Path(overrideBase, BASE_BLOBS_DIR_NAME);
        try {
            if (hadoopConf != null) {
                hbs = new HdfsBlobStoreImpl(baseDir, conf, hadoopConf);
            } else {
                hbs = new HdfsBlobStoreImpl(baseDir, conf);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public AtomicOutputStream createBlob(String key, SettableBlobMeta meta, Subject who)
            throws AuthorizationException, KeyAlreadyExistsException {
        if (meta.get_replication_factor() <= 0) {
            meta.set_replication_factor((int) conf.get(Config.STORM_BLOBSTORE_REPLICATION_FACTOR));
        }
        who = checkAndGetSubject(who);
        validateKey(key);
        aclHandler.normalizeSettableBlobMeta(key, meta, who, READ | WRITE | ADMIN);
        BlobStoreAclHandler.validateSettableACLs(key, meta.get_acl());
        aclHandler.hasPermissions(meta.get_acl(), READ | WRITE | ADMIN, who, key);
        if (hbs.exists(DATA_PREFIX + key)) {
            throw new WrappedKeyAlreadyExistsException(key);
        }
        BlobStoreFileOutputStream outputStream = null;
        try {
            BlobStoreFile metaFile = hbs.write(META_PREFIX + key, true);
            metaFile.setMetadata(meta);
            outputStream = new BlobStoreFileOutputStream(metaFile);
            outputStream.write(Utils.thriftSerialize(meta));
            outputStream.close();
            outputStream = null;
            BlobStoreFile dataFile = hbs.write(DATA_PREFIX + key, true);
            dataFile.setMetadata(meta);
            cacheMetas.put(key, meta);
            return new BlobStoreFileOutputStream(dataFile);
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
    public AtomicOutputStream updateBlob(String key, Subject who)
            throws AuthorizationException, KeyNotFoundException {
        who = checkAndGetSubject(who);
        SettableBlobMeta meta = extractBlobMeta(key);
        validateKey(key);
        aclHandler.hasPermissions(meta.get_acl(), WRITE, who, key);
        try {
            BlobStoreFile dataFile = hbs.write(DATA_PREFIX + key, false);
            dataFile.setMetadata(meta);
            return new BlobStoreFileOutputStream(dataFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private SettableBlobMeta getStoredBlobMeta(String key) throws KeyNotFoundException {
        InputStream in = null;
        try {
            BlobStoreFile pf = hbs.read(META_PREFIX + key);
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
            SettableBlobMeta blobMeta = Utils.thriftDeserialize(SettableBlobMeta.class, out.toByteArray());
            return blobMeta;
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
    public ReadableBlobMeta getBlobMeta(String key, Subject who)
            throws AuthorizationException, KeyNotFoundException {
        who = checkAndGetSubject(who);
        validateKey(key);
        SettableBlobMeta meta = extractBlobMeta(key);
        aclHandler.validateUserCanReadMeta(meta.get_acl(), who, key);
        ReadableBlobMeta rbm = new ReadableBlobMeta();
        rbm.set_settable(meta);
        try {
            BlobStoreFile pf = hbs.read(DATA_PREFIX + key);
            rbm.set_version(pf.getModTime());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return rbm;
    }

    /**
     * Sets leader elector (only used by LocalFsBlobStore to help sync blobs between Nimbi.
     *
     * @param leaderElector the leader elector
     */
    @Override
    public void setLeaderElector(ILeaderElector leaderElector) {
        // NO-OP
    }

    @Override
    public void setBlobMeta(String key, SettableBlobMeta meta, Subject who)
            throws AuthorizationException, KeyNotFoundException {
        if (meta.get_replication_factor() <= 0) {
            meta.set_replication_factor((int) conf.get(Config.STORM_BLOBSTORE_REPLICATION_FACTOR));
        }
        who = checkAndGetSubject(who);
        validateKey(key);
        aclHandler.normalizeSettableBlobMeta(key,  meta, who, ADMIN);
        BlobStoreAclHandler.validateSettableACLs(key, meta.get_acl());
        SettableBlobMeta orig = extractBlobMeta(key);
        aclHandler.hasPermissions(orig.get_acl(), ADMIN, who, key);
        writeMetadata(key, meta);
    }

    @Override
    public void deleteBlob(String key, Subject who)
            throws AuthorizationException, KeyNotFoundException {
        who = checkAndGetSubject(who);
        validateKey(key);
        SettableBlobMeta meta = extractBlobMeta(key);
        aclHandler.hasPermissions(meta.get_acl(), WRITE, who, key);
        try {
            hbs.deleteKey(DATA_PREFIX + key);
            hbs.deleteKey(META_PREFIX + key);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        cacheMetas.invalidate(key);
        cachedReplicationCount.invalidate(key);
    }

    @Override
    public InputStreamWithMeta getBlob(String key, Subject who)
            throws AuthorizationException, KeyNotFoundException {
        who = checkAndGetSubject(who);
        validateKey(key);
        SettableBlobMeta meta = extractBlobMeta(key);
        aclHandler.hasPermissions(meta.get_acl(), READ, who, key);
        try {
            return new BlobStoreFileInputStream(hbs.read(DATA_PREFIX + key));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Checks if a blob exists.
     *
     * @param key blobstore key
     * @param who subject
     * @throws AuthorizationException if authorization is failed
     */
    public boolean blobExists(String key, Subject who) throws AuthorizationException {
        try {
            who = checkAndGetSubject(who);
            validateKey(key);
            SettableBlobMeta meta = extractBlobMeta(key);
            aclHandler.hasPermissions(meta.get_acl(), READ, who, key);
        } catch (KeyNotFoundException e) {
            return false;
        }
        return true;
    }

    @Override
    public Iterator<String> listKeys() {
        try {
            return new KeyTranslationIterator(hbs.listKeys(), DATA_PREFIX);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void shutdown() {
        //Empty
    }

    @Override
    public int getBlobReplication(String key, Subject who) throws AuthorizationException, KeyNotFoundException {
        who = checkAndGetSubject(who);
        validateKey(key);
        SettableBlobMeta meta = extractBlobMeta(key);
        aclHandler.hasAnyPermissions(meta.get_acl(), READ | WRITE | ADMIN, who, key);
        try {
            Integer cachedCount = cachedReplicationCount.getIfPresent(key);
            int blobReplication = 0;
            if (cachedCount != null) {
                blobReplication = cachedCount.intValue();
            } else {
                blobReplication = hbs.getBlobReplication(DATA_PREFIX + key);
                cachedReplicationCount.put(key, blobReplication);
            }
            return blobReplication;
        } catch (IOException exp) {
            throw new RuntimeException(exp);
        }
    }

    private SettableBlobMeta extractBlobMeta(String key) throws KeyNotFoundException {
        if (key == null) {
            throw new WrappedKeyNotFoundException("null can not be blob key");
        }
        SettableBlobMeta meta = cacheMetas.getIfPresent(key);
        if (meta == null) {
            meta = getStoredBlobMeta(key);
            cacheMetas.put(key, meta);
        }
        return meta;
    }

    @Override
    public int updateBlobReplication(String key, int replication, Subject who) throws AuthorizationException, KeyNotFoundException {
        who = checkAndGetSubject(who);
        validateKey(key);
        SettableBlobMeta meta = extractBlobMeta(key);
        meta.set_replication_factor(replication);
        aclHandler.hasAnyPermissions(meta.get_acl(), WRITE | ADMIN, who, key);
        try {
            writeMetadata(key, meta);
            int updatedReplCount = hbs.updateBlobReplication(DATA_PREFIX + key, replication);
            cachedReplicationCount.put(key, updatedReplCount);
            return updatedReplCount;
        } catch (IOException exp) {
            throw new RuntimeException(exp);
        }
    }

    public void writeMetadata(String key, SettableBlobMeta meta)
            throws AuthorizationException, KeyNotFoundException {
        BlobStoreFileOutputStream outputStream = null;
        try {
            BlobStoreFile hdfsFile = hbs.write(META_PREFIX + key, false);
            hdfsFile.setMetadata(meta);
            outputStream = new BlobStoreFileOutputStream(hdfsFile);
            outputStream.write(Utils.thriftSerialize(meta));
            outputStream.close();
            outputStream = null;
            cacheMetas.put(key, meta);
        } catch (IOException exp) {
            throw new RuntimeException(exp);
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

    public void fullCleanup(long age) throws IOException {
        hbs.fullCleanup(age);
    }

    public long getLastBlobUpdateTime() throws IOException {
        return hbs.getLastBlobUpdateTime();
    }

    @Override
    public void updateLastBlobUpdateTime() throws IOException {
        hbs.updateLastBlobUpdateTime();
    }

    @Override
    public void validateBlobUpdateTime() throws IOException {
        hbs.validateBlobUpdateTime();
    }
}
