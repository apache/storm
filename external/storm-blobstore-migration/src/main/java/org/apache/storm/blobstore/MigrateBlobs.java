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

package org.apache.storm.blobstore;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;

import org.apache.storm.Config;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.LocalFsBlobStore;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyAlreadyExistsException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.hdfs.blobstore.HdfsBlobStore;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.utils.Utils;

public class MigrateBlobs {
    
    protected static void deleteAllBlobStoreKeys(BlobStore bs, Subject who) throws AuthorizationException, KeyNotFoundException {
        Iterable<String> hdfsKeys = () -> bs.listKeys();
        for (String key : hdfsKeys) {
            System.out.println(key);
            bs.deleteBlob(key, who);
        }
    }
    
    protected static void copyBlobStoreKeys(BlobStore bsFrom,
            Subject whoFrom,
            BlobStore bsTo, Subject whoTo) throws AuthorizationException,
            KeyAlreadyExistsException,
            IOException,
            KeyNotFoundException {
        Iterable<String> lfsKeys = () -> bsFrom.listKeys();
        for (String key : lfsKeys) {
            ReadableBlobMeta readableMeta = bsFrom.getBlobMeta(key, whoFrom);
            SettableBlobMeta meta = readableMeta.get_settable();
            InputStream in = bsFrom.getBlob(key, whoFrom);
            System.out.println("COPYING BLOB " + key + " FROM " + bsFrom + " TO " + bsTo);
            bsTo.createBlob(key, in, meta, whoTo);
            System.out.println("DONE CREATING BLOB " + key);
        }
    }
    
    
    public static void main(String[] args) throws Exception {
        Map<String, Object> hdfsConf = Utils.readStormConfig();
        
        if (args.length < 2) {
            System.out.println("Need at least 2 arguments, but have " + Integer.toString(args.length));
            System.out.println("migrate <local_blobstore_dir> <hdfs_blobstore_path> <hdfs_principal> <keytab>");
            System.out.println("Migrates blobs from LocalFsBlobStore to HdfsBlobStore");
            System.out.println("Example: migrate '/srv/storm' "
                    + "'hdfs://some-hdfs-namenode:8080/srv/storm/my-storm-blobstore' "
                    + "'stormUser/my-nimbus-host.example.com@STORM.EXAMPLE.COM' '/srv/my-keytab/stormUser.kt'");
            System.exit(1);
        }

        String hdfsBlobstorePath = args[1];
        
        hdfsConf.put(Config.BLOBSTORE_DIR, hdfsBlobstorePath);
        hdfsConf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, "org.apache.storm.security.auth.DefaultPrincipalToLocal");
        if (args.length >= 3) {
            System.out.println("SETTING HDFS PRINCIPAL!");
            hdfsConf.put(Config.STORM_HDFS_LOGIN_PRINCIPAL, args[2]);
        }
        if (args.length >= 4) {
            System.out.println("SETTING HDFS KEYTAB!");
            hdfsConf.put(Config.STORM_HDFS_LOGIN_KEYTAB, args[3]);
        }
        hdfsConf.put(Config.STORM_BLOBSTORE_REPLICATION_FACTOR, 7);
        
        Map<String, Object> lfsConf = Utils.readStormConfig();
        String localBlobstoreDir = args[0];
        lfsConf.put(Config.BLOBSTORE_DIR, localBlobstoreDir);
        lfsConf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, "org.apache.storm.security.auth.DefaultPrincipalToLocal");
        

        /* CREATE THE BLOBSTORES */
        LocalFsBlobStore lfsBlobStore = new LocalFsBlobStore();
        lfsBlobStore.prepare(lfsConf, null, NimbusInfo.fromConf(lfsConf), null);
        
        HdfsBlobStore hdfsBlobStore = new HdfsBlobStore();
        hdfsBlobStore.prepare(hdfsConf, null, null, null);
        
        
        /* LOOK AT LOCAL BLOBSTORE */
        System.out.println("Listing local blobstore keys.");
        MigratorMain.listBlobStoreKeys(lfsBlobStore, null);
        System.out.println("Done listing local blobstore keys.");
        
        /* LOOK AT HDFS BLOBSTORE */
        System.out.println("Listing HDFS blobstore keys.");
        MigratorMain.listBlobStoreKeys(hdfsBlobStore, null);
        System.out.println("Done listing HDFS blobstore keys.");
        
        
        System.out.println("Going to delete everything in HDFS, then copy all local blobs to HDFS. Continue? [Y/n]");
        String resp = System.console().readLine().toLowerCase().trim();
        if (!(resp.equals("y") || resp.equals(""))) {
            System.out.println("Not copying blobs. Exiting. [" + resp.toLowerCase().trim() + "]");
            System.exit(1);
        }
        
        /* DELETE EVERYTHING IN HDFS */
        System.out.println("Deleting blobs from HDFS.");
        deleteAllBlobStoreKeys(hdfsBlobStore, null);
        System.out.println("DONE deleting blobs from HDFS.");
        
        /* COPY EVERYTHING FROM LOCAL BLOBSTORE TO HDFS */
        System.out.println("Copying local blobstore keys.");
        copyBlobStoreKeys(lfsBlobStore, null, hdfsBlobStore, null);
        System.out.println("DONE Copying local blobstore keys.");
        
        /* LOOK AT HDFS BLOBSTORE AGAIN */
        System.out.println("Listing HDFS blobstore keys.");
        MigratorMain.listBlobStoreKeys(hdfsBlobStore, null);
        System.out.println("Done listing HDFS blobstore keys.");
        
        hdfsBlobStore.shutdown();
        System.out.println("Done Migrating!");
    }
}
