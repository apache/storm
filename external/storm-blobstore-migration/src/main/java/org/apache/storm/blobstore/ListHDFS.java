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

import java.util.Map;

import javax.security.auth.Subject;

import org.apache.storm.Config;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.hdfs.blobstore.HdfsBlobStore;
import org.apache.storm.hdfs.blobstore.HdfsClientBlobStore;
import org.apache.storm.utils.Utils;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class ListHDFS {
    
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Need at least 1 argument (hdfs_blobstore_path), but have " + Integer.toString(args.length));
            System.out.println("listHDFS <hdfs_blobstore_path> <hdfs_principal> <keytab>");
            System.out.println("Lists blobs in HdfsBlobStore");
            System.out.println("Example: listHDFS "
                    + "'hdfs://some-hdfs-namenode:8080/srv/storm/my-storm-blobstore' "
                    + "'stormUser/my-nimbus-host.example.com@STORM.EXAMPLE.COM' '/srv/my-keytab/stormUser.kt'");
            System.exit(1);
        }
        
        Map<String, Object> hdfsConf = Utils.readStormConfig();
        String hdfsBlobstorePath = args[0];
        
        hdfsConf.put(Config.BLOBSTORE_DIR, hdfsBlobstorePath);
        hdfsConf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, "org.apache.storm.security.auth.DefaultPrincipalToLocal");
        if (args.length >= 2) {
            System.out.println("SETTING HDFS PRINCIPAL!");
            hdfsConf.put(Config.STORM_HDFS_LOGIN_PRINCIPAL, args[1]);
        }
        if (args.length >= 3) {
            System.out.println("SETTING HDFS KEYTAB!");
            hdfsConf.put(Config.STORM_HDFS_LOGIN_KEYTAB, args[2]);
        }
        
        /* CREATE THE BLOBSTORES */
        HdfsBlobStore hdfsBlobStore = new HdfsBlobStore();
        hdfsBlobStore.prepare(hdfsConf, null, null, null);
        
        /* LOOK AT HDFS BLOBSTORE */
        System.out.println("Listing HDFS blobstore keys.");
        MigratorMain.listBlobStoreKeys(hdfsBlobStore, null);
        System.out.println("Done Listing HDFS blobstore keys.");
        
        hdfsBlobStore.shutdown();
    }
}
