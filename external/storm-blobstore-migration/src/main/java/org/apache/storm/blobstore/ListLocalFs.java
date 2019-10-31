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
import org.apache.storm.blobstore.LocalFsBlobStore;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.utils.Utils;

public class ListLocalFs {
    
    public static void main(String[] args) throws Exception {
        
        if (args.length != 1) {
            System.out.println("Need 1 arguments, but have " + Integer.toString(args.length));
            System.out.println("listLocalFs <local_blobstore_dir>");
            System.out.println("Migrates blobs from LocalFsBlobStore to HdfsBlobStore");
            System.out.println("Example: listLocalFs '/srv/storm'");
            System.exit(1);
        }
                
        Map<String, Object> lfsConf = Utils.readStormConfig();
        lfsConf.put(Config.BLOBSTORE_DIR, args[0]);
        lfsConf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, "org.apache.storm.security.auth.DefaultPrincipalToLocal");
        
        /* CREATE THE BLOBSTORE */
        LocalFsBlobStore lfsBlobStore = new LocalFsBlobStore();
        lfsBlobStore.prepare(lfsConf, null, NimbusInfo.fromConf(lfsConf), null);
        
        /* LOOK AT HDFS BLOBSTORE */
        System.out.println("Listing Local blobstore keys.");
        MigratorMain.listBlobStoreKeys(lfsBlobStore, null);
        System.out.println("Done Listing Local blobstore keys.");
        
        lfsBlobStore.shutdown();
    }
}
