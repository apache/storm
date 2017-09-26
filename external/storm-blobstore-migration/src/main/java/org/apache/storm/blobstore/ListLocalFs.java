package org.apache.storm.blobstore;

import java.util.Map;

import javax.security.auth.Subject;

import org.apache.storm.Config;
import org.apache.storm.blobstore.LocalFsBlobStore;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.utils.Utils;

public class ListLocalFs {
    
    public static void main(String[] args) throws Exception {
        
        if(args.length != 1) {
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
        System.out.println("Creating Local Blobstore.");
        LocalFsBlobStore lfsBlobStore = new LocalFsBlobStore();
        lfsBlobStore.prepare(lfsConf, null, NimbusInfo.fromConf(lfsConf));
        System.out.println("Done.");
        
        /* LOOK AT HDFS BLOBSTORE */
        System.out.println("Listing Local blobstore keys.");
        MigratorMain.listBlobStoreKeys(lfsBlobStore, null);
        System.out.println("Done.");
        
        lfsBlobStore.shutdown();
        System.out.println("Done.");
    }
}
