package org.apache.storm.blobstore;

import java.util.Arrays;

import javax.security.auth.Subject;

public class MigratorMain {
	
	public static void listBlobStoreKeys(BlobStore bs, Subject who) {
        Iterable<String> bsKeys = () -> bs.listKeys();
        for(String key : bsKeys) {
            System.out.println(key);
        }
    }

    private static void usage() {
        System.out.println("Commands:");
        System.out.println("\tlistHDFS");
        System.out.println("\tlistLocalFs");
        System.out.println("\tmigrate");
    }
    
    public static void main(String[] args) throws Exception {
        if(args.length == 0) {
            usage();
        }
        
        if(args[0].equals("listHDFS")) {
            ListHDFS.main(Arrays.copyOfRange(args, 1, args.length));
        }
        else if(args[0].equals("listLocalFs")) {
            ListLocalFs.main(Arrays.copyOfRange(args, 1, args.length));
        }
        else if(args[0].equals("migrate")) {
            MigrateBlobs.main(Arrays.copyOfRange(args, 1, args.length));
        }
        else {
            System.out.println("Not recognized: " + args[0]);
            usage();
        }
    }
}
