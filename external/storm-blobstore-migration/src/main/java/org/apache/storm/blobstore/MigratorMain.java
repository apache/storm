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

import java.util.Arrays;

import javax.security.auth.Subject;

public class MigratorMain {

    public static void listBlobStoreKeys(BlobStore bs, Subject who) {
        Iterable<String> bsKeys = () -> bs.listKeys();
        for (String key : bsKeys) {
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
        if (args.length == 0) {
            usage();
        }
        
        if (args[0].equals("listHDFS")) {
            ListHDFS.main(Arrays.copyOfRange(args, 1, args.length));
        } else if (args[0].equals("listLocalFs")) {
            ListLocalFs.main(Arrays.copyOfRange(args, 1, args.length));
        } else if (args[0].equals("migrate")) {
            MigrateBlobs.main(Arrays.copyOfRange(args, 1, args.length));
        } else {
            System.out.println("Not recognized: " + args[0]);
            usage();
        }
    }
}
