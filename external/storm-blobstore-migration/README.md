# Blobstore Migrator

## Basic Use
-----

### Build The Thing
Use make to build a tarball with everything needed.
```
$ make
```

### Use The Thing
Copy and extract the tarball
```
$ scp blobstore-migrator.tgz my-nimbus-host.example.com:~/
$ ssh my-nimbus-host.example.com
... On my-nimbus-host ...
$ tar -xvzf blobstore-migrator.tgz
```

This will expand into a blobstore-migrator directory with all the scripts and the jar.
```
$ cd blobstore-migrator
$ ls
blobstore-migrator-2.0.jar listHDFS.sh  listLocal.sh  migrate.sh
```

To run, first create a config for the cluster.
The config must be named 'config'
It must contain definitions for `HDFS_BLOBSTORE_DIR`, `LOCAL_BLOBSTORE_DIR`, and `HADOOP_CLASSPATH`.
Hadoop jars are packaged with neither storm nor this package, so they must be installed separately.

Optional configs used to configure security are: `BLOBSTORE_PRINCIPAL`, `KEYTAB_FILE`, and `JAAS_CONF`

Example:
```
$ cat config
HDFS_BLOBSTORE_DIR='hdfs://some-hdfs-namenode:8080/srv/storm/my-storm-blobstore'
LOCAL_BLOBSTORE_DIR='/srv/storm'
HADOOP_CLASSPATH='/hadoop/share/hdfs/*:/hadoop/common/*'

# My security configs: 
BLOBSTORE_PRINCIPAL='stormUser/my-nimbus-host.example.com@STORM.EXAMPLE.COM'
KEYTAB_FILE='/srv/my-keytab/stormUser.kt'
JAAS_CONF='/storm/conf/storm_jaas.conf'
```

Now you can run any of the scripts, all of which require config to exist:
 - listHDFS.sh: lists all blobs currently in the HDFS Blobstore
 - listLocal.sh: lists all blobs currently in the local Blobstore
 - migrate.sh: Begins the migration process for Nimbus. (Read instructions below first)
 
 
#### Migrating
##### Nimbus
To migrate blobs from nimbus, the following steps are necessary:

1. Shut down all Nimbus Instances
2. Backup storm config
3. Change the following settings in Nimbus' storm config:
   * blobstore.dir
   * blobstore.hdfs.principal
   * blobstore.hdfs.keytab
   * blobstore.replication.factor
   * nimbus.blobstore.class
4. Configure server so that the environment variable `STORM_EXT_CLASSPATH` includes whatever `HADOOP_CLASSPATH` contains when `storm nimbus` is run.
5. Run the migrate.sh script on the master Nimbus. It will migrate the blobs from the LocalFsBlobStore to the HdfsBlobStore, and then exit.
6. Double check to make sure the storm configs look sane, and the blobs are where they should be. (listHDFS.sh, listLocal.sh)

Once everything looks good, start the Nimbus Instances and the Nimbus BlobStore migration will be done.
 
If something goes wrong during this process, restore the config that you backed up in step 1 and then start Nimbus. Nimbus will use the Local Blobstore as before.

##### Supervisors
Supervisors can be upgraded by performing the following steps:
1. Shut down the supervisor.
2. Putting the following blobstore settings in place:
   * blobstore.dir
   * blobstore.hdfs.principal
   * blobstore.hdfs.keytab
   * blobstore.replication.factor
   * supervisor.blobstore.class
3. Kill all remaining worker processes (this is ugly)
4. Wipe the local state
5. Start the supervisor.

The reason for the hard wipe of the supervisor state is due to spurious errors during supervisor migration that were only solved by wiping out the local state. This may not be the best solution, but it does seem to work predictably.

## License

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
