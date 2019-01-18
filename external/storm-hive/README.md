# Storm Hive Bolt & Trident State

  Hive offers streaming API that allows data to be written continuously into Hive. The incoming data 
  can be continuously committed in small batches of records into existing Hive partition or table. Once the data
  is committed its immediately visible to all hive queries. More info on Hive Streaming API 
  https://cwiki.apache.org/confluence/display/Hive/Streaming+Data+Ingest
  
  With the help of Hive Streaming API, HiveBolt and HiveState allows users to stream data from Storm into Hive directly.
  To use Hive streaming API users need to create a bucketed table with ORC format.  Example below
  
  ```code
  create table test_table ( id INT, name STRING, phone STRING, street STRING) partitioned by (city STRING, state STRING) stored as orc tblproperties ("orc.compress"="NONE");
  ```
  

## HiveBolt (org.apache.storm.hive.bolt.HiveBolt)

HiveBolt streams tuples directly into Hive. Tuples are written using Hive Transactions. 
Partitions to which HiveBolt will stream to can either created or pre-created or optionally
HiveBolt can create them if they are missing. Fields from Tuples are mapped to table columns.
User should make sure that Tuple field names are matched to the table column names.

```java
DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
            .withColumnFields(new Fields(colNames));
HiveOptions hiveOptions = new HiveOptions(metaStoreURI,dbName,tblName,mapper);
HiveBolt hiveBolt = new HiveBolt(hiveOptions);
```

### RecordHiveMapper
   This class maps Tuple field names to Hive table column names.
   There are two implementaitons available
 
   
   + DelimitedRecordHiveMapper (org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper)
   + JsonRecordHiveMapper (org.apache.storm.hive.bolt.mapper.JsonRecordHiveMapper)
   
   ```java
   DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
            .withColumnFields(new Fields(colNames))
            .withPartitionFields(new Fields(partNames));
    or
   DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
            .withColumnFields(new Fields(colNames))
            .withTimeAsPartitionField("YYYY/MM/DD");
   ```

|Arg | Description | Type
|--- |--- |---
|withColumnFields| field names in a tuple to be mapped to table column names | Fields (required) |
|withPartitionFields| field names in a tuple can be mapped to hive table partitions | Fields |
|withTimeAsPartitionField| users can select system time as partition in hive table| String . Date format|

### HiveOptions (org.apache.storm.hive.common.HiveOptions)
  
HiveBolt takes in HiveOptions as a constructor arg.

  ```java
  HiveOptions hiveOptions = new HiveOptions(metaStoreURI,dbName,tblName,mapper)
                                .withTxnsPerBatch(10)
                				.withBatchSize(1000)
                	     		.withIdleTimeout(10)
  ```


HiveOptions params

|Arg  |Description | Type
|---	|--- |---
|metaStoreURI | hive meta store URI (can be found in hive-site.xml) | String (required) |
|dbName | database name | String (required) |
|tblName | table name | String (required) |
|mapper| Mapper class to map Tuple field names to Table column names | DelimitedRecordHiveMapper or JsonRecordHiveMapper (required) |
|withTxnsPerBatch | Hive grants a *batch of transactions* instead of single transactions to streaming clients like HiveBolt.This setting configures the number of desired transactions per Transaction Batch. Data from all transactions in a single batch end up in a single file. HiveBolt will write a maximum of batchSize events in each transaction in the batch. This setting in conjunction with batchSize provides control over the size of each file. Note that eventually Hive will transparently compact these files into larger files.| Integer . default 100 |
|withMaxOpenConnections| Allow only this number of open connections. If this number is exceeded, the least recently used connection is closed.| Integer . default 100|
|withBatchSize| Max number of events written to Hive in a single Hive transaction| Integer. default 15000|
|withCallTimeout| (In milliseconds) Timeout for Hive & HDFS I/O operations, such as openTxn, write, commit, abort. | Integer. default 10000|
|withHeartBeatInterval| (In seconds) Interval between consecutive heartbeats sent to Hive to keep unused transactions from expiring. Set this value to 0 to disable heartbeats.| Integer. default 240 |
|withAutoCreatePartitions| HiveBolt will automatically create the necessary Hive partitions to stream to. |Boolean. default true |
|withKerberosPrinicipal| Kerberos user principal for accessing secure Hive | String|
|withKerberosKeytab| Kerberos keytab for accessing secure Hive | String |
|withTickTupleInterval| (In seconds) If > 0 then the Hive Bolt will periodically flush transaction batches. Enabling this is recommended to avoid tuple timeouts while waiting for a batch to fill up.| Integer. default 0|


 
## HiveState (org.apache.storm.hive.trident.HiveTrident)

Hive Trident state also follows similar pattern to HiveBolt it takes in HiveOptions as an arg.

```code
   DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
            .withColumnFields(new Fields(colNames))
            .withTimeAsPartitionField("YYYY/MM/DD");
            
   HiveOptions hiveOptions = new HiveOptions(metaStoreURI,dbName,tblName,mapper)
                                .withTxnsPerBatch(10)
                				.withBatchSize(1000)
                	     		.withIdleTimeout(10)
                	     		
   StateFactory factory = new HiveStateFactory().withOptions(hiveOptions);
   TridentState state = stream.partitionPersist(factory, hiveFields, new HiveUpdater(), new Fields());
```
   

##Working with Secure Hive
If your topology is going to interact with secure Hive, your bolts/states needs to be authenticated by Hive Server. We 
currently have 2 options to support this:

### Using keytabs on all worker hosts
If you have distributed the keytab files for hive user on all potential worker hosts then you can use this method. You should specify
hive configs using the methods HiveOptions.withKerberosKeytab(), HiveOptions.withKerberosPrincipal() methods.

On worker hosts the bolt/trident-state code will use the keytab file with principal provided in the config to authenticate with 
Hive. This method is little dangerous as you need to ensure all workers have the keytab file at the same location and you need
to remember this as you bring up new hosts in the cluster.


### Using Hive MetaStore delegation tokens 
Your administrator can configure nimbus to automatically get delegation tokens on behalf of the topology submitter user.
Since Hive depends on HDFS, we should also configure HDFS delegation tokens.

More details about Hadoop Tokens here: https://github.com/apache/storm/blob/master/docs/storm-hdfs.md

The nimbus should be started with following configurations:

```
nimbus.autocredential.plugins.classes : ["org.apache.storm.hive.security.AutoHive", "org.apache.storm.hdfs.security.AutoHDFS"]
nimbus.credential.renewers.classes : ["org.apache.storm.hive.security.AutoHive", "org.apache.storm.hdfs.security.AutoHDFS"]
nimbus.credential.renewers.freq.secs : 82800 (23 hours)

hive.keytab.file: "/path/to/keytab/on/nimbus" (Keytab of The Hive metastore thrift server service principal. This is used to impersonate other users.)
hive.kerberos.principal: "hive-metastore/_HOST@EXAMPLE.com" (The service principal for the metastore thrift server.)
hive.metastore.uris: "thrift://server:9083"

//hdfs configs
hdfs.keytab.file: "/path/to/keytab/on/nimbus" (This is the keytab of hdfs super user that can impersonate other users.)
hdfs.kerberos.principal: "superuser@EXAMPLE.com"
```


Your topology configuration should have:

```
topology.auto-credentials :["org.apache.storm.hive.security.AutoHive", "org.apache.storm.hdfs.security.AutoHDFS"]
```

If nimbus did not have the above configuration you need to add and then restart it. Ensure the hadoop configuration 
files (core-site.xml, hdfs-site.xml and hive-site.xml) and the storm-hive connector jar with all the dependencies is present in nimbus's classpath.

As an alternative to adding the configuration files (core-site.xml, hdfs-site.xml and hive-site.xml) to the classpath, you could specify the configurations
as a part of the topology configuration. E.g. in you custom storm.yaml (or -c option while submitting the topology),


```
hiveCredentialsConfigKeys : ["hivecluster1", "hivecluster2"] (the hive clusters you want to fetch the tokens from)
"hivecluster1": {"config1": "value1", "config2": "value2", ... } (A map of config key-values specific to cluster1)
"hivecluster2": {"config1": "value1", "hive.keytab.file": "/path/to/keytab/for/cluster2/on/nimubs", "hive.kerberos.principal": "cluster2user@EXAMPLE.com", "hive.metastore.uris": "thrift://server:9083"} (here along with other configs, we have custom keytab and principal for "cluster2" which will override the keytab/principal specified at topology level)

hdfsCredentialsConfigKeys : ["hdfscluster1", "hdfscluster2"] (the hdfs clusters you want to fetch the tokens from)
"hdfscluster1": {"config1": "value1", "config2": "value2", ... } (A map of config key-values specific to cluster1)
"hdfscluster2": {"config1": "value1", "hdfs.keytab.file": "/path/to/keytab/for/cluster2/on/nimubs", "hdfs.kerberos.principal": "cluster2user@EXAMPLE.com"} (here along with other configs, we have custom keytab and principal for "cluster2" which will override the keytab/principal specified at topology level)
```

Instead of specifying key values you may also directly specify the resource files for e.g.,

```
"cluster1": {"resources": ["/path/to/core-site1.xml", "/path/to/hdfs-site1.xml", "/path/to/hive-site1.xml"]}
"cluster2": {"resources": ["/path/to/core-site2.xml", "/path/to/hdfs-site2.xml", "/path/to/hive-site2.xml"]}
```

Storm will download the tokens separately for each of the clusters and populate it into the subject and also renew the tokens periodically. This way it would be possible to run multiple bolts connecting to separate Hive cluster within the same topology.

Nimbus will use the keytab and principal specified in the config to authenticate with Hive metastore. From then on for every
topology submission, nimbus will impersonate the topology submitter user and acquire delegation tokens on behalf of the
topology submitter user. If topology was started with topology.auto-credentials set to AutoHive, nimbus will push the
delegation tokens to all the workers for your topology and the hive bolt/state will authenticate with Hive Server using 
these tokens.

As nimbus is impersonating topology submitter user, you need to ensure the user specified in hive.kerberos.principal 
has permissions to acquire tokens on behalf of other users.
 
## Committer Sponsors
 * Sriharha Chintalapani ([sriharsha@apache.org](mailto:sriharsha@apache.org))
 * P. Taylor Goetz ([ptgoetz@apache.org](mailto:ptgoetz@apache.org))
 * Bobby Evans ([bobby@apache.org](mailto:bobby@apache.org))



 
