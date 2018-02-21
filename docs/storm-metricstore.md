---
title: Storm Metricstore
layout: documentation
documentation: true
---
A metric store ([`MetricStore`]({{page.git-blob-base}}/storm-server/src/main/java/org/apache/storm/metricstore/MetricStore.java)) interface was added 
to Nimbus to allow storing metric information ([`Metric`]({{page.git-blob-base}}/storm-server/src/main/java/org/apache/storm/metricstore/Metric.java)) 
to a database.  The default implementation 
([`RocksDbStore`]({{page.git-blob-base}}/storm-server/src/main/java/org/apache/storm/metricstore/rocksdb/RocksDbStore.java)) is using RocksDB, 
a key-value store.

As metrics are stored in RocksDB, their string values (for topology ID and executor ID, etc.) are converted to unique integer IDs, and these strings 
are also stored to the database as metadata indexed by the integer ID. When a metric is stored, it is also aggregated with any existing metric 
within the same 1, 10, and 60 minute timeframe.  

The [`FilterOptions`]({{page.git-blob-base}}/storm-server/src/main/java/org/apache/storm/metricstore/FilterOptions.java) class provides an interface
to select which options can be used to scan the metrics.


### Configuration

The following configuation options exist for the RocksDB implementation:

```yaml
storm.metricstore.class: "org.apache.storm.metricstore.rocksdb.RocksDbStore"
storm.metricprocessor.class: "org.apache.storm.metricstore.NimbusMetricProcessor"
storm.metricstore.rocksdb.location: "storm_rocks"
storm.metricstore.rocksdb.create_if_missing: true
storm.metricstore.rocksdb.metadata_string_cache_capacity: 4000
storm.metricstore.rocksdb.retention_hours: 240
```

* storm.metricstore.class is the class that implements the 
([`MetricStore`]({{page.git-blob-base}}/storm-server/src/main/java/org/apache/storm/metricstore/MetricStore.java)).
* storm.metricprocessor.class is the class that implements the 
([`WorkerMetricsProcessor`]({{page.git-blob-base}}/storm-server/src/main/java/org/apache/storm/metricstore/WorkerMetricsProcessor.java)).
* storm.metricstore.rocksdb.location provides to location of the RocksDB database on Nimbus
* storm.metricstore.rocksdb.create_if_missing permits creating a RocksDB database if missing
* storm.metricstore.rocksdb.metadata_string_cache_capacity controls the number of metadata strings cached in memory.
* storm.metricstore.rocksdb.retention_hours sets the length of time metrics will remain active.


### RocksDB Schema

The RocksDB Key (represented by [`RocksDbKey`]({{page.git-blob-base}}/storm-server/src/main/java/org/apache/storm/metricstore/rocksdb/RocksDbKey.java))
fields are as follows:


| Field             | Size | Offset | Description                                                                                                  |
|-------------------|------|--------|--------------------------------------------------------------------------------------------------------------|
| Type              | 1    | 0      | The type maps to the KeyType enum, specifying a metric or various types of metadata strings                  |
| Aggregation Level | 1    | 1      | The aggregation level for a metric (see AggLevel enum). Set to 0 for metadata.                               |
| Topology Id       | 4    | 2      | The metadata string Id representing a topologyId for a metric, or the unique string Id for a metadata string |
| Timestamp         | 8    | 6      | The timestamp for a metric, unused for metadata                                                              |
| Metric Id         | 4    | 14     | The metadata string Id for the metric name                                                                   |
| Component Id      | 4    | 18     | The metadata string Id for the component Id                                                                  |
| Executor Id       | 4    | 22     | The metadata string Id for the executor Id                                                                   |
| Host Id           | 4    | 26     | The metadata string Id for the host Id                                                                       |
| Port              | 4    | 30     | The port number                                                                                              |
| Stream Id         | 4    | 34     | The metadata string Id for the stream Id                                                                     |


The RocksDB Value fields for metadata strings (represented by 
[`RocksDbValue`]({{page.git-blob-base}}/storm-server/src/main/java/org/apache/storm/metricstore/rocksdb/RocksDbValue.java)) are as follows:


| Field           | Size | Offset | Description                                                                            |
|-----------------|------|--------|----------------------------------------------------------------------------------------|
| Version         | 1    | 0      | The current metadata version - allows migrating if the format changes in the future    |
| Timestamp       | 8    | 1      | The time when the metadata was last used by a metric. Allows deleting of old metadata. |
| Metadata String | any  | 9      | The metadata string                                                                    |


RocksDB Value fields for metric data are as follows:

| Field   | Size | Offset | Description                                                                       |
|---------|------|--------|-----------------------------------------------------------------------------------|
| Version | 1    | 0      | The current metric version - allows migrating if the format changes in the future |
| Value   | 8    | 1      | The metric value                                                                  |
| Count   | 8    | 9      | The metric count                                                                  |
| Min     | 8    | 17     | The minimum metric value                                                          |
| Max     | 8    | 25     | The maximum metric value                                                          |
| Sum     | 8    | 33     | The sum of the metric values                                                      |


## HBaseStore implementation

[`HBaseStore`]({{page.git-blob-base}}/external/storm-hbasemetricstore/src/main/java/org/apache/storm/hbasemetricstore/HBaseStore.java) is
an alternate MetricStore implementation that can write metrics to HBase.  The Supervisors can also write metrics to
HBase instead of sending the data to Nimbus to insert by using the HBaseMetricProcessor implementation of the 
MetricProcessor.  The HBase insertion design is multithreaded, allowing a configurable number of threads to perform inserts on 
each host.

### Configuration

The following configuation options exist for the HBaseStore implementation:

```yaml
storm.metricstore.class: "org.apache.storm.hbasemetricstore.HBaseStore"
storm.metricprocessor.class: "org.apache.storm.hbasemetricstore.HBaseMetricProcessor"
storm.hbasestore.keytab.file
storm.hbasestore.principal
storm.hbasestore.hbase.site
storm.hbasestore.insertion.threads
storm.hbasestore.table.namespace
storm.hbasestore.table.name
```

* storm.hbasestore.keytab.file is the keytab used to access HBase for secure setups
* storm.hbasestore.principal is the principal to be used if accessing a secure HBase setup
* storm.hbasestore.hbase.site is the file location for the hbase-site.xml to be used
* storm.hbasestore.insertion.threads specifies the number of threads to be used for metric insertion on a specified host.  Defaults to 1.
* storm.hbasestore.table.namespace is the namespace to be used for the metrics table.  Defaults to "default".
* storm.hbasestore.table.name is the name for the metrics table.  Defaults to "metrics".


### HBase Schema

The HBase Key (represented by [`HBaseMetricKey`]({{page.git-blob-base}}/external/storm-hbasemetricstore/src/main/java/org/apache/storm/hbasemetricstore/HBaseMetricKey.java))
fields are as follows for a metric:


| Field             | Size | Offset | Description                                                                      |
|-------------------|------|--------|----------------------------------------------------------------------------------|
| Aggregation Level | 1    | 0      | The aggregation level for a metric (see AggLevel enum). Set to 0 for metadata.   |
| Topology Id       | 4    | 1      | The metadata string Id representing a topologyId for a metric                    |
| Metric Id         | 4    | 5      | The metadata string Id for the metric name                                       |
| Component Id      | 4    | 9      | The metadata string Id for the component Id                                      |
| Executor Id       | 4    | 13     | The metadata string Id for the executor Id                                       |
| Host Id           | 4    | 17     | The metadata string Id for the host Id                                           |
| Port              | 4    | 21     | The port number                                                                  |
| Stream Id         | 4    | 25     | The metadata string Id for the stream Id                                         |

A metric has the following columns in the "metricdata" column family: count, max, min, sum, value, version.  The version field is used to 
allow a thread to validate it has atomically updated an aggregated metric safely.  The other columns represent the metric
data and have an associated timestamp for each entry.

Metadata strings (topology names, hostnames, etc.) are mapped to unique ids.  This mapping is stored in HBase in the same table.  The following types 
of metadata strings exist - TOPOLOGY, STREAM_ID, HOST_ID, COMPONENT_ID, METRIC_NAME and EXECUTOR_ID.  Each of these types is a column under the "metadata"
column family.  As new strings are added for each type, a reference counter for the type is incremented, and a new string is added to the appropriate 
column.  There is currently a limitation of Integer.MAX_VALUE unique metadata strings for each type.

A table for the metric store can be created with the following sample command line:

```
> create 'metrics',{NAME=>'metricdata',VERSIONS=>2147483647},{NAME=>'metadata'}
```

Make sure to set the desired permissions.  The metricdata column family requires multiple versions to save different values for all metrics that match but the 
same key but having differing timestamps.




