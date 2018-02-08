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

The following configuation options exist:

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



