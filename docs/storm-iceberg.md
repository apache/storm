---
title: Storm Apache Iceberg Integration
layout: documentation
documentation: true
---

Trident state implementation for writing data to [Apache Iceberg](https://iceberg.apache.org/) tables
directly from a Storm topology — no Kafka Connect or Spark job in between — with exactly-once semantics.

## Usage

```java
Map<String, String> catalogProps = new HashMap<>();
catalogProps.put("type", "rest");
catalogProps.put("uri", "http://rest-catalog:8181");

IcebergOptions options = new IcebergOptions.Builder()
    .withCatalogProperties(catalogProps)
    .withTable("db.events")
    .build();

TridentTopology topology = new TridentTopology();
topology.newStream("spout", spout)
    .partitionPersist(new IcebergStateFactory(options),
        new Fields("id", "name", "ts"),
        new IcebergStateUpdater());
```

The catalog properties are passed verbatim to Iceberg's `CatalogUtil.buildIcebergCatalog(...)`,
so every Iceberg catalog works with its standard configuration keys: `type` = `hive`, `hadoop`,
`rest`, or `catalog-impl` for Glue, Nessie, JDBC, etc.

### Options

| Option | Default | Description |
|---|---|---|
| `withCatalogProperties(Map)` | required | Iceberg catalog configuration |
| `withTable(String)` | required | Target table identifier, e.g. `db.events` |
| `withRecordMapper(RecordMapper)` | `FieldNameRecordMapper` | Tuple → `Record` conversion |
| `withFileFormat(FileFormat)` | `PARQUET` | Data file format |
| `withTargetFileSizeBytes(long)` | table property `write.target-file-size-bytes` | Rolling file size |
| `withAutoCreate(Schema, PartitionSpec)` | disabled | Create the table on first use if missing |
| `withCommitIntervalBytes(long)` | disabled | Buffer batches until this many bytes, then commit once |
| `withCommitIntervalMillis(long)` | disabled | Flush the buffered window once it is this old |

### Tuple mapping

By default tuple fields are matched to table columns **by name** (`FieldNameRecordMapper`):
numeric values are widened to the column type, `Instant` / `java.util.Date` / epoch-millis
`Long` values are converted for timestamp columns, `byte[]` becomes `ByteBuffer`. A required
column with no tuple value fails the topology loudly. For anything custom (structs, lists,
renames), implement `RecordMapper`:

```java
public interface RecordMapper extends Serializable {
    Record map(TridentTuple tuple, Schema schema);
}
```

### Partitioned tables

Partitioned tables need no extra configuration: the state derives each record's partition from
the table's current `PartitionSpec` and keeps one open data file per partition (Iceberg's fanout
writer), so a single batch can span any number of partitions.

```java
Schema schema = new Schema(
    Types.NestedField.required(1, "id", Types.LongType.get()),
    Types.NestedField.required(2, "region", Types.StringType.get()),
    Types.NestedField.required(3, "event_time", Types.TimestampType.withZone()));

PartitionSpec spec = PartitionSpec.builderFor(schema)
    .identity("region")
    .day("event_time")
    .build();

IcebergOptions options = new IcebergOptions.Builder()
    .withCatalogProperties(catalogProps)
    .withTable("db.events")
    .withAutoCreate(schema, spec)
    .build();
```

The `PartitionSpec` above is only used when the table is auto-created; for an existing table the
spec stored in the catalog wins. Note that a batch spread over many partitions opens many files
at once — partition the stream on the partition columns (`partitionBy`) to keep file counts down.

### Commit batching (trades exactly-once for fewer snapshots)

By default **every Trident batch is one Iceberg commit and one snapshot**. On a high-rate stream
with small batches that produces thousands of snapshots and as many small files, which degrades
metastore and reader planning.

`withCommitIntervalBytes` and `withCommitIntervalMillis` buffer batches and commit them together:

```java
IcebergOptions options = new IcebergOptions.Builder()
    .withCatalogProperties(catalogProps)
    .withTable("db.events")
    .withCommitIntervalBytes(128L * 1024 * 1024)
    .withCommitIntervalMillis(60_000)
    .build();
```

The writer stays open across the buffered batches, so data files also grow towards
`write.target-file-size-bytes` instead of being one-file-per-batch. Whichever threshold is
reached first flushes the window; setting neither keeps the default one-commit-per-batch.

> **This weakens the delivery guarantee, and cannot be made not to.** Trident treats a batch as
> delivered the moment `commit()` returns. Batches that are buffered have already been
> acknowledged, so if the worker dies before the flush **they are lost** — not duplicated, lost.
> Use it only where losing the last window on a crash is acceptable, and leave both settings
> unset when you need the exactly-once guarantee described below.

Two further consequences of buffering:

- A batch that fails after earlier batches were buffered forces the whole window to be dropped,
  since an open writer cannot un-write the failed attempt's records. Watch
  `iceberg-windows-dropped`.
- The time threshold is evaluated when the *next* batch is committed, so a stream that stops
  entirely leaves its last window uncommitted until data resumes. This matches how
  `TimedRotationPolicy` behaves for `HdfsState`; Trident delivers no tick tuples to states and
  skips empty batches, so there is no in-state timer that could do better without introducing
  concurrency on the commit path.

### Metrics

The state registers these metrics-v2 metrics per `partitionPersist` task, so they show up in
whatever reporter the cluster is configured with:

| Metric | Type | Meaning |
|---|---|---|
| `iceberg-records-written` | counter | Records handed to the Iceberg writer |
| `iceberg-data-files-committed` | counter | Data files made visible by commits |
| `iceberg-bytes-committed` | counter | Bytes in those data files |
| `iceberg-commit-latency` | timer | Duration of the Iceberg commit transaction |
| `iceberg-commit-failures` | counter | Commits that failed (including unknown state) |
| `iceberg-batches-skipped` | counter | Replayed batches skipped as already committed |
| `iceberg-batches-buffered` | counter | Batches held back waiting for the commit threshold |
| `iceberg-windows-dropped` | counter | Buffered windows discarded; their batches were lost |

A steadily rising `iceberg-data-files-committed` with a flat `iceberg-bytes-committed` is the
small-files signature: consider fewer, larger batches or a compaction job.

### Table refresh

Each batch refreshes the table metadata before writing, so schema and partition spec evolution
performed outside the topology is picked up without restarting the workers. This costs one
catalog round-trip per batch on top of the commit's own.

### Resource cleanup

Trident's `State` has no shutdown callback, so the state registers a JVM shutdown hook to close
the catalog (REST/HTTP client, Hive metastore connection, object store client) when the worker
exits.

## Examples

`examples/storm-iceberg-examples` contains two runnable topologies writing to a local Hadoop
catalog: `IcebergTridentExampleTopology` (unpartitioned) and
`IcebergPartitionedTridentExampleTopology` (partitioned by `identity(region)` and
`days(event_time)`).

## Exactly-once semantics

Each Trident batch is committed to Iceberg with a single atomic `Transaction` that both appends
the batch's data files and records the transaction id in the table property
`storm.trident.<topologyName>.<partitionIndex>.last-committed-txid`. When Trident replays a
batch whose txid is already recorded, the state skips it. Because the txid marker and the data
commit are atomic, this stays correct across worker crashes, replays, and commits whose outcome
was unknown to the writer.

### Caveats

- Exactly-once requires a **transactional spout** (the same txid must carry the same batch
  content on replay), exactly as for `HdfsState`.
- **Resubmitting a topology from scratch** (fresh transactional state in ZooKeeper) restarts
  Trident txids from low values, so markers from the previous run would wrongly skip batches.
  Resubmit under a new topology name, or clear the `storm.trident.<topologyName>.*` table
  properties first.
- Each state partition performs its own Iceberg commit per batch. Iceberg resolves concurrent
  append commits with optimistic retries (tune with the table property
  `commit.retry.num-retries`), but beyond roughly 10–20 partitions consider reducing the
  parallelism of the `partitionPersist`.
- Batches that fail before commit can leave never-committed (invisible) data files behind.
  Standard Iceberg maintenance (`remove_orphan_files`) cleans them up.
