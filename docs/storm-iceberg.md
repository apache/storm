---
title: Storm Apache Iceberg Integration
layout: documentation
documentation: true
---

Bolt for writing data to [Apache Iceberg](https://iceberg.apache.org/) tables directly from a
Storm topology — no Kafka Connect or Spark job in between — with **atomic commits and
at-least-once delivery**.

## Guarantees

Read this before anything else; it is the part that decides whether this module fits.

- **Atomic commits.** A batch becomes visible in one Iceberg append, or not at all. Readers never
  see part of a batch, and a crash costs orphan data files rather than a broken table.
- **At-least-once delivery.** Tuples are acked only after the commit containing them has landed,
  so nothing is silently lost. A batch that fails is replayed by the source and written again.
- **Duplicates are possible and are not removed.** The sink appends; it writes no equality
  deletes. Rows from a replayed batch stay visible until something downstream deduplicates them.

This module does **not** promise exactly-once. Exactly-once would need a deterministic identity of
the input — the same batch content under the same identifier on replay — and that comes from the
source, not from the sink. A general-purpose module cannot assume every user has a replayable,
deterministically addressed source, so the claim is not made. An extractor SPI for sources that
can do better is a possible future addition, not a current guarantee.

The target must be an **append-only, format-version-2** Iceberg table. Row-level deletes,
upserts, and merge-on-read are out of scope.

## Usage

```java
Map<String, String> catalogProps = new HashMap<>();
catalogProps.put("type", "rest");
catalogProps.put("uri", "http://rest-catalog:8181");

IcebergOptions options = new IcebergOptions.Builder()
    .withCatalogProperties(catalogProps)
    .withTable("db.events")
    .withCommitIntervalBytes(128L * 1024 * 1024)
    .build();

TopologyBuilder builder = new TopologyBuilder();
builder.setSpout("events", spout, 2);
builder.setBolt("iceberg", new IcebergBolt(options), 4)
    .shuffleGrouping("events");

Config conf = new Config();
// Bounds how long a partial batch waits when the stream goes quiet.
conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 30);
```

The catalog properties are passed verbatim to Iceberg's `CatalogUtil.buildIcebergCatalog(...)`,
so every Iceberg catalog works with its standard configuration keys: `type` = `hive`, `hadoop`,
`rest`, or `catalog-impl` for Glue, Nessie, JDBC, etc. The catalog implementations themselves are
**not** pulled in transitively — see [Dependencies](#dependencies).

### Options

| Option | Default | Description |
|---|---|---|
| `withCatalogProperties(Map)` | required | Iceberg catalog configuration |
| `withTable(String)` | required | Target table identifier, e.g. `db.events` |
| `withRecordMapper(RecordMapper)` | `FieldNameRecordMapper` | Tuple → `Record` conversion |
| `withFileFormat(FileFormat)` | `PARQUET` | Data file format |
| `withTargetFileSizeBytes(long)` | table property `write.target-file-size-bytes` | Rolling file size |
| `withAutoCreate(Schema, PartitionSpec)` | disabled | Create the table on first use if missing |
| `withCommitIntervalRecords(int)` | 1000, when no other threshold is set | Close the batch after this many tuples |
| `withCommitIntervalBytes(long)` | disabled | Close the batch after roughly this many bytes |
| `withCommitIntervalMillis(long)` | disabled | Close the batch once it has been open this long |

### Batch sizing

Committing every tuple would mean one snapshot and one small file per tuple, which degrades
catalog and reader planning quickly. The bolt therefore accumulates tuples and commits them
together, closing the batch when the first configured threshold is crossed. If you configure none,
it falls back to `withCommitIntervalRecords(1000)` so a batch can never wait indefinitely.

**Buffering costs latency and replay volume, not durability.** Buffered tuples are not acked, so a
worker that dies mid-batch has them replayed rather than losing them. The trade-off to weigh is
how much work a crash repeats, and how long rows wait before becoming visible — not whether they
survive.

Configure `Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS` as well. `withCommitIntervalMillis` is evaluated
when the next tuple arrives, so on a stream that stops entirely only a tick tuple can close the
final partial batch.

Set `topology.max.spout.pending` comfortably above the number of tuples one batch accumulates: a
batch's tuples stay un-acked until it commits, so too low a value stalls the topology.

### Tuple mapping

By default tuple fields are matched to table columns **by name** (`FieldNameRecordMapper`):
numeric values are widened to the column type, `Instant` / `java.util.Date` / epoch-millis
`Long` values are converted for timestamp columns, `byte[]` becomes `ByteBuffer`. A required
column with no tuple value fails the topology loudly. For anything custom (structs, lists,
renames), implement `RecordMapper`:

```java
public interface RecordMapper extends Serializable {
    Record map(ITuple tuple, Schema schema);
}
```

The mapper takes `ITuple`, which both a bolt's `Tuple` and a `TridentTuple` satisfy, so a mapper
can be shared if you also write to Iceberg from elsewhere.

### Partitioned tables

Partitioned tables need no extra configuration: the writer derives each record's partition from
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
spec stored in the catalog wins. A batch spread over many partitions opens many files at once —
group the stream by the partition columns (`fieldsGrouping`) to keep file counts down.

## How a commit is made recoverable

The window between "the data files are durable" and "the table references them" is the only place
a crash can do damage, and a write-ahead log closes it.

1. The batch's data files are closed and become durable. Nothing is visible to readers yet.
2. A WAL entry naming those files is written under
   `<table location>/metadata/_storm_wal/<topologyName>/<taskId>/`, through the table's own
   `FileIO`, carrying a freshly minted commit id. It lives with the table, not on worker-local
   disk, so a task relaunched on another host still finds it.
3. The files are appended in a single Iceberg operation that stamps that commit id on the
   resulting snapshot's summary (`storm.iceberg.commit-id`).
4. The WAL entry is deleted, and only then are the tuples acked.

On startup, before writing anything new, each task settles whatever its previous incarnation left
behind. For every pending entry it asks the table whether a snapshot carries that commit id: if
one does, the commit landed and the entry is simply dropped; if none does, the commit never landed
and the data files — still durable — are appended again. The table itself answers the question, so
no identity from the source is needed.

### When a commit fails

A failed commit is resolved immediately, while the batch is still in hand, rather than left to the
next startup. The sink asks the table whether the commit landed:

- **It landed** — the append reported an error but the snapshot carries the commit id, the classic
  `CommitStateUnknownException`. The batch is visible, so its tuples are acked and the WAL entry
  is dropped. No replay, no duplicates.
- **It did not land** — the WAL entry is dropped *before* the tuples are failed. The source
  replays them and they are written exactly once; the abandoned data files become orphans. Were
  the entry left in place, the next startup would append those files as well, duplicating the rows
  the replay had already written.
- **The table cannot be reached** — the outcome is genuinely unknown, so the entry is left for
  startup to settle. This is the only path that can produce duplicate rows from a failed commit.

Note what the WAL does and does not protect. It protects the *reference* to durable data, which is
why atomic commits survive a crash. It does not give exactly-once: a batch that failed before its
WAL entry existed is replayed from the source and written afresh, duplicates included.

A crash before step 2 leaves orphan data files. They are invisible to readers, and cleaned up by
Iceberg's standard `remove_orphan_files` maintenance.

## Table maintenance is still your job

This module writes; it does not maintain. A production table needs, on a schedule and from
outside the topology:

- **`remove_orphan_files`** — reclaims files left by crashes and failed batches.
- **`rewrite_data_files`** (compaction) — streaming ingestion produces more, smaller files than
  batch ingestion, whatever the commit interval.
- **`expire_snapshots`** — one snapshot per commit adds up quickly.

Skipping these degrades read planning and grows storage cost over time. Iceberg ships these as
catalog procedures and as Spark actions; use whichever your platform already runs.

## Metrics

The bolt registers these metrics-v2 metrics per task, so they show up in whatever reporter the
cluster is configured with:

| Metric | Type | Meaning |
|---|---|---|
| `iceberg-records-written` | counter | Records handed to the Iceberg writer |
| `iceberg-data-files-committed` | counter | Data files made visible by commits |
| `iceberg-bytes-committed` | counter | Bytes in those data files |
| `iceberg-commit-latency` | timer | Duration of the Iceberg commit |
| `iceberg-commit-failures` | counter | Commits that did **not** become visible |

The counters follow the outcome, not the exception: an append that reported an error but whose
data is visible counts under `iceberg-data-files-committed`, not `iceberg-commit-failures`. Every
increment of `iceberg-commit-failures` therefore corresponds to tuples that were failed and
replayed.

A steadily rising `iceberg-data-files-committed` with a flat `iceberg-bytes-committed` is the
small-files signature: raise the commit interval, or schedule compaction.

## Table refresh

The table metadata is refreshed between batches, so schema and partition spec evolution performed
outside the topology is picked up without restarting the workers. Every file within one commit is
written against the same metadata.

## Dependencies

`storm-iceberg` is **not bundled in the binary distributions**, in line with the direction of
[#8819](https://github.com/apache/storm/pull/8819): it is a topology-side library, and the
distribution's `external/` directory is on no classpath.

It depends on the Iceberg core and format artifacts only. **Catalog implementations are not pulled
in transitively** — a topology writing through Glue, Nessie, or JDBC adds that catalog itself, and
one writing to S3 adds `iceberg-aws` and the AWS SDK. This keeps the dependency footprint of the
module small and puts the choice of object-store and catalog bindings where it belongs.

The Iceberg version is pinned explicitly rather than tracking the newest release, so the module
stays buildable against Storm's Java baseline.

## Examples

`examples/storm-iceberg-examples` contains two runnable topologies writing to a local Hadoop
catalog: `IcebergBoltExampleTopology` (unpartitioned) and `IcebergPartitionedBoltExampleTopology`
(partitioned by `identity(region)` and `days(event_time)`).

## Caveats

- The commit WAL needs a `FileIO` that supports prefix listing. Iceberg's `HadoopFileIO`,
  `S3FileIO` and `ResolvingFileIO` all do; an exotic custom `FileIO` may not.
- Each task commits independently. Iceberg resolves concurrent append commits with optimistic
  retries (tune with the table property `commit.retry.num-retries`), but beyond roughly 10–20
  concurrent writers consider reducing the bolt's parallelism.
- If the table cannot be reached at all when a commit fails, the outcome stays unknown and the WAL
  entry is left for the next startup to settle. That is the one case where a commit may be
  replayed on top of tuples the source also replayed, producing duplicate rows. It is the
  deliberate choice: replaying a commit is recoverable, losing one is not.
