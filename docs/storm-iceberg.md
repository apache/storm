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
| `withGroupCommitIntervalMillis(long)` | 5000 | `IcebergCommitterBolt` only: commit once the oldest accumulated batch is this old |
| `withGroupCommitMaxDataFiles(int)` | 1000 | `IcebergCommitterBolt` only: commit once this many data files have accumulated |
| `withTickIntervalSecs(int)` | none — inherits `topology.tick.tuple.freq.secs` | Per-component tick frequency |
| `withWalNamespace(String)` | none | Separates the commit WAL of deployments sharing one table |

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

## Splitting the sink: writer and committer

`IcebergBolt` writes and commits in the same task, so a sink at parallelism N committing every T
seconds produces `N/T` snapshots per second — each a metadata rewrite plus a compare-and-swap on
the catalog. Past a certain parallelism the only way to keep that load down is to commit less
often, which is to say to accept more latency.

`IcebergWriterBolt` and `IcebergCommitterBolt` break that coupling. Writers seal batches on the
same thresholds `IcebergBolt` uses, but instead of committing they emit one descriptor tuple
carrying the batch's data files, **anchored to every tuple of the batch**, and then ack those
tuples. Anchoring is what preserves the guarantee: acking an input after emitting an anchored child
does not close the spout tuple's ack tree, so the source still advances only once the descriptor
itself is acked. A single committer accumulates descriptors from every writer, appends them all in
one Iceberg commit, and acks. Commit cost stops scaling with the sink's parallelism.

```java
builder.setBolt("iceberg-writer", new IcebergWriterBolt(writerOptions), 8)
    .fieldsGrouping("events", new Fields("region"));
builder.setBolt("iceberg-committer", new IcebergCommitterBolt(committerOptions), 1)
    .globalGrouping("iceberg-writer");
```

The committer must run at a parallelism of one behind a `globalGrouping`. It logs a warning if it
finds more than one task of its own component, because each task would commit independently — the
cost the split exists to remove.

The trade-offs against the monolithic bolt:

- **Wider failure blast radius.** A failed append fails every writer's batch in the group, not one
  task's. The delivery guarantee is unchanged — at-least-once, atomic commits — but a single
  catalog hiccup replays more work.
- **The committer is a single point of throughput.** It handles one tuple per writer seal, carrying
  file metadata rather than data, so this is rarely the constraint; liveness is what to watch, via
  `iceberg-oldest-pending-age-ms`.
- **Descriptors are not small.** They serialize each data file through Iceberg's
  `ContentFileParser`, per-column metrics included, so a wide schema sealing many partitions at once
  puts hundreds of KB on the wire per seal. `withGroupCommitMaxDataFiles` bounds what the committer
  accumulates; nothing bounds a single writer's seal but its own commit thresholds.

**The committer needs a tick tuple.** `withGroupCommitIntervalMillis` is evaluated only when a
descriptor arrives, so after a lull one accumulated descriptor sits uncommitted indefinitely, its
tuples un-acked with it, until either another descriptor or a tick arrives. Configure
`Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS` for the topology, or `withTickIntervalSecs` on the
committer's own options. The same applies to the writers' `withCommitIntervalMillis` and to
`IcebergBolt`'s; each of the three bolts declares whatever `withTickIntervalSecs` it was given.

## How a commit is made recoverable

The window between "the data files are durable" and "the table references them" is the only place
a crash can do damage, and a write-ahead log closes it.

1. The batch's data files are closed and become durable. Nothing is visible to readers yet.
2. A WAL entry naming those files is written under
   `<table location>/metadata/_storm_wal/<topologyName>/<componentId>/<taskIndex>/`, through the
   table's own `FileIO`, carrying a freshly minted commit id. It lives with the table, not on
   worker-local disk, so a task relaunched on another host still finds it.
3. The files are appended in a single Iceberg operation that stamps that commit id on the
   resulting snapshot's summary (`storm.iceberg.commit-id`).
4. The WAL entry is deleted, and only then are the tuples acked.

The WAL belongs to whichever component commits: `IcebergBolt`, or `IcebergCommitterBolt` in the
split sink. `IcebergWriterBolt` writes no WAL entries, because it makes nothing visible.

On startup, before writing anything new, each committing task clears whatever its previous
incarnation left behind. Every pending entry is dropped and its data files are left as orphans. An
entry only survives to startup if the task died before its commit resolved, and in that case the
batch was never acked, so the source replays it: appending the entry's files here would add a
second copy of the rows that replay writes. For a reliable source this path can only add
duplicates, never prevent loss — which is why a failed commit is settled while the batch is still
in hand instead.

### When a commit fails

A failed commit is resolved immediately, while the batch is still in hand, rather than left to the
next startup. The sink asks the table whether the commit landed:

- **A snapshot carries the commit id** — the append reported an error but reached the table, the
  classic `CommitStateUnknownException`. The batch is visible, so its tuples are acked and nothing
  is replayed.
- **No snapshot carries it** — the tuples are failed, the source replays them, and the abandoned
  data files become orphans.
- **The table cannot be reached** — the outcome stays unknown, and the tuples are failed.

The second answer is one sample, not a verdict. A REST catalog maps HTTP 500, 502, 503 and 504 to
`CommitStateUnknownException`, and `SnapshotProducer.commit()` retries only `CommitFailedException`,
so a commit the backend applies *after* the sink has looked reads as absent. The replay then writes
those rows a second time. Waiting longer narrows that window without closing it, so the sink does
not wait: duplicates are inside the contract this module offers.

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
| `iceberg-data-files-sealed` | counter | Data files closed and handed downstream (`IcebergWriterBolt`) |
| `iceberg-seal-latency` | timer | Duration of closing a batch's files (`IcebergWriterBolt`) |
| `iceberg-pending-data-files` | gauge | Files accumulated but not yet committed (`IcebergCommitterBolt`) |
| `iceberg-oldest-pending-age-ms` | gauge | Age of the oldest batch waiting to commit (`IcebergCommitterBolt`) |

`iceberg-oldest-pending-age-ms` is the one to alert on: a committer that has stopped making
progress shows up there long before its upstream tuples start timing out. Both gauges keep
reporting for the duration of an in-flight commit, which is when a stalled catalog shows up.

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

`examples/storm-iceberg-examples` contains three runnable topologies writing to a local Hadoop
catalog: `IcebergBoltExampleTopology` (unpartitioned), `IcebergPartitionedBoltExampleTopology`
(partitioned by `identity(region)` and `days(event_time)`), and `IcebergSplitSinkExampleTopology`
(the writer/committer split).

## Caveats

- The commit WAL needs a `FileIO` that supports prefix listing. Iceberg's `HadoopFileIO`,
  `S3FileIO` and `ResolvingFileIO` all do; an exotic custom `FileIO` may not.
- With `IcebergBolt`, each task commits independently. Iceberg resolves concurrent append commits
  with optimistic retries (tune with the table property `commit.retry.num-retries`), but beyond
  roughly 10–20 concurrent writers consider reducing the bolt's parallelism or moving to the
  writer/committer split.
- A commit whose outcome the catalog leaves unknown is settled by a single look at the table. A
  backend that applies the commit after that look — an HTTP 504 in front of a REST catalog, say —
  leaves the replay to write those rows a second time.
- The WAL lives under the table, keyed by topology name, component id and task index. Two clusters
  running a same-named topology against one table would share a WAL path and clear each other's
  entries, so give each deployment its own `withWalNamespace(...)`.
