# Storm Iceberg

Bolt for writing data to [Apache Iceberg](https://iceberg.apache.org/) tables directly from a
Storm topology — no Kafka Connect or Spark job in between — with **atomic commits and
at-least-once delivery**.

Readers never see a partial batch. Tuples are acked only once the commit containing them has
landed, so nothing is lost; a replayed batch is written again, and because the sink is append-only
and writes no equality deletes, those duplicate rows stay visible until something downstream
removes them. The target table must be append-only and format version 2.

Full documentation: [`docs/storm-iceberg.md`](../../docs/storm-iceberg.md).

## Getting the jars

Like every other `external/*` connector, `storm-iceberg` is **not bundled in either binary
distribution** — only this README ships. It is a topology-side library: the bolt runs in the
workers, and the distribution's `external/` directory is not on any classpath (see
`get_classpath()` in `bin/storm.py`).

### Option 1 — depend on it from the topology (recommended)

```xml
<dependency>
    <groupId>org.apache.storm</groupId>
    <artifactId>storm-iceberg</artifactId>
    <version>${storm.version}</version>
</dependency>
```

Shade it into the topology jar and nothing has to be installed on the cluster at all.

### Option 2 — put it on the worker classpath

If you would rather keep the Iceberg and Hadoop dependency trees out of the topology jar, the
distribution ships a helper that resolves `storm-iceberg` and its runtime dependencies from
Maven Central into `$STORM_HOME/extlib`:

```bash
$STORM_HOME/bin/storm-iceberg-fetch
```

That is `extlib`, not `extlib-daemon`: the bolt runs inside the workers, and `extlib` is on
both the worker and the daemon classpath. Run it on every host running workers.

It detects the Storm version from `$STORM_HOME/RELEASE`. Useful options:

```bash
# explicit version / target directory
bin/storm-iceberg-fetch --version 3.0.0 --dest /opt/storm/extlib

# pass extra arguments through to Maven (internal mirror / offline repo)
bin/storm-iceberg-fetch -- -s /etc/maven/settings.xml
bin/storm-iceberg-fetch -- -Dmaven.repo.local=/srv/offline-repo -o
```

Maven must be available on the host running the script (it does not have to be installed on
the cluster nodes — you can run it once and copy the resulting jars to every worker host).
Resubmit the topology afterwards so the new classpath takes effect.

Neither route brings in catalog implementations or object-store bindings, by design: writing to
S3 additionally needs `iceberg-aws` and the AWS SDK, and catalogs such as Glue, Nessie or JDBC
need their own artifacts. None of them are dependencies of this module.

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
| `withCommitIntervalRecords(int)` | 1000, when no other threshold is set | Close the batch after this many tuples |
| `withCommitIntervalBytes(long)` | disabled | Close the batch after roughly this many bytes |
| `withCommitIntervalMillis(long)` | disabled | Close the batch once it has been open this long |
| `withGroupCommitIntervalMillis(long)` | 5000 | Committer only: commit once the oldest accumulated batch is this old |
| `withGroupCommitMaxDataFiles(int)` | 1000 | Committer only: commit once this many data files have accumulated |
| `withTickIntervalSecs(int)` | none — inherits `topology.tick.tuple.freq.secs` | Per-component tick frequency |
| `withWalNamespace(String)` | none | Separates the commit WAL of deployments sharing one table |

Buffering costs latency and replay volume, not durability: buffered tuples are not acked, so a
worker that dies mid-batch has them replayed rather than losing them.

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

## Choosing a topology shape

`IcebergBolt` writes and commits in the same task. It is the simplest shape and the right one when
the sink's parallelism is low: with parallelism N committing every T seconds, the table receives
`N/T` snapshots per second, each a metadata rewrite plus a compare-and-swap on the catalog.

`IcebergWriterBolt` plus `IcebergCommitterBolt` splits those two jobs. Writers seal batches and emit
their data files, anchored to the batch's tuples; a single committer appends every writer's files in
one commit and acks. The spout still advances only after the commit is visible — the ack tree keeps
it open — but the commit cost no longer scales with parallelism, so the interval can be an order of
magnitude shorter at the same load on the catalog.

```java
builder.setBolt("iceberg-writer", new IcebergWriterBolt(writerOptions), 8)
    .fieldsGrouping("events", new Fields("region"));
builder.setBolt("iceberg-committer", new IcebergCommitterBolt(committerOptions), 1)
    .globalGrouping("iceberg-writer");
```

The committer must have a parallelism of one and a `globalGrouping`. It logs a warning if it finds
itself running with more tasks than that, because each of them would commit independently — the
very cost the split exists to remove.

**The committer needs a tick.** Its group-commit interval is only evaluated when a descriptor
arrives: after a lull, a single accumulated descriptor sits uncommitted indefinitely, and its
tuples stay un-acked with it. Set `topology.tick.tuple.freq.secs` for the topology, or
`withTickIntervalSecs` on the committer's options to give it a tick of its own. The same is true of
the writers' time-based threshold, and of `IcebergBolt`'s — each bolt declares whatever
`withTickIntervalSecs` it was given, and inherits the topology-wide setting otherwise.

Working examples of both shapes live under
[`examples/storm-iceberg-examples`](../../examples/storm-iceberg-examples/src/main/java/org/apache/storm/iceberg/examples):
`IcebergPartitionedBoltExampleTopology` for the monolithic bolt, `IcebergSplitSinkExampleTopology`
for the writer/committer split.

## Sizing

Two inequalities govern whether a topology behaves. Both are about ack latency, which with either
shape is the time from a tuple arriving to its commit becoming visible.

```
topology.message.timeout.secs > ack latency + margin
topology.max.spout.pending    ≳ target rate × ack latency
```

Violating the first replays tuples whose commit is still succeeding, so the duplicate is guaranteed
rather than merely possible. Violating the second caps throughput at
`max.spout.pending / ack latency` regardless of how fast the rest of the pipeline runs.

One thing to size for with the split sink: a descriptor carries `ContentFileParser`'s
serialization of every data file in the seal, per-column metrics included, so a wide schema sealing
many partitions at once puts hundreds of KB on the wire each seal. `groupCommitMaxDataFiles` bounds
what the committer holds; nothing bounds a single writer's seal but its own commit thresholds.

One trap worth checking: if `commitIntervalRecords` exceeds `topology.max.spout.pending`, the record
threshold can never fire, because the spout stops emitting before the batch fills. Every batch then
ends up tick-driven and the thresholds appear to be ignored.

## Metrics

| Metric | Component | Meaning |
| --- | --- | --- |
| `iceberg-records-written` | writer | records appended to open files |
| `iceberg-data-files-sealed` | writer | files closed and handed downstream |
| `iceberg-seal-latency` | writer | time to close a batch's files |
| `iceberg-data-files-committed` | committer | files made visible |
| `iceberg-bytes-committed` | committer | bytes made visible |
| `iceberg-commit-latency` | committer | time to append |
| `iceberg-commit-failures` | committer | failed appends |
| `iceberg-pending-data-files` | committer | files accumulated but not yet committed |
| `iceberg-oldest-pending-age-ms` | committer | age of the oldest batch waiting to commit |

`iceberg-oldest-pending-age-ms` is the one to alert on: a committer that has stopped making progress
shows up there long before the upstream tuples start timing out.

## Recovery

Data files are made durable first, then a write-ahead log entry naming them is written under
`<table location>/metadata/_storm_wal/<topologyName>/<componentId>/<taskIndex>/` with a freshly
minted commit id, then the files are appended in a single Iceberg operation that stamps that
commit id on the snapshot summary, then the entry is deleted and the tuples are acked.

On startup a task drops whatever it left pending, leaving those data files as orphans. An entry
only survives to startup if the task died before its commit resolved, and in that case the batch
was never acked, so the source replays it: appending the entry's files then would only add a second
copy of the rows that replay writes.

A crash before the WAL entry exists leaves orphan data files: invisible to readers, and removed by
Iceberg's `remove_orphan_files`.

A commit that *fails* is settled straight away rather than at the next startup: the sink asks the
table whether a snapshot carries the commit id. If one does — an append that reported an error but
reached the table — the tuples are acked and nothing is replayed. Otherwise they are failed and the
source replays them, leaving the abandoned files as orphans.

That look at the table is one sample, not a verdict. A REST catalog maps HTTP 5xx to
`CommitStateUnknownException` and `SnapshotProducer.commit()` retries only `CommitFailedException`,
so a commit the backend applies afterwards reads as absent and the replay writes those rows again.
Waiting longer narrows the window without closing it; duplicates are inside the contract.

## Maintenance

This module writes; it does not maintain. Schedule `remove_orphan_files`, `rewrite_data_files`
and `expire_snapshots` from outside the topology — streaming ingestion produces more, smaller
files and far more snapshots than batch ingestion does.
