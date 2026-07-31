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

## Recovery

Data files are made durable first, then a write-ahead log entry naming them is written under
`<table location>/metadata/_storm_wal/<topologyName>/<taskId>/` with a freshly minted commit id,
then the files are appended in a single Iceberg operation that stamps that commit id on the
snapshot summary, then the entry is deleted and the tuples are acked.

On startup a task settles whatever it left pending: it asks the table whether a snapshot carries
each entry's commit id, dropping the entry if the commit landed and re-appending the files if it
did not. The table answers the question, so no identity from the source is required — which is
exactly why the guarantee is at-least-once rather than exactly-once.

A crash before the WAL entry exists leaves orphan data files: invisible to readers, and removed by
Iceberg's `remove_orphan_files`.

A commit that *fails* is settled straight away rather than at the next startup: the sink asks the
table whether it landed. If it did — an append that reported an error but whose snapshot is
present — the tuples are acked and nothing is replayed. If it did not, the entry is dropped before
the tuples are failed, so the source's replay writes those rows exactly once and the abandoned
files become orphans. Only when the table itself is unreachable is the entry left for startup,
which is the one path that can duplicate rows.

## Maintenance

This module writes; it does not maintain. Schedule `remove_orphan_files`, `rewrite_data_files`
and `expire_snapshots` from outside the topology — streaming ingestion produces more, smaller
files and far more snapshots than batch ingestion does.
