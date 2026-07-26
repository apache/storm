# Storm Iceberg

Trident state implementation for writing data to [Apache Iceberg](https://iceberg.apache.org/) tables
directly from a Storm topology — no Kafka Connect or Spark job in between — with exactly-once semantics.

## Getting the jars

Like every other `external/*` connector, `storm-iceberg` is **not bundled in either binary
distribution** — only this README ships. It is a topology-side library: the state runs in the
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

That is `extlib`, not `extlib-daemon`: the state runs inside the workers, and `extlib` is on
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

Note that neither route brings in the object-store bindings: writing to S3 additionally needs
`iceberg-aws` and the AWS SDK, which are not dependencies of this module.

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
