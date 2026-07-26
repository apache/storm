# Storm Auto Credentials (HDFS / HBase)

`storm-autocreds` lets Storm automatically acquire, distribute and renew
**Hadoop delegation tokens** so that topologies can talk to a secure (Kerberos)
HDFS or HBase cluster without distributing keytabs to every worker host.

* On topology submission, **Nimbus** obtains delegation tokens on behalf of the
  submitting user and ships them with the topology.
* **Workers** unpack the tokens into their `Subject` / `UserGroupInformation`.
* **Nimbus** periodically renews the tokens for long-running topologies.

See `docs/SECURITY.md` ("Automatic Credentials Push and Renewal") for the full
design.

## Which distribution has the jars?

These plugins run on the **daemon** classpath (Nimbus/Supervisor) and pull in the
full Hadoop and HBase client dependency trees, which is a large amount of weight
that only secure-Hadoop deployments need. Therefore:

| Distribution | storm-autocreds jars |
|---|---|
| `apache-storm-x.x.x.tar.gz` (full) | bundled under `external/storm-autocreds` |
| `apache-storm-x.x.x-lite.tar.gz` (lite) | not bundled, README only |

If you use the **lite** distribution, install the jars with the helper script
below (or just use the full distribution).

## Installing

The plugins must be present on the **daemon** classpath, i.e. in
`$STORM_HOME/extlib-daemon` on Nimbus and the Supervisors. With the full
distribution, copy them from `external/storm-autocreds`:

```bash
cp $STORM_HOME/external/storm-autocreds/*.jar $STORM_HOME/extlib-daemon/
```

### Option 1 — use the helper script (recommended for the lite distribution)

The distribution ships a helper that resolves `storm-autocreds` and its runtime
dependencies from Maven Central and copies them into `extlib-daemon`:

```bash
$STORM_HOME/bin/storm-autocreds-fetch
```

It detects the Storm version from `$STORM_HOME/RELEASE`. Useful options:

```bash
# explicit version / target directory
bin/storm-autocreds-fetch --version 3.0.0 --dest /opt/storm/extlib-daemon

# pass extra arguments through to Maven (internal mirror / offline repo)
bin/storm-autocreds-fetch -- -s /etc/maven/settings.xml
bin/storm-autocreds-fetch -- -Dmaven.repo.local=/srv/offline-repo -o
```

Maven must be available on the host running the script (it does not have to be
installed on the cluster nodes — you can run it once and copy the resulting jars
to every daemon host).

### Option 2 — build from source

```bash
mvn -pl external/storm-autocreds -am package
cp external/storm-autocreds/target/storm-autocreds-*.jar \
   $(find ~/.m2 -name 'hadoop-auth-*.jar' -o -name 'hbase-client-*.jar') \
   $STORM_HOME/extlib-daemon/
```

(Prefer Option 1 — it resolves the complete, correct dependency closure for you.)

Restart Nimbus and the Supervisors after adding the jars so the new classpath
takes effect.

## Configuring

Add the following to `storm.yaml`. The `*Nimbus` classes run on Nimbus (acquire
and renew tokens); the non-`Nimbus` classes run in the worker (unpack tokens).

```yaml
# Worker side: unpack the tokens into the worker Subject.
topology.auto-credentials:
    - org.apache.storm.hdfs.security.AutoHDFS
    - org.apache.storm.hbase.security.AutoHBase

# Nimbus side: obtain the tokens on behalf of the submitter.
nimbus.autocredential.plugins.classes:
    - org.apache.storm.hdfs.security.AutoHDFSNimbus
    - org.apache.storm.hbase.security.AutoHBaseNimbus

# Nimbus side: renew the tokens for long-running topologies.
nimbus.credential.renewers.classes:
    - org.apache.storm.hdfs.security.AutoHDFSNimbus
    - org.apache.storm.hbase.security.AutoHBaseNimbus
```

Relevant credential settings:

| Setting | Purpose |
|---|---|
| `hdfs.keytab.file` / `hdfs.kerberos.principal` | Nimbus principal used to fetch HDFS tokens |
| `hdfs.kerberos.principal` | HDFS service principal |
| `hbase.keytab.file` / `hbase.kerberos.principal` | Nimbus principal used to fetch HBase tokens |
| `topology.hdfs.uri` | NameNode URI (defaults to the cluster `fs.defaultFS`) |
| `hdfsCredentialsConfigKeys` / `hbaseCredentialsConfigKeys` | optional list of per-cluster config keys when talking to multiple clusters |

Use only the HDFS or only the HBase entries if you need just one of them.

For the full secure-cluster setup (Kerberos, impersonation, ACLs) see
`docs/SECURITY.md`.
