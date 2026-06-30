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

## Why the jars are not bundled

Because these plugins run on the **daemon** classpath (Nimbus/Supervisor) and
pull in the full Hadoop and HBase client dependency trees, they are **not**
shipped inside the binary distribution — only secure-Hadoop deployments need
them, and bundling them would bloat the distribution for everyone. This is the
same convention used by the other `external/*` connectors.

## Installing

The plugins must be present on the **daemon** classpath, i.e. in
`$STORM_HOME/extlib-daemon` on Nimbus and the Supervisors.

### Option 1 — use the helper script (recommended)

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
