---
title: Binary Distributions
layout: documentation
documentation: true
---

Storm 3.0.0 ships two binary distributions. Both contain identical core functionality; they differ only in which optional plugins are bundled.

| Distribution | Filename | Approx. size |
|---|---|---|
| **Full** | `apache-storm-<version>.tar.gz` | ~395 MB of jars |
| **Lite** | `apache-storm-<version>-lite.tar.gz` | ~207 MB of jars (~47% smaller) |

## What the lite distribution omits

### storm-autocreds (−79 MB)

`storm-autocreds` provides Nimbus and Supervisor with the ability to populate and renew HDFS and HBase delegation tokens on **Kerberos-secured clusters**. It pulls in the full Hadoop and HBase client trees. The plugin is off by default and is only needed on secure Hadoop deployments.

In the lite distribution, only the `external/storm-autocreds/README` ships. To install the plugin on demand, run:

```
bin/storm-autocreds-fetch
```

This uses Maven to resolve `org.apache.storm:storm-autocreds` and its runtime dependencies from Maven Central and copies them into `extlib-daemon/`. Restart Nimbus and Supervisor afterwards.

Pass `--version` and `--dest` to override the detected Storm version or target directory. Pass `--` to forward arguments to Maven (e.g. to use an internal mirror or an offline local repository):

```
bin/storm-autocreds-fetch -- -s /path/to/settings.xml
bin/storm-autocreds-fetch -- -Dmaven.repo.local=/path/to/offline-repo -o
```

### storm-kafka-monitor (−38 MB)

`storm-kafka-monitor` displays Kafka spout consumer lag in the Storm UI and powers the `bin/storm-kafka-monitor` command. It pulls in the Kafka client library and is only needed when running Kafka spouts and wanting lag metrics. When it is absent, the UI degrades gracefully: the lag column shows an actionable message rather than failing, and the `bin/storm-kafka-monitor` wrapper prints a hint to run the fetch command.

In the lite distribution, only the `external/storm-kafka-monitor/README` ships. To install it on demand, run:

```
bin/storm-kafka-monitor-fetch
```

This resolves `org.apache.storm:storm-kafka-monitor` and its runtime dependencies into `lib-tools/storm-kafka-monitor/`. No daemon restart is required; the UI picks it up on the next request.

The same `--version`, `--dest`, and `--` passthrough options are available as for `storm-autocreds-fetch`.

## lib-common: shared jar de-duplication

Both distributions include a structural change that is transparent to users: jars that were previously duplicated across the daemon classpath (`lib/`) and the worker classpath (`lib-worker/`) are now kept in a single `lib-common/` directory. `bin/storm.py` adds `lib-common` to both classpaths, so the runtime behaviour is unchanged. Only byte-identical jars (same filename and SHA-256) were de-duplicated; no version was silently merged. This accounts for approximately 71 MB of the reduction.

## Which distribution should I use?

Use the **lite distribution** unless you specifically need one of the unbundled plugins:

- If you run topologies on a **Kerberos-secured Hadoop or HBase cluster**, run `bin/storm-autocreds-fetch` after installing from the lite distribution, or use the full distribution.
- If you want **Kafka spout lag** in the Storm UI or the `storm-kafka-monitor` command, run `bin/storm-kafka-monitor-fetch` after installing from the lite distribution, or use the full distribution.

For all other use cases — including running Kafka spout topologies without the lag UI feature — the lite distribution is sufficient and carries no functional difference from the full distribution.
