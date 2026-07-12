# Local Storm cluster (cluster mode) with Docker Compose

Brings up a real, distributed Storm cluster on your machine — **dev ZooKeeper +
Nimbus + two Supervisors + UI** plus an observability stack (**Pushgateway +
Prometheus + Grafana**).

Two supervisors with **2 worker slots each (4 slots total)** are intentional: a
topology submitted with `topology.workers >= 2` lands one worker per supervisor
container, so its inter-worker tuple traffic actually crosses the network — the
only path where tuple serialization happens. The
slot count lives in `storm.yaml` (`supervisor.slots.ports`); raise it there if
you want more workers.

## Architecture

All containers share one Docker bridge network (`storm`) and resolve each other
by service name. Host-published ports are shown in `()`. The metrics plane is
detailed under [Metrics & reports](#metrics--reports-prometheus--grafana).

```text
   host:  docker compose exec nimbus  storm jar ...
                          |  submit topology
  ========================|=========== docker network: storm ==============
                          v
   +-----------+      +---------------+      +-----------+
   | ZooKeeper |<---->| Nimbus  :6627 |<---->|  UI :8080 |
   +-----------+      +-------+-------+      +-----------+
                              |  assign workers
              +---------------+----------------+
              v                                v
   +--------------------+   tuples     +--------------------+
   | supervisor1        |<============>| supervisor2        |
   |   worker :6700     | (network hop)|   worker :6700     |
   +--------------------+              +--------------------+

   metrics:  Nimbus --> Pushgateway   and   workers --> graphite-exporter,
             both scraped by  Prometheus :9090  -->  Grafana :3000
```

## Layout

| File | Purpose |
|------|---------|
| `Dockerfile` | Runtime image `FROM eclipse-temurin:25-jre`, unpacks the built dist into `/opt/storm`. |
| `Dockerfile.dockerignore` | Keeps the build context to just the dist tarball. |
| `storm.yaml` | Cluster config (ZK + Nimbus seeds + slots), bind-mounted into every daemon. |
| `docker-compose.yml` | dev ZooKeeper, Nimbus, supervisor1, supervisor2, UI, Pushgateway, graphite-exporter, Prometheus, Grafana. |
| `FileReadWordCountTopo-cluster.yaml` | Sample topology config for the smoke test below. |
| `storm-client.yaml` | Client config to submit topologies from the host (e.g. from IntelliJ). |
| `build-image.sh` | One command: rebuild the dist from current source (lib **and** lib-worker), the Docker image, and (unless `--no-extlib`) the `extlib-daemon/` jars. |
| `prepare-extlib.sh` | Builds the Prometheus reporter + deps into `extlib-daemon/` (mounted on Nimbus); run by `build-image.sh`, or standalone. |
| `netsim.sh` | Inject network delay/jitter/loss between worker hosts (tc/netem) to test the network path. |
| `prometheus/prometheus.yml` | Prometheus scrape config (Pushgateway + graphite-exporter). |
| `graphite/graphite-mapping.yml` | Maps Storm metrics-v2 Graphite names into labelled Prometheus series. |
| `grafana/` | Provisioned datasource + the **Storm Cluster** and **Storm Metrics v2** dashboards. |

## Prerequisites

> **Platform:** Linux or macOS (or Windows via WSL2). The helper scripts are
> bash and call `mvn` (not `mvn.cmd`), and `netsim.sh` relies on Linux
> `tc`/`netem`. Native Windows is not supported yet.

Build the distribution, the Docker image, **and** stage the Prometheus reporter
onto Nimbus's classpath — one command:

```bash
dev-tools/cluster/build-image.sh
```

It rebuilds `storm-client-bin` + `final-package` (so both the daemon `lib` and
the worker `lib-worker` classpaths reflect your code), then builds the
`storm-local` image. Building only `final-package -am` is **not** enough: it
leaves `lib-worker` (the worker classpath) stale, so workers run old code.

As a final step it runs `prepare-extlib.sh`, which fills `extlib-daemon/`
(git-ignored build artifacts) with the Prometheus reporter jar + runtime deps
that docker-compose mounts onto Nimbus. Pass `--no-extlib` (or set
`PREPARE_EXTLIB=0`) to skip it, or run it standalone after changing only that
module:

```bash
cd dev-tools/cluster
./prepare-extlib.sh
```

The Storm version is taken from the repo root `pom.xml` (`project.version`).
`build-image.sh` reads it and writes `dev-tools/cluster/.env`; the compose file
references it as `${STORM_VERSION}` (image tag, build arg, and the storm-perf jar
path), so everything tracks the pom automatically. To pin a different version,
run with `STORM_VERSION=x.y.z` or edit `.env`.

## Run

```bash
cd dev-tools/cluster
docker compose up --build -d      # build the image and start everything
docker compose ps                 # all services Up, zookeeper healthy
docker compose logs -f nimbus     # follow a daemon
```

| Service | URL | Notes |
|---------|-----|-------|
| Storm UI | http://localhost:8080 | topologies, workers, capacity |
| Grafana | http://localhost:3000 | login `admin` / `admin`; **Storm Cluster** + **Storm Metrics v2** dashboards |
| Prometheus | http://localhost:9090 | raw queries / targets |
| Nimbus Thrift | localhost:6627 | submit topologies from the host |

Tear down — **use `-v`** so the metrics are deleted too:

```bash
docker compose down -v
```

Prometheus and Grafana store their data on disk in the named volumes
`prometheus-data` / `grafana-data` (Prometheus retention is capped at
`--storage.tsdb.retention.time=2h` to keep them small). A plain `docker compose
down` keeps the containers' networks gone but **leaves those volumes on disk**;
`down -v` is what deletes them. The datasource and dashboards are re-provisioned
from files on the next `up`, so wiping the volumes loses only metrics history
and ad-hoc Grafana UI edits.

## Smoke test: submit a topology

The Nimbus container has the `storm-perf` jar, a sample input file and the
config mounted under `/topology`. Submit the word-count topology (runs ~120s):

```bash
docker compose exec -d nimbus \
  storm jar /topology/storm-perf.jar \
  org.apache.storm.perf.FileReadWordCountTopo 120 /topology/topo.yaml
```

Watch it in the UI, or via REST:

```bash
curl -s http://localhost:8080/api/v1/topology/summary | python3 -m json.tool
```

`FileReadWordCountTopo-cluster.yaml` sets `topology.workers: 2`, so the two
workers land on `supervisor1` and `supervisor2` — verify with the topology page
(Worker Resources) that the two workers sit on different hosts.

It is a 3-stage pipeline; spreading it across two workers makes at least one
edge a network hop (where tuple serialization happens):

```text
   FileReadSpout  --shuffle (network hop)-->  SplitSentenceBolt  --fieldsGrouping-->  CountBolt
   (emits text lines)                         (emits words)                           (counts)
```

## Simulating network latency and jitter

Inter-worker traffic between containers is near-instant (~0.05 ms), which hides
the network cost. `netsim.sh` adds realistic
latency/jitter/loss to the worker hosts with Linux `tc`/`netem`. The Storm image
has no `tc`, so the script injects the qdisc from a throwaway helper container
sharing each supervisor's network namespace — no image rebuild needed.

```bash
./netsim.sh add 50 10 0   # 50 ms delay, 10 ms jitter, 0% loss on each supervisor
./netsim.sh ping          # verify: worker<->worker RTT jumps to ~100 ms (2x egress)
./netsim.sh show          # inspect the active qdisc
./netsim.sh clear         # remove shaping
```

netem shapes **all** egress from each supervisor (inter-worker tuples *and*
heartbeats to Nimbus/ZK), so keep the delay moderate (≤ ~150 ms) or heartbeats
may time out. With both supervisors delayed by `D`, worker round-trip latency is
~`2*D`.

> **Why the script sets a huge queue `limit`.** netem's default queue is only
> 1000 packets. Under a high-throughput perf topology that buffer overflows at
> the added delay and drops tuples even with `loss 0%`, which collapses TCP and
> back-pressures the spout to **zero throughput** (you'll see `transferred 0`).
> `netsim.sh` therefore sets `limit 1000000` (override as the 4th arg) so the
> queue can hold `rate * delay` without dropping. If you ever apply `tc netem`
> by hand, remember to add a large `limit`.

## Metrics & reports (Prometheus + Grafana)

Two metric paths feed Prometheus, both push-based (so ephemeral workers need no
scrape targets), and Grafana auto-loads a dashboard for each:

```text
   Nimbus --push--> Pushgateway:9091 -----------------scrape-------------+
                                                                         v
   supervisor1 worker --+                                          Prometheus:9090 --> Grafana:3000
                        +-- graphite:9109 --> graphite-exporter:9108 --scrape--+         |
   supervisor2 worker --+                                                                +--> "Storm Cluster"
                                                                                         +--> "Storm Metrics v2"
```


1. **Cluster summary** — *Storm Cluster* dashboard
   `Nimbus → Pushgateway → Prometheus`. Nimbus runs Storm's
   `PrometheusPreparableReporter` (enabled via `-c` overrides in
   `docker-compose.yml`, jars from `extlib-daemon/`) and pushes cluster-summary
   metrics every 10s. Prometheus scrapes the Pushgateway (`honor_labels` keeps
   `job="nimbus"`).
2. **Metrics v2 (per-worker/topology)** — *Storm Metrics v2* dashboard
   `workers → graphite-exporter → Prometheus`. Every worker runs the
   `GraphiteStormReporter` (configured in `storm.yaml` under
   `topology.metrics.reporters`) and emits its full Dropwizard metric set in
   Graphite plaintext to the graphite-exporter, which `graphite-mapping.yml`
   turns into labelled `storm_worker{...}` / `storm_topology{...}` series.

The pushed series are cluster-level (not per-topology): `summary_cluster_num_supervisors`,
`summary_cluster_num_topologies`, `summary_cluster_num_total_workers`,
`summary_cluster_num_total_used_workers`, `nimbus_total_cpu`,
`nimbus_available_cpu_non_negative`, `nimbus_total_memory`, and the
`summary_topologies_assigned_*` histograms. Quick check:

```bash
curl -s 'http://localhost:9090/api/v1/query?query=summary_cluster_num_total_workers'
```

### Storm Metrics v2 dashboard

Metrics v2 are emitted **per task** (`org.apache.storm.metrics2.TaskMetrics`), so
the dashboard is filtered by a chained `topology → host → component → task`
variable set, and every series carries `topology_id`, `host`, `component`,
`task`, `port` labels.

`graphite-mapping.yml` models `TaskMetrics` explicitly into clean metrics. Each
is per `(component, task)`; the `key` label is the metric key — the **own output
stream** for emit/transfer, or the **`sourceComponent:sourceStream`** for the
input metrics (execute/ack/fail/latency):

| Prometheus metric | TaskMetrics source | type |
|---|---|---|
| `storm_emit_rate` / `storm_emit_total` | `__emit-count` (`.m1_rate` / `.count`) | RateCounter |
| `storm_transfer_rate` / `storm_transfer_total` | `__transfer-count` | RateCounter |
| `storm_execute_rate` / `storm_execute_total` | `__execute-count` | RateCounter |
| `storm_ack_rate` / `storm_ack_total` | `__ack-count` | RateCounter |
| `storm_fail_rate` / `storm_fail_total` | `__fail-count` | RateCounter |
| `storm_execute_latency_ms` | `__execute-latency` | RollingAverageGauge (ms) |
| `storm_process_latency_ms` | `__process-latency` | RollingAverageGauge (ms) |
| `storm_complete_latency_ms` | `__complete-latency` (spout) | RollingAverageGauge (ms) |
| `storm_execute_jitter_ms` | `__execute-jitter` | EwmaGauge (ms) |
| `storm_process_jitter_ms` | `__process-jitter` | EwmaGauge (ms) |
| `storm_complete_jitter_ms` | `__complete-jitter` (spout) | EwmaGauge (ms) |
| `storm_capacity` | `__capacity` (over all streams) | RollingAverageGauge (0..1) |

Counts/rates are **sampling-scaled** (`topology.stats.sample.rate`), so they
estimate true values; `.m1_rate` is tuples/s averaged over 1 minute. The
**jitter** metrics are RFC 3550 EWMA latency-variation estimators and only flow
when `topology.stats.ewma.enable: true` (set in `storm.yaml`) — pair them with
`netsim.sh` to see network jitter propagate into per-task latency variation.

Everything else falls through to generic series, still fully queryable:
- `storm_worker{metric=...}` — `__skipped-*`, `__backpressure-last-overflow-count`,
  `__send-iconnection-*`, `doHeartbeat-calls`.
- `storm_topology{component="__system"}` — per-worker JVM (`task=-1`):
  `memory.heap.*`, `memory.non-heap.*`, `memory.pools.*`, `GC.*.{count,time}`,
  `threads.*`.

List everything currently flowing:

```bash
curl -s http://localhost:9090/api/v1/label/metric/values | python3 -m json.tool
```

## Notes

- The bundled `storm dev-zookeeper` is single-node and for development only; it
  does not snapshot. Swap in a real ZooKeeper for anything beyond local testing.
- Heaps are kept small in `storm.yaml` so the whole cluster fits on a laptop.
  Bump `worker.childopts` / `*.childopts` for heavier topologies.
- To run a different topology, mount its jar into the `nimbus` service (see the
  `volumes:` of that service) and `storm jar` it the same way.
