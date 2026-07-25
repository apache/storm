---
title: Local Development Cluster
layout: documentation
documentation: true
---

Storm ships a Docker Compose configuration under `docker/` that provisions a fully distributed cluster on a single machine. Unlike [local mode](Local-mode.html), this setup forces inter-worker traffic across real sockets, making it suitable for benchmarking serialization, compression, and network-sensitive behaviours that only appear with genuine inter-process communication.

## What's included

- **Nimbus**, **ZooKeeper**, and two **Supervisors** on an isolated Docker network
- **Prometheus** scraping Storm's Metrics V2 endpoint
- **Grafana** with a pre-built per-task dashboard covering throughput, latency, and jitter
- **`netsim.sh`** — a utility that injects controlled latency and jitter between Supervisors using `tc netem`

## Starting the cluster

```
cd docker/
docker compose up -d
```

Once healthy, Grafana is available at `http://localhost:3000` (default credentials: `admin` / `admin`). The Storm UI is at `http://localhost:8080`.

## Submitting a topology

The `storm-perf` module contains topologies designed for this environment:

```
storm jar storm-perf/target/storm-perf-*.jar \
  org.apache.storm.perf.FileReadWordCountTopo \
  -c nimbus.seeds='["nimbus"]' \
  -c storm.zookeeper.servers='["zookeeper"]'
```

Any topology JAR works. Point `nimbus.seeds` and `storm.zookeeper.servers` at the Docker hostnames as shown above.

## Network simulation

`netsim.sh` wraps `tc netem` to shape traffic between the two Supervisor containers:

```
# Add 3 ms latency and 1 ms jitter (typical intra-datacenter profile)
./netsim.sh apply 3ms 1ms

# Remove all shaping
./netsim.sh reset

# Show current tc settings on both supervisors
./netsim.sh status

# Round-trip ping between supervisors
./netsim.sh ping
```

Changes take effect immediately and are visible in the Grafana latency panels in real time.

## Teardown

```
docker compose down
```

Persistent volumes are not configured; all topology data and metrics are lost on teardown. This cluster is intended for development and benchmarking, not production use.
