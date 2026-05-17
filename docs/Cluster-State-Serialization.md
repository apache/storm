---
title: Cluster State Serialization
layout: documentation
documentation: true
---

This page describes how Storm serializes the *meta* state it persists in
ZooKeeper (and other configured state stores) such as topology assignments, Nimbus
summaries, `StormBase` records, log configs, credentials, worker heartbeats,
profile requests, errors, etc.

It is distinct from
[tuple serialization](Serialization.html), which covers payloads exchanged
between spouts and bolts at runtime via Kryo.

## Background

All cluster state writes go through `Utils.serialize(...)` /
`Utils.deserialize(...)`, which in turn delegate to a pluggable
`SerializationDelegate` selected by the
`storm.meta.serialization.delegate` config.

## Configuration

| Key | Default | Range | Description |
|---|---|---|---|
| `storm.meta.serialization.delegate` | `org.apache.storm.serialization.ZstdBridgeThriftSerializationDelegate` | any `SerializationDelegate` impl | Class used to (de)serialize cluster state. |
| `storm.compression.zstd.level` | `3` | `1`–`19` | Zstandard compression level. Higher = smaller + slower. Levels 20–22 are rejected by the validator. |
| `storm.compression.zstd.max.decompressed.bytes` | `104857600` (100 MiB) | `> 0` | Hard cap on the size of any zstd-decompressed payload. |
| `storm.compression.gzip.max.decompressed.bytes` | `104857600` (100 MiB) | `> 0` | Hard cap on the size of any gzip-decompressed payload. Also enforced by `GzipSerializationDelegate`. |

## Choosing a delegate

* **`ZstdBridgeThriftSerializationDelegate`** *(default)* — recommended.
  Writes zstd, reads anything previously written. Use this unless you
  have a specific reason not to.
* **`ZstdThriftSerializationDelegate`** — pure zstd, refuses non-zstd
  input. Only safe to deploy after every znode in your state store has
  been rewritten by a bridge delegate (e.g. by submitting / killing each
  topology, or by force-rewriting Nimbus state). Use only when you want
  to *enforce* the new format.
* **`GzipBridgeThriftSerializationDelegate`** — legacy default; still
  available for clusters that want to roll forward without touching the
  codec.
* **`ThriftSerializationDelegate`** — raw Thrift.

## Migration to Zstandard compression

Starting with Apache Storm 3.X, Zstandard is supported as the default
compression codec for cluster state, replacing gzip for better
performance — faster compression and decompression at comparable or
better ratios. Earlier versions used `GzipThriftSerializationDelegate`,
wrapped by `GzipBridgeThriftSerializationDelegate` to allow rolling
upgrades from clusters that had previously stored raw Thrift bytes; the
new `ZstdBridgeThriftSerializationDelegate` plays the equivalent bridge
role for the gzip to zstd transition.

| Area | Gzip                                                    | Zstandard                                           |
|---|---------------------------------------------------------|-----------------------------------------------------|
| Default delegate | `GzipThriftSerializationDelegate` (via `GzipBridge...`) | `ZstdBridgeThriftSerializationDelegate`             |
| Compression codec | gzip (`java.util.zip`)                                  | Zstandard (via `commons-compress` + `zstd-jni`)     |
| Decompression bound | none                                                    | bounded (`BoundedInputStream`), default 100 MiB     |
| Format detection | gzip magic only                                         | gzip magic *and* zstd magic                         |
| Config validation | none for compression                                    | `ZstdLevelValidator` (1–19), positive bounds checks |

### Zstandard `SerializationDelegate` implementations

* `ZstdThriftSerializationDelegate`: pure zstd Thrift codec. Serializes
  any `TBase` with zstd at the configured level; deserialization
  requires the input to begin with the zstd magic number
  (`0xFD2FB528`).
* `ZstdBridgeThriftSerializationDelegate`: the new default, implemented to
  allow rolling upgrades from clusters that had previously stored payloads
  as gzip-compressed. Always *writes* zstd. On read, dispatches based on a
  magic-byte sniff:

```
ZstdBridgeThriftSerializationDelegate.deserialize(bytes)
  ├── bytes starts with zstd magic (0xFD2FB528) delegates to ZstdThriftSerializationDelegate
  └── otherwise, delegates to GzipBridgeThriftSerializationDelegate.deserialize(bytes)
                                 ├── bytes starts with gzip magic (0x1F8B) delegates to GzipThriftSerializationDelegate
                                 └── otherwise delegates to ThriftSerializationDelegate (raw Thrift)
```

This delegation chain is the key property that makes the new default
rolling-upgrade safe: nodes running the new code can still read every
older payload that may already exist in ZooKeeper, while new writes use
zstd.

### Zip-bomb protection

`GzipUtils.decompress` and `ZstdUtils.decompress` (both in
`org.apache.storm.utils.Utils`) wrap the decompressor stream in an Apache
Commons `BoundedInputStream` with `maxCount` set to the configured cap.
After draining the bounded stream, the underlying decompressor is probed
with one extra `read()`; if any byte remains, the call fails with:

```
Decompression threshold exceeded! Possible security risk or invalid data size.
```

The same guard is applied to the legacy `GzipSerializationDelegate` (the
non-Thrift Java-serialization variant).

### Upgrading an existing cluster

1. **Roll Nimbus and Supervisors onto the new build.** The bridge
   delegate is the default, so no config change is required for a safe
   upgrade.
2. **(Optional) Tune `storm.compression.zstd.level`** if you want a
   tighter compression / latency trade-off. Most state writes are
   infrequent; level 3 is a good default.
3. **(Optional) Tune `storm.compression.zstd.max.decompressed.bytes`** if
   you legitimately persist payloads larger than 100 MiB. The cap
   guards against malformed or hostile data, raise it deliberately.
4. **(Optional) Switch to the strict `ZstdThriftSerializationDelegate`**
   *only* after every legacy payload has been rewritten. The bridge
   delegate is sufficient for the vast majority of deployments.

### Dependencies

The zstd codec is provided by Apache Commons Compress
(`org.apache.commons:commons-compress`) backed by the `com.github.luben:zstd-jni`
native binding.
