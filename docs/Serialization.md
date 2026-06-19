---
title: Serialization
layout: documentation
documentation: true
---
This page is about how the serialization system in Storm works for versions 0.6.0 and onwards. Storm used a different serialization system prior to 0.6.0 which is documented on [Serialization (prior to 0.6.0)](Serialization-\(prior-to-0.6.0\).html). 

> This page covers **tuple** serialization (data flowing between spouts and bolts). For how Storm serializes the meta state it persists in ZooKeeper and related configuration, see [Cluster State Serialization](Cluster-State-Serialization.html).

Tuples can be comprised of objects of any types. Since Storm is a distributed system, it needs to know how to serialize and deserialize objects when they're passed between tasks.

Storm uses [Kryo](https://github.com/EsotericSoftware/kryo) for serialization. Kryo is a flexible and fast serialization library that produces small serializations.

By default, Storm can serialize primitive types, strings, byte arrays, ArrayList, HashMap, and HashSet. If you want to use another type in your tuples, you'll need to register a custom serializer.

### Dynamic typing

There are no type declarations for fields in a Tuple. You put objects in fields and Storm figures out the serialization dynamically. Before we get to the interface for serialization, let's spend a moment understanding why Storm's tuples are dynamically typed.

Adding static typing to tuple fields would add large amount of complexity to Storm's API. Hadoop, for example, statically types its keys and values but requires a huge amount of annotations on the part of the user. Hadoop's API is a burden to use and the "type safety" isn't worth it. Dynamic typing is simply easier to use.

Further than that, it's not possible to statically type Storm's tuples in any reasonable way. Suppose a Bolt subscribes to multiple streams. The tuples from all those streams may have different types across the fields. When a Bolt receives a `Tuple` in `execute`, that tuple could have come from any stream and so could have any combination of types. There might be some reflection magic you can do to declare a different method for every tuple stream a bolt subscribes to, but Storm opts for the simpler, straightforward approach of dynamic typing.

Finally, another reason for using dynamic typing is so Storm can be used in a straightforward manner from dynamically typed languages like JRuby.

### Custom serialization

As mentioned, Storm uses Kryo for serialization. To implement custom serializers, you need to register new serializers with Kryo. It's highly recommended that you read over [Kryo's home page](https://github.com/EsotericSoftware/kryo) to understand how it handles custom serialization.

Adding custom serializers is done through the "topology.kryo.register" property in your topology config or through a ServiceLoader described later. The config takes a list of registrations, where each registration can take one of two forms:

1. The name of a class to register. In this case, Storm will use Kryo's `FieldsSerializer` to serialize the class. This may or may not be optimal for the class -- see the Kryo docs for more details.
2. A map from the name of a class to register to an implementation of [com.esotericsoftware.kryo.Serializer](https://github.com/EsotericSoftware/kryo/blob/master/src/com/esotericsoftware/kryo/Serializer.java).

Let's look at an example.

```
topology.kryo.register:
  - com.mycompany.CustomType1
  - com.mycompany.CustomType2: com.mycompany.serializer.CustomType2Serializer
  - com.mycompany.CustomType3
```

`com.mycompany.CustomType1` and `com.mycompany.CustomType3` will use the `FieldsSerializer`, whereas `com.mycompany.CustomType2` will use `com.mycompany.serializer.CustomType2Serializer` for serialization.

Storm provides helpers for registering serializers in a topology config. The [Config](https://javadoc.io/doc/org.apache.storm/storm-client/3.0.0/org/apache/storm/Config.html) class has a method called `registerSerialization` that takes in a registration to add to the config.

There's an advanced config called `Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS`. If you set this to true, Storm will ignore any serializations that are registered but do not have their code available on the classpath. Otherwise, Storm will throw errors when it can't find a serialization. This is useful if you run many topologies on a cluster that each have different serializations, but you want to declare all the serializations across all topologies in the `storm.yaml` files.

#### SerializationRegister Service Loader

If you want to provide language bindings to storm, have a library that you want to interact cleanly with storm or have some other reason to provide serialization bindings and don't want to force the user to update their configs you can use the org.apache.storm.serialization.SerializationRegister service loader.

You may use this like any other service loader and storm will register the bindings without forceing users to update their configs.

### Java serialization

When `Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION` is set true, if Storm encounters a type for which it doesn't have a serialization registered, it will use Java serialization if possible. If the object can't be serialized with Java serialization, then Storm will throw an error.

Beware that Java serialization is extremely expensive, both in terms of CPU cost as well as the size of the serialized object. It is highly recommended that you register custom serializers when you put the topology in production. The Java serialization behavior is there so that it's easy to prototype new topologies.

You can turn on/off the behavior to fall back on Java serialization by setting the `Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION` config to true/false. The default value is false for security reasons.

### Tuple compression

For inter-worker (remote) traffic, Storm can optionally compress serialized tuples with [Zstandard](https://facebook.github.io/zstd/) before they are sent over the network. This is intended for one specific scenario: components that emit **large** payloads to a remote worker, where the bytes saved on the wire outweigh the CPU cost of compression. A good example is a spout that emits entire lines of text to a downstream bolt running on a different worker.

Compression is **disabled by default** and follows the serialization lifecycle exactly:

- **Intra-worker (local) traffic** bypasses `KryoTupleSerializer` altogether, so it is never compressed regardless of configuration. You do not pay any CPU cost for tuples that stay inside a worker process.
- **Inter-worker (remote) traffic** is compressed only when compression is enabled for the source component *and* the serialized tuple is larger than the configured threshold. Small tuples (single words, IDs, etc.) are left uncompressed, since the framing overhead of a compressed payload can exceed the original size.

#### Enabling compression per component

Compression is controlled by the component-specific configuration `topology.tuple.compression.enable`. Because Storm merges component-specific configuration over the topology configuration, you can enable it for just the components that emit large tuples, leaving the rest of the topology untouched:

```java
TopologyBuilder builder = new TopologyBuilder();

builder.setSpout(SPOUT_ID, new FileReadSpout(inputFile), spoutNum)
       .addConfiguration(Config.TOPOLOGY_TUPLE_COMPRESSION_ENABLE, true);

builder.setBolt(SPLIT_ID, new SplitSentenceBolt(), spBoltNum)
       .localOrShuffleGrouping(SPOUT_ID);
builder.setBolt(COUNT_ID, new CountBolt(), cntBoltNum)
       .fieldsGrouping(SPLIT_ID, new Fields(SplitSentenceBolt.FIELDS));
```

You can also enable it topology-wide (or cluster-wide via `storm.yaml`) by setting `topology.tuple.compression.enable: true`, but enabling it only where large tuples are actually emitted is recommended.

#### Flux

[Flux](flux.html) supports per-component configuration. In addition to parallelism, number of tasks, memory/CPU load, and groupings, each spout and bolt definition accepts a `config:` block that `FluxBuilder` applies to the underlying declarer via `addConfigurations(...)`. This is the Flux equivalent of `declarer.addConfiguration(...)`, so you can enable compression for just the components that emit large tuples:

```yaml
spouts:
  - id: "file-read-spout"
    className: "org.apache.storm.perf.spout.FileReadSpout"
    parallelism: 1
    # enable compression for this spout only
    config:
      topology.tuple.compression.enable: true

bolts:
  - id: "split"
    className: "org.apache.storm.perf.bolt.SplitSentenceBolt"
    parallelism: 1
```

You can also enable it topology-wide by setting it in the topology-level `config:`

```yaml
config:
  topology.tuple.compression.enable: true
  topology.tuple.compression.threshold: 1460
```

Be aware that the topology-wide form enables compression for *every* remote-bound tuple in the topology that exceeds the threshold.

#### Configuration reference

| Config | Default | Description |
| --- | --- | --- |
| `topology.tuple.compression.enable` | `false` | Enables Zstd compression of serialized tuples before remote transfer. Best set per component via `addConfiguration`. |
| `topology.tuple.compression.threshold` | `1460` | Minimum serialized tuple size, in bytes, before compression is attempted. Tuples at or below this size are sent uncompressed. The default matches the typical Ethernet TCP MSS, so payloads that already fit in a single network frame are never compressed. |
| `storm.compression.zstd.level` | `3` | Zstd compression level. Supported range is 1–19; levels 20–22 (ultra mode) are prohibited because of their memory requirements. |
| `topology.tuple.compression.max.decompressed.bytes` | `10485760` (10 MB) | Upper bound on the decompressed size of a single tuple. Decompression that would exceed this limit fails, guarding against malicious or corrupt payloads. |

#### How decompression works

Compression is self-describing on the wire, so **no extra configuration is required on the receiving side**. The deserializer inspects the leading bytes of each incoming payload: if they match the Zstd magic header it decompresses the payload (bounded by `topology.tuple.compression.max.decompressed.bytes`) before deserializing, otherwise it deserializes the bytes directly. A single deserializer therefore transparently handles a mix of compressed and uncompressed tuples.

As an optimization, the deserializer determines once — when the worker starts — whether *any* component in the topology enables compression (by scanning the merged per-component configurations). If none does, the magic-header check is skipped entirely and the Zstd code path is never touched, so topologies that do not use the feature pay no per-tuple cost. The corollary is that compression must be enabled somewhere in the topology config for compressed tuples to be decompressed on receipt; since the setting is part of the topology configuration shared by all of its workers, this is always the case for tuples produced within the same topology.

#### Indicative benchmark

> **Disclaimer:** These numbers were gathered in a limited capacity while developing this feature and should be treated as a rough guide only, not as a performance guarantee. They were produced on a specific, deliberately favourable setup and your results will vary with topology shape, tuple size, network characteristics, and hardware.

The benchmark ran two equivalent word-count topologies defined in `storm-perf` — one with tuple compression enabled in Spout component (`FileReadWordCountSpoutCompressionTopo`) and one without (`FileReadWordCountTopo`) — across workers connected by a simulated network with **10 ms latency** and **0.5 ms jitter**. This does not represent a typical intra-datacenter network; it deliberately emphasizes the maximum advantage the feature can offer when configured well. The tuple size used is the smallest that still yields a real benefit from compression (~1.5 KB).

Sample round-trip ping between two supervisors on the Docker network:
```
--- cluster-supervisor2-1 ping statistics ---
5 packets transmitted, 5 received, 0% packet loss, time 4004ms
rtt min/avg/max/mdev = 18.767/24.353/42.486/9.083 ms
```

Results (compression vs. no compression):

| Metric | Compression | No compression | Difference | Better |
| --- | --- | --- | --- | --- |
| Avg transfer rate (msg/s) | 776,389 | 744,544 | +31,845 (+4.3%) | Compression |
| Peak transfer rate (msg/s) | 805,700 | 790,300 | +15,400 | Compression |
| Avg spout throughput (acks/s) | 98,167 | 92,844 | +5,323 (+5.8%) | Compression |
| Peak spout throughput (acks/s) | 100,300 | 98,666 | +1,634 | Compression |
| Avg complete latency (ms) | 362.48 | 376.73 | -14.25 (-3.8%) | Compression |
| Max complete latency (ms) | 366.44 | 385.72 | -19.28 | Compression |
| Runtime stability | More consistent | More fluctuation | — | Compression |

In this configuration, compression improved transfer rate and spout throughput by roughly 4–6% and reduced complete latency by a few percent, while also producing more consistent per-task behaviour (less jitter across tasks). The takeaway is qualitative: when large tuples cross a high-latency link, trading CPU for fewer bytes on the wire can pay off — but you should measure with your own workload before enabling it broadly.

### Component-specific serialization registrations

Storm 0.7.0 lets you set component-specific configurations (read more about this at [Configuration](Configuration.html)). Of course, if one component defines a serialization that serialization will need to be available to other bolts -- otherwise they won't be able to receive messages from that component!

When a topology is submitted, a single set of serializations is chosen to be used by all components in the topology for sending messages. This is done by merging the component-specific serializer registrations with the regular set of serialization registrations. If two components define serializers for the same class, one of the serializers is chosen arbitrarily.

To force a serializer for a particular class if there's a conflict between two component-specific registrations, just define the serializer you want to use in the topology-specific configuration. The topology-specific configuration has precedence over component-specific configurations for serialization registrations.
