---
title: Storm State Management
layout: documentation
documentation: true
---
# State support in core storm
Storm core has abstractions for bolts to save and retrieve the state of its operations. There is a default in-memory
based state implementation and also a Redis backed implementation that provides state persistence.

## State management
Bolts that requires its state to be managed and persisted by the framework should implement the `IStatefulBolt` interface or
extend the `BaseStatefulBolt` and implement `void initState(T state)` method. The `initState` method is invoked by the framework
during the bolt initialization with the previously saved state of the bolt. This is invoked after prepare but before the bolt starts
processing any tuples.

Currently the only kind of `State` implementation that is supported is `KeyValueState` which provides key-value mapping.

For example a word count bolt could use the key value state abstraction for the word counts as follows.

1. Extend the BaseStatefulBolt and type parameterize it with KeyValueState which would store the mapping of word to count.
2. The bolt gets initialized with its previously saved state in the init method. This will contain the word counts
last committed by the framework during the previous run.
3. In the execute method, update the word count.

```java
    public class WordCountBolt extends BaseStatefulBolt<KeyValueState<String, Long>> {
    private KeyValueState<String, Long> wordCounts;
    private OutputCollector collector;
    ...
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
          this.collector = collector;
        }
        @Override
        public void initState(KeyValueState<String, Long> state) {
          wordCounts = state;
        }
        @Override
        public void execute(Tuple tuple) {
          String word = tuple.getString(0);
          Integer count = wordCounts.get(word, 0);
          count++;
          wordCounts.put(word, count);
          collector.emit(tuple, new Values(word, count));
          collector.ack(tuple);
        }
    ...
    }
```

4. The framework periodically checkpoints the state of the bolt (default every second). The frequency
can be changed by setting the storm config `topology.state.checkpoint.interval.ms`
5. For state persistence, use a state provider that supports persistence by setting the `topology.state.provider` in the
storm config. E.g. for using Redis based key-value state implementation set `topology.state.provider: org.apache.storm.redis.state.RedisKeyValueStateProvider`
in storm.yaml. The provider implementation jar should be in the class path, which in this case means adding `storm-redis` 
to dependency of your topology, or adding `--artifacts "org.apache.storm:storm-sql-redis:<storm-version>"` when submitting your topology with `storm jar`.
6. The state provider properties can be overridden by setting `topology.state.provider.config`. For Redis state this is a
json config with the following properties.

```
{
  "keyClass": "Optional fully qualified class name of the Key type.",
  "valueClass": "Optional fully qualified class name of the Value type.",
  "keySerializerClass": "Optional Key serializer implementation class.",
  "valueSerializerClass": "Optional Value Serializer implementation class.",
  "jedisPoolConfig": {
    "host": "localhost",
    "port": 6379,
    "timeout": 2000,
    "database": 0,
    "password": "xyz"
    }
}
```
 
For Redis Cluster state this is a json config with the following properties.
 
```
 {
   "keyClass": "Optional fully qualified class name of the Key type.",
   "valueClass": "Optional fully qualified class name of the Value type.",
   "keySerializerClass": "Optional Key serializer implementation class.",
   "valueSerializerClass": "Optional Value Serializer implementation class.",
   "jedisClusterConfig": {
     "nodes": ["localhost:7379", "localhost:7380", "localhost:7381"],
     "timeout": 2000,
     "maxRedirections": 5
   }
 }
```

NOTE: If you used Redis state with Storm version 1.1.0 or earlier, you would need to also migrate your state since the representation of state has changed  
from Base64-encoded string to binary to reduce huge overhead. Storm provides a migration tool to help, which is placed on `storm-redis-example` module.

Please download the source from download page or clone the project, and type below command:

```
mvn clean install -DskipTests
cd examples/storm-redis-examples
<storm-installation-dir>/bin/storm jar target/storm-redis-examples-*.jar org.apache.storm.redis.tools.Base64ToBinaryStateMigrationUtil [options]
```

Supported options are listed here:

```
 -d,--dbnum <arg>       Redis DB number (default: 0)
 -h,--host <arg>        Redis hostname (default: localhost)
 -n,--namespace <arg>   REQUIRED the list of namespace to migrate.
 -p,--port <arg>        Redis port (default: 6379)
    --password <arg>    Redis password (default: no password)
```

You can provide multiple `namespace` options to migrate multiple namespaces at once. 
(e.g.: `--namespace total-7 --namespace partialsum-3`)
Other options are not mandatory.
Please note that you need to also migrate the key starting with "$checkpointspout-" since it's internal namespace of state. 

## Checkpoint mechanism
Checkpoint is triggered by an internal checkpoint spout at the specified `topology.state.checkpoint.interval.ms`. If there is
at-least one `IStatefulBolt` in the topology, the checkpoint spout is automatically added by the topology builder . For stateful topologies,
the topology builder wraps the `IStatefulBolt` in a `StatefulBoltExecutor` which handles the state commits on receiving the checkpoint tuples.
The non stateful bolts are wrapped in a `CheckpointTupleForwarder` which just forwards the checkpoint tuples so that the checkpoint tuples
can flow through the topology DAG. The checkpoint tuples flow through a separate internal stream namely `$checkpoint`. The topology builder
wires the checkpoint stream across the whole topology with the checkpoint spout at the root.

```
              default                         default               default
[spout1]   ---------------> [statefulbolt1] ----------> [bolt1] --------------> [statefulbolt2]
                          |                 ---------->         -------------->
                          |                   ($chpt)               ($chpt)
                          |
[$checkpointspout] _______| ($chpt)
```

At checkpoint intervals the checkpoint tuples are emitted by the checkpoint spout. On receiving a checkpoint tuple, the state of the bolt
is saved and then the checkpoint tuple is forwarded to the next component. Each bolt waits for the checkpoint to arrive on all its input
streams before it saves its state so that the state represents a consistent state across the topology. Once the checkpoint spout receives
ACK from all the bolts, the state commit is complete and the transaction is recorded as committed by the checkpoint spout.

The state checkpointing does not currently checkpoint the state of the spout. Yet, once the state of all bolts are checkpointed, and once the checkpoint tuples are acked, the tuples emitted by the spout are also acked. 
It also implies that `topology.state.checkpoint.interval.ms` is lower than `topology.message.timeout.secs`. 

The state commit works like a three phase commit protocol with a prepare and commit phase so that the state across the topology is saved
in a consistent and atomic manner.

### Recovery
The recovery phase is triggered when the topology is started for the first time. If the previous transaction was not successfully
prepared, a `rollback` message is sent across the topology so that if a bolt has some prepared transactions it can be discarded.
If the previous transaction was prepared successfully but not committed, a `commit` message is sent across the topology so that
the prepared transactions can be committed. After these steps are complete, the bolts are initialized with the state.

The recovery is also triggered if one of the bolts fails to acknowledge the checkpoint message or say a worker crashed in
the middle. Thus when the worker is restarted by the supervisor, the checkpoint mechanism makes sure that the bolt gets
initialized with its previous state and the checkpointing continues from the point where it left off.

### Guarantee
Storm relies on the acking mechanism to replay tuples in case of failures. It is possible that the state is committed
but the worker crashes before acking the tuples. In this case the tuples are replayed causing duplicate state updates.
Also currently the StatefulBoltExecutor continues to process the tuples from a stream after it has received a checkpoint
tuple on one stream while waiting for checkpoint to arrive on other input streams for saving the state. This can also cause
duplicate state updates during recovery.

The state abstraction does not eliminate duplicate evaluations and currently provides only at-least once guarantee.

In order to provide the at-least once guarantee, all bolts in a stateful topology are expected to anchor the tuples 
while emitting and ack the input tuples once its processed. For non-stateful bolts, the anchoring/acking can be automatically 
managed by extending the `BaseBasicBolt`. Stateful bolts are expected to anchor tuples while emitting and ack the tuple 
after processing like in the `WordCountBolt` example in the State management section above.

### IStateful bolt hooks
IStateful bolt interface provides hook methods where in the stateful bolts could implement some custom actions.

```java
    /**
     * This is a hook for the component to perform some actions just before the
     * framework commits its state.
     */
    void preCommit(long txid);

    /**
     * This is a hook for the component to perform some actions just before the
     * framework prepares its state.
     */
    void prePrepare(long txid);

    /**
     * This is a hook for the component to perform some actions just before the
     * framework rolls back the prepared state.
     */
    void preRollback();
```

This is optional and stateful bolts are not expected to provide any implementation. This is provided so that other
system level components can be built on top of the stateful abstractions where we might want to take some actions before the
stateful bolt's state is prepared, committed or rolled back.

## Providing custom state implementations
Currently the only kind of `State` implementation supported is `KeyValueState` which provides key-value mapping.

Custom state implementations should provide implementations for the methods defined in the `org.apache.storm.State` interface.
These are the `void prepareCommit(long txid)`, `void commit(long txid)`, `rollback()` methods. `commit()` method is optional
and is useful if the bolt manages the state on its own. This is currently used only by the internal system bolts,
for e.g. the CheckpointSpout to save its state.

`KeyValueState` implementation should also implement the methods defined in the `org.apache.storm.state.KeyValueState` interface.

### State provider
The framework instantiates the state via the corresponding `StateProvider` implementation. A custom state should also provide
a `StateProvider` implementation which can load and return the state based on the namespace. Each state belongs to a unique namespace.
The namespace is typically unique per task so that each task can have its own state. The StateProvider and the corresponding
State implementation should be available in the class path of Storm (by placing them in the extlib directory).


### Supported State Backends

#### Redis

* State provider class name (`topology.state.provider`)

`org.apache.storm.redis.state.RedisKeyValueStateProvider`

* Provider config (`topology.state.provider.config`)

```
 {
   "keyClass": "Optional fully qualified class name of the Key type.",
   "valueClass": "Optional fully qualified class name of the Value type.",
   "keySerializerClass": "Optional Key serializer implementation class.",
   "valueSerializerClass": "Optional Value Serializer implementation class.",
   "jedisPoolConfig": {
     "host": "localhost",
     "port": 6379,
     "timeout": 2000,
     "database": 0,
     "password": "xyz"
   }
 }
 ```
 
* Artifacts to add (`--artifacts`)

`org.apache.storm:storm-redis:<storm-version>`

#### HBase

In order to make state scalable, HBaseKeyValueState stores state KV to a row. This introduces `non-atomic` commit phase and guarantee 
eventual consistency on HBase side. It doesn't matter in point of state's view because HBaseKeyValueState can still provide not-yet-committed value.
Even if worker crashes at commit phase, after restart it will read pending-commit states (stored atomically) from HBase and states will be stored eventually. 

NOTE: HBase state provider uses pre-created table and column family, so users need to create and provide one to the provider config.

You can simply create table via `create 'state', 'cf'` in `hbase shell` but in production you may want to give some more properties.

* State provider class name (`topology.state.provider`)

`org.apache.storm.hbase.state.HBaseKeyValueStateProvider`

* Provider config (`topology.state.provider.config`)
        
```
 {
   "keyClass": "Optional fully qualified class name of the Key type.",
   "valueClass": "Optional fully qualified class name of the Value type.",
   "keySerializerClass": "Optional Key serializer implementation class.",
   "valueSerializerClass": "Optional Value Serializer implementation class.",
   "hbaseConfigKey": "config key to load hbase configuration from storm root configuration. (similar to storm-hbase)",
   "tableName": "Pre-created table name for state.",
   "columnFamily": "Pre-created column family for state."
 }
 ```

If you want to initialize HBase state provider from codebase, please see below example:

```
Config conf = new Config();
    Map<String, Object> hbConf = new HashMap<String, Object>();
    hbConf.put("hbase.rootdir", "file:///tmp/hbase");
    conf.put("hbase.conf", hbConf);
    conf.put("topology.state.provider",  "org.apache.storm.hbase.state.HBaseKeyValueStateProvider");
    conf.put("topology.state.provider.config", "{" +
            "   \"hbaseConfigKey\": \"hbase.conf\"," +
            "   \"tableName\": \"state\"," +
            "   \"columnFamily\": \"cf\"" +
            " }");
```

* Artifacts to add (`--artifacts`)

`org.apache.storm:storm-hbase:<storm-version>`