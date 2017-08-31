Storm Cassandra Integration (CQL).
-------------------

[Apache Storm](https://storm.apache.org/) is a free and open source distributed realtime computation system.

## Bolt API implementation for Apache Cassandra

This library provides core storm bolt on top of Apache Cassandra.
Provides simple DSL to map storm *Tuple* to Cassandra Query Language *Statement*.


## Configuration
The following properties may be passed to storm configuration.

| **Property name**                            | **Description** | **Default**         |
| ---------------------------------------------| ----------------| --------------------|
| **cassandra.keyspace**                       | -               |                     |
| **cassandra.nodes**                          | -               | {"localhost"}       |
| **cassandra.username**                       | -               | -                   |
| **cassandra.password**                       | -               | -                   |
| **cassandra.port**                           | -               | 9092                |
| **cassandra.output.consistencyLevel**        | -               | ONE                 |
| **cassandra.batch.size.rows**                | -               | 100                 |
| **cassandra.retryPolicy**                    | -               | DefaultRetryPolicy  |
| **cassandra.reconnectionPolicy.baseDelayMs** | -               | 100 (ms)            |
| **cassandra.reconnectionPolicy.maxDelayMs**  | -               | 60000 (ms)          |
| **cassandra.pool.max.size**                  | -               | 256                 |
| **cassandra.loadBalancingPolicy**            | -               | TokenAwarePolicy    |
| **cassandra.datacenter.name**                | -               | -                   |
| **cassandra.max.requests.per.con.local**     | -               | 1024                |
| **cassandra.max.requests.per.con.remote**    | -               | 256                 |
| **cassandra.heartbeat.interval.sec**         | -               | 30                  |
| **cassandra.idle.timeout.sec**               | -               | 60                  |
| **cassandra.socket.read.timeout.millis**     | -               | 12000               |
| **cassandra.socket.connect.timeout.millis**  | -               | 5000                |

## CassandraWriterBolt

###Static import
```java

import static org.apache.storm.cassandra.DynamicStatementBuilder.*

```

### Insert Query Builder
#### Insert query including only the specified tuple fields.
```java

    new CassandraWriterBolt(
        async(
            simpleQuery("INSERT INTO album (title,year,performer,genre,tracks) VALUES (?, ?, ?, ?, ?);")
                .with(
                    fields("title", "year", "performer", "genre", "tracks")
                 )
            )
    );
```

#### Insert query including all tuple fields.
```java

    new CassandraWriterBolt(
        async(
            simpleQuery("INSERT INTO album (title,year,performer,genre,tracks) VALUES (?, ?, ?, ?, ?);")
                .with( all() )
            )
    );
```

#### Insert multiple queries from one input tuple.
```java

    new CassandraWriterBolt(
        async(
            simpleQuery("INSERT INTO titles_per_album (title,year,performer,genre,tracks) VALUES (?, ?, ?, ?, ?);").with(all())),
            simpleQuery("INSERT INTO titles_per_performer (title,year,performer,genre,tracks) VALUES (?, ?, ?, ?, ?);").with(all()))
        )
    );
```

#### Insert query using QueryBuilder
```java

    new CassandraWriterBolt(
        async(
            simpleQuery("INSERT INTO album (title,year,perfomer,genre,tracks) VALUES (?, ?, ?, ?, ?);")
                .with(all()))
            )
    )
```

#### Insert query with static bound query
```java

    new CassandraWriterBolt(
         async(
            boundQuery("INSERT INTO album (title,year,performer,genre,tracks) VALUES (?, ?, ?, ?, ?);")
                .bind(all());
         )
    );
```

#### Insert query with static bound query using named setters and aliases
```java

    new CassandraWriterBolt(
         async(
            boundQuery("INSERT INTO album (title,year,performer,genre,tracks) VALUES (:ti, :ye, :pe, :ge, :tr);")
                .bind(
                    field("ti"),as("title"),
                    field("ye").as("year")),
                    field("pe").as("performer")),
                    field("ge").as("genre")),
                    field("tr").as("tracks"))
                ).byNamedSetters()
         )
    );
```

#### Insert query with bound statement load from storm configuration
```java

    new CassandraWriterBolt(
         boundQuery(named("insertIntoAlbum"))
            .bind(all());
```

#### Insert query with bound statement load from tuple field
```java

    new CassandraWriterBolt(
         boundQuery(namedByField("cql"))
            .bind(all());
```

#### Insert query with batch statement
```java

    // Logged
    new CassandraWriterBolt(loggedBatch(
            simpleQuery("INSERT INTO titles_per_album (title,year,performer,genre,tracks) VALUES (?, ?, ?, ?, ?);").with(all())),
            simpleQuery("INSERT INTO titles_per_performer (title,year,performer,genre,tracks) VALUES (?, ?, ?, ?, ?);").with(all()))
        )
    );
// UnLogged
    new CassandraWriterBolt(unLoggedBatch(
            simpleQuery("INSERT INTO titles_per_album (title,year,performer,genre,tracks) VALUES (?, ?, ?, ?, ?);").with(all())),
            simpleQuery("INSERT INTO titles_per_performer (title,year,performer,genre,tracks) VALUES (?, ?, ?, ?, ?);").with(all()))
        )
    );
```

#### Writing using Cassandra Object Mapper

Instead of defining CQL statements by hand, it is possible to define CQL using cassandra object mapper.

In the topology we need to define what fields in the tuple will hold the operation (INSERT/DELETE) and the actual value:

```java
    new CassandraWriterBolt(new ObjectMapperCQLStatementMapperBuilder("operation", "model"))
```

Define some class using object mapper:

```java
@Table(keyspace = "my_keyspace", name = "my_table")
public class ValueObject {
    ...
}
```

And in the bolt that emits to the cassandra bolt:

```java
    collector.emit(new Values(ObjectMapperOperation.SAVE, new ValueObject("foo", "bar")));

```
##### Custom codecs

To add custom type codes to the mapping you need to define a lambda expression to work around TypeCodecs not being serializable:

```java
    new ObjectMapperCQLStatementMapperBuilder("operation", "model")
            .withCodecs(Arrays.asList(() -> new EnumNameCodec<>(MyEnum.class)));
```
                                       
### How to handle query execution results

The interface *ExecutionResultHandler* can be used to custom how an execution result should be handle.

```java
public interface ExecutionResultHandler extends Serializable {
    void onQueryValidationException(QueryValidationException e, OutputCollector collector, Tuple tuple);

    void onReadTimeoutException(ReadTimeoutException e, OutputCollector collector, Tuple tuple);

    void onWriteTimeoutException(WriteTimeoutException e, OutputCollector collector, Tuple tuple);

    void onUnavailableException(UnavailableException e, OutputCollector collector, Tuple tuple);

    void onQuerySuccess(OutputCollector collector, Tuple tuple);
}
```

By default, the CassandraBolt fails a tuple on all Cassandra Exception (see [BaseExecutionResultHandler](https://github.com/apache/storm/tree/master/external/storm-cassandra/blob/master/src/main/java/org/apache/storm/cassandra/BaseExecutionResultHandler.java)) .

```java
    new CassandraWriterBolt(insertInto("album").values(with(all()).build())
            .withResultHandler(new MyCustomResultHandler());
```

### Declare Output fields

A CassandraBolt can declare output fields / stream output fields.
For instance, this may be used to remit a new tuple on error, or to chain queries.

```java
    new CassandraWriterBolt(insertInto("album").values(withFields(all()).build())
            .withResultHandler(new EmitOnDriverExceptionResultHandler());
            .withStreamOutputFields("stream_error", new Fields("message");

    public static class EmitOnDriverExceptionResultHandler extends BaseExecutionResultHandler {
        @Override
        protected void onDriverException(DriverException e, OutputCollector collector, Tuple tuple) {
            LOG.error("An error occurred while executing cassandra statement", e);
            collector.emit("stream_error", new Values(e.getMessage()));
            collector.ack(tuple);
        }
    }
```

### Murmur3FieldGrouping

[Murmur3StreamGrouping](https://github.com/apache/storm/tree/master/external/storm-cassandra/blob/master/src/main/java/org/apache/storm/cassandra/Murmur3StreamGrouping.java)  can be used to optimise cassandra writes.
The stream is partitioned among the bolt's tasks based on the specified row partition keys.

```java
CassandraWriterBolt bolt = new CassandraWriterBolt(
    insertInto("album")
        .values(
            with(fields("title", "year", "performer", "genre", "tracks")
            ).build());
builder.setBolt("BOLT_WRITER", bolt, 4)
        .customGrouping("spout", new Murmur3StreamGrouping("title"))
```

## Trident State Support

For a state factory which writes output to Cassandra, use ```CassandraStateFactory``` with an ```INSERT INTO``` statement:

```java

        // Build state
        CQLStatementTupleMapper insertTemperatureValues = boundQuery(
                "INSERT INTO weather.temperature(weather_station_id, weather_station_name, event_time, temperature) VALUES(?, ?, ?, ?)")
                .bind(field("weather_station_id"), field("name").as("weather_station_name"), field("event_time").now(), field("temperature"))
                .build();

        CassandraState.Options options = new CassandraState.Options(new CassandraContext())
                .withCQLStatementTupleMapper(insertTemperatureValues);

        CassandraStateFactory insertValuesStateFactory =  new CassandraStateFactory(options);
        
        // Use state in existing stream
        stream.partitionPersist(insertValuesStateFactory, new Fields("weather_station_id", "name", "event_time", "temperature"), new CassandraStateUpdater());

```

For a state factory which can query Cassandra, use ```CassandraStateFactory``` with a ```SELECT``` statment:

```java

        // Build state
        CQLStatementTupleMapper selectStationName = boundQuery("SELECT name FROM weather.station WHERE id = ?")
                .bind(field("weather_station_id").as("id"))
                .build();
        CassandraState.Options options = new CassandraState.Options(new CassandraContext())
                .withCQLStatementTupleMapper(selectStationName)
                .withCQLResultSetValuesMapper(new TridentResultSetValuesMapper(new Fields("name")));
        CassandraStateFactory selectWeatherStationStateFactory = new CassandraStateFactory(options);
        
        // Append query to existing stream
        stream.stateQuery(selectWeatherStationStateFactory, new Fields("weather_station_id"), new CassandraQuery(), new Fields("name"));

```

## Trident MapState Support

For a MapState with Cassandra IBackingMap, the simplest option is to use a ```MapStateBuilder``` which generates CQL statements automatically. 
The builder supports opaque, transactional and non-transactional map states.

To store values in Cassandra you need to provide a ```StateMapper``` that maps the value to fields.  

For simple values, the ```SimpleStateMapper``` can be used:

```java
        StateFactory mapState = MapStateFactoryBuilder.opaque()
                .withTable("mykeyspace", "year_month_state")
                .withKeys("year", "month")
                .withStateMapper(SimpleStateMapper.opqaue("txid", "sum", "prevSum"))
                .build();
```

For complex values you can either custom build a state mapper, or use binary serialization:

```java
        StateFactory mapState = MapStateFactoryBuilder.opaque()
                .withTable("mykeyspace", "year_month_state")
                .withKeys("year", "month")
                .withJSONBinaryState("state")
                .build();
```

The JSONBinary methods use the storm JSON serializers, but you can also provide custom serializers if you want.

For instance, the ```NonTransactionalTupleStateMapper```, ```TransactionalTupleStateMapper``` or ```OpaqueTupleStateMapper```
classes can be used if the map state uses tuples as values.

```java
        StateFactory mapState = MapStateFactoryBuilder.<ITuple>nontransactional()
                .withTable("mykeyspace", "year_month_state")
                .withKeys("year", "month")
                .withStateMapper(new NonTransactionalTupleStateMapper("latest_value"))
                .build();
```

Alternatively, you can construct a ```CassandraMapStateFactory``` yourself:

```java

        CQLStatementTupleMapper get = simpleQuery("SELECT state FROM words_ks.words_table WHERE word = ?")
                .with(fields("word"))
                .build();

        CQLStatementTupleMapper put = simpleQuery("INSERT INTO words_ks.words_table (word, state) VALUES (?, ?)")
                .with(fields("word", "state"))
                .build();

        CassandraBackingMap.Options<Integer> mapStateOptions = new CassandraBackingMap.Options<Integer>(new CassandraContext())
                .withBatching(BatchStatement.Type.UNLOGGED)
                .withKeys(new Fields("word"))
                .withNonTransactionalJSONBinaryState("state")
                .withMultiGetCQLStatementMapper(get)
                .withMultiPutCQLStatementMapper(put);

        CassandraMapStateFactory factory = CassandraMapStateFactory.nonTransactional(mapStateOptions)
                .withCache(0);

```

### MapState Parallelism

The backing map implementation submits queries (gets and puts) in parallel to the Cassandra cluster.
The default number of parallel requests based on the driver configuration, which ends up being 128 with
default driver configuration. The maximum parallelism applies to the cluster as a whole, and to each 
state instance (per worker, not executor).

The default calculation is:
  default = min(max local, max remote) / 2
  
which normally means:
  min(1024, 256) / 2 = 128

This is deliberately conservative to avoid issues in most setups. If this does not provide sufficient 
throughput you can either explicitly override the max parallelism on the state builder/factory/backingmap, 
or you can update the driver configuration.

## License

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

## Committer Sponsors
 * Sriharha Chintalapani ([sriharsha@apache.org](mailto:sriharsha@apache.org))
 * P. Taylor Goetz ([ptgoetz@apache.org](mailto:ptgoetz@apache.org))
