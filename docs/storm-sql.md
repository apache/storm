---
title: Storm SQL integration
layout: documentation
documentation: true
---

The Storm SQL integration allows users to run SQL queries over streaming data in Storm. Not only the SQL interface allows faster development cycles on streaming analytics, but also opens up the opportunities to unify batch data processing like [Apache Hive](///hive.apache.org) and real-time streaming data analytics.

At a very high level StormSQL compiles the SQL queries to [Trident](Trident-API-Overview.html) topologies and executes them in Storm clusters. This document provides information of how to use StormSQL as end users. For people that are interested in more details in the design and the implementation of StormSQL please refer to the [this](storm-sql-internal.html) page.

Storm SQL integration is an `experimental` feature, so the internal of Storm SQL and supported features are subject to change.
But small change will not affect the user experience. We will notice/announce the user when breaking UX change is introduced.

## Usage

Run the ``storm sql`` command to compile SQL statements into Trident topology, and submit it to the Storm cluster

```
$ bin/storm sql <sql-file> <topo-name>
```

In which `sql-file` contains a list of SQL statements to be executed, and `topo-name` is the name of the topology.

StormSQL activates `explain mode` and shows query plan instead of submitting topology when user specifies `topo-name` as `--explain`.
Detailed explanation is available from `Showing Query Plan (explain mode)` section.

## Supported Features

The following features are supported in the current repository:

* Streaming from and to external data sources
* Filtering tuples
* Projections
* User defined function (scalar)

Aggregations and Join are not supported by intention. When Storm SQL will support native `Streaming SQL`, these features will be introduced.    

## Specifying External Data Sources

In StormSQL data is represented by external tables. Users can specify data sources using the `CREATE EXTERNAL TABLE` statement. The syntax of `CREATE EXTERNAL TABLE` closely follows the one defined in [Hive Data Definition Language](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL):

```
CREATE EXTERNAL TABLE table_name field_list
    [ STORED AS
      INPUTFORMAT input_format_classname
      OUTPUTFORMAT output_format_classname
    ]
    LOCATION location
    [ PARALLELISM parallelism ]
    [ TBLPROPERTIES tbl_properties ]
    [ AS select_stmt ]
```

You can find detailed explanations of the properties in [Hive Data Definition Language](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL). 

`PARALLELISM` is StormSQL's own keyword which describes parallelism hint for input data source. This is same as providing parallelism hint to Trident Spout.
As same as Trident, downstream operators are executed with same parallelism before repartition (Aggregation triggers repartition).

Default value is 1, and this option is no effect on output data source. (We might change if needed. Normally repartition is the thing to avoid.)

For example, the following statement specifies a Kafka spout and sink:

```
CREATE EXTERNAL TABLE FOO (ID INT PRIMARY KEY) LOCATION 'kafka://localhost:2181/brokers?topic=test' TBLPROPERTIES '{"producer":{"bootstrap.servers":"localhost:9092","acks":"1","key.serializer":"org.apache.org.apache.storm.kafka.IntSerializer","value.serializer":"org.apache.org.apache.storm.kafka.ByteBufferSerializer"}}'
```

## Plugging in External Data Sources

Users plug in external data sources through implementing the `ISqlTridentDataSource` interface and registers them using the mechanisms of Java's service loader. The external data source will be chosen based on the scheme of the URI of the tables. Please refer to the implementation of `storm-sql-kafka` for more details.

## Specifying User Defined Function (UDF)

Users can define user defined function (scalar or aggregate) using `CREATE FUNCTION` statement.
For example, the following statement defines `MYPLUS` function which uses `org.apache.storm.sql.TestUtils$MyPlus` class.

```
CREATE FUNCTION MYPLUS AS 'org.apache.storm.sql.TestUtils$MyPlus'
```

Storm SQL determines whether the function as scalar or aggregate by checking which methods are defined.
If the class defines `evaluate` method, Storm SQL treats the function as `scalar`.

Example of class for scalar function is here:

```
  public class MyPlus {
    public static Integer evaluate(Integer x, Integer y) {
      return x + y;
    }
  }

```

## Example: Filtering Kafka Stream

Let's say there is a Kafka stream that represents the transactions of orders. Each message in the stream contains the id of the order, the unit price of the product and the quantity of the orders. The goal is to filter orders where the transactions are significant and to insert these orders into another Kafka stream for further analysis.

The user can specify the following SQL statements in the SQL file:

```
CREATE EXTERNAL TABLE ORDERS (ID INT PRIMARY KEY, UNIT_PRICE INT, QUANTITY INT) LOCATION 'kafka://localhost:2181/brokers?topic=orders'
CREATE EXTERNAL TABLE LARGE_ORDERS (ID INT PRIMARY KEY, TOTAL INT) LOCATION 'kafka://localhost:2181/brokers?topic=large_orders' TBLPROPERTIES '{"producer":{"bootstrap.servers":"localhost:9092","acks":"1","key.serializer":"org.apache.org.apache.storm.kafka.IntSerializer","value.serializer":"org.apache.org.apache.storm.kafka.ByteBufferSerializer"}}'
INSERT INTO LARGE_ORDERS SELECT ID, UNIT_PRICE * QUANTITY AS TOTAL FROM ORDERS WHERE UNIT_PRICE * QUANTITY > 50
```

The first statement defines the table `ORDER` which represents the input stream. The `LOCATION` clause specifies the ZkHost (`localhost:2181`), the path of the brokers in ZooKeeper (`/brokers`) and the topic (`orders`). 

Similarly, the second statement specifies the table `LARGE_ORDERS` which represents the output stream. The `TBLPROPERTIES` clause specifies the configuration of [KafkaProducer](http://kafka.apache.org/documentation.html#producerconfigs) and is required for a Kafka sink table. 

The third statement is a `SELECT` statement which defines the topology: it instructs StormSQL to filter all orders in the external table `ORDERS`, calculates the total price and inserts matching records into the Kafka stream specified by `LARGE_ORDER`.

To run this example, users need to include the data sources (`storm-sql-kafka` in this case) and its dependency in the
class path. Dependencies for Storm SQL are automatically handled when users run `storm sql`. Users can include data sources at the submission step like below:

```
$ bin/storm sql order_filtering.sql order_filtering --artifacts "org.apache.storm:storm-sql-kafka:2.0.0-SNAPSHOT,org.apache.storm:storm-kafka:2.0.0-SNAPSHOT,org.apache.kafka:kafka_2.10:0.8.2.2^org.slf4j:slf4j-log4j12,org.apache.kafka:kafka-clients:0.8.2.2"
```

Above command submits the SQL statements to StormSQL. Users need to modify each artifacts' version if users are using different version of Storm or Kafka. 

By now you should be able to see the `order_filtering` topology in the Storm UI.

## Showing Query Plan (explain mode)

Like `explain` on SQL statement, StormSQL provides `explain mode` when running Storm SQL Runner. In explain mode, StormSQL analyzes each query statement (only DML) and show plan instead of submitting topology.

In order to run `explain mode`, you need to provide topology name as `--explain` and run `storm sql` as same as submitting.

For example, when you run the example seen above with explain mode:
 
```
$ bin/storm sql order_filtering.sql --explain --artifacts "org.apache.storm:storm-sql-kafka:2.0.0-SNAPSHOT,org.apache.storm:storm-kafka:2.0.0-SNAPSHOT,org.apache.kafka:kafka_2.10:0.8.2.2\!org.slf4j:slf4j-log4j12,org.apache.kafka:kafka-clients:0.8.2.2"
```

StormSQL prints out like below:
 
```

===========================================================
query>
CREATE EXTERNAL TABLE ORDERS (ID INT PRIMARY KEY, UNIT_PRICE INT, QUANTITY INT) LOCATION 'kafka://localhost:2181/brokers?topic=orders' TBLPROPERTIES '{"producer":{"bootstrap.servers":"localhost:9092","acks":"1","key.serializer":"org.apache.storm.kafka.IntSerializer","value.serializer":"org.apache.storm.kafka.ByteBufferSerializer"}}'
-----------------------------------------------------------
16:53:43.951 [main] INFO  o.a.s.s.r.DataSourcesRegistry - Registering scheme kafka with org.apache.storm.sql.kafka.KafkaDataSourcesProvider@4d1bf319
No plan presented on DDL
===========================================================
===========================================================
query>
CREATE EXTERNAL TABLE LARGE_ORDERS (ID INT PRIMARY KEY, TOTAL INT) LOCATION 'kafka://localhost:2181/brokers?topic=large_orders' TBLPROPERTIES '{"producer":{"bootstrap.servers":"localhost:9092","acks":"1","key.serializer":"org.apache.storm.kafka.IntSerializer","value.serializer":"org.apache.storm.kafka.ByteBufferSerializer"}}'
-----------------------------------------------------------
No plan presented on DDL
===========================================================
===========================================================
query>
INSERT INTO LARGE_ORDERS SELECT ID, UNIT_PRICE * QUANTITY AS TOTAL FROM ORDERS WHERE UNIT_PRICE * QUANTITY > 50
-----------------------------------------------------------
plan>
LogicalTableModify(table=[[LARGE_ORDERS]], operation=[INSERT], updateColumnList=[[]], flattened=[true]), id = 8
  LogicalProject(ID=[$0], TOTAL=[*($1, $2)]), id = 7
    LogicalFilter(condition=[>(*($1, $2), 50)]), id = 6
      EnumerableTableScan(table=[[ORDERS]]), id = 5

===========================================================

```

## Current Limitations

- Windowing is yet to be implemented.
- Aggregation and join are not supported (waiting for `Streaming SQL` to be matured)
