---
title: Storm SQL integration
layout: documentation
documentation: true
---

The Storm SQL integration allows users to run SQL queries over streaming data in Storm. Not only the SQL interface allows faster development cycles on streaming analytics, but also opens up the opportunities to unify batch data processing like [Apache Hive](///hive.apache.org) and real-time streaming data analytics.

At a very high level StormSQL compiles the SQL queries to Storm topologies leveraging Streams API and executes them in Storm clusters. This document provides information of how to use StormSQL as end users. For people that are interested in more details in the design and the implementation of StormSQL please refer to [this](storm-sql-internal.html) page.

Storm SQL integration is an `experimental` feature, so the internal of Storm SQL and supported features are subject to change.
But small change will not affect the user experience. We will notice/announce the user when breaking UX change is introduced.

## Usage

Run the ``storm sql`` command to compile SQL statements into Storm topology, and submit it to the Storm cluster

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

Aggregations and Join are not supported for now. When Storm SQL will support native `Streaming SQL`, these features will be introduced.

Please be aware that as Storm uses [Apache Calcite](///calcite.apache.org) to parse the supplied SQL, you will likely benefit from skimming the Calcite documentation, in particular the section on [identifiers](https://calcite.apache.org/docs/reference.html#identifiers).   

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

`PARALLELISM` is StormSQL's own keyword which describes parallelism hint for input data source. This is same as providing parallelism hint to Spout.
Downstream operators are executed with same parallelism before repartition (Aggregation triggers repartition).

Default value is 1, and this option is no effect on output data source. (We might change if needed. Normally repartition is the thing to avoid.)

For example, the following statement specifies a Kafka spout and sink:

```
CREATE EXTERNAL TABLE FOO (ID INT PRIMARY KEY) LOCATION 'kafka://test?bootstrap-servers=localhost:9092' TBLPROPERTIES '{"producer":{"acks":"1","key.serializer":"org.apache.storm.kafka.IntSerializer"}}'
```

## Plugging in External Data Sources

Users plug in external data sources through implementing the `ISqlStreamsDataSource` interface and registers them using the mechanisms of Java's service loader. The external data source will be chosen based on the scheme of the URI of the tables. Please refer to the implementation of `storm-sql-kafka` for more details.

## Specifying User Defined Function (UDF)

Users can define user defined function (scalar) using `CREATE FUNCTION` statement.
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
CREATE EXTERNAL TABLE ORDERS (ID INT PRIMARY KEY, UNIT_PRICE INT, QUANTITY INT) LOCATION 'kafka://orders?bootstrap-servers=localhost:9092,localhost:9093'
CREATE EXTERNAL TABLE LARGE_ORDERS (ID INT PRIMARY KEY, TOTAL INT) LOCATION 'kafka://large_orders?bootstrap-servers=localhost:9092,localhost:9093' TBLPROPERTIES '{"producer":{"acks":"1","key.serializer":"org.apache.storm.kafka.IntSerializer"}}'
INSERT INTO LARGE_ORDERS SELECT ID, UNIT_PRICE * QUANTITY AS TOTAL FROM ORDERS WHERE UNIT_PRICE * QUANTITY > 50
```

The first statement defines the table `ORDER` which represents the input stream. The `LOCATION` clause specifies the Kafka bootstrap servers (`localhost:9092,localhost:9093`) and the topic (`orders`). 

Similarly, the second statement specifies the table `LARGE_ORDERS` which represents the output stream. The `TBLPROPERTIES` clause specifies the configuration of [KafkaProducer](http://kafka.apache.org/documentation.html#producerconfigs) and is required for a Kafka sink table. 

The third statement is a `SELECT` statement which defines the topology: it instructs StormSQL to filter all orders in the external table `ORDERS`, calculates the total price and inserts matching records into the Kafka stream specified by `LARGE_ORDER`.

To run this example, users need to include the data sources (`storm-sql-kafka` in this case) and its dependency in the
class path. The Storm SQL core dependencies are automatically handled when users run `storm sql`. Users can include data sources at the submission step like below:

```
$ bin/storm sql order_filtering.sql order_filtering --artifacts "org.apache.storm:storm-sql-kafka:2.0.0-SNAPSHOT,org.apache.storm:storm-kafka-client:2.0.0-SNAPSHOT,org.apache.kafka:kafka-clients:1.1.0^org.slf4j:slf4j-log4j12"
```

Above command submits the SQL statements to StormSQL. Users need to modify each artifacts' version if users are using different version of Storm or Kafka. 

Having run the above command, you should now be able to see the `order_filtering` topology in Storm UI.

## Showing Query Plan (explain mode)

Like `explain` on SQL statement, StormSQL provides `explain mode` when running Storm SQL Runner. In explain mode, StormSQL analyzes each query statement (only DML) and show plan instead of submitting topology.

In order to run `explain mode`, you need to provide topology name as `--explain` and run `storm sql` as same as submitting.

For example, when you run the example seen above with explain mode:
 
```
$ bin/storm sql order_filtering.sql --explain --artifacts "org.apache.storm:storm-sql-kafka:2.0.0-SNAPSHOT,org.apache.storm:storm-kafka-client:2.0.0-SNAPSHOT,org.apache.kafka:kafka-clients:1.1.0^org.slf4j:slf4j-log4j12"
```

StormSQL prints out like below:
 
```

===========================================================
query>
CREATE EXTERNAL TABLE ORDERS (ID INT PRIMARY KEY, UNIT_PRICE INT, QUANTITY INT) LOCATION 'kafka://orders?bootstrap-servers=localhost:9092,localhost:9093'
-----------------------------------------------------------
17:03:06.040 [main] INFO  o.a.s.s.r.DataSourcesRegistry - Registering scheme socket with org.apache.storm.sql.runtime.datasource.socket.SocketDataSourcesProvider@d62fe5b
17:03:06.090 [main] INFO  o.a.s.s.r.DataSourcesRegistry - Registering scheme kafka with org.apache.storm.sql.kafka.KafkaDataSourcesProvider@34158c08
17:03:06.290 [main] INFO  o.a.s.k.s.KafkaSpoutConfig - Setting Kafka consumer property 'auto.offset.reset' to 'earliest' to ensure at-least-once processing
17:03:06.290 [main] INFO  o.a.s.k.s.KafkaSpoutConfig - Setting Kafka consumer property 'enable.auto.commit' to 'false', because the spout does not support auto-commit
No plan presented on DDL
===========================================================
===========================================================
query>
CREATE EXTERNAL TABLE LARGE_ORDERS (ID INT PRIMARY KEY, TOTAL INT) LOCATION 'kafka://large_orders?bootstrap-servers=localhost:9092,localhost:9093' TBLPROPERTIES '{"producer":{"acks":"1","key.serializer":"org.apache.storm.kafka.IntSerializer"}}'
-----------------------------------------------------------
17:03:06.504 [main] INFO  o.a.s.k.s.KafkaSpoutConfig - Setting Kafka consumer property 'auto.offset.reset' to 'earliest' to ensure at-least-once processing
17:03:06.504 [main] INFO  o.a.s.k.s.KafkaSpoutConfig - Setting Kafka consumer property 'enable.auto.commit' to 'false', because the spout does not support auto-commit
No plan presented on DDL
===========================================================
===========================================================
query>
INSERT INTO LARGE_ORDERS SELECT ID, UNIT_PRICE * QUANTITY AS TOTAL FROM ORDERS WHERE UNIT_PRICE * QUANTITY > 50
-----------------------------------------------------------
plan>
LogicalTableModify(table=[[LARGE_ORDERS]], operation=[INSERT], flattened=[true])
  LogicalProject(ID=[$0], TOTAL=[*($1, $2)])
    LogicalFilter(condition=[>(*($1, $2), 50)])
      EnumerableTableScan(table=[[ORDERS]])

===========================================================

```

## Debugging expressions
When a Storm SQL topology is submitted, the code generated based on the SQL expressions will be logged. For example, the WHERE clause in the order filtering example will produce the following log during submit:

```
17:37:55.344 [main] INFO  o.a.s.s.r.s.f.EvaluationCalc - Expression code for filter:
public class Generated_STREAMSCALCREL_33_0 implements org.apache.storm.sql.runtime.calcite.ExecutableExpression {
  public void execute(org.apache.calcite.interpreter.Context context, Object[] outputValues) {
    final Object[] current = context.values;
    outputValues[0] = org.apache.calcite.runtime.SqlFunctions.toInt(current[1]) * org.apache.calcite.runtime.SqlFunctions.toInt(current[2]) > 50;
  }

  public Object execute(org.apache.calcite.interpreter.Context context) {
    final Object[] values = new Object[1];
    this.execute(context, values);
    return values[0];
  }

}
```

This can be helpful in case you need to debug the topology.

## Current Limitations

- Windowing is yet to be implemented.
- Aggregation and join will be introduced after supporting windowing.
