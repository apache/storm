---
title: Storm SQL example
layout: documentation
documentation: true
---

This page shows how to use Storm SQL by showing the example of processing Apache logs. 
This page is written by "how-to" style so you can follow the step and learn how to utilize Storm SQL step by step. 

## Preparation

This page assumes that Apache Zookeeper, Apache Storm and Apache Kafka are installed locally and running properly configured.
For convenience, this page assumes that Apache Kafka 0.10.0 is installed via `brew`.

We'll use below tools to prepare the JSON data which will be fed to the input data source. 
Since they're Python projects, this page assumes Python 2.7 with `pip`, `virtualenv` is installed locally. 
If you're using Python 3, you may need to convert some places to be compatible with 3 manually while feeding data.  

* https://github.com/kiritbasu/Fake-Apache-Log-Generator
* https://github.com/rory/apache-log-parser

## Creating Topics

In this page, we will use four topics, `apache-logs`, `apache-errorlogs`, `apache-slowlogs`.
Please create topics according to your environment. 

For Apache Kafka 0.10.0 with brew installed,

```
kafka-topics --create --topic apache-logs --zookeeper localhost:2181 --replication-factor 1 --partitions 5
kafka-topics --create --topic apache-errorlogs --zookeeper localhost:2181 --replication-factor 1 --partitions 5
kafka-topics --create --topic apache-slowlogs --zookeeper localhost:2181 --replication-factor 1 --partitions 5
```

## Feeding Data

Let's feed the data to input topics. In this page we will generate fake Apache logs, and parse to JSON format, and feed JSON to Kafka topic. 

Let's create your working directory, since we will clone the project and also setup virtualenv.

In your working directory, `virtualenv env` to setup virtualenv to env directory, and activate.

```
$ virtualenv env
$ source env/bin/activate
```

Feel free to `deactivate` when you're done with example.
 
### Install and modify Fake-Apache-Log-Generator

`Fake-Apache-Log-Generator` is not presented to package, and also we need to modify the script.

```
$ git clone https://github.com/kiritbasu/Fake-Apache-Log-Generator.git
$ cd Fake-Apache-Log-Generator
```

Open `apache-fake-log-gen.py` and replace `while (flag):` statements to below:

```
        elapsed_us = random.randint(1 * 1000,1000 * 1000) # 1 ms to 1 sec
        seconds=random.randint(30,300)
        increment = datetime.timedelta(seconds=seconds)
        otime += increment

        ip = faker.ipv4()
        dt = otime.strftime('%d/%b/%Y:%H:%M:%S')
        tz = datetime.datetime.now(pytz.timezone('US/Pacific')).strftime('%z')
        vrb = numpy.random.choice(verb,p=[0.6,0.1,0.1,0.2])

        uri = random.choice(resources)
        if uri.find("apps")>0:
                uri += `random.randint(1000,10000)`

        resp = numpy.random.choice(response,p=[0.9,0.04,0.02,0.04])
        byt = int(random.gauss(5000,50))
        referer = faker.uri()
        useragent = numpy.random.choice(ualist,p=[0.5,0.3,0.1,0.05,0.05] )()
        f.write('%s - - [%s %s] %s "%s %s HTTP/1.0" %s %s "%s" "%s"\n' % (ip,dt,tz,elapsed_us,vrb,uri,resp,byt,referer,useragent))

        log_lines = log_lines - 1
        flag = False if log_lines == 0 else True
```

to make sure fake elapsed_us is included to fake log.

For convenience, you can skip cloning project and download modified file from here: [apache-fake-log-gen.py (gist)](https://gist.github.com/HeartSaVioR/79fd4e461604fabecf535ffece47e6c2)

### Install apache-log-parser and write parsing script

`apache-log-parser` can be installed via `pip`.

```
$ pip install apache-log-parser
```

Since apache-log-parser is a library, in order to parse fake log we need to write small python script.
Let's create file `parse-fake-log-gen-to-json-with-incrementing-id.py` with below content: 

```
import sys
import apache_log_parser
import json

auto_incr_id = 1
parser_format = '%a - - %t %D "%r" %s %b "%{Referer}i" "%{User-Agent}i"'
line_parser = apache_log_parser.make_parser(parser_format)
while True:
  # we'll use pipe
  line = sys.stdin.readline()
  if not line:
    break
  parsed_dict = line_parser(line)
  parsed_dict['id'] = auto_incr_id
  auto_incr_id += 1

  # works only python 2, but I don't care cause it's just a test module :)
  parsed_dict = {k.upper(): v for k, v in parsed_dict.iteritems() if not k.endswith('datetimeobj')}
  print json.dumps(parsed_dict)
```

### Feed parsed JSON Apache Log to Kafka

OK! We're prepared to feed the data to Kafka topic. Let's use `kafka-console-producer` to feed parsed JSON.

```
$ python apache-fake-log-gen.py -n 0 | python parse-fake-log-gen-to-json-with-incrementing-id.py | kafka-console-producer --broker-list localhost:9092 --topic apache-logs
```

and execute below to another terminal session to confirm data is being fed.

```
$ kafka-console-consumer --zookeeper localhost:2181 --topic apache-logs
```

If you can see the json like below, it's done:

```
{"TIME_US": "757467", "REQUEST_FIRST_LINE": "GET /wp-content HTTP/1.0", "REQUEST_METHOD": "GET", "RESPONSE_BYTES_CLF": "4988", "TIME_RECEIVED_ISOFORMAT": "2021-06-30T22:02:53", "TIME_RECEIVED_TZ_ISOFORMAT": "2021-06-30T22:02:53-07:00", "REQUEST_HTTP_VER": "1.0", "REQUEST_HEADER_USER_AGENT__BROWSER__FAMILY": "Firefox", "REQUEST_HEADER_USER_AGENT__IS_MOBILE": false, "REQUEST_HEADER_USER_AGENT__BROWSER__VERSION_STRING": "3.6.13", "REQUEST_URL_FRAGMENT": "", "REQUEST_HEADER_USER_AGENT": "Mozilla/5.0 (X11; Linux x86_64; rv:1.9.7.20) Gecko/2010-10-13 13:52:34 Firefox/3.6.13", "REQUEST_URL_SCHEME": "", "REQUEST_URL_PATH": "/wp-content", "REQUEST_URL_QUERY_SIMPLE_DICT": {}, "TIME_RECEIVED_UTC_ISOFORMAT": "2021-07-01T05:02:53+00:00", "REQUEST_URL_QUERY_DICT": {}, "STATUS": "200", "REQUEST_URL_NETLOC": "", "REQUEST_URL_QUERY_LIST": [], "REQUEST_URL_QUERY": "", "REQUEST_URL_USERNAME": null, "REQUEST_HEADER_USER_AGENT__OS__VERSION_STRING": "", "REQUEST_URL_HOSTNAME": null, "REQUEST_HEADER_USER_AGENT__OS__FAMILY": "Linux", "REQUEST_URL": "/wp-content", "ID": 904128, "REQUEST_HEADER_REFERER": "http://white.com/terms/", "REQUEST_URL_PORT": null, "REQUEST_URL_PASSWORD": null, "TIME_RECEIVED": "[30/Jun/2021:22:02:53 -0700]", "REMOTE_IP": "88.203.90.62"}
```

## Example: filtering error logs
 
In this example we'll filter error logs from entire logs and store them to another topics. `project` and `filter` features will be used.

The content of script file is here:

```
CREATE EXTERNAL TABLE APACHE_LOGS (ID INT PRIMARY KEY, REMOTE_IP VARCHAR, REQUEST_URL VARCHAR, REQUEST_METHOD VARCHAR, STATUS VARCHAR, REQUEST_HEADER_USER_AGENT VARCHAR, TIME_RECEIVED_UTC_ISOFORMAT VARCHAR, TIME_US DOUBLE) LOCATION 'kafka://apache-logs?bootstrap-servers=localhost:9092'
CREATE EXTERNAL TABLE APACHE_ERROR_LOGS (ID INT PRIMARY KEY, REMOTE_IP VARCHAR, REQUEST_URL VARCHAR, REQUEST_METHOD VARCHAR, STATUS INT, REQUEST_HEADER_USER_AGENT VARCHAR, TIME_RECEIVED_UTC_ISOFORMAT VARCHAR, TIME_ELAPSED_MS INT) LOCATION 'kafka://apache-error-logs?bootstrap-servers=localhost:9092' TBLPROPERTIES '{"producer":{"acks":"1","key.serializer":"org.apache.storm.kafka.IntSerializer"}}'
INSERT INTO APACHE_ERROR_LOGS SELECT ID, REMOTE_IP, REQUEST_URL, REQUEST_METHOD, CAST(STATUS AS INT) AS STATUS_INT, REQUEST_HEADER_USER_AGENT, TIME_RECEIVED_UTC_ISOFORMAT, (TIME_US / 1000) AS TIME_ELAPSED_MS FROM APACHE_LOGS WHERE (CAST(STATUS AS INT) / 100) >= 4
```

Save this file to `apache_log_error_filtering.sql`.

Let's take a look at the script.

The first statement defines the table `APACHE_LOGS` which represents the input stream. The `LOCATION` clause specifies the Kafka host (`localhost:9092`) and the topic (`apache-logs`).
Note that Kafka data source requires primary key to be defined. That's why we put integer id for parsed JSON data.

Similarly, the second statement specifies the table `APACHE_ERROR_LOGS` which represents the output stream. The `TBLPROPERTIES` clause specifies the configuration of [KafkaProducer](http://kafka.apache.org/documentation.html#producerconfigs) and is required for a Kafka sink table.
 
The last statement defines the topology. Storm SQL only define the topology and run topology on DML statement. 
DDL statements define input data source, output data source, and user defined function which will be referred by DML statement.

Let's look at the `where` statement first. Since we want to filter error logs, we divide status by 100 and compare quotient is equal or greater than 4. (easier representation is `>= 400`)
Since status in JSON is string format (hence represented as VARCHAR for APACHE_LOGS table), we apply CAST(STATUS AS INT) to convert to integer type before applying division.
Now we have filtered only error logs. 

Let's transform some columns to match the output stream. In this statement we apply CAST(STATUS AS INT) to convert to integer type, and divide TIME_US by 1000 to convert microsecond to millisecond.

Last, insert statement stores filtered and transformed rows (tuples) to the output stream.  

To run this example, users need to include the data sources (`storm-sql-kafka` in this case) and its dependency in the
class path. The Storm SQL core dependencies are automatically handled when users run `storm sql`. 
Users can include data sources at the submission step like below:

```
$ $STORM_DIR/bin/storm sql apache_log_error_filtering.sql apache_log_error_filtering --artifacts "org.apache.storm:storm-sql-kafka:2.0.0-SNAPSHOT,org.apache.storm:storm-kafka-client:2.0.0-SNAPSHOT,org.apache.kafka:kafka-clients:1.1.0^org.slf4j:slf4j-log4j12"
```

Above command submits the SQL statements to StormSQL. The command line syntax of Storm SQL is `storm sql [script file] [topology name]`. 
Users need to modify each artifacts' version if users are using different version of Storm or Kafka.
 
If your statements pass the validation phase, topology will be shown to Storm UI page.

You can see the output via console:

```
$ kafka-console-consumer --zookeeper localhost:2181 --topic apache-error-logs
```

and the output will be similar to:

```
{"ID":854643,"REMOTE_IP":"4.227.214.159","REQUEST_URL":"/wp-content","REQUEST_METHOD":"GET","STATUS":404,"REQUEST_HEADER_USER_AGENT":"Mozilla/5.0 (Windows 98; Win 9x 4.90; it-IT; rv:1.9.2.20) Gecko/2015-06-03 11:20:16 Firefox/3.6.17","TIME_RECEIVED_UTC_ISOFORMAT":"2021-03-28T19:14:44+00:00","TIME_RECEIVED_TIMESTAMP":1616958884000,"TIME_ELAPSED_MS":274.222}
{"ID":854693,"REMOTE_IP":"223.50.249.7","REQUEST_URL":"/apps/cart.jsp?appID=5578","REQUEST_METHOD":"GET","STATUS":404,"REQUEST_HEADER_USER_AGENT":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_6; rv:1.9.2.20) Gecko/2015-11-06 00:20:43 Firefox/3.8","TIME_RECEIVED_UTC_ISOFORMAT":"2021-03-28T21:41:02+00:00","TIME_RECEIVED_TIMESTAMP":1616967662000,"TIME_ELAPSED_MS":716.851}
...
```

You can also run Storm SQL runner to see the logical plan via placing `--explain` to topology name:

```
$ $STORM_DIR/bin/storm sql apache_log_error_filtering.sql --explain --artifacts "org.apache.storm:storm-sql-kafka:2.0.0-SNAPSHOT,org.apache.storm:storm-kafka-client:2.0.0-SNAPSHOT,org.apache.kafka:kafka-clients:1.1.0^org.slf4j:slf4j-log4j12"
```

and the output will be similar to:

```
LogicalTableModify(table=[[APACHE_ERROR_LOGS]], operation=[INSERT], updateColumnList=[[]], flattened=[true]), id = 8
  LogicalProject(ID=[$0], REMOTE_IP=[$1], REQUEST_URL=[$2], REQUEST_METHOD=[$3], STATUS=[CAST($4):INTEGER NOT NULL], REQUEST_HEADER_USER_AGENT=[$5], TIME_RECEIVED_UTC_ISOFORMAT=[$6], TIME_ELAPSED_MS=[/($7, 1000)]), id = 7
    LogicalFilter(condition=[>=(/(CAST($4):INTEGER NOT NULL, 100), 4)]), id = 6
      EnumerableTableScan(table=[[APACHE_LOGS]]), id = 5
```

It might be not same as you are seeing if Storm SQL applies query optimizations.

We're executing the first Storm SQL topology! Please kill the topology when you see enough output and the logs.

To be concise, we'll skip explaining the things we've already seen.

## Example: filtering slow logs

In this example we'll filter slow logs from entire logs and store them to another topics. `project` and `filter`, and `User Defined Function (UDF)` features will be used.
This is very similar to `filtering error logs` but we'll see how to define `User Defined Function (UDF)`.

The content of script file is here:

```
CREATE EXTERNAL TABLE APACHE_LOGS (ID INT PRIMARY KEY, REMOTE_IP VARCHAR, REQUEST_URL VARCHAR, REQUEST_METHOD VARCHAR, STATUS VARCHAR, REQUEST_HEADER_USER_AGENT VARCHAR, TIME_RECEIVED_UTC_ISOFORMAT VARCHAR, TIME_US DOUBLE) LOCATION 'kafka://apache-logs?bootstrap-servers=localhost:9092' TBLPROPERTIES '{"producer":{"acks":"1","key.serializer":"org.apache.storm.kafka.IntSerializer"}}'
CREATE EXTERNAL TABLE APACHE_SLOW_LOGS (ID INT PRIMARY KEY, REMOTE_IP VARCHAR, REQUEST_URL VARCHAR, REQUEST_METHOD VARCHAR, STATUS INT, REQUEST_HEADER_USER_AGENT VARCHAR, TIME_RECEIVED_UTC_ISOFORMAT VARCHAR, TIME_RECEIVED_TIMESTAMP BIGINT, TIME_ELAPSED_MS INT) LOCATION 'kafka://apache-slow-logs?bootstrap-servers=localhost:9092' TBLPROPERTIES '{"producer":{"acks":"1","key.serializer":"org.apache.storm.kafka.IntSerializer"}}'
CREATE FUNCTION GET_TIME AS 'org.apache.storm.sql.runtime.functions.scalar.datetime.GetTime2'
INSERT INTO APACHE_SLOW_LOGS SELECT ID, REMOTE_IP, REQUEST_URL, REQUEST_METHOD, CAST(STATUS AS INT) AS STATUS_INT, REQUEST_HEADER_USER_AGENT, TIME_RECEIVED_UTC_ISOFORMAT, GET_TIME(TIME_RECEIVED_UTC_ISOFORMAT, 'yyyy-MM-dd''T''HH:mm:ssZZ') AS TIME_RECEIVED_TIMESTAMP, TIME_US / 1000 AS TIME_ELAPSED_MS FROM APACHE_LOGS WHERE (TIME_US / 1000) >= 100
```

Save this file to `apache_log_slow_filtering.sql`.

We can skip the first 2 statements since it's almost same to the last example.

The third statement defines the `User defined function`. We're defining `GET_TIME` which uses `org.apache.storm.sql.runtime.functions.scalar.datetime.GetTime2` class.

The implementation of GetTime2 is here:

```
package org.apache.storm.sql.runtime.functions.scalar.datetime;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class GetTime2 {
    public static Long evaluate(String dateString, String dateFormat) {
        try {
            DateTimeFormatter df = DateTimeFormat.forPattern(dateFormat).withZoneUTC();
            return df.parseDateTime(dateString).getMillis();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
```

This class can be used for UDF since it defines static `evaluate` method. The SQL type of parameters and return are determined by Calcite which Storm SQL depends on. 

Note that this class should be in classpath, so in order to define UDF, you need to create jar file which contains UDF classes and run `storm sql` with `--jar` option.
This page assumes that GetTime2 is in classpath, for simplicity.
 
The last statement is very similar to filtering error logs. The only new thing is that we call `GET_TIME(TIME_RECEIVED_UTC_ISOFORMAT, 'yyyy-MM-dd''T''HH:mm:ssZZ')` to convert string time to unix timestamp (BIGINT).

Let's execute it.

```
$ $STORM_DIR/bin/storm sql apache_log_slow_filtering.sql apache_log_slow_filtering --artifacts "org.apache.storm:storm-sql-kafka:2.0.0-SNAPSHOT,org.apache.storm:storm-kafka-client:2.0.0-SNAPSHOT,org.apache.kafka:kafka-clients:1.1.0^org.slf4j:slf4j-log4j12"
```

You can see the output via console:

```
$ kafka-console-consumer --zookeeper localhost:2181 --topic apache-slow-logs
```

and the output will be similar to:

```
{"ID":890502,"REMOTE_IP":"136.156.159.160","REQUEST_URL":"/list","REQUEST_METHOD":"GET","STATUS":200,"REQUEST_HEADER_USER_AGENT":"Mozilla/5.0 (Windows NT 5.01) AppleWebKit/5311 (KHTML, like Gecko) Chrome/13.0.860.0 Safari/5311","TIME_RECEIVED_UTC_ISOFORMAT":"2021-06-05T03:44:59+00:00","TIME_RECEIVED_TIMESTAMP":1622864699000,"TIME_ELAPSED_MS":638.579}
{"ID":890542,"REMOTE_IP":"105.146.3.190","REQUEST_URL":"/search/tag/list","REQUEST_METHOD":"DELETE","STATUS":200,"REQUEST_HEADER_USER_AGENT":"Mozilla/5.0 (X11; Linux i686) AppleWebKit/5332 (KHTML, like Gecko) Chrome/13.0.891.0 Safari/5332","TIME_RECEIVED_UTC_ISOFORMAT":"2021-06-05T05:54:27+00:00","TIME_RECEIVED_TIMESTAMP":1622872467000,"TIME_ELAPSED_MS":403.957}
...
```

That's it! Supposing we have UDF which queries geo location via remote ip, we can filter via geo location, or enrich geo location to transformed result.

## Summary

We looked through several simple use cases for Storm SQL to learn Storm SQL features. If you haven't looked at [Storm SQL integration](storm-sql.html) and [Storm SQL language](storm-sql-reference.html), you need to read it to see full supported features. 