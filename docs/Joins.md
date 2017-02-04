---
title: Joining Streams in Storm Core
layout: documentation
documentation: true
---

Storm core supports joining multiple data streams into one with the help of `JoinBolt`.
`JoinBolt` is a Windowed bolt, i.e. it waits for the configured window duration to match up the
tuples among the streams being joined. This helps align the streams within the Window boundary.

Each of `JoinBolt`'s incoming data streams must be Fields Grouped on a single field. A stream 
should only be joined with the other streams using the field on which it has been FieldsGrouped.  
Knowing this will help understand the join syntax described below.  

## Performing Joins
Consider the following SQL join involving 4 tables called: stream1, stream2, stream3 & stream4:

```sql
select  userId, key4, key2, key3
from    stream1 
join       stream2  on stream2.userId =  stream1.key1
join       stream3  on stream3.key3   =  stream2.userId
left join  stream4  on stream4.key4   =  stream3.key3
```

This could be expressed using `JoinBolt` over 4 similarly named streams as: 

```java
new JoinBolt(JoinBolt.Selector.STREAM, "stream1", "key1")               // from stream1  
                            .join     ("stream2", "userId",  "stream1") // join      stream2  on stream2.userId = stream1.key1
                            .join     ("stream3", "key3",    "stream2") // join      stream3  on stream3.key3   = stream2.userId   
                            .leftjoin ("stream4", "key4",    "stream3") // left join stream4  on stream4.key4   = stream3.key3
                            .select("userId, key4, key2, key3")         // chose output fields
                            .withTumblingWindow( new Duration(10, TimeUnit.MINUTES) ) ;
```

The constructor takes three arguments. The first argument `JoinBolt.Selector.STREAM` informs the bolt that
`stream1/2/3/4` refer to named streams. The next argument introduces ``stream1`` as the first stream
and specifies that it will always use field `key1` when joining with the others streams. So `stream1` must
be fields grouped on `key1`. Similarly, each of the `leftJoin()` and `join()` method calls introduce a new
stream along with the field to use for joins. The same FieldsGrouping requirement applies to these streams
as well. The 3rd argument to the join methods refers to another stream with which to join.

The `select()` method is used to specify the output fields.  

The `withTumblingWindow()` method configures the join window to be a 10 minute tumbling window. Since `JoinBolt` is 
a Windowed Bolt, we can also use the `withWindow` method to configure it as a sliding window (see tips section below). 

## Stream names and Join order
1. Stream names must be introduced (in constructor or as 1st arg to various join methods) before being referred
to (in the 3rd argument of the join methods). Forward referencing of stream names, as shown below, is not allowed:

```java
new JoinBolt(JoinBolt.Selector.STREAM, "stream1", "key1")                 
                            .join     ("stream2", "userId",  "stream3") //not allowed. stream3 not yet introduced
                            .join     ("stream3", "key3",    "stream1")    
```
2. Internally, the joins will be performed in the same order as they are expressed by the user.

## Joining based on source component names

For simplicity, many Storm topologies tend to use the `default` stream instead of named streams. In such cases,
there is only 1 `default` incoming stream from the `JoinBolt`'s perspective, even if there are multiple
upstream components sending it data. To support such topologies, `JoinBolt` can be configured to use names of
*immediate* upstream sources (spout/bolt) instead of stream names via the constructor's first argument:

```java
new JoinBolt(JoinBolt.Selector.SOURCE, "spout1", "key1" ) 
    ...
```

The below example joins tuples coming in *directly* from 4 spouts:

```java
new JoinBolt(JoinBolt.Selector.SOURCE,  "kafkaSpout1", "key1") // Here kafkaSpout1, hdfsSpout3 etc will refer to upstream source components 
                             .join     ("kafkaSpout2", "userId",    "kafkaSpout1" )    
                             .join     ("hdfsSpout3",  "key3",      "kafkaSpout2")
                             .leftjoin ("mqttSpout1",  "key4",      "hdfsSpout3")
                             .select ("userId, key4, key2, key3")
                             .withTumblingWindow( new Duration(10, TimeUnit.MINUTES) ) ;
```



## Limitations: 
1. Currently only INNER and LEFT joins are supported. 

2. Unlike SQL, which allows joining the same table on different keys to different tables, here the same one field must be used
   on a stream. This is because we are dealing with live data flow which needs to be partitioned by Fields Grouping and routed
   to the appropriate instance of the Bolt for correct results. The FieldsGrouping field must be the same as the join field.

## Tips:

1. Joins can be CPU and memory intensive. The larger the data accumulated in the current window (usually proportional to window
   length), the longer it takes to do the join.
   Having a short sliding interval (few seconds for example) causes frequent join computations. Consequently performance can suffer
   if using large window lengths or small sliding intervals.

2. Duplication of joined records across windows is possible when using Sliding Windows. This is because the tuples continue to exist
   across multiple windows when using Sliding Windows.


