# Storm OpenTSDB Bolt and TridentState
  
OpenTSDB offers a scalable and highly available storage for time series data. It consists of a
Time Series Daemon (TSD) servers along with command line utilities. Each TSD connects to the 
configured HBase cluster to push/query the data.

Time series data point consists of:
 - a metric name.
 - a UNIX timestamp (seconds or milliseconds since Epoch).
 - a value (64 bit integer or single-precision floating point value).
 - a set of tags (key-value pairs) that describe the time series the point belongs to.

Storm bolt and trident state creates the above time series data from a tuple based on the given `TupleMetricPointMapper`
  
This module provides core Storm and Trident bolt implementations for writing data to OpenTSDB. 

Time series data points are written with at-least-once guarantee and duplicate data points should be handled as mentioned [here](http://opentsdb.net/docs/build/html/user_guide/writing.html#duplicate-data-points) in OpenTSDB. 

## Examples

### Core Bolt
Below example describes the usage of core bolt which is `org.apache.storm.opentsdb.bolt.OpenTsdbBolt`

```java

        OpenTsdbClient.Builder builder =  OpenTsdbClient.newBuilder(openTsdbUrl).sync(30_000).returnDetails();
        final OpenTsdbBolt openTsdbBolt = new OpenTsdbBolt(builder, Collections.singletonList(TupleOpenTsdbDatapointMapper.DEFAULT_MAPPER));
        openTsdbBolt.withBatchSize(10).withFlushInterval(2000);
        topologyBuilder.setBolt("opentsdb", openTsdbBolt).shuffleGrouping("metric-gen");
        
```


### Trident State

```java

        final OpenTsdbStateFactory openTsdbStateFactory =
                new OpenTsdbStateFactory(OpenTsdbClient.newBuilder(tsdbUrl),
                        Collections.singletonList(TupleOpenTsdbDatapointMapper.DEFAULT_MAPPER));
        TridentTopology tridentTopology = new TridentTopology();
        
        final Stream stream = tridentTopology.newStream("metric-tsdb-stream", new MetricGenSpout());
        
        stream.peek(new Consumer() {
            @Override
            public void accept(TridentTuple input) {
                LOG.info("########### Received tuple: [{}]", input);
            }
        }).partitionPersist(openTsdbStateFactory, MetricGenSpout.DEFAULT_METRIC_FIELDS, new OpenTsdbStateUpdater());
        
```

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
 * Jungtaek Lim ([@HeartSaVioR](https://github.com/HeartSaVioR))
