# Storm Druid Bolt and TridentState

This module provides core Storm and Trident bolt implementations for writing data to [Druid](http://druid.io/) data store.
This implementation uses Druid's [Tranquility library](https://github.com/druid-io/tranquility) to send messages to druid.

Some of the implementation details are borrowed from existing [Tranquility Storm Bolt](https://github.com/druid-io/tranquility/blob/master/docs/storm.md).
This new Bolt added to support latest storm release and maintain the bolt in the storm repo.

### Core Bolt
Below example describes the usage of core bolt which is `org.apache.storm.druid.bolt.DruidBeamBolt`
By default this Bolt expects to receive tuples in which "event" field gives your event type.
This logic can be changed by implementing ITupleDruidEventMapper interface.

```java

   DruidBeamFactory druidBeamFactory = new SampleDruidBeamFactoryImpl(new HashMap<String, Object>());
   DruidConfig druidConfig = DruidConfig.newBuilder().discardStreamId(DruidConfig.DEFAULT_DISCARD_STREAM_ID).build();
   ITupleDruidEventMapper<Map<String, Object>> eventMapper = new TupleDruidEventMapper<>(TupleDruidEventMapper.DEFAULT_FIELD_NAME);
   DruidBeamBolt<Map<String, Object>> druidBolt = new DruidBeamBolt<Map<String, Object>>(druidBeamFactory, eventMapper, druidConfig);
   topologyBuilder.setBolt("druid-bolt", druidBolt).shuffleGrouping("event-gen");
   topologyBuilder.setBolt("printer-bolt", new PrinterBolt()).shuffleGrouping("druid-bolt" , druidConfig.getDiscardStreamId());

```


### Trident State

```java
    DruidBeamFactory druidBeamFactory = new SampleDruidBeamFactoryImpl(new HashMap<String, Object>());
    ITupleDruidEventMapper<Map<String, Object>> eventMapper = new TupleDruidEventMapper<>(TupleDruidEventMapper.DEFAULT_FIELD_NAME);

    final Stream stream = tridentTopology.newStream("batch-event-gen", new SimpleBatchSpout(10));

    stream.peek(new Consumer() {
        @Override
        public void accept(TridentTuple input) {
             LOG.info("########### Received tuple: [{}]", input);
         }
    }).partitionPersist(new DruidBeamStateFactory<Map<String, Object>>(druidBeamFactory, eventMapper), new Fields("event"), new DruidBeamStateUpdater());

```

### Sample Beam Factory Implementation
Druid bolt must be supplied with a BeamFactory. You can implement one of these using the [DruidBeams builder's] (https://github.com/druid-io/tranquility/blob/master/core/src/main/scala/com/metamx/tranquility/druid/DruidBeams.scala) "buildBeam()" method.
See the [Configuration documentation](https://github.com/druid-io/tranquility/blob/master/docs/configuration.md) for details.
For more details refer [Tranquility library](https://github.com/druid-io/tranquility) docs.

```java

public class SampleDruidBeamFactoryImpl implements DruidBeamFactory<Map<String, Object>> {

    @Override
    public Beam<Map<String, Object>> makeBeam(Map<?, ?> conf, IMetricsContext metrics) {


        final String indexService = "druid/overlord"; // The druid.service name of the indexing service Overlord node.
        final String discoveryPath = "/druid/discovery"; // Curator service discovery path. config: druid.discovery.curator.path
        final String dataSource = "test"; //The name of the ingested datasource. Datasources can be thought of as tables.
        final List<String> dimensions = ImmutableList.of("publisher", "advertiser");
        List<AggregatorFactory> aggregators = ImmutableList.<AggregatorFactory>of(
                new CountAggregatorFactory(
                        "click"
                )
        );
        // Tranquility needs to be able to extract timestamps from your object type (in this case, Map<String, Object>).
        final Timestamper<Map<String, Object>> timestamper = new Timestamper<Map<String, Object>>()
        {
            @Override
            public DateTime timestamp(Map<String, Object> theMap)
            {
                return new DateTime(theMap.get("timestamp"));
            }
        };

        // Tranquility uses ZooKeeper (through Curator) for coordination.
        final CuratorFramework curator = CuratorFrameworkFactory
                .builder()
                .connectString((String)conf.get("druid.tranquility.zk.connect")) //take config from storm conf
                .retryPolicy(new ExponentialBackoffRetry(1000, 20, 30000))
                .build();
        curator.start();

        // The JSON serialization of your object must have a timestamp field in a format that Druid understands. By default,
        // Druid expects the field to be called "timestamp" and to be an ISO8601 timestamp.
        final TimestampSpec timestampSpec = new TimestampSpec("timestamp", "auto", null);

        // Tranquility needs to be able to serialize your object type to JSON for transmission to Druid. By default this is
        // done with Jackson. If you want to provide an alternate serializer, you can provide your own via ```.objectWriter(...)```.
        // In this case, we won't provide one, so we're just using Jackson.
        final Beam<Map<String, Object>> beam = DruidBeams
                .builder(timestamper)
                .curator(curator)
                .discoveryPath(discoveryPath)
                .location(DruidLocation.create(indexService, dataSource))
                .timestampSpec(timestampSpec)
                .rollup(DruidRollup.create(DruidDimensions.specific(dimensions), aggregators, QueryGranularities.MINUTE))
                .tuning(
                        ClusteredBeamTuning
                                .builder()
                                .segmentGranularity(Granularity.HOUR)
                                .windowPeriod(new Period("PT10M"))
                                .partitions(1)
                                .replicants(1)
                                .build()
                )
                .druidBeamConfig(
                      DruidBeamConfig
                           .builder()
                           .indexRetryPeriod(new Period("PT10M"))
                           .build())
                .buildBeam();

        return beam;
    }
}

```

Example code is available [here.](https://github.com/apache/storm/tree/master/external/storm-druid/src/test/java/org/apache/storm/druid)

This version is built to work with Druid 0.8.x. This connector uses the Tranquility module built for Scala 2.11.
This provides a shaded jar with all Tranquility dependencies. You should include Scala 2.11 dependency in your
application.

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
 * Satish Duggana ([satishd@apache.org](mailto:satishd@apache.org))
