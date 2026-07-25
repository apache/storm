/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.iceberg.examples;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.iceberg.trident.IcebergOptions;
import org.apache.storm.iceberg.trident.IcebergStateFactory;
import org.apache.storm.iceberg.trident.IcebergStateUpdater;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * Writes a small fixed stream into a partitioned Iceberg table on a local Hadoop catalog.
 *
 * <p>The table is partitioned by {@code identity(region)} and {@code days(event_time)}, so a single
 * batch spans several partitions and the state opens one data file per partition through the
 * Iceberg fanout writer.
 *
 * <p>Run locally with:
 * {@code storm local storm-iceberg-examples-*.jar
 * org.apache.storm.iceberg.examples.IcebergPartitionedTridentExampleTopology}
 * then inspect the warehouse directory (default {@code file:///tmp/storm-iceberg-warehouse}): the
 * data files are laid out under {@code example/events/data/region=.../event_time_day=...}.
 */
public final class IcebergPartitionedTridentExampleTopology {

    private IcebergPartitionedTridentExampleTopology() {
    }

    public static void main(String[] args) throws Exception {
        String warehouse = args.length > 0 ? args[0] : "file:///tmp/storm-iceberg-warehouse";
        String topologyName = args.length > 1 ? args[1] : "iceberg-partitioned-example";

        Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "region", Types.StringType.get()),
            Types.NestedField.required(3, "event_time", Types.TimestampType.withZone()));

        PartitionSpec spec = PartitionSpec.builderFor(schema)
            .identity("region")
            .day("event_time")
            .build();

        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put(CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP);
        catalogProps.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);

        IcebergOptions options = new IcebergOptions.Builder()
            .withCatalogProperties(catalogProps)
            .withTable("example.events")
            .withAutoCreate(schema, spec)
            .build();

        // Two regions x two days -> four partitions, written by a single state instance.
        Instant today = Instant.now();
        Instant yesterday = today.minus(1, ChronoUnit.DAYS);
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("id", "region", "event_time"), 3,
            new Values(1L, "eu-west", yesterday), new Values(2L, "us-east", yesterday),
            new Values(3L, "eu-west", today), new Values(4L, "us-east", today),
            new Values(5L, "eu-west", today), new Values(6L, "us-east", yesterday));
        spout.setCycle(false);

        TridentTopology topology = new TridentTopology();
        topology.newStream("events", spout)
            .partitionPersist(new IcebergStateFactory(options),
                new Fields("id", "region", "event_time"), new IcebergStateUpdater());

        Config conf = new Config();
        conf.setMaxSpoutPending(3);
        StormSubmitter.submitTopology(topologyName, conf, topology.build());
    }
}
