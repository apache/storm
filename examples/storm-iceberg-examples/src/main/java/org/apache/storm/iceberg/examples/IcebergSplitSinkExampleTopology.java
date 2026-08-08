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
import org.apache.storm.iceberg.bolt.IcebergCommitterBolt;
import org.apache.storm.iceberg.bolt.IcebergWriterBolt;
import org.apache.storm.iceberg.common.IcebergOptions;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * The Iceberg sink split into parallel writers and one committer.
 *
 * <p>Compared with {@link IcebergPartitionedBoltExampleTopology}, the commit cost no longer
 * multiplies by the sink's parallelism: four writers still produce one snapshot per group commit
 * rather than four. That is what makes the short intervals used here affordable — a five second
 * group commit keeps ack latency far below the message timeout, which is what stops tuples from
 * timing out and being replayed as duplicates.
 *
 * <p>Run with {@code storm jar storm-iceberg-examples.jar
 * org.apache.storm.iceberg.examples.IcebergSplitSinkExampleTopology}, then inspect the warehouse
 * directory (default {@code file:///tmp/storm-iceberg-warehouse}) and count the snapshots.
 */
public final class IcebergSplitSinkExampleTopology {

    private static final long TOTAL_TUPLES = 10_000_000L;
    private static final long SEAL_INTERVAL_BYTES = 1024L * 1024;
    private static final long GROUP_COMMIT_INTERVAL_MILLIS = 5_000L;
    private static final int WRITER_PARALLELISM = 4;
    private static final int SPOUT_PARALLELISM = 2;
    private static final int WRITER_TICK_SECS = 5;
    private static final int COMMITTER_TICK_SECS = 5;
    private static final String[] REGIONS = {"eu-west", "us-east", "eu-central", "ap-south"};

    private IcebergSplitSinkExampleTopology() {
    }

    public static void main(String[] args) throws Exception {
        String warehouse = args.length > 0 ? args[0] : "file:///tmp/storm-iceberg-warehouse";
        String topologyName = args.length > 1 ? args[1] : "iceberg-split-example";
        long totalTuples = args.length > 2 ? Long.parseLong(args[2]) : TOTAL_TUPLES;

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

        IcebergOptions writerOptions = new IcebergOptions.Builder()
            .withCatalogProperties(catalogProps)
            .withTable("example.events")
            .withAutoCreate(schema, spec)
            .withCommitIntervalBytes(SEAL_INTERVAL_BYTES)
            .withTickIntervalSecs(WRITER_TICK_SECS)
            .build();

        IcebergOptions committerOptions = new IcebergOptions.Builder()
            .withCatalogProperties(catalogProps)
            .withTable("example.events")
            .withAutoCreate(schema, spec)
            .withGroupCommitIntervalMillis(GROUP_COMMIT_INTERVAL_MILLIS)
            .withTickIntervalSecs(COMMITTER_TICK_SECS)
            .build();

        Instant today = Instant.now();
        Instant yesterday = today.minus(1, ChronoUnit.DAYS);
        BoundedTupleSpout spout = new BoundedTupleSpout(totalTuples,
            new Fields("id", "region", "event_time"),
            index -> new Values(index,
                REGIONS[(int) (index % REGIONS.length)],
                // The day split's period (8) must not equal the region cycle's period
                // (REGIONS.length, 4), or every region would collapse onto a single day.
                index % 8 < 4 ? yesterday : today));

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("events", spout, SPOUT_PARALLELISM);
        // fieldsGrouping on region: each writer owns a subset of regions and their day-partitions,
        // rather than every task fanning out across all of them the way a shuffle would. With four
        // regions and four writers, every writer task ends up with traffic.
        builder.setBolt("iceberg-writer", new IcebergWriterBolt(writerOptions), WRITER_PARALLELISM)
            .fieldsGrouping("events", new Fields("region"));
        // Parallelism one and a global grouping: the whole point is a single commit per group.
        builder.setBolt("iceberg-committer", new IcebergCommitterBolt(committerOptions), 1)
            .globalGrouping("iceberg-writer");

        Config conf = new Config();
        // The timeout now spans two hops: writer queue, seal, committer queue, group commit and
        // the append itself. Each term is small, but there are more of them.
        conf.setMessageTimeoutSecs(120);
        conf.setMaxSpoutPending(200_000);
        StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
    }
}
