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
import org.apache.storm.iceberg.bolt.IcebergBolt;
import org.apache.storm.iceberg.common.IcebergOptions;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * Ingests 10 million generated rows into a partitioned Iceberg table on a local Hadoop catalog,
 * committing roughly every 1 MB written rather than once per tuple.
 *
 * <p>The table is partitioned by {@code identity(region)} and {@code days(event_time)}, so a
 * single batch can still span more than one partition and the sink opens one data file per
 * partition through the Iceberg fanout writer. The stream is field-grouped on {@code region}, so
 * each task only ever sees one region; with two days in the generated data, expect roughly two
 * files per commit per task rather than one per partition in the whole table.
 *
 * <p>Buffering costs latency and replay volume, not durability: tuples are not acked until the
 * commit that contains them lands, so a worker that dies mid-batch has them replayed. A tick tuple
 * every {@value #TICK_SECS} seconds bounds how long a partial batch waits when the stream stalls.
 *
 * <p>The sink is declared at {@value #SINK_PARALLELISM}-way parallelism, matching the number of
 * regions: the field grouping routes each region to exactly one task, so any task beyond that
 * would never receive a tuple. Each task buffers and commits independently against its own
 * region's share of the stream.
 *
 * <p>The row count and the commit threshold can be overridden on the command line:
 * {@code <warehouse> <topologyName> <totalTuples> <commitIntervalBytes>}.
 *
 * <p>Run locally with:
 * {@code storm local storm-iceberg-examples-*.jar
 * org.apache.storm.iceberg.examples.IcebergPartitionedBoltExampleTopology}
 * then inspect the warehouse directory (default {@code file:///tmp/storm-iceberg-warehouse}): the
 * data files are laid out under {@code example/events/data/region=.../event_time_day=...}.
 */
public final class IcebergPartitionedBoltExampleTopology {

    private static final long TOTAL_TUPLES = 10_000_000L;
    private static final long COMMIT_INTERVAL_BYTES = 1024L * 1024;
    private static final int SINK_PARALLELISM = 2;
    private static final int SPOUT_PARALLELISM = 2;
    private static final int TICK_SECS = 30;
    private static final String[] REGIONS = {"eu-west", "us-east"};

    private IcebergPartitionedBoltExampleTopology() {
    }

    public static void main(String[] args) throws Exception {
        String warehouse = args.length > 0 ? args[0] : "file:///tmp/storm-iceberg-warehouse";
        String topologyName = args.length > 1 ? args[1] : "iceberg-partitioned-example";
        long totalTuples = args.length > 2 ? Long.parseLong(args[2]) : TOTAL_TUPLES;
        long commitIntervalBytes =
            args.length > 3 ? Long.parseLong(args[3]) : COMMIT_INTERVAL_BYTES;

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
            .withCommitIntervalBytes(commitIntervalBytes)
            .build();

        // Two regions x two days -> four partitions, split two-and-two across sink tasks by region.
        Instant today = Instant.now();
        Instant yesterday = today.minus(1, ChronoUnit.DAYS);
        BoundedTupleSpout spout = new BoundedTupleSpout(totalTuples,
            new Fields("id", "region", "event_time"),
            index -> new Values(index,
                REGIONS[(int) (index % REGIONS.length)],
                index % 4 < 2 ? yesterday : today));

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("events", spout, SPOUT_PARALLELISM);
        // fieldsGrouping on region: each task owns one region and its day-partitions, rather
        // than every task fanning out across all four partitions the way a shuffle would.
        builder.setBolt("iceberg", new IcebergBolt(options), SINK_PARALLELISM)
            .fieldsGrouping("events", new Fields("region"));

        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, TICK_SECS);
        // Un-acked tuples in flight; must stay comfortably above what one batch accumulates,
        // since the sink holds a batch's tuples un-acked until it commits.
        conf.setMaxSpoutPending(200_000);
        StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
    }
}
