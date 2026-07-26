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
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * Ingests 10 million generated rows into a partitioned Iceberg table on a local Hadoop catalog,
 * committing roughly every 1 MB written instead of once per Trident batch.
 *
 * <p>The table is partitioned by {@code identity(region)} and {@code days(event_time)}, so a single
 * batch spans several partitions and the state opens one data file per partition through the
 * Iceberg fanout writer. The threshold covers all four partitions together, so expect roughly
 * four files per commit.
 *
 * <p><strong>Commit batching weakens the delivery guarantee</strong>: batches buffered towards the
 * next commit are lost if the worker dies, and a final partial window is never flushed, so the
 * committed row count may end up slightly below the count ingested. See the unpartitioned example
 * and {@code docs/storm-iceberg.md} for the full trade-off.
 *
 * <p>The state runs at {@value #STATE_PARALLELISM}-way parallelism: each partition buffers and
 * commits independently against its own share of the stream, so the 1 MB threshold is crossed
 * roughly {@value #STATE_PARALLELISM} times more slowly per partition than it would be at
 * parallelism 1.
 *
 * <p>The row count and the commit threshold can be overridden on the command line:
 * {@code <warehouse> <topologyName> <totalTuples> <commitIntervalBytes>}.
 *
 * <p>Run locally with:
 * {@code storm local storm-iceberg-examples-*.jar
 * org.apache.storm.iceberg.examples.IcebergPartitionedTridentExampleTopology}
 * then inspect the warehouse directory (default {@code file:///tmp/storm-iceberg-warehouse}): the
 * data files are laid out under {@code example/events/data/region=.../event_time_day=...}.
 */
public final class IcebergPartitionedTridentExampleTopology {

    private static final int BATCH_SIZE = 50_000;
    private static final long TOTAL_TUPLES = 10_000_000L;
    private static final long COMMIT_INTERVAL_BYTES = 1024L * 1024;
    private static final int STATE_PARALLELISM = 4;
    private static final String[] REGIONS = {"eu-west", "us-east"};

    private IcebergPartitionedTridentExampleTopology() {
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

        // Two regions x two days -> four partitions, written by a single state instance.
        Instant today = Instant.now();
        Instant yesterday = today.minus(1, ChronoUnit.DAYS);
        BoundedTupleSpout spout = new BoundedTupleSpout(totalTuples, BATCH_SIZE,
            new Fields("id", "region", "event_time"),
            index -> new Values(index,
                REGIONS[(int) (index % REGIONS.length)],
                index % 4 < 2 ? yesterday : today));

        TridentTopology topology = new TridentTopology();
        topology.newStream("events", spout)
            // IBatchSpout has no notion of partition index: parallelizing the spout itself would
            // make every task call emitBatch() for the same txid, each emitting the full batch.
            // shuffle() repartitions after the (single-task) spout, so the persist stage below can
            // run at a different parallelism. The hint has to be set on the TridentState returned
            // by partitionPersist(), not on the Stream before it: partitionPersist() creates its
            // own node that does not inherit a hint set upstream.
            .shuffle()
            .partitionPersist(new IcebergStateFactory(options),
                new Fields("id", "region", "event_time"), new IcebergStateUpdater())
            .parallelismHint(STATE_PARALLELISM);

        Config conf = new Config();
        // In batches, not tuples: up to 8 Trident batches (400k tuples with BATCH_SIZE = 50_000)
        // may be in flight, overlapping this batch's writes with the previous one's commit.
        // Commits still land in txid order regardless of this setting.
        conf.setMaxSpoutPending(8);
        StormSubmitter.submitTopology(topologyName, conf, topology.build());
    }
}
