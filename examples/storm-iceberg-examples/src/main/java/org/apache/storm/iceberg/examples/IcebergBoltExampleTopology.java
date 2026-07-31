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
 * Ingests 10 million generated rows into an Iceberg table on a local Hadoop catalog, committing
 * roughly every 1 MB written rather than once per tuple.
 *
 * <p>Batching commits this way keeps the snapshot and file counts sane — at one commit per tuple,
 * 10M rows would mean 10M snapshots — and costs nothing in durability: the bolt does not ack a
 * tuple until the commit containing it has landed, so a worker that dies mid-batch has those
 * tuples replayed by the spout rather than losing them.
 *
 * <p>A tick tuple every {@value #TICK_SECS} seconds bounds how long the last, partial batch can
 * sit unwritten when the stream goes quiet. Without it a batch below the size threshold would
 * wait for traffic that may never come.
 *
 * <p>The sink runs at {@value #SINK_PARALLELISM}-way parallelism: each task buffers and commits
 * independently, sees roughly a quarter of the stream, and therefore takes about four times as
 * many rows to cross the 1 MB threshold on its own. Expect roughly {@value #SINK_PARALLELISM}
 * snapshots per flush round, not one.
 *
 * <p>The row count and the commit threshold can be overridden on the command line, which is the
 * quick way to try the topology out:
 * {@code <warehouse> <topologyName> <totalTuples> <commitIntervalBytes>}.
 *
 * <p>Run locally with:
 * {@code storm local storm-iceberg-examples-*.jar
 * org.apache.storm.iceberg.examples.IcebergBoltExampleTopology}
 * then inspect the warehouse directory (default {@code file:///tmp/storm-iceberg-warehouse}).
 */
public final class IcebergBoltExampleTopology {

    private static final long TOTAL_TUPLES = 10_000_000L;
    private static final long COMMIT_INTERVAL_BYTES = 1024L * 1024;
    private static final int SINK_PARALLELISM = 4;
    private static final int SPOUT_PARALLELISM = 2;
    private static final int TICK_SECS = 30;
    private static final String[] WORDS =
        {"storm", "iceberg", "bolt", "lakehouse", "parquet", "snapshot"};

    private IcebergBoltExampleTopology() {
    }

    public static void main(String[] args) throws Exception {
        String warehouse = args.length > 0 ? args[0] : "file:///tmp/storm-iceberg-warehouse";
        String topologyName = args.length > 1 ? args[1] : "iceberg-example";
        long totalTuples = args.length > 2 ? Long.parseLong(args[2]) : TOTAL_TUPLES;
        long commitIntervalBytes =
            args.length > 3 ? Long.parseLong(args[3]) : COMMIT_INTERVAL_BYTES;

        Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "word", Types.StringType.get()));

        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put(CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP);
        catalogProps.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);

        IcebergOptions options = new IcebergOptions.Builder()
            .withCatalogProperties(catalogProps)
            .withTable("example.words")
            .withAutoCreate(schema, PartitionSpec.unpartitioned())
            .withCommitIntervalBytes(commitIntervalBytes)
            .build();

        // A distinct word per row on purpose: a handful of repeated values would be dictionary
        // encoded down to about a byte per row, and no realistic size threshold would ever be
        // reached. High cardinality keeps the example representative of real ingestion.
        BoundedTupleSpout spout = new BoundedTupleSpout(totalTuples,
            new Fields("id", "word"),
            index -> new Values(index, WORDS[(int) (index % WORDS.length)] + "-" + index));

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("words", spout, SPOUT_PARALLELISM);
        builder.setBolt("iceberg", new IcebergBolt(options), SINK_PARALLELISM)
            .shuffleGrouping("words");

        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, TICK_SECS);
        // Un-acked tuples in flight. The sink holds a batch's tuples un-acked until it commits,
        // so this bounds how much is replayed when a worker dies, and must stay comfortably above
        // the number of tuples one batch accumulates.
        conf.setMaxSpoutPending(200_000);
        StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
    }
}
