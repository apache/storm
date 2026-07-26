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
import org.apache.storm.iceberg.trident.IcebergOptions;
import org.apache.storm.iceberg.trident.IcebergStateFactory;
import org.apache.storm.iceberg.trident.IcebergStateUpdater;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * Ingests 10 million generated rows into an Iceberg table on a local Hadoop catalog, committing
 * roughly every 1 MB written instead of once per Trident batch.
 *
 * <p><strong>Commit batching weakens the delivery guarantee.</strong> Trident treats a batch as
 * delivered when {@code commit()} returns, so the batches buffered towards the next commit are
 * lost if the worker dies. This example opts into that trade to keep the snapshot count sane: at
 * one commit per batch, 10M rows in batches of {@value #BATCH_SIZE} would produce
 * {@code 10_000_000 / BATCH_SIZE} snapshots.
 *
 * <p>For the same reason a final partial window is never flushed: no further batch arrives to
 * cross a threshold. Whether any rows are left behind depends on where the last batch falls
 * relative to the threshold, so the committed count may be at or slightly below the row count
 * ingested. That is the documented behaviour of commit batching, not a defect of the example.
 *
 * <p>The state runs at {@value #STATE_PARALLELISM}-way parallelism: each of those partitions
 * buffers and commits independently, sees roughly a quarter of the stream, and therefore takes
 * about four times as many rows to cross the 1 MB threshold on its own. Expect roughly
 * {@value #STATE_PARALLELISM} snapshots per flush round, not one.
 *
 * <p>The row count and the commit threshold can be overridden on the command line, which is the
 * quick way to try the topology out:
 * {@code <warehouse> <topologyName> <totalTuples> <commitIntervalBytes>}.
 *
 * <p>Run locally with:
 * {@code storm local storm-iceberg-examples-*.jar
 * org.apache.storm.iceberg.examples.IcebergTridentExampleTopology}
 * then inspect the warehouse directory (default {@code file:///tmp/storm-iceberg-warehouse}).
 */
public final class IcebergTridentExampleTopology {

    static final int BATCH_SIZE = 50_000;
    private static final long TOTAL_TUPLES = 10_000_000L;
    private static final long COMMIT_INTERVAL_BYTES = 1024L * 1024;
    private static final int STATE_PARALLELISM = 4;
    private static final String[] WORDS =
        {"storm", "iceberg", "trident", "lakehouse", "parquet", "snapshot"};

    private IcebergTridentExampleTopology() {
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
        BoundedTupleSpout spout = new BoundedTupleSpout(totalTuples, BATCH_SIZE,
            new Fields("id", "word"),
            index -> new Values(index, WORDS[(int) (index % WORDS.length)] + "-" + index));

        TridentTopology topology = new TridentTopology();
        topology.newStream("words", spout)
            // IBatchSpout has no notion of partition index: parallelizing the spout itself would
            // make every task call emitBatch() for the same txid, each emitting the full batch.
            // shuffle() repartitions after the (single-task) spout, so the persist stage below can
            // run at a different parallelism. The hint has to be set on the TridentState returned
            // by partitionPersist(), not on the Stream before it: partitionPersist() creates its
            // own node that does not inherit a hint set upstream.
            .shuffle()
            .partitionPersist(new IcebergStateFactory(options),
                new Fields("id", "word"), new IcebergStateUpdater())
            .parallelismHint(STATE_PARALLELISM);

        Config conf = new Config();
        // In batches, not tuples: up to 8 Trident batches (400k tuples with BATCH_SIZE = 50_000)
        // may be in flight, overlapping this batch's writes with the previous one's commit.
        // Commits still land in txid order regardless of this setting.
        conf.setMaxSpoutPending(8);
        StormSubmitter.submitTopology(topologyName, conf, topology.build());
    }
}
