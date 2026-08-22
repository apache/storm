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

package org.apache.storm.iceberg.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.DataFile;
import org.apache.storm.Config;
import org.apache.storm.iceberg.common.DataFileCodec;
import org.apache.storm.iceberg.common.IcebergMetrics;
import org.apache.storm.iceberg.common.IcebergOptions;
import org.apache.storm.iceberg.common.IcebergWriter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseTickTupleAwareRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes tuples to Iceberg data files and hands their descriptors to the committer bolt,
 * which makes them visible in one commit covering every writer.
 *
 * <p>A sealed batch is emitted <em>anchored to all of its tuples</em> and only then acked. Acking
 * an input after emitting an anchored child does not close the spout tuple's ack tree: the tree
 * stays open until the committer acks the descriptor, so the source still advances only after the
 * commit is visible. The guarantee is therefore the one {@link IcebergBolt} gives — atomic commits
 * with at-least-once delivery — with the commit cost no longer multiplied by this bolt's
 * parallelism.
 *
 * <p>Because the tuples are released as soon as the descriptor is emitted, a writer's heap does not
 * grow with the commit interval the way the monolithic sink's does.
 */
public class IcebergWriterBolt extends BaseTickTupleAwareRichBolt {

    /** Which writer produced the batch. Carried for diagnostics only. */
    public static final String FIELD_WRITER_TASK_ID = "writer-task-id";
    /** The batch's data files, encoded by {@link DataFileCodec}. */
    public static final String FIELD_DATA_FILES = "data-files";

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(IcebergWriterBolt.class);

    private final IcebergOptions options;

    private transient OutputCollector collector;
    private transient IcebergWriter writer;
    private transient IcebergMetrics metrics;
    private transient List<Tuple> pending;
    private transient long batchStartNanos;
    private transient int taskId;

    public IcebergWriterBolt(IcebergOptions options) {
        this.options = options;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.pending = new ArrayList<>();
        this.metrics = new IcebergMetrics(context);
        this.taskId = context.getThisTaskId();
        this.writer = new IcebergWriter(options, taskId);
        writer.open();
        // No recovery here: the committer owns the write-ahead log, because it owns the commits.
    }

    @Override
    protected void process(Tuple tuple) {
        try {
            if (pending.isEmpty()) {
                // Schema and partition spec evolution is picked up between batches, not mid-batch.
                writer.refreshTable();
                batchStartNanos = System.nanoTime();
            }
            writer.write(tuple);
        } catch (Exception e) {
            LOG.error("Failed writing tuple to Iceberg, failing the open batch", e);
            failBatch();
            // The try block above never adds to pending, so failBatch() provably did not cover
            // this tuple: fail it here too, or it would only be replayed on timeout.
            collector.fail(tuple);
            return;
        }
        pending.add(tuple);
        metrics.recordsWritten(1);
        if (shouldSeal()) {
            seal();
        }
    }

    @Override
    protected void onTickTuple(Tuple tuple) {
        if (!pending.isEmpty()) {
            seal();
        }
    }

    private boolean shouldSeal() {
        Integer intervalRecords = options.getCommitIntervalRecords();
        if (intervalRecords != null && pending.size() >= intervalRecords) {
            return true;
        }
        Long intervalBytes = options.getCommitIntervalBytes();
        if (intervalBytes != null && writer.bufferedBytes() >= intervalBytes) {
            return true;
        }
        Long intervalMillis = options.getCommitIntervalMillis();
        return intervalMillis != null
            && TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - batchStartNanos) >= intervalMillis;
    }

    /**
     * Close the batch's files and hand them downstream. Anything that goes wrong before the emit
     * fails the whole batch, so the source replays it — the files written so far become orphans.
     */
    private void seal() {
        List<Tuple> sealing = new ArrayList<>(pending);
        pending.clear();
        List<DataFile> dataFiles;
        String descriptor;
        long startNanos = System.nanoTime();
        try {
            dataFiles = writer.complete();
            descriptor = dataFiles.isEmpty() ? null : DataFileCodec.toJson(dataFiles, writer.table());
        } catch (Exception e) {
            LOG.error("Failed sealing {} tuple(s) for Iceberg, failing them for replay",
                sealing.size(), e);
            writer.abort();
            sealing.forEach(collector::fail);
            return;
        }
        metrics.sealed(dataFiles, System.nanoTime() - startNanos);
        if (descriptor != null) {
            collector.emit(sealing, new Values(taskId, descriptor));
        }
        sealing.forEach(collector::ack);
    }

    private void failBatch() {
        writer.abort();
        pending.forEach(collector::fail);
        pending.clear();
    }

    @Override
    public void cleanup() {
        // Whatever is still buffered was never handed downstream, so it will be replayed.
        if (pending != null && !pending.isEmpty()) {
            failBatch();
        }
        if (writer != null) {
            writer.close();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FIELD_WRITER_TASK_ID, FIELD_DATA_FILES));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        if (options.getTickIntervalSecs() == null) {
            return null;
        }
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, options.getTickIntervalSecs());
        return conf;
    }
}
