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
import org.apache.storm.iceberg.common.CommitWal;
import org.apache.storm.iceberg.common.IcebergCommitter;
import org.apache.storm.iceberg.common.IcebergMetrics;
import org.apache.storm.iceberg.common.IcebergOptions;
import org.apache.storm.iceberg.common.IcebergWriter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseTickTupleAwareRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Appends tuples to an Apache Iceberg table, committing them in atomic batches.
 *
 * <p>Tuples are written to Iceberg data files as they arrive but are <em>not</em> acked until the
 * commit that makes them visible has landed. A batch therefore either becomes visible in full and
 * is acked, or is failed and replayed by the source. Readers never see part of a batch.
 *
 * <p>The guarantee is <strong>atomic commits with at-least-once delivery</strong>. A replayed
 * batch is written again, and because the table is append-only with no equality deletes, the
 * duplicate rows stay visible until something downstream removes them. A crash between writing the
 * files and committing them leaves orphan data files: invisible to readers, and cleaned up by
 * Iceberg's standard orphan-file maintenance, which this module does not run for you.
 *
 * <p>Batches are closed when any configured threshold is crossed — records, bytes, or, if tick
 * tuples are configured, elapsed time. Because nothing is acked early, a larger batch costs
 * latency and replay volume, not durability.
 */
public class IcebergBolt extends BaseTickTupleAwareRichBolt {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(IcebergBolt.class);

    private final IcebergOptions options;

    private transient OutputCollector collector;
    private transient IcebergWriter writer;
    private transient IcebergCommitter committer;
    private transient IcebergMetrics metrics;
    private transient List<Tuple> pending;
    private transient long batchStartNanos;

    public IcebergBolt(IcebergOptions options) {
        this.options = options;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.pending = new ArrayList<>();
        this.metrics = new IcebergMetrics(context);
        int taskId = context.getThisTaskId();
        this.writer = new IcebergWriter(options, taskId);
        writer.open();
        String topologyName = String.valueOf(topoConf.get(Config.TOPOLOGY_NAME));
        CommitWal wal = new CommitWal(writer.table(), options.getWalNamespace(), topologyName,
            context.getThisComponentId(), context.getThisTaskIndex());
        this.committer = new IcebergCommitter(writer.table(), wal, metrics);
        // Clear whatever an earlier run of this task left half-committed, before writing anything
        // new. Those batches were never acked, so the source replays them.
        int abandoned = committer.recover();
        if (abandoned > 0) {
            LOG.info("Abandoned {} commit(s) left pending by an earlier run of global task id {} ({}/{})",
                abandoned, taskId, context.getThisComponentId(), context.getThisTaskIndex());
        }
    }

    @Override
    protected void process(Tuple tuple) {
        try {
            if (pending.isEmpty()) {
                // Schema and partition spec evolution is picked up between batches, not mid-batch:
                // every file in one commit is written against the same metadata.
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
        if (shouldFlush()) {
            flush();
        }
    }

    @Override
    protected void onTickTuple(Tuple tuple) {
        if (!pending.isEmpty()) {
            flush();
        }
    }

    private boolean shouldFlush() {
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
     * Close the batch's files, commit them, and only then ack. Any failure fails the whole batch:
     * the source replays it, which is where at-least-once comes from.
     */
    private void flush() {
        List<Tuple> committing = new ArrayList<>(pending);
        pending.clear();
        try {
            List<DataFile> dataFiles = writer.complete();
            committer.commit(dataFiles);
        } catch (Exception e) {
            LOG.error("Failed committing {} tuple(s) to Iceberg, failing them for replay",
                committing.size(), e);
            writer.abort();
            committing.forEach(collector::fail);
            return;
        }
        committing.forEach(collector::ack);
    }

    private void failBatch() {
        writer.abort();
        pending.forEach(collector::fail);
        pending.clear();
    }

    @Override
    public void cleanup() {
        // Whatever is still buffered was never acked, so it will be replayed; drop it rather than
        // race a commit against shutdown.
        if (pending != null && !pending.isEmpty()) {
            failBatch();
        }
        if (writer != null) {
            writer.close();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Terminal sink: nothing is emitted downstream.
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
