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
import org.apache.storm.iceberg.common.DataFileCodec;
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
 * Makes the files produced by every {@link IcebergWriterBolt} visible in a single Iceberg commit.
 *
 * <p>Run with a parallelism of one and a {@code globalGrouping}: the point of this component is
 * that commit cost stops scaling with the sink's parallelism, so a topology can commit an order of
 * magnitude more often at the same load on the catalog. That, in turn, is what keeps ack latency
 * below the message timeout without acking anything early.
 *
 * <p>Descriptors are acked only after the append is visible, so the source still replays a failed
 * batch. The failure blast radius is wider than the monolithic sink's — a failed append replays
 * every writer's batch in the group, not one task's — which is the price of the aggregation.
 *
 * <p>Throughput is not a concern here: this bolt sees one tuple per writer seal, carrying file
 * descriptors rather than data. Liveness is, which is what the pending gauges are for.
 */
public class IcebergCommitterBolt extends BaseTickTupleAwareRichBolt {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(IcebergCommitterBolt.class);

    private final IcebergOptions options;

    private transient OutputCollector collector;
    private transient IcebergWriter writer;
    private transient IcebergCommitter committer;
    private transient IcebergMetrics metrics;
    private transient List<Tuple> sealed;
    private transient List<DataFile> pendingFiles;
    // Read by the metrics thread through the pending gauges, written by the executor thread.
    private transient volatile long groupStartNanos;

    public IcebergCommitterBolt(IcebergOptions options) {
        this.options = options;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        int committerTasks = context.getComponentTasks(context.getThisComponentId()).size();
        if (committerTasks > 1) {
            LOG.warn("{} is running with a parallelism of {}: every task commits independently, so the "
                    + "table receives {} snapshots per interval instead of one. Set its parallelism to 1 "
                    + "and feed it with a globalGrouping.",
                context.getThisComponentId(), committerTasks, committerTasks);
        }
        this.collector = collector;
        this.sealed = new ArrayList<>();
        this.pendingFiles = new ArrayList<>();
        this.metrics = new IcebergMetrics(context);
        // The writer is held only for its table handle and catalog lifecycle; it never writes here.
        this.writer = new IcebergWriter(options, context.getThisTaskId());
        writer.open();
        String topologyName = String.valueOf(topoConf.get(Config.TOPOLOGY_NAME));
        CommitWal wal = new CommitWal(writer.table(), options.getWalNamespace(), topologyName,
            context.getThisComponentId(), context.getThisTaskIndex());
        this.committer = new IcebergCommitter(writer.table(), wal, metrics);
        metrics.registerPendingGauges(context, () -> pendingFiles.size(), this::oldestPendingAgeMs);
        // Clear whatever an earlier run left half-committed before appending anything new. Those
        // batches were never acked, so the sources replay them.
        int abandoned = committer.recover();
        if (abandoned > 0) {
            LOG.info("Abandoned {} commit(s) left pending by an earlier run", abandoned);
        }
    }

    @Override
    protected void process(Tuple tuple) {
        // Refreshed on every descriptor, not just the first of a group: a writer refreshes per
        // batch, so within one still-open group a later descriptor can name a spec this task has
        // not loaded yet. The cost is one catalog loadTable per writer seal — negligible at 8
        // writers sealing every 5s, appreciable at 64 writers sealing every second. Preferred to a
        // spec-id precheck anyway: a precheck cannot tell an unknown spec from a stale one.
        try {
            writer.refreshTable();
        } catch (Exception e) {
            LOG.error("Failed refreshing table metadata for the descriptor from writer task {}, "
                + "failing it for replay", tuple.getIntegerByField(IcebergWriterBolt.FIELD_WRITER_TASK_ID), e);
            collector.fail(tuple);
            return;
        }
        if (sealed.isEmpty()) {
            groupStartNanos = System.nanoTime();
        }
        List<DataFile> dataFiles;
        try {
            dataFiles = DataFileCodec.fromJson(
                tuple.getStringByField(IcebergWriterBolt.FIELD_DATA_FILES), writer.table().specs());
        } catch (Exception e) {
            LOG.error("Unreadable data file descriptor from writer task {}, failing it for replay",
                tuple.getIntegerByField(IcebergWriterBolt.FIELD_WRITER_TASK_ID), e);
            collector.fail(tuple);
            return;
        }
        pendingFiles.addAll(dataFiles);
        sealed.add(tuple);
        if (shouldCommit()) {
            commitGroup();
        }
    }

    @Override
    protected void onTickTuple(Tuple tuple) {
        if (!sealed.isEmpty()) {
            commitGroup();
        }
    }

    private boolean shouldCommit() {
        return pendingFiles.size() >= options.getGroupCommitMaxDataFiles()
            || oldestPendingAgeMs() >= options.getGroupCommitIntervalMillis();
    }

    private long oldestPendingAgeMs() {
        return sealed.isEmpty() ? 0L : TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - groupStartNanos);
    }

    /**
     * Append every accumulated batch in one commit, then ack. A failure fails all of them: they are
     * replayed from the source, and the files written for them are left as orphans.
     */
    private void commitGroup() {
        List<Tuple> committing = new ArrayList<>(sealed);
        List<DataFile> dataFiles = new ArrayList<>(pendingFiles);
        // Cleared only once the append resolves: this executor is single-threaded, so clearing up
        // front would leave the pending gauges reading zero for the whole duration of a commit
        // hung on an unreachable catalog — silence exactly when the alarm is supposed to fire.
        try {
            committer.commit(dataFiles);
        } catch (Exception e) {
            LOG.error("Failed committing {} data file(s) from {} writer batch(es), failing them for replay",
                dataFiles.size(), committing.size(), e);
            committing.forEach(collector::fail);
            return;
        } finally {
            sealed.clear();
            pendingFiles.clear();
        }
        committing.forEach(collector::ack);
    }

    @Override
    public void cleanup() {
        // Nothing accumulated has been made visible, so it has to be replayed rather than raced
        // against shutdown.
        if (sealed != null && !sealed.isEmpty()) {
            sealed.forEach(collector::fail);
            sealed.clear();
            pendingFiles.clear();
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
