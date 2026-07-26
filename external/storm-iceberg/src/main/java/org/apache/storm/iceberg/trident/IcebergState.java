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

package org.apache.storm.iceberg.trident;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.storm.Config;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Trident {@link State} that appends batches to an Apache Iceberg table with exactly-once
 * semantics.
 *
 * <p>Each Trident batch is committed with an Iceberg {@link org.apache.iceberg.Transaction} that
 * atomically appends the batch's data files and records the transaction id in the table property
 * {@code storm.trident.&lt;topologyName&gt;.&lt;partitionIndex&gt;.last-committed-txid}. On replay,
 * {@link #beginCommit(Long)} detects already-committed transaction ids and skips the batch.
 */
public class IcebergState implements State {

    static final String TXID_PROPERTY_FORMAT = "storm.trident.%s.%d.last-committed-txid";
    private static final Logger LOG = LoggerFactory.getLogger(IcebergState.class);
    private static final String CATALOG_NAME = "storm-iceberg";

    private final IcebergOptions options;
    private final int partitionIndex;

    private IcebergStateMetrics metrics;
    private Catalog catalog;
    private Table table;
    private String txidPropertyKey;
    private long lastCommittedTxId;
    private boolean skipBatch;
    private TaskWriter<Record> writer;
    private CountingAppenderFactory countingAppenderFactory;
    private long highestBufferedTxId;
    private long windowStartNanos;
    private Thread shutdownHook;
    private volatile boolean closed;

    IcebergState(IcebergOptions options, int partitionIndex) {
        this.options = options;
        this.partitionIndex = partitionIndex;
    }

    void prepare(Map<String, Object> topoConf, IMetricsContext metricsContext) {
        String topologyName = (String) topoConf.get(Config.TOPOLOGY_NAME);
        this.metrics = new IcebergStateMetrics(metricsContext);
        this.txidPropertyKey = String.format(TXID_PROPERTY_FORMAT, topologyName, partitionIndex);
        this.catalog = CatalogUtil.buildIcebergCatalog(CATALOG_NAME, options.getCatalogProperties(), new Configuration());
        TableIdentifier identifier = TableIdentifier.parse(options.getTableIdentifier());
        this.table = loadOrCreateTable(identifier);
        String lastTxid = table.properties().get(txidPropertyKey);
        this.lastCommittedTxId = lastTxid == null ? 0L : Long.parseLong(lastTxid);
        // Trident's State has no lifecycle callback, so a shutdown hook is the only place left to
        // release the catalog's client (REST/HTTP, Hive metastore, object store) on worker exit.
        this.shutdownHook = new Thread(this::close, "iceberg-state-close-" + partitionIndex);
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        LOG.info("Prepared IcebergState for table {}, partition {}, lastCommittedTxId {}",
            identifier, partitionIndex, lastCommittedTxId);
    }

    private Table loadOrCreateTable(TableIdentifier identifier) {
        if (options.getAutoCreateSchema() != null && !catalog.tableExists(identifier)) {
            PartitionSpec spec = options.getAutoCreateSpec() == null
                ? PartitionSpec.unpartitioned()
                : options.getAutoCreateSpec();
            try {
                LOG.info("Auto-creating Iceberg table {}", identifier);
                return catalog.createTable(identifier, options.getAutoCreateSchema(), spec);
            } catch (AlreadyExistsException e) {
                LOG.info("Table {} was concurrently created by another state partition", identifier);
            }
        }
        return catalog.loadTable(identifier);
    }

    long getLastCommittedTxId() {
        return lastCommittedTxId;
    }

    @Override
    public void beginCommit(Long txId) {
        // A failed batch attempt can leave a writer with partially written records. Without
        // buffering that is always the case here, so the writer is simply dropped. With buffering
        // the writer also carries earlier, successfully delivered batches, so it may only be
        // dropped when this txId is itself a replay of something already in the window: an open
        // writer cannot un-write the failed attempt's records, and keeping them would duplicate
        // rows once the replay writes them again.
        if (!options.isCommitBatchingEnabled() || txId <= highestBufferedTxId) {
            if (writer != null && txId <= highestBufferedTxId) {
                LOG.warn("txId {} replays a batch already buffered (up to {}); dropping the "
                    + "buffered window, its batches are lost", txId, highestBufferedTxId);
                metrics.windowDropped();
            }
            abortWriter();
        }
        refreshTable();
        skipBatch = txId <= lastCommittedTxId;
        if (skipBatch) {
            LOG.info("txId {} is already committed (lastCommittedTxId {}), skipping replayed batch",
                txId, lastCommittedTxId);
            metrics.batchSkipped();
        }
    }

    /**
     * Pick up schema and partition spec evolution before the batch's writer is created. Without
     * this the state would keep writing against the metadata read at {@link #prepare} until the
     * worker restarts. Costs one catalog round-trip per batch, alongside the commit's own.
     */
    private void refreshTable() {
        try {
            table.refresh();
        } catch (RuntimeException e) {
            throw new FailedException("Failed refreshing Iceberg table metadata, failing batch", e);
        }
    }

    public void updateState(List<TridentTuple> tuples, TridentCollector collector) {
        if (skipBatch) {
            return;
        }
        try {
            if (writer == null) {
                writer = createWriter();
            }
            for (TridentTuple tuple : tuples) {
                writer.write(options.getRecordMapper().map(tuple, table.schema()));
            }
            metrics.recordsWritten(tuples.size());
        } catch (IOException e) {
            abortWriter();
            throw new FailedException("Failed writing records to Iceberg, failing batch", e);
        } catch (RuntimeException e) {
            abortWriter();
            throw e;
        }
    }

    @Override
    public void commit(Long txId) {
        if (skipBatch) {
            skipBatch = false;
            return;
        }
        highestBufferedTxId = txId;
        if (windowStartNanos == 0L) {
            windowStartNanos = System.nanoTime();
        }
        if (!shouldFlush()) {
            metrics.batchBuffered();
            LOG.debug("Buffering txId {} ({} bytes so far in the window)", txId, bufferedBytes());
            return;
        }
        flush(txId);
    }

    /**
     * Decide whether the buffered window has to be committed now. With no interval configured
     * every batch is flushed, which is the exactly-once behaviour.
     */
    private boolean shouldFlush() {
        if (!options.isCommitBatchingEnabled()) {
            return true;
        }
        Long intervalBytes = options.getCommitIntervalBytes();
        if (intervalBytes != null && bufferedBytes() >= intervalBytes) {
            return true;
        }
        Long intervalMillis = options.getCommitIntervalMillis();
        if (intervalMillis != null) {
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - windowStartNanos);
            return elapsedMillis >= intervalMillis;
        }
        return false;
    }

    private long bufferedBytes() {
        return countingAppenderFactory == null ? 0L : countingAppenderFactory.estimatedBytes();
    }

    private void flush(Long txId) {
        DataFile[] dataFiles = completeWriter();
        long startNanos = System.nanoTime();
        try {
            Transaction txn = table.newTransaction();
            txn.updateProperties().set(txidPropertyKey, String.valueOf(txId)).commit();
            if (dataFiles.length > 0) {
                AppendFiles append = txn.newAppend();
                for (DataFile dataFile : dataFiles) {
                    append.appendFile(dataFile);
                }
                append.commit();
            }
            txn.commitTransaction();
            lastCommittedTxId = txId;
            metrics.committed(dataFiles, System.nanoTime() - startNanos);
        } catch (CommitStateUnknownException e) {
            // The commit may or may not have reached the catalog. Do not delete the data files:
            // beginCommit on the replayed batch reads the txid property (atomic with the append)
            // and resolves the ambiguity.
            metrics.commitFailed();
            throw new FailedException("Iceberg commit state unknown, failing batch for replay", e);
        } catch (RuntimeException e) {
            metrics.commitFailed();
            throw new FailedException("Iceberg commit failed, failing batch", e);
        }
    }

    private TaskWriter<Record> createWriter() {
        Schema schema = table.schema();
        PartitionSpec spec = table.spec();
        FileFormat format = options.getFileFormat();
        long targetFileSize = options.getTargetFileSizeBytes() != null
            ? options.getTargetFileSizeBytes()
            : PropertyUtil.propertyAsLong(table.properties(),
                TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
                TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
        CountingAppenderFactory appenderFactory =
            new CountingAppenderFactory(new GenericAppenderFactory(schema, spec));
        this.countingAppenderFactory = appenderFactory;
        // A fresh OutputFileFactory per batch: its random operation id keeps file names from
        // replayed batches unique.
        OutputFileFactory fileFactory = OutputFileFactory
            .builderFor(table, partitionIndex, System.currentTimeMillis())
            .format(format)
            .build();
        if (spec.isUnpartitioned()) {
            return new UnpartitionedWriter<>(spec, format, appenderFactory, fileFactory, table.io(), targetFileSize);
        }
        return new PartitionedRecordWriter(spec, format, appenderFactory, fileFactory, table.io(), targetFileSize, schema);
    }

    private DataFile[] completeWriter() {
        try {
            return writer == null ? new DataFile[0] : writer.complete().dataFiles();
        } catch (IOException e) {
            // Files already closed by the writer may remain as never-committed orphans; they are
            // invisible to readers and removed by standard Iceberg orphan-file maintenance.
            throw new FailedException("Failed closing Iceberg writer, failing batch", e);
        } finally {
            writer = null;
            // The window's writers are done either way: if the commit below fails, its files
            // become orphans and the next batch must start counting from zero.
            resetWindow();
        }
    }

    private void abortWriter() {
        if (writer == null) {
            resetWindow();
            return;
        }
        try {
            writer.abort();
        } catch (IOException e) {
            LOG.warn("Failed aborting Iceberg writer; uncommitted files may remain as orphans", e);
        } finally {
            writer = null;
            resetWindow();
        }
    }

    private void resetWindow() {
        windowStartNanos = 0L;
        if (countingAppenderFactory != null) {
            countingAppenderFactory.reset();
        }
    }

    synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        removeShutdownHook();
        abortWriter();
        if (catalog instanceof Closeable) {
            try {
                ((Closeable) catalog).close();
            } catch (IOException e) {
                LOG.warn("Failed closing Iceberg catalog", e);
            }
        }
    }

    boolean isClosed() {
        return closed;
    }

    Thread getShutdownHook() {
        return shutdownHook;
    }

    private void removeShutdownHook() {
        Thread hook = shutdownHook;
        shutdownHook = null;
        // Removing a hook from within the hook itself is both pointless and illegal.
        if (hook == null || hook == Thread.currentThread()) {
            return;
        }
        try {
            Runtime.getRuntime().removeShutdownHook(hook);
        } catch (IllegalStateException e) {
            // The JVM is already shutting down and will run the hook itself; close() is idempotent.
            LOG.debug("JVM already shutting down, leaving the close hook in place", e);
        }
    }
}
