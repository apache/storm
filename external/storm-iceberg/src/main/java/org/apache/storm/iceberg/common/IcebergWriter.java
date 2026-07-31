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

package org.apache.storm.iceberg.common;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.storm.tuple.ITuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Turns tuples into durable Iceberg data files. Knows nothing about how they are delivered, so it
 * serves any Storm API; making the files visible is {@link IcebergCommitter}'s job.
 *
 * <p>Files are written eagerly as tuples arrive and closed by {@link #complete()}. Until then
 * nothing is visible to readers: an abandoned writer leaves orphan files, never partial rows.
 */
public class IcebergWriter implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergWriter.class);
    private static final String CATALOG_NAME = "storm-iceberg";

    private final IcebergOptions options;
    private final int taskId;

    private Catalog catalog;
    private Table table;
    private TaskWriter<Record> writer;
    private CountingAppenderFactory countingAppenderFactory;

    public IcebergWriter(IcebergOptions options, int taskId) {
        this.options = options;
        this.taskId = taskId;
    }

    /** Build the catalog and load — or, when configured, create — the target table. */
    public void open() {
        this.catalog = CatalogUtil.buildIcebergCatalog(
            CATALOG_NAME, options.getCatalogProperties(), new Configuration());
        TableIdentifier identifier = TableIdentifier.parse(options.getTableIdentifier());
        this.table = loadOrCreateTable(identifier);
        LOG.info("Opened Iceberg writer for table {}, task {}", identifier, taskId);
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
                LOG.info("Table {} was concurrently created by another task", identifier);
            }
        }
        return catalog.loadTable(identifier);
    }

    public Table table() {
        return table;
    }

    /** Map a tuple to a record and write it to the open file set. */
    public void write(ITuple tuple) throws IOException {
        if (writer == null) {
            writer = createWriter();
        }
        writer.write(options.getRecordMapper().map(tuple, table.schema()));
    }

    /**
     * Close the open files and hand back what was written, leaving a fresh buffer behind. The
     * files are durable but not yet referenced by the table.
     */
    public List<DataFile> complete() throws IOException {
        if (writer == null) {
            return List.of();
        }
        try {
            return Arrays.asList(writer.complete().dataFiles());
        } finally {
            writer = null;
            resetBuffer();
        }
    }

    /** Discard what has been written. Files already closed remain as orphans. */
    public void abort() {
        if (writer == null) {
            return;
        }
        try {
            writer.abort();
        } catch (IOException e) {
            LOG.warn("Failed aborting Iceberg writer; uncommitted files may remain as orphans", e);
        } finally {
            writer = null;
            resetBuffer();
        }
    }

    /** Roughly how many bytes the open files hold, for size-based flushing. */
    public long bufferedBytes() {
        return countingAppenderFactory == null ? 0L : countingAppenderFactory.estimatedBytes();
    }

    /**
     * Pick up schema and partition spec evolution. Without this the writer keeps using the
     * metadata read at {@link #open()} until the worker restarts.
     */
    public void refreshTable() {
        table.refresh();
    }

    private void resetBuffer() {
        if (countingAppenderFactory != null) {
            countingAppenderFactory.reset();
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
        // A fresh OutputFileFactory per file set: its random operation id keeps file names from
        // replayed tuples unique.
        OutputFileFactory fileFactory = OutputFileFactory
            .builderFor(table, taskId, System.currentTimeMillis())
            .format(format)
            .build();
        if (spec.isUnpartitioned()) {
            return new UnpartitionedWriter<>(spec, format, appenderFactory, fileFactory, table.io(), targetFileSize);
        }
        return new PartitionedRecordWriter(spec, format, appenderFactory, fileFactory, table.io(), targetFileSize, schema);
    }

    @Override
    public void close() {
        abort();
        if (catalog instanceof Closeable) {
            try {
                ((Closeable) catalog).close();
            } catch (IOException e) {
                LOG.warn("Failed closing Iceberg catalog", e);
            }
        }
    }
}
