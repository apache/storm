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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.storm.tuple.ITuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IcebergWriterTest {

    private static final Schema SCHEMA = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()));
    private static final TableIdentifier TABLE_ID = TableIdentifier.of("db", "events");

    @TempDir
    Path tempDir;

    private String warehouse;
    private HadoopCatalog verifyCatalog;

    @BeforeEach
    void setUp() {
        warehouse = tempDir.toUri().toString();
        verifyCatalog = new HadoopCatalog(new Configuration(), warehouse);
    }

    @AfterEach
    void tearDown() throws IOException {
        verifyCatalog.close();
    }

    private IcebergOptions.Builder baseOptions() {
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put(CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP);
        catalogProps.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        return new IcebergOptions.Builder()
            .withCatalogProperties(catalogProps)
            .withTable("db.events");
    }

    private ITuple tuple(long id, String name) {
        ITuple tuple = mock(ITuple.class);
        when(tuple.contains(anyString())).thenReturn(false);
        when(tuple.contains("id")).thenReturn(true);
        when(tuple.contains("name")).thenReturn(true);
        when(tuple.getValueByField("id")).thenReturn(id);
        when(tuple.getValueByField("name")).thenReturn(name);
        return tuple;
    }

    private List<Record> readRows() {
        Table table = verifyCatalog.loadTable(TABLE_ID);
        table.refresh();
        List<Record> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            records.forEach(rows::add);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return rows;
    }

    @Test
    void writtenTuplesBecomeReadableRowsOnceCommitted() throws IOException {
        verifyCatalog.createTable(TABLE_ID, SCHEMA, PartitionSpec.unpartitioned());
        try (IcebergWriter writer = new IcebergWriter(baseOptions().build(), 0)) {
            writer.open();
            writer.write(tuple(1L, "alice"));
            writer.write(tuple(2L, "bob"));

            List<DataFile> dataFiles = writer.complete();
            assertFalse(dataFiles.isEmpty(), "completing a non-empty writer yields data files");

            CommitWal wal = new CommitWal(writer.table(), "topo", "iceberg", 0);
            new IcebergCommitter(writer.table(), wal, new IcebergMetrics(null)).commit(dataFiles);
        }

        assertEquals(2, readRows().size());
    }

    @Test
    void tableIsCreatedOnFirstUseWhenAutoCreateIsConfigured() throws IOException {
        try (IcebergWriter writer = new IcebergWriter(
            baseOptions().withAutoCreate(SCHEMA, PartitionSpec.unpartitioned()).build(), 0)) {
            writer.open();
            assertTrue(verifyCatalog.tableExists(TABLE_ID));
        }
    }

    @Test
    void completingAnEmptyWriterYieldsNoDataFiles() throws IOException {
        verifyCatalog.createTable(TABLE_ID, SCHEMA, PartitionSpec.unpartitioned());
        try (IcebergWriter writer = new IcebergWriter(baseOptions().build(), 0)) {
            writer.open();
            assertEquals(List.of(), writer.complete());
        }
    }

    @Test
    void partitionedTablesFanOutOneFilePerPartition() throws IOException {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("name").build();
        verifyCatalog.createTable(TABLE_ID, SCHEMA, spec);
        try (IcebergWriter writer = new IcebergWriter(baseOptions().build(), 0)) {
            writer.open();
            writer.write(tuple(1L, "alice"));
            writer.write(tuple(2L, "bob"));

            assertEquals(2, writer.complete().size(), "one data file per partition value");
        }
    }

    @Test
    void bufferedBytesGrowWithWritesAndResetOnComplete() throws IOException {
        verifyCatalog.createTable(TABLE_ID, SCHEMA, PartitionSpec.unpartitioned());
        try (IcebergWriter writer = new IcebergWriter(baseOptions().build(), 0)) {
            writer.open();
            assertEquals(0L, writer.bufferedBytes());
            writer.write(tuple(1L, "alice"));
            assertTrue(writer.bufferedBytes() > 0L, "a written tuple counts towards the buffer");

            writer.complete();
            assertEquals(0L, writer.bufferedBytes(), "completing starts a fresh buffer");
        }
    }
}
