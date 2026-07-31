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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CommitWalTest {

    private static final Schema SCHEMA = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()));
    private static final TableIdentifier TABLE_ID = TableIdentifier.of("db", "events");

    @TempDir
    Path tempDir;

    private HadoopCatalog catalog;
    private Table table;

    @BeforeEach
    void setUp() {
        catalog = new HadoopCatalog(new Configuration(), tempDir.toUri().toString());
        table = catalog.createTable(TABLE_ID, SCHEMA, PartitionSpec.unpartitioned());
    }

    @AfterEach
    void tearDown() throws IOException {
        catalog.close();
    }

    private DataFile dataFile(String name, long records) {
        return DataFiles.builder(table.spec())
            .withPath(table.location() + "/data/" + name)
            .withFileSizeInBytes(1024L)
            .withRecordCount(records)
            .withFormat(FileFormat.PARQUET)
            .build();
    }

    @Test
    void aTaskThatHasNeverWrittenHasNothingPending() {
        assertEquals(List.of(), new CommitWal(table, "topo", 0).listPending());
    }

    @Test
    void aPendingEntryCarriesTheTimeItWasWritten() {
        long before = System.currentTimeMillis();
        CommitWal wal = new CommitWal(table, "topo", 0);

        CommitWal.WalEntry written = wal.write(List.of(dataFile("a.parquet", 1L)));
        long after = System.currentTimeMillis();

        assertTrue(written.createdAtMs() >= before && written.createdAtMs() <= after,
            "the entry records when it was written");
        assertEquals(written.createdAtMs(), wal.listPending().get(0).createdAtMs(),
            "and listing recovers it without reading the entry");
    }

    @Test
    void pendingEntryReadsBackTheDataFilesItWasWritten() {
        CommitWal wal = new CommitWal(table, "topo", 3);
        DataFile first = dataFile("a.parquet", 5L);
        DataFile second = dataFile("b.parquet", 7L);

        CommitWal.WalEntry entry = wal.write(List.of(first, second));

        List<CommitWal.WalEntry> pending = wal.listPending();
        assertEquals(1, pending.size());
        assertEquals(entry.commitId(), pending.get(0).commitId());

        List<DataFile> recovered = wal.read(pending.get(0));
        assertEquals(2, recovered.size());
        assertEquals(first.location(), recovered.get(0).location());
        assertEquals(5L, recovered.get(0).recordCount());
        assertEquals(second.location(), recovered.get(1).location());
        assertTrue(entry.location().contains("topo"), "WAL path should be scoped by topology");
    }
}
