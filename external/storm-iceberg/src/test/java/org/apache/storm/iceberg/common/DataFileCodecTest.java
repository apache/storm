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

class DataFileCodecTest {

    private static final Schema SCHEMA = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(2, "region", Types.StringType.get()));
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

    private DataFile unpartitionedFile(String name, long records) {
        return DataFiles.builder(table.spec())
            .withPath(table.location() + "/data/" + name)
            .withFileSizeInBytes(1024L)
            .withRecordCount(records)
            .withFormat(FileFormat.PARQUET)
            .build();
    }

    @Test
    void anEmptyListRoundTrips() {
        String json = DataFileCodec.toJson(List.of(), table);

        assertEquals(List.of(), DataFileCodec.fromJson(json, table.specs()));
    }

    @Test
    void unpartitionedFilesRoundTripWithTheirCounts() {
        DataFile first = unpartitionedFile("a.parquet", 5L);
        DataFile second = unpartitionedFile("b.parquet", 7L);

        List<DataFile> recovered =
            DataFileCodec.fromJson(DataFileCodec.toJson(List.of(first, second), table), table.specs());

        assertEquals(2, recovered.size());
        assertEquals(first.location(), recovered.get(0).location());
        assertEquals(5L, recovered.get(0).recordCount());
        assertEquals(second.location(), recovered.get(1).location());
        assertEquals(7L, recovered.get(1).recordCount());
    }

    @Test
    void aFileWrittenAgainstAnOlderSpecResolvesThroughTheTableSpecs() {
        DataFile beforeEvolution = unpartitionedFile("old.parquet", 3L);
        String json = DataFileCodec.toJson(List.of(beforeEvolution), table);
        table.updateSpec().addField("region").commit();
        table.refresh();

        List<DataFile> recovered = DataFileCodec.fromJson(json, table.specs());

        assertEquals(1, recovered.size());
        assertEquals(beforeEvolution.specId(), recovered.get(0).specId(),
            "the file keeps the spec it was written with");
        assertEquals(3L, recovered.get(0).recordCount());
    }
}
