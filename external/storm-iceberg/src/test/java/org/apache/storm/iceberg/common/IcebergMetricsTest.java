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

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IcebergMetricsTest {

    private static final Schema SCHEMA = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()));

    @TempDir
    Path tempDir;

    private Table table;
    private RecordingMetricsContext context;

    @BeforeEach
    void setUp() {
        HadoopCatalog catalog = new HadoopCatalog(new Configuration(), tempDir.toUri().toString());
        table = catalog.createTable(TableIdentifier.of("db", "events"), SCHEMA,
            PartitionSpec.unpartitioned());
        context = new RecordingMetricsContext();
    }

    private DataFile dataFile(String name) {
        return DataFiles.builder(table.spec())
            .withPath(table.location() + "/data/" + name)
            .withFileSizeInBytes(1024L)
            .withRecordCount(1L)
            .withFormat(FileFormat.PARQUET)
            .build();
    }

    @Test
    void sealingCountsFilesAndTimesTheSeal() {
        IcebergMetrics metrics = new IcebergMetrics(context);

        metrics.sealed(List.of(dataFile("a.parquet"), dataFile("b.parquet")),
            TimeUnit.MILLISECONDS.toNanos(3));

        assertEquals(2L, context.counter(IcebergMetrics.DATA_FILES_SEALED));
        assertEquals(1L, context.timerCount(IcebergMetrics.SEAL_LATENCY));
    }

    @Test
    void pendingGaugesReportWhatTheirSuppliersSay() {
        IcebergMetrics metrics = new IcebergMetrics(context);

        metrics.registerPendingGauges(context, () -> 12, () -> 3400L);

        assertEquals(12, context.gauge(IcebergMetrics.PENDING_DATA_FILES));
        assertEquals(3400L, context.gauge(IcebergMetrics.OLDEST_PENDING_AGE_MS));
    }

    @Test
    void aNullMetricsContextIsTolerated() {
        IcebergMetrics metrics = new IcebergMetrics(null);

        metrics.sealed(List.of(dataFile("a.parquet")), 1L);
        metrics.registerPendingGauges(null, () -> 1, () -> 1L);
    }
}
