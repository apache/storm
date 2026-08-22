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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.iceberg.common.CommitWal;
import org.apache.storm.iceberg.common.DataFileCodec;
import org.apache.storm.iceberg.common.IcebergCommitter;
import org.apache.storm.iceberg.common.IcebergOptions;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IcebergCommitterBoltTest {

    private static final Schema SCHEMA = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()));
    private static final TableIdentifier TABLE_ID = TableIdentifier.of("db", "events");

    @TempDir
    Path tempDir;

    private String warehouse;
    private HadoopCatalog verifyCatalog;
    private Table table;
    private OutputCollector collector;

    @BeforeEach
    void setUp() {
        warehouse = tempDir.toUri().toString();
        verifyCatalog = new HadoopCatalog(new Configuration(), warehouse);
        table = verifyCatalog.createTable(TABLE_ID, SCHEMA, PartitionSpec.unpartitioned());
        collector = mock(OutputCollector.class);
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

    private IcebergCommitterBolt prepared(IcebergOptions options) {
        IcebergCommitterBolt bolt = new IcebergCommitterBolt(options);
        Map<String, Object> topoConf = new HashMap<>();
        topoConf.put(Config.TOPOLOGY_NAME, "test-topology");
        TopologyContext context = mock(TopologyContext.class);
        when(context.getThisTaskId()).thenReturn(9);
        when(context.getThisComponentId()).thenReturn("iceberg-committer");
        when(context.getThisTaskIndex()).thenReturn(0);
        when(context.getComponentTasks("iceberg-committer")).thenReturn(List.of(9));
        bolt.prepare(topoConf, context, collector);
        return bolt;
    }

    private DataFile dataFile(String name) {
        return DataFiles.builder(table.spec())
            .withPath(table.location() + "/data/" + name)
            .withFileSizeInBytes(1024L)
            .withRecordCount(1L)
            .withFormat(FileFormat.PARQUET)
            .build();
    }

    private Tuple descriptor(int writerTaskId, String fileName) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn("iceberg-writer");
        when(tuple.getIntegerByField(IcebergWriterBolt.FIELD_WRITER_TASK_ID)).thenReturn(writerTaskId);
        when(tuple.getStringByField(IcebergWriterBolt.FIELD_DATA_FILES))
            .thenReturn(DataFileCodec.toJson(List.of(dataFile(fileName)), table));
        return tuple;
    }

    private Tuple tickTuple() {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn(Constants.SYSTEM_COMPONENT_ID);
        when(tuple.getSourceStreamId()).thenReturn(Constants.SYSTEM_TICK_STREAM_ID);
        return tuple;
    }

    private int snapshotCount() {
        table.refresh();
        int count = 0;
        for (Object ignored : table.snapshots()) {
            count++;
        }
        return count;
    }

    @Test
    void descriptorsFromSeveralWritersLandInOneSnapshot() {
        IcebergCommitterBolt bolt = prepared(baseOptions()
            .withGroupCommitMaxDataFiles(3)
            .withGroupCommitIntervalMillis(600_000L)
            .build());

        bolt.execute(descriptor(1, "a.parquet"));
        bolt.execute(descriptor(2, "b.parquet"));
        bolt.execute(descriptor(3, "c.parquet"));

        assertEquals(1, snapshotCount(), "one append covers every writer");
        verify(collector, times(3)).ack(any());
        bolt.cleanup();
    }

    @Test
    void nothingIsCommittedOrAckedBelowTheThreshold() {
        IcebergCommitterBolt bolt = prepared(baseOptions()
            .withGroupCommitMaxDataFiles(5)
            .withGroupCommitIntervalMillis(600_000L)
            .build());

        bolt.execute(descriptor(1, "a.parquet"));

        assertEquals(0, snapshotCount());
        verify(collector, never()).ack(any());
        bolt.cleanup();
    }

    @Test
    void aTickTupleCommitsWhatHasAccumulated() {
        IcebergCommitterBolt bolt = prepared(baseOptions()
            .withGroupCommitMaxDataFiles(100)
            .withGroupCommitIntervalMillis(600_000L)
            .build());

        bolt.execute(descriptor(1, "a.parquet"));
        bolt.execute(tickTuple());

        assertEquals(1, snapshotCount());
        verify(collector, times(1)).ack(any());
        bolt.cleanup();
    }

    @Test
    void aTickTupleWithNothingAccumulatedCommitsNothing() {
        IcebergCommitterBolt bolt = prepared(baseOptions().build());

        bolt.execute(tickTuple());

        assertEquals(0, snapshotCount());
        verify(collector, never()).ack(any());
        bolt.cleanup();
    }

    @Test
    void aFailedCommitFailsEveryAccumulatedDescriptor() {
        IcebergCommitterBolt bolt = prepared(baseOptions()
            .withGroupCommitMaxDataFiles(100)
            .withGroupCommitIntervalMillis(600_000L)
            .build());
        Tuple first = descriptor(1, "a.parquet");
        Tuple second = descriptor(2, "b.parquet");
        bolt.execute(first);
        bolt.execute(second);
        // The table disappears underneath the committer, so the append cannot land. A tick tuple
        // forces commitGroup() directly, without an intervening refresh that would instead fail
        // just the tuple that triggered it (see anUnreadableDescriptorIsFailedWithoutPoisoningTheGroup).
        verifyCatalog.dropTable(TABLE_ID, true);

        bolt.execute(tickTuple());

        verify(collector, never()).ack(any());
        verify(collector, times(2)).fail(any());
    }

    @Test
    void anUnreadableDescriptorIsFailedWithoutPoisoningTheGroup() {
        IcebergCommitterBolt bolt = prepared(baseOptions()
            .withGroupCommitMaxDataFiles(1)
            .withGroupCommitIntervalMillis(600_000L)
            .build());
        Tuple broken = mock(Tuple.class);
        when(broken.getSourceComponent()).thenReturn("iceberg-writer");
        when(broken.getIntegerByField(IcebergWriterBolt.FIELD_WRITER_TASK_ID)).thenReturn(1);
        when(broken.getStringByField(IcebergWriterBolt.FIELD_DATA_FILES)).thenReturn("not json");

        bolt.execute(broken);
        bolt.execute(descriptor(2, "b.parquet"));

        verify(collector, times(1)).fail(broken);
        verify(collector, times(1)).ack(any());
        assertEquals(1, snapshotCount(), "the good descriptor still commits");
        bolt.cleanup();
    }

    @Test
    void prepareAbandonsACommitLeftPendingByAnEarlierRun() {
        CommitWal wal = new CommitWal(table, null, "test-topology", "iceberg-committer", 0);
        wal.write(List.of(dataFile("left-behind.parquet")));

        IcebergCommitterBolt bolt = prepared(baseOptions().build());

        table.refresh();
        // Those descriptors were never acked, so the writers' batches are replayed from the source.
        assertEquals(0, snapshotCount(), "the pending commit is not appended on startup");
        assertEquals(List.of(), wal.listPending(), "and its WAL entry is cleared");
        bolt.cleanup();
    }

    @Test
    void cleanupFailsWhatWasNeverCommitted() {
        IcebergCommitterBolt bolt = prepared(baseOptions()
            .withGroupCommitMaxDataFiles(100)
            .withGroupCommitIntervalMillis(600_000L)
            .build());
        bolt.execute(descriptor(1, "a.parquet"));

        bolt.cleanup();

        assertEquals(0, snapshotCount());
        verify(collector, never()).ack(any());
        verify(collector, times(1)).fail(any());
    }
}
