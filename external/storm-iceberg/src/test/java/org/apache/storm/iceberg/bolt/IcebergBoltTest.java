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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.iceberg.common.CommitWal;
import org.apache.storm.iceberg.common.IcebergCommitter;
import org.apache.storm.iceberg.common.IcebergOptions;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IcebergBoltTest {

    private static final Schema SCHEMA = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()));
    private static final TableIdentifier TABLE_ID = TableIdentifier.of("db", "events");

    @TempDir
    Path tempDir;

    private String warehouse;
    private HadoopCatalog verifyCatalog;
    private OutputCollector collector;

    @BeforeEach
    void setUp() {
        warehouse = tempDir.toUri().toString();
        verifyCatalog = new HadoopCatalog(new Configuration(), warehouse);
        verifyCatalog.createTable(TABLE_ID, SCHEMA, PartitionSpec.unpartitioned());
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

    private IcebergBolt prepared(IcebergOptions options) {
        IcebergBolt bolt = new IcebergBolt(options);
        Map<String, Object> topoConf = new HashMap<>();
        topoConf.put(Config.TOPOLOGY_NAME, "test-topology");
        TopologyContext context = mock(TopologyContext.class);
        when(context.getThisTaskId()).thenReturn(7);
        when(context.getThisComponentId()).thenReturn("iceberg");
        when(context.getThisTaskIndex()).thenReturn(0);
        bolt.prepare(topoConf, context, collector);
        return bolt;
    }

    private Tuple tuple(long id, String name) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn("spout");
        when(tuple.contains(anyString())).thenReturn(false);
        when(tuple.contains("id")).thenReturn(true);
        when(tuple.contains("name")).thenReturn(true);
        when(tuple.getValueByField("id")).thenReturn(id);
        when(tuple.getValueByField("name")).thenReturn(name);
        return tuple;
    }

    private Tuple tickTuple() {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn(Constants.SYSTEM_COMPONENT_ID);
        when(tuple.getSourceStreamId()).thenReturn(Constants.SYSTEM_TICK_STREAM_ID);
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
    void tuplesAreNotAckedBeforeTheirCommitLands() {
        IcebergBolt bolt = prepared(baseOptions().withCommitIntervalRecords(3).build());

        bolt.execute(tuple(1L, "alice"));
        bolt.execute(tuple(2L, "bob"));

        verify(collector, never()).ack(org.mockito.ArgumentMatchers.any());
        assertEquals(0, readRows().size(), "nothing is visible before the commit");
        bolt.cleanup();
    }

    @Test
    void reachingTheRecordThresholdCommitsAndAcksEveryBufferedTuple() {
        IcebergBolt bolt = prepared(baseOptions().withCommitIntervalRecords(2).build());

        bolt.execute(tuple(1L, "alice"));
        bolt.execute(tuple(2L, "bob"));

        verify(collector, times(2)).ack(org.mockito.ArgumentMatchers.any());
        assertEquals(2, readRows().size());
        bolt.cleanup();
    }

    @Test
    void aTickTupleFlushesWhatIsBuffered() {
        IcebergBolt bolt = prepared(baseOptions().withCommitIntervalRecords(100).build());

        bolt.execute(tuple(1L, "alice"));
        bolt.execute(tickTuple());

        verify(collector, times(1)).ack(org.mockito.ArgumentMatchers.any());
        assertEquals(1, readRows().size());
        bolt.cleanup();
    }

    @Test
    void aTickTupleWithNothingBufferedCommitsNothing() {
        IcebergBolt bolt = prepared(baseOptions().withCommitIntervalRecords(100).build());

        bolt.execute(tickTuple());

        verify(collector, never()).ack(org.mockito.ArgumentMatchers.any());
        assertEquals(0, readRows().size());
        bolt.cleanup();
    }

    @Test
    void aFailedCommitFailsTheBufferedTuplesInsteadOfAckingThem() {
        IcebergBolt bolt = prepared(baseOptions().withCommitIntervalRecords(2).build());
        bolt.execute(tuple(1L, "alice"));
        // The table disappears underneath the bolt, so the commit cannot land.
        verifyCatalog.dropTable(TABLE_ID, true);

        bolt.execute(tuple(2L, "bob"));

        verify(collector, never()).ack(org.mockito.ArgumentMatchers.any());
        verify(collector, times(2)).fail(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void aWriteFailureFailsTheOpenBatchAndTheOffendingTuple() {
        IcebergBolt bolt = prepared(baseOptions().withCommitIntervalRecords(5).build());
        bolt.execute(tuple(1L, "alice"));
        Tuple wrongType = mock(Tuple.class);
        when(wrongType.getSourceComponent()).thenReturn("spout");
        when(wrongType.contains(anyString())).thenReturn(false);
        when(wrongType.contains("id")).thenReturn(true);
        when(wrongType.contains("name")).thenReturn(true);
        when(wrongType.getValueByField("id")).thenReturn("not-a-long");
        when(wrongType.getValueByField("name")).thenReturn("carol");

        bolt.execute(wrongType);

        verify(collector, never()).ack(org.mockito.ArgumentMatchers.any());
        // The tuple that failed to map is failed too, rather than left to time out.
        verify(collector, times(1)).fail(wrongType);
        verify(collector, times(2)).fail(org.mockito.ArgumentMatchers.any());
        assertEquals(0, readRows().size());
    }

    @Test
    void theBoltDeclaresItsOwnTickInterval() {
        // Without the declaration the time-based threshold is only ever evaluated when the next
        // tuple arrives, so a stalled stream leaves the last batch open and un-acked indefinitely.
        IcebergBolt bolt = new IcebergBolt(baseOptions().withTickIntervalSecs(7).build());

        assertEquals(7, bolt.getComponentConfiguration().get(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS));
        assertNull(new IcebergBolt(baseOptions().build()).getComponentConfiguration(),
            "and inherits the topology-wide setting when none is configured");
    }

    @Test
    void prepareAbandonsACommitLeftPendingByAnEarlierRun() {
        Table table = verifyCatalog.loadTable(TABLE_ID);
        CommitWal wal = new CommitWal(table, null, "test-topology", "iceberg", 0);
        wal.write(List.of(DataFiles.builder(table.spec())
            .withPath(table.location() + "/data/left-behind.parquet")
            .withFileSizeInBytes(1024L)
            .withRecordCount(1L)
            .withFormat(FileFormat.PARQUET)
            .build()));

        IcebergBolt bolt = prepared(baseOptions().withCommitIntervalRecords(100).build());

        table.refresh();
        // The batch was never acked, so the source replays it; appending the entry's files here
        // would add a second copy of the rows that replay writes.
        assertNull(table.currentSnapshot(), "the pending commit is not appended on startup");
        assertEquals(List.of(), wal.listPending(), "and its WAL entry is cleared");
        bolt.cleanup();
    }
}
