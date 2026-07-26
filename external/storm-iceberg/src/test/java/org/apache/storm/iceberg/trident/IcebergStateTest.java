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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.storm.Config;
import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.tuple.TridentTuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IcebergStateTest {

    private static final Schema SCHEMA = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()));
    private static final TableIdentifier TABLE_ID = TableIdentifier.of("db", "events");
    private static final String TOPOLOGY_NAME = "test-topology";

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

    private Map<String, Object> topoConf() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_NAME, TOPOLOGY_NAME);
        return conf;
    }

    private IcebergState newState(IcebergOptions options) {
        return newState(options, null);
    }

    private IcebergState newState(IcebergOptions options, RecordingMetricsContext metrics) {
        IcebergState state = new IcebergState(options, 0);
        state.prepare(topoConf(), metrics);
        return state;
    }

    private TridentTuple tuple(long id, String name) {
        TridentTuple tuple = mock(TridentTuple.class);
        when(tuple.contains(anyString())).thenReturn(false);
        when(tuple.contains("id")).thenReturn(true);
        when(tuple.contains("name")).thenReturn(true);
        when(tuple.getValueByField("id")).thenReturn(id);
        when(tuple.getValueByField("name")).thenReturn(name);
        return tuple;
    }

    private TridentTuple tupleWithExtra(long id, String name, String extra) {
        TridentTuple tuple = tuple(id, name);
        when(tuple.contains("extra")).thenReturn(true);
        when(tuple.getValueByField("extra")).thenReturn(extra);
        return tuple;
    }

    private int countSnapshots() {
        Table table = verifyCatalog.loadTable(TABLE_ID);
        table.refresh();
        int count = 0;
        for (Object ignored : table.snapshots()) {
            count++;
        }
        return count;
    }

    private List<Record> readRows() throws IOException {
        Table table = verifyCatalog.loadTable(TABLE_ID);
        List<Record> rows = new ArrayList<>();
        try (CloseableIterable<Record> iterable = IcebergGenerics.read(table).build()) {
            iterable.forEach(rows::add);
        }
        return rows;
    }

    @Test
    void prepareFailsWhenTableMissingAndNoAutoCreate() {
        IcebergState state = new IcebergState(baseOptions().build(), 0);
        assertThrows(NoSuchTableException.class, () -> state.prepare(topoConf(), null));
    }

    @Test
    void prepareAutoCreatesTable() {
        IcebergState state = newState(baseOptions()
            .withAutoCreate(SCHEMA, PartitionSpec.unpartitioned())
            .build());
        try {
            assertTrue(verifyCatalog.tableExists(TABLE_ID));
            assertEquals(0L, state.getLastCommittedTxId());
        } finally {
            state.close();
        }
    }

    @Test
    void prepareLoadsExistingTableAndReadsTxid() {
        Table table = verifyCatalog.createTable(TABLE_ID, SCHEMA);
        table.updateProperties()
            .set("storm.trident." + TOPOLOGY_NAME + ".0.last-committed-txid", "7")
            .commit();

        IcebergState state = newState(baseOptions().build());
        try {
            assertEquals(7L, state.getLastCommittedTxId());
        } finally {
            state.close();
        }
    }

    @Test
    void writesBatchAndCommitsAtomically() throws IOException {
        verifyCatalog.createTable(TABLE_ID, SCHEMA);
        IcebergState state = newState(baseOptions().build());
        try {
            state.beginCommit(1L);
            state.updateState(List.of(tuple(1L, "a"), tuple(2L, "b")), null);
            state.commit(1L);

            List<Record> rows = readRows();
            assertEquals(2, rows.size());
            Table table = verifyCatalog.loadTable(TABLE_ID);
            assertEquals("1", table.properties().get("storm.trident." + TOPOLOGY_NAME + ".0.last-committed-txid"));
        } finally {
            state.close();
        }
    }

    @Test
    void emptyBatchStillAdvancesTxid() throws IOException {
        verifyCatalog.createTable(TABLE_ID, SCHEMA);
        IcebergState state = newState(baseOptions().build());
        try {
            state.beginCommit(1L);
            state.commit(1L);

            assertEquals(0, readRows().size());
            Table table = verifyCatalog.loadTable(TABLE_ID);
            assertEquals("1", table.properties().get("storm.trident." + TOPOLOGY_NAME + ".0.last-committed-txid"));
            assertEquals(1L, state.getLastCommittedTxId());
        } finally {
            state.close();
        }
    }

    @Test
    void consecutiveBatchesAppend() throws IOException {
        verifyCatalog.createTable(TABLE_ID, SCHEMA);
        IcebergState state = newState(baseOptions().build());
        try {
            state.beginCommit(1L);
            state.updateState(List.of(tuple(1L, "a")), null);
            state.commit(1L);

            state.beginCommit(2L);
            state.updateState(List.of(tuple(2L, "b")), null);
            state.commit(2L);

            assertEquals(2, readRows().size());
        } finally {
            state.close();
        }
    }

    @Test
    void replayedBatchIsSkipped() throws IOException {
        verifyCatalog.createTable(TABLE_ID, SCHEMA);
        IcebergState state = newState(baseOptions().build());
        try {
            state.beginCommit(1L);
            state.updateState(List.of(tuple(1L, "a")), null);
            state.commit(1L);

            // Trident replays txid 1 (e.g. commit acknowledgement was lost)
            state.beginCommit(1L);
            state.updateState(List.of(tuple(1L, "a")), null);
            state.commit(1L);

            assertEquals(1, readRows().size());
        } finally {
            state.close();
        }
    }

    @Test
    void replayAfterRestartIsSkipped() throws IOException {
        verifyCatalog.createTable(TABLE_ID, SCHEMA);
        IcebergOptions options = baseOptions().build();

        IcebergState first = newState(options);
        try {
            first.beginCommit(1L);
            first.updateState(List.of(tuple(1L, "a")), null);
            first.commit(1L);
        } finally {
            first.close();
        }

        // Worker restarts: a fresh state must recover the txid from the table and skip the replay.
        IcebergState second = newState(options);
        try {
            assertEquals(1L, second.getLastCommittedTxId());
            second.beginCommit(1L);
            second.updateState(List.of(tuple(1L, "a")), null);
            second.commit(1L);

            assertEquals(1, readRows().size());

            // The next batch proceeds normally.
            second.beginCommit(2L);
            second.updateState(List.of(tuple(2L, "b")), null);
            second.commit(2L);
            assertEquals(2, readRows().size());
        } finally {
            second.close();
        }
    }

    @Test
    void crashBeforeCommitLeavesNoVisibleData() throws IOException {
        verifyCatalog.createTable(TABLE_ID, SCHEMA);
        IcebergOptions options = baseOptions().build();

        IcebergState first = newState(options);
        first.beginCommit(1L);
        first.updateState(List.of(tuple(1L, "a")), null);
        // Simulated crash: no commit(1L). Written files are never committed.
        first.close();

        assertEquals(0, readRows().size());

        IcebergState second = newState(options);
        try {
            second.beginCommit(1L);
            second.updateState(List.of(tuple(1L, "a")), null);
            second.commit(1L);

            assertEquals(1, readRows().size());
        } finally {
            second.close();
        }
    }

    @Test
    void writesToPartitionedTable() throws IOException {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("name").build();
        verifyCatalog.createTable(TABLE_ID, SCHEMA, spec);

        IcebergState state = newState(baseOptions().build());
        try {
            state.beginCommit(1L);
            state.updateState(List.of(tuple(1L, "alpha"), tuple(2L, "beta"), tuple(3L, "alpha")), null);
            state.commit(1L);

            assertEquals(3, readRows().size());
            Table table = verifyCatalog.loadTable(TABLE_ID);
            // identity("name") with two distinct values -> one data file per partition
            assertEquals("2", table.currentSnapshot().summary().get("added-data-files"));
        } finally {
            state.close();
        }
    }

    @Test
    void failedAttemptThenReplayDoesNotDuplicateWithinBatch() throws IOException {
        verifyCatalog.createTable(TABLE_ID, SCHEMA);
        IcebergState state = newState(baseOptions().build());
        try {
            // First attempt of txid 1 writes some tuples, then the batch fails before commit
            // (e.g. another component of the topology failed the batch).
            state.beginCommit(1L);
            state.updateState(List.of(tuple(1L, "a")), null);

            // Replayed attempt of the same txid: beginCommit must discard the leftover writer.
            state.beginCommit(1L);
            state.updateState(List.of(tuple(1L, "a")), null);
            state.commit(1L);

            assertEquals(1, readRows().size());
        } finally {
            state.close();
        }
    }

    /** Mapper that fails on tuples whose "name" field equals "poison". */
    private static class PoisonRecordMapper extends FieldNameRecordMapper {
        private static final long serialVersionUID = 1L;

        @Override
        public Record map(TridentTuple tuple, Schema schema) {
            if ("poison".equals(tuple.getValueByField("name"))) {
                throw new IllegalStateException("poisoned tuple");
            }
            return super.map(tuple, schema);
        }
    }

    @Test
    void mappingFailureAbortsWriterAndPropagates() throws IOException {
        verifyCatalog.createTable(TABLE_ID, SCHEMA);
        IcebergState state = newState(baseOptions()
            .withRecordMapper(new PoisonRecordMapper())
            .build());
        try {
            state.beginCommit(1L);
            assertThrows(IllegalStateException.class,
                () -> state.updateState(List.of(tuple(1L, "ok"), tuple(2L, "poison")), null));

            // Replay of the same txid with clean data succeeds and contains no leftovers
            // from the failed attempt.
            state.beginCommit(1L);
            state.updateState(List.of(tuple(1L, "ok"), tuple(2L, "fixed")), null);
            state.commit(1L);

            assertEquals(2, readRows().size());
        } finally {
            state.close();
        }
    }

    @Test
    void closeIsIdempotentAndDeregistersTheShutdownHook() {
        verifyCatalog.createTable(TABLE_ID, SCHEMA);
        IcebergState state = newState(baseOptions().build());
        Thread hook = state.getShutdownHook();
        assertNotNull(hook, "prepare must register a shutdown hook to release the catalog");

        state.close();
        assertTrue(state.isClosed());
        assertNull(state.getShutdownHook());
        // false means the JVM no longer holds the hook, i.e. close() really deregistered it.
        assertFalse(Runtime.getRuntime().removeShutdownHook(hook));

        state.close();
        assertTrue(state.isClosed());
    }

    @Test
    void picksUpSchemaEvolutionBetweenBatches() throws IOException {
        verifyCatalog.createTable(TABLE_ID, SCHEMA);
        IcebergState state = newState(baseOptions().build());
        try {
            state.beginCommit(1L);
            state.updateState(List.of(tuple(1L, "before")), null);
            state.commit(1L);

            // The table gains a column while the topology is running.
            verifyCatalog.loadTable(TABLE_ID).updateSchema()
                .addColumn("extra", Types.StringType.get())
                .commit();

            state.beginCommit(2L);
            state.updateState(List.of(tupleWithExtra(2L, "after", "value")), null);
            state.commit(2L);

            List<Record> rows = readRows();
            assertEquals(2, rows.size());
            // Without a refresh the state would still map against the two-column schema captured
            // at prepare() and silently drop the new column.
            assertTrue(rows.stream().anyMatch(r -> "value".equals(r.getField("extra"))));
        } finally {
            state.close();
        }
    }

    @Test
    void metricsCountRecordsFilesAndCommits() {
        verifyCatalog.createTable(TABLE_ID, SCHEMA);
        RecordingMetricsContext metrics = new RecordingMetricsContext();
        IcebergState state = newState(baseOptions().build(), metrics);
        try {
            state.beginCommit(1L);
            state.updateState(List.of(tuple(1L, "a"), tuple(2L, "b"), tuple(3L, "c")), null);
            state.commit(1L);

            assertEquals(3L, metrics.counter(IcebergStateMetrics.RECORDS_WRITTEN));
            assertEquals(1L, metrics.counter(IcebergStateMetrics.DATA_FILES_COMMITTED));
            assertTrue(metrics.counter(IcebergStateMetrics.BYTES_COMMITTED) > 0L);
            assertEquals(1L, metrics.timerCount(IcebergStateMetrics.COMMIT_LATENCY));
            assertEquals(0L, metrics.counter(IcebergStateMetrics.COMMIT_FAILURES));
            assertEquals(0L, metrics.counter(IcebergStateMetrics.BATCHES_SKIPPED));
        } finally {
            state.close();
        }
    }

    @Test
    void metricsCountSkippedReplays() {
        verifyCatalog.createTable(TABLE_ID, SCHEMA);
        RecordingMetricsContext metrics = new RecordingMetricsContext();
        IcebergState state = newState(baseOptions().build(), metrics);
        try {
            state.beginCommit(1L);
            state.updateState(List.of(tuple(1L, "a")), null);
            state.commit(1L);

            // Replay of an already committed txid.
            state.beginCommit(1L);
            state.updateState(List.of(tuple(1L, "a")), null);
            state.commit(1L);

            assertEquals(1L, metrics.counter(IcebergStateMetrics.BATCHES_SKIPPED));
            assertEquals(1L, metrics.counter(IcebergStateMetrics.RECORDS_WRITTEN));
            assertEquals(1L, metrics.timerCount(IcebergStateMetrics.COMMIT_LATENCY));
        } finally {
            state.close();
        }
    }

    @Test
    void metricsCountCommitFailures() {
        verifyCatalog.createTable(TABLE_ID, SCHEMA);
        RecordingMetricsContext metrics = new RecordingMetricsContext();
        IcebergState state = newState(baseOptions().build(), metrics);
        try {
            state.beginCommit(1L);
            state.updateState(List.of(tuple(1L, "a")), null);
            // The table disappears underneath the state before it can commit.
            verifyCatalog.dropTable(TABLE_ID, false);

            assertThrows(FailedException.class, () -> state.commit(1L));
            assertEquals(1L, metrics.counter(IcebergStateMetrics.COMMIT_FAILURES));
            assertEquals(0L, metrics.timerCount(IcebergStateMetrics.COMMIT_LATENCY));
        } finally {
            state.close();
        }
    }

    @Test
    void withoutCommitIntervalEveryBatchIsCommitted() throws IOException {
        verifyCatalog.createTable(TABLE_ID, SCHEMA);
        IcebergState state = newState(baseOptions().build());
        try {
            for (long txId = 1; txId <= 3; txId++) {
                state.beginCommit(txId);
                state.updateState(List.of(tuple(txId, "row" + txId)), null);
                state.commit(txId);
            }
            assertEquals(3, readRows().size());
            assertEquals(3, countSnapshots());
        } finally {
            state.close();
        }
    }

    @Test
    void commitIntervalBytesBuffersBatchesIntoASingleSnapshot() throws IOException {
        verifyCatalog.createTable(TABLE_ID, SCHEMA);
        RecordingMetricsContext metrics = new RecordingMetricsContext();
        // Far larger than the few hundred bytes these batches produce, so only the last batch,
        // driven by the time interval below, triggers the flush.
        IcebergState state = newState(baseOptions()
            .withCommitIntervalBytes(10L * 1024 * 1024)
            .build(), metrics);
        try {
            for (long txId = 1; txId <= 3; txId++) {
                state.beginCommit(txId);
                state.updateState(List.of(tuple(txId, "row" + txId)), null);
                state.commit(txId);
            }
            // Nothing is visible yet: three batches are buffered, no snapshot exists.
            assertEquals(0, countSnapshots());
            assertEquals(3L, metrics.counter(IcebergStateMetrics.BATCHES_BUFFERED));
            assertEquals(0L, metrics.timerCount(IcebergStateMetrics.COMMIT_LATENCY));
        } finally {
            state.close();
        }
    }

    @Test
    void bufferedWindowIsFlushedOnceTheByteThresholdIsCrossed() throws IOException {
        verifyCatalog.createTable(TABLE_ID, SCHEMA);
        RecordingMetricsContext metrics = new RecordingMetricsContext();
        // One byte: the first batch already exceeds it, but only once its bytes are counted.
        IcebergState state = newState(baseOptions().withCommitIntervalBytes(1L).build(), metrics);
        try {
            state.beginCommit(1L);
            state.updateState(List.of(tuple(1L, "a")), null);
            state.commit(1L);

            assertEquals(1, readRows().size());
            assertEquals(1, countSnapshots());
            assertEquals(1L, metrics.timerCount(IcebergStateMetrics.COMMIT_LATENCY));
            assertEquals(0L, metrics.counter(IcebergStateMetrics.BATCHES_BUFFERED));
        } finally {
            state.close();
        }
    }

    @Test
    void commitIntervalMillisFlushesTheWindowOnTheNextBatch() throws IOException, InterruptedException {
        verifyCatalog.createTable(TABLE_ID, SCHEMA);
        IcebergState state = newState(baseOptions()
            .withCommitIntervalBytes(10L * 1024 * 1024)
            .withCommitIntervalMillis(50L)
            .build());
        try {
            state.beginCommit(1L);
            state.updateState(List.of(tuple(1L, "a")), null);
            state.commit(1L);
            assertEquals(0, countSnapshots(), "the first batch only opens the window");

            // Once the deadline has passed, the next batch commits the whole window.
            Thread.sleep(80L);
            state.beginCommit(2L);
            state.updateState(List.of(tuple(2L, "b")), null);
            state.commit(2L);

            assertEquals(2, readRows().size());
            assertEquals(1, countSnapshots(), "both batches must land in one snapshot");
        } finally {
            state.close();
        }
    }

    @Test
    void replayOfABufferedBatchDropsTheWindow() throws IOException {
        verifyCatalog.createTable(TABLE_ID, SCHEMA);
        RecordingMetricsContext metrics = new RecordingMetricsContext();
        IcebergState state = newState(baseOptions()
            .withCommitIntervalBytes(10L * 1024 * 1024)
            .withCommitIntervalMillis(300L)
            .build(), metrics);
        try {
            state.beginCommit(1L);
            state.updateState(List.of(tuple(1L, "a")), null);
            state.commit(1L);
            state.beginCommit(2L);
            state.updateState(List.of(tuple(2L, "b")), null);
            state.commit(2L);
            assertEquals(0, countSnapshots(), "both batches are still buffered");

            // txId 2 is replayed: it is buffered but not committed, so the window has to be
            // discarded rather than replayed on top of itself.
            state.beginCommit(2L);
            assertEquals(1L, metrics.counter(IcebergStateMetrics.WINDOWS_DROPPED));

            state.updateState(List.of(tuple(2L, "b")), null);
            state.commit(2L);
            state.beginCommit(3L);
            state.updateState(List.of(tuple(3L, "c")), null);
            state.commit(3L);

            // Past the 300 ms deadline the next batch flushes the window. Batch 1 was lost with
            // the dropped window (the documented cost of commit batching); 2, 3 and 4 land once.
            Thread.sleep(350L);
            state.beginCommit(4L);
            state.updateState(List.of(tuple(4L, "d")), null);
            state.commit(4L);

            List<String> names = new ArrayList<>();
            readRows().forEach(r -> names.add((String) r.getField("name")));
            names.sort(String::compareTo);
            assertEquals(List.of("b", "c", "d"), names);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } finally {
            state.close();
        }
    }

    @Test
    void requiredNullFieldFailsLoudly() {
        verifyCatalog.createTable(TABLE_ID, SCHEMA);
        IcebergState state = newState(baseOptions().build());
        try {
            state.beginCommit(1L);
            TridentTuple bad = mock(TridentTuple.class);
            when(bad.contains(anyString())).thenReturn(false);
            when(bad.contains("id")).thenReturn(true);
            when(bad.getValueByField("id")).thenReturn(1L);
            // required "name" missing -> IllegalArgumentException (not FailedException: this is
            // a topology programming error that replays cannot fix)
            assertThrows(IllegalArgumentException.class,
                () -> state.updateState(List.of(bad), null));
        } finally {
            state.close();
        }
    }
}
