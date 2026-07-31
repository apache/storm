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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IcebergCommitterTest {

    private static final Schema SCHEMA = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()));
    private static final TableIdentifier TABLE_ID = TableIdentifier.of("db", "events");

    @TempDir
    Path tempDir;

    private HadoopCatalog catalog;
    private Table table;
    private CommitWal wal;
    private IcebergCommitter committer;

    @BeforeEach
    void setUp() {
        catalog = new HadoopCatalog(new Configuration(), tempDir.toUri().toString());
        table = catalog.createTable(TABLE_ID, SCHEMA, PartitionSpec.unpartitioned());
        wal = new CommitWal(table, "topo", 0);
        committer = new IcebergCommitter(table, wal, new IcebergMetrics(null));
    }

    @AfterEach
    void tearDown() throws IOException {
        catalog.close();
    }

    private DataFile dataFile(String name) {
        return DataFiles.builder(table.spec())
            .withPath(table.location() + "/data/" + name)
            .withFileSizeInBytes(1024L)
            .withRecordCount(4L)
            .withFormat(FileFormat.PARQUET)
            .build();
    }

    private List<String> committedFileNames() {
        table.refresh();
        Snapshot snapshot = table.currentSnapshot();
        if (snapshot == null) {
            return List.of();
        }
        List<String> names = new ArrayList<>();
        for (DataFile file : snapshot.addedDataFiles(table.io())) {
            names.add(file.location().substring(file.location().lastIndexOf('/') + 1));
        }
        names.sort(String::compareTo);
        return names;
    }

    @Test
    void commitMakesFilesVisibleAndClearsTheWal() {
        committer.commit(List.of(dataFile("a.parquet")));

        assertEquals(List.of("a.parquet"), committedFileNames());
        assertTrue(wal.listPending().isEmpty(), "a completed commit leaves no WAL entry");
    }

    @Test
    void commitStampsItsCommitIdOnTheSnapshot() {
        committer.commit(List.of(dataFile("a.parquet")));

        table.refresh();
        String stamped = table.currentSnapshot().summary().get(IcebergCommitter.COMMIT_ID_PROPERTY);
        assertTrue(stamped != null && !stamped.isBlank(), "snapshot should carry the commit id");
    }

    @Test
    void theScanWindowStartsBeforeTheEntryByTheClockSkewAllowance() {
        long entryCreatedAtMs = 1_000_000_000L;
        long slack = IcebergCommitter.CLOCK_SKEW_ALLOWANCE_MS;

        // The snapshot that carries a commit is always written after its WAL entry, so anything
        // older than the entry — beyond what clock skew between hosts can explain — cannot be it.
        assertTrue(IcebergCommitter.withinScanWindow(entryCreatedAtMs + 1, entryCreatedAtMs));
        assertTrue(IcebergCommitter.withinScanWindow(entryCreatedAtMs - slack, entryCreatedAtMs));
        assertFalse(IcebergCommitter.withinScanWindow(entryCreatedAtMs - slack - 1, entryCreatedAtMs));
    }

    /**
     * A committer whose append either throws before reaching the table, or reaches it and then
     * throws as if the outcome were unknown. Everything else is the real table.
     */
    private IcebergCommitter committerWithFlakyAppend(boolean landBeforeThrowing) {
        return committerWithFlakyAppend(landBeforeThrowing, new RecordingMetricsContext());
    }

    private IcebergCommitter committerWithFlakyAppend(boolean landBeforeThrowing,
                                                      RecordingMetricsContext metrics) {
        Table flakyTable = spy(table);
        doAnswer(invocation -> {
            AppendFiles real = table.newAppend();
            AppendFiles flaky = mock(AppendFiles.class);
            when(flaky.set(anyString(), anyString())).thenAnswer(call -> {
                real.set(call.getArgument(0), call.getArgument(1));
                return flaky;
            });
            when(flaky.appendFile(any())).thenAnswer(call -> {
                real.appendFile(call.getArgument(0));
                return flaky;
            });
            doAnswer(commit -> {
                if (landBeforeThrowing) {
                    real.commit();
                }
                throw new CommitStateUnknownException(new RuntimeException("commit outcome unknown"));
            }).when(flaky).commit();
            return flaky;
        }).when(flakyTable).newAppend();
        return new IcebergCommitter(flakyTable, wal, new IcebergMetrics(metrics));
    }

    @Test
    void aCommitThatLandedDespiteAFailureIsCountedAsCommittedNotFailed() {
        RecordingMetricsContext metrics = new RecordingMetricsContext();

        committerWithFlakyAppend(true, metrics).commit(List.of(dataFile("a.parquet")));

        assertEquals(1L, metrics.counter(IcebergMetrics.DATA_FILES_COMMITTED),
            "the file is visible, so it counts as committed");
        assertEquals(1024L, metrics.counter(IcebergMetrics.BYTES_COMMITTED));
        assertEquals(1L, metrics.timerCount(IcebergMetrics.COMMIT_LATENCY));
        assertEquals(0L, metrics.counter(IcebergMetrics.COMMIT_FAILURES),
            "a commit that landed is not a failure");
    }

    @Test
    void aCommitThatDidNotLandIsCountedAsFailed() {
        RecordingMetricsContext metrics = new RecordingMetricsContext();
        IcebergCommitter flaky = committerWithFlakyAppend(false, metrics);

        assertThrows(CommitStateUnknownException.class,
            () -> flaky.commit(List.of(dataFile("a.parquet"))));

        assertEquals(1L, metrics.counter(IcebergMetrics.COMMIT_FAILURES));
        assertEquals(0L, metrics.counter(IcebergMetrics.DATA_FILES_COMMITTED),
            "nothing became visible, so nothing counts as committed");
    }

    @Test
    void aCommitThatLandedDespiteAFailureIsTreatedAsSuccessful() {
        // CommitStateUnknownException where the commit did reach the table: the snapshot carries
        // the commit id, which settles the question, so there is nothing to fail or replay.
        committerWithFlakyAppend(true).commit(List.of(dataFile("a.parquet")));

        assertEquals(List.of("a.parquet"), committedFileNames());
        assertTrue(wal.listPending().isEmpty(), "the settled entry is cleared in flight");
    }

    @Test
    void aCommitThatDidNotLandClearsItsWalEntryBeforeFailing() {
        // The tuples are about to be failed and replayed. Leaving the entry behind would make the
        // next startup append these files too, duplicating what the replay writes.
        IcebergCommitter flaky = committerWithFlakyAppend(false);

        assertThrows(CommitStateUnknownException.class,
            () -> flaky.commit(List.of(dataFile("a.parquet"))));

        assertEquals(List.of(), committedFileNames(), "nothing became visible");
        assertTrue(wal.listPending().isEmpty(), "and no entry is left for startup to replay");
    }

    @Test
    void recoveryReplaysAPreparedCommitThatNeverBecameVisible() {
        // A crash between "data files durable, WAL written" and "Iceberg commit": the entry exists,
        // no snapshot references it.
        wal.write(List.of(dataFile("lost.parquet")));
        assertEquals(List.of(), committedFileNames());

        int replayed = committer.recover();

        assertEquals(1, replayed);
        assertEquals(List.of("lost.parquet"), committedFileNames());
        assertTrue(wal.listPending().isEmpty(), "the replayed entry is cleared");
    }

    @Test
    void recoveryDoesNotReappendACommitThatIsAlreadyVisible() {
        // A crash between the Iceberg commit and the WAL delete: the snapshot is there, so the
        // entry must be dropped rather than replayed, or the batch would be committed twice.
        CommitWal.WalEntry entry = wal.write(List.of(dataFile("a.parquet")));
        table.newAppend()
            .appendFile(dataFile("a.parquet"))
            .set(IcebergCommitter.COMMIT_ID_PROPERTY, entry.commitId())
            .commit();

        int replayed = committer.recover();

        assertEquals(0, replayed);
        assertEquals(List.of("a.parquet"), committedFileNames());
        assertTrue(wal.listPending().isEmpty(), "the settled entry is cleared");
    }
}
