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

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Makes durable data files visible in an Iceberg table, atomically and recoverably.
 *
 * <p>A commit is prepared in the {@link CommitWal} first, then appended in a single Iceberg
 * operation that stamps the commit id on the resulting snapshot, then cleared from the WAL.
 * Because the append is atomic, readers never observe part of a batch. Because the snapshot
 * carries the commit id, a commit whose outcome the catalog left unknown can be settled by asking
 * the table whether it landed, without needing any identity from the source.
 *
 * <p>That question is asked while the batch is still in hand, not at the next startup: an entry
 * that survives to startup belongs to a batch that was never acked, which the source replays on
 * its own, so {@link #recover()} abandons it rather than appending it a second time.
 *
 * <p>This yields atomic commits with at-least-once delivery. A crash leaves orphan data files,
 * which are invisible to readers and removed by Iceberg's standard orphan-file maintenance; a
 * replayed batch is written and committed again, and its rows stay visible until something
 * downstream removes them.
 *
 * <p>One commit may cover many batches — the aggregated committer hands it the files of every
 * writer it collected — but it is still a single atomic append carrying a single commit id.
 */
public class IcebergCommitter {

    public static final String COMMIT_ID_PROPERTY = "storm.iceberg.commit-id";
    /**
     * How far before a WAL entry's own timestamp the snapshot scan still looks. The snapshot is
     * always written after the entry, so only clock skew between the worker that wrote the entry
     * and whatever stamped the snapshot's timestamp can put it earlier.
     */
    static final long CLOCK_SKEW_ALLOWANCE_MS = TimeUnit.MINUTES.toMillis(10);

    private static final Logger LOG = LoggerFactory.getLogger(IcebergCommitter.class);

    private final Table table;
    private final CommitWal wal;
    private final IcebergMetrics metrics;

    public IcebergCommitter(Table table, CommitWal wal, IcebergMetrics metrics) {
        this.table = table;
        this.wal = wal;
        this.metrics = metrics;
    }

    /**
     * Log, append and clear one commit. Does nothing when there is nothing to append.
     *
     * <p>Returns normally only when the batch is visible in the table — including the case where
     * the append reported a failure but had in fact landed. Throwing means the batch is not
     * visible and its tuples must be replayed.
     */
    public void commit(List<DataFile> dataFiles) {
        if (dataFiles.isEmpty()) {
            return;
        }
        CommitWal.WalEntry entry = wal.write(dataFiles);
        long startNanos = System.nanoTime();
        try {
            append(entry, dataFiles);
        } catch (RuntimeException e) {
            settleFailedCommit(entry, dataFiles, startNanos, e);
            return;
        }
        metrics.committed(dataFiles, System.nanoTime() - startNanos);
        deleteQuietly(entry);
    }

    /**
     * Drop a WAL entry whose commit is settled, without letting the drop itself unsettle it. The
     * append has already landed by the time this runs, so propagating a delete failure would make
     * the caller fail tuples that are visible in the table and have them replayed. A leftover entry
     * is harmless: startup discards it.
     */
    private void deleteQuietly(CommitWal.WalEntry entry) {
        try {
            wal.delete(entry);
        } catch (RuntimeException e) {
            LOG.warn("Failed deleting WAL entry {} for a commit that has landed; "
                + "it will be discarded at the next startup", entry.location(), e);
        }
    }

    /**
     * Resolve a failed commit while the batch is still in hand, rather than leaving it to the next
     * startup. Asking the table whether the commit landed turns an unknown outcome into a known
     * one at the only moment when it can still be acted on.
     *
     * <p>If it landed, the batch is visible and the caller may ack. If it did not, the entry is
     * dropped before the exception propagates: the caller will fail those tuples, the source will
     * replay them, and a WAL entry left behind would make the next startup append the original
     * files too — duplicating what the replay writes. The abandoned files become orphans instead,
     * which is what orphan-file maintenance is for.
     */
    private void settleFailedCommit(CommitWal.WalEntry entry, List<DataFile> dataFiles,
                                    long startNanos, RuntimeException failure) {
        boolean landed;
        try {
            landed = isVisible(entry);
        } catch (RuntimeException e) {
            // The table cannot be reached, so the outcome stays unknown. Leave the entry: startup
            // will settle it, and replaying a commit is recoverable in a way that losing it is not.
            metrics.commitFailed();
            failure.addSuppressed(e);
            throw failure;
        }
        deleteQuietly(entry);
        if (landed) {
            // The data is visible, so it counts as committed however the append reported itself.
            metrics.committed(dataFiles, System.nanoTime() - startNanos);
            LOG.warn("Commit {} reported a failure but its snapshot is present; "
                + "treating it as successful", entry.commitId(), failure);
            return;
        }
        metrics.commitFailed();
        LOG.error("Commit {} did not land; its data files are left as orphans and its tuples "
            + "will be replayed", entry.commitId(), failure);
        throw failure;
    }

    /**
     * Discard every commit this task prepared but did not finish.
     *
     * <p>An entry only survives to startup if the task died before its commit resolved, and in that
     * case the batch was never acked, so the source replays it. Appending the entry's files here
     * would therefore add a second copy of rows the replay writes anyway — for a reliable source
     * this path can only add duplicates, never prevent loss. Dropping the entry and letting its
     * files orphan is strictly the cheaper outcome, and it keeps startup off the commit path: no
     * unbounded replay, no dependence on a snapshot that {@code expire_snapshots} may already have
     * removed, and no unreadable entry that blocks every restart.
     *
     * @return how many prepared commits were abandoned
     */
    public int recover() {
        int abandoned = 0;
        for (CommitWal.WalEntry entry : wal.listPending()) {
            LOG.info("Commit {} was prepared but never resolved; abandoning it. Its tuples were "
                + "never acked, so the source replays them; its data files are left as orphans",
                entry.commitId());
            wal.delete(entry);
            abandoned++;
        }
        return abandoned;
    }

    /**
     * The Iceberg append itself. Deliberately records no metrics: only the caller knows whether a
     * thrown exception means the commit is absent or merely unconfirmed.
     */
    private void append(CommitWal.WalEntry entry, List<DataFile> dataFiles) {
        AppendFiles append = table.newAppend().set(COMMIT_ID_PROPERTY, entry.commitId());
        for (DataFile dataFile : dataFiles) {
            append.appendFile(dataFile);
        }
        append.commit();
    }

    /**
     * Whether a snapshot carrying this entry's commit id exists.
     *
     * <p>Only snapshots from the entry's own era are examined: a commit cannot have landed before
     * the entry that describes it was written. On a table with a long history that skips most of
     * the snapshot list, and it costs nothing in accuracy — an older snapshot could not carry this
     * commit id, since the id is minted when the entry is written.
     */
    private boolean isVisible(CommitWal.WalEntry entry) {
        table.refresh();
        for (Snapshot snapshot : table.snapshots()) {
            if (withinScanWindow(snapshot.timestampMillis(), entry.createdAtMs())
                && entry.commitId().equals(snapshot.summary().get(COMMIT_ID_PROPERTY))) {
                return true;
            }
        }
        return false;
    }

    static boolean withinScanWindow(long snapshotTimestampMs, long entryCreatedAtMs) {
        return snapshotTimestampMs >= entryCreatedAtMs - CLOCK_SKEW_ALLOWANCE_MS;
    }
}
