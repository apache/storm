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
 * carries the commit id, {@link #recover()} can tell a commit that landed from one that did not,
 * without needing any identity from the source: the table itself answers the question.
 *
 * <p>This yields atomic commits with at-least-once delivery. A crash before the WAL entry exists
 * leaves orphan data files, which are invisible to readers and removed by Iceberg's standard
 * orphan-file maintenance; a replayed batch is written and committed again, and its rows stay
 * visible until something downstream removes them.
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
        wal.delete(entry);
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
        wal.delete(entry);
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
     * Settle every commit this task prepared but did not finish, replaying the ones whose snapshot
     * never appeared and dropping the ones that are already visible.
     *
     * @return how many commits had to be replayed
     */
    public int recover() {
        int replayed = 0;
        for (CommitWal.WalEntry entry : wal.listPending()) {
            if (isVisible(entry)) {
                LOG.info("Commit {} is already visible; dropping its WAL entry", entry.commitId());
            } else {
                List<DataFile> dataFiles = wal.read(entry);
                LOG.info("Commit {} never became visible; replaying {} data files",
                    entry.commitId(), dataFiles.size());
                long startNanos = System.nanoTime();
                try {
                    append(entry, dataFiles);
                } catch (RuntimeException e) {
                    metrics.commitFailed();
                    throw e;
                }
                metrics.committed(dataFiles, System.nanoTime() - startNanos);
                replayed++;
            }
            wal.delete(entry);
        }
        return replayed;
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
