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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Timer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.DataFile;
import org.apache.storm.task.IMetricsContext;

/**
 * Storm metrics-v2 instrumentation for the Iceberg sink.
 *
 * <p>When no {@link IMetricsContext} is available the metrics are still recorded, on unregistered
 * instances, so callers never need a null check.
 */
public class IcebergMetrics {

    static final String RECORDS_WRITTEN = "iceberg-records-written";
    static final String DATA_FILES_COMMITTED = "iceberg-data-files-committed";
    static final String BYTES_COMMITTED = "iceberg-bytes-committed";
    static final String COMMIT_LATENCY = "iceberg-commit-latency";
    static final String COMMIT_FAILURES = "iceberg-commit-failures";
    static final String DATA_FILES_SEALED = "iceberg-data-files-sealed";
    static final String SEAL_LATENCY = "iceberg-seal-latency";
    static final String PENDING_DATA_FILES = "iceberg-pending-data-files";
    static final String OLDEST_PENDING_AGE_MS = "iceberg-oldest-pending-age-ms";

    private final Counter recordsWritten;
    private final Counter dataFilesCommitted;
    private final Counter bytesCommitted;
    private final Timer commitLatency;
    private final Counter commitFailures;
    private final Counter dataFilesSealed;
    private final Timer sealLatency;

    public IcebergMetrics(IMetricsContext metrics) {
        this.recordsWritten = counter(metrics, RECORDS_WRITTEN);
        this.dataFilesCommitted = counter(metrics, DATA_FILES_COMMITTED);
        this.bytesCommitted = counter(metrics, BYTES_COMMITTED);
        this.commitFailures = counter(metrics, COMMIT_FAILURES);
        this.commitLatency = timer(metrics, COMMIT_LATENCY);
        this.dataFilesSealed = counter(metrics, DATA_FILES_SEALED);
        this.sealLatency = timer(metrics, SEAL_LATENCY);
    }

    private static Counter counter(IMetricsContext metrics, String name) {
        Counter registered = metrics == null ? null : metrics.registerCounter(name);
        // Fall back to an unregistered instance: instrumentation must never be able to break the
        // sink, whatever the metrics context does or does not hand back.
        return registered == null ? new Counter() : registered;
    }

    private static Timer timer(IMetricsContext metrics, String name) {
        Timer registered = metrics == null ? null : metrics.registerTimer(name);
        return registered == null ? new Timer() : registered;
    }

    public void recordsWritten(long count) {
        recordsWritten.inc(count);
    }

    /** Counts the files and bytes made visible by a successful commit. */
    public void committed(List<DataFile> dataFiles, long durationNanos) {
        dataFilesCommitted.inc(dataFiles.size());
        for (DataFile dataFile : dataFiles) {
            bytesCommitted.inc(dataFile.fileSizeInBytes());
        }
        commitLatency.update(durationNanos, TimeUnit.NANOSECONDS);
    }

    public void commitFailed() {
        commitFailures.inc();
    }

    /** Counts the files a writer closed and handed to the committer. */
    public void sealed(List<DataFile> dataFiles, long durationNanos) {
        dataFilesSealed.inc(dataFiles.size());
        sealLatency.update(durationNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Expose how much is waiting to be committed. A committer that has stopped making progress
     * shows up here well before the upstream tuples start timing out, which is the only warning an
     * operator gets that visibility has stalled.
     */
    public void registerPendingGauges(IMetricsContext metrics, Gauge<Integer> pendingDataFiles,
                                      Gauge<Long> oldestPendingAgeMs) {
        if (metrics == null) {
            return;
        }
        metrics.registerGauge(PENDING_DATA_FILES, pendingDataFiles);
        metrics.registerGauge(OLDEST_PENDING_AGE_MS, oldestPendingAgeMs);
    }
}
