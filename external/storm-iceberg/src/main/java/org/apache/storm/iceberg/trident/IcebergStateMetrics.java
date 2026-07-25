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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.DataFile;
import org.apache.storm.task.IMetricsContext;

/**
 * Storm metrics-v2 instrumentation for {@link IcebergState}.
 *
 * <p>When no {@link IMetricsContext} is available the metrics are still recorded, on unregistered
 * instances, so callers never need a null check.
 */
class IcebergStateMetrics {

    static final String RECORDS_WRITTEN = "iceberg-records-written";
    static final String DATA_FILES_COMMITTED = "iceberg-data-files-committed";
    static final String BYTES_COMMITTED = "iceberg-bytes-committed";
    static final String COMMIT_LATENCY = "iceberg-commit-latency";
    static final String COMMIT_FAILURES = "iceberg-commit-failures";
    static final String BATCHES_SKIPPED = "iceberg-batches-skipped";

    private final Counter recordsWritten;
    private final Counter dataFilesCommitted;
    private final Counter bytesCommitted;
    private final Timer commitLatency;
    private final Counter commitFailures;
    private final Counter batchesSkipped;

    IcebergStateMetrics(IMetricsContext metrics) {
        this.recordsWritten = counter(metrics, RECORDS_WRITTEN);
        this.dataFilesCommitted = counter(metrics, DATA_FILES_COMMITTED);
        this.bytesCommitted = counter(metrics, BYTES_COMMITTED);
        this.commitFailures = counter(metrics, COMMIT_FAILURES);
        this.batchesSkipped = counter(metrics, BATCHES_SKIPPED);
        this.commitLatency = metrics == null ? new Timer() : metrics.registerTimer(COMMIT_LATENCY);
    }

    private static Counter counter(IMetricsContext metrics, String name) {
        return metrics == null ? new Counter() : metrics.registerCounter(name);
    }

    void recordsWritten(long count) {
        recordsWritten.inc(count);
    }

    /** Counts the files and bytes made visible by a successful commit. */
    void committed(DataFile[] dataFiles, long durationNanos) {
        dataFilesCommitted.inc(dataFiles.length);
        for (DataFile dataFile : dataFiles) {
            bytesCommitted.inc(dataFile.fileSizeInBytes());
        }
        commitLatency.update(durationNanos, TimeUnit.NANOSECONDS);
    }

    void commitFailed() {
        commitFailures.inc();
    }

    void batchSkipped() {
        batchesSkipped.inc();
    }
}
