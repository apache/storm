/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.metricstore.rocksdb;

import com.codahale.metrics.Meter;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.metricstore.FilterOptions;
import org.apache.storm.metricstore.MetricException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for removing expired metrics and unused metadata from the RocksDB store.
 */
public class MetricsCleaner implements Runnable, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsCleaner.class);
    private static final long DEFAULT_SLEEP_MS = 4L * 60L * 60L * 1000L;
    private final RocksDbStore store;
    private final long retentionHours;
    private final Meter failureMeter;
    private volatile boolean shutdown = false;
    private long sleepMs = DEFAULT_SLEEP_MS;
    private long purgeTimestamp = 0L;

    MetricsCleaner(RocksDbStore store, int retentionHours, int hourlyPeriod, Meter failureMeter, StormMetricsRegistry metricsRegistry) {
        this.store = store;
        this.retentionHours = retentionHours;
        if (hourlyPeriod > 0) {
            this.sleepMs = hourlyPeriod * 60L * 60L * 1000L;
        }
        this.failureMeter = failureMeter;

        metricsRegistry.registerGauge("MetricsCleaner:purgeTimestamp", () -> purgeTimestamp);
    }

    @Override
    public void close() {
        shutdown = true;
    }

    @Override
    public void run() {
        while (!shutdown) {
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                LOG.error("Sleep interrupted", e);
                continue;
            }

            try {
                purgeMetrics();
            } catch (MetricException e) {
                LOG.error("Failed to purge metrics", e);
                if (this.failureMeter != null) {
                    this.failureMeter.mark();
                }
            }
        }
    }

    void purgeMetrics() throws MetricException {
        purgeTimestamp = System.currentTimeMillis() - this.retentionHours * 60L * 60L * 1000L;

        LOG.info("Purging metrics before {}", purgeTimestamp);

        FilterOptions filter = new FilterOptions();
        long endTime = purgeTimestamp - 1L;
        filter.setEndTime(endTime);
        store.deleteMetrics(filter);

        LOG.info("Purging metadata before " + purgeTimestamp);
        store.deleteMetadataBefore(purgeTimestamp);
    }
}
