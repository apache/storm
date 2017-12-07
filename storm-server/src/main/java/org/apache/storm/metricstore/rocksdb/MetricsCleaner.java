/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.storm.metricstore.rocksdb;

import com.codahale.metrics.Meter;
import org.apache.storm.metricstore.FilterOptions;
import org.apache.storm.metricstore.MetricException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for removing expired metrics and unused metadata from the RocksDB store.
 */
public class MetricsCleaner implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsCleaner.class);
    private static long DEFAULT_SLEEP_MS = 4L * 60L * 60L * 1000L;
    private RocksDbStore store;
    private long retentionHours;
    private volatile boolean shutdown = false;
    private long sleepMs = DEFAULT_SLEEP_MS;
    private Meter failureMeter;

    MetricsCleaner(RocksDbStore store, long retentionHours, long hourlyPeriod) {
        this.store = store;
        this.retentionHours = retentionHours;
        if (hourlyPeriod > 0) {
            this.sleepMs = hourlyPeriod * 60L * 60L * 1000L;
        }
    }

    void shutdown() {
        shutdown = true;
    }

    @Override
    public void run() {
        while (true) {
            if (shutdown) {
                return;
            }
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
                this.failureMeter.mark();
            }
        }
    }

    void purgeMetrics() throws MetricException {
        long firstValidTimestamp = System.currentTimeMillis() - this.retentionHours * 60L * 60L * 1000L;

        LOG.info("Purging metrics before " + firstValidTimestamp);

        FilterOptions filter = new FilterOptions();
        long endTime = firstValidTimestamp - 1L;
        filter.setEndTime(endTime);
        store.deleteMetrics(filter);

        LOG.info("Purging metadata before " + firstValidTimestamp);
        store.deleteMetadataBefore(firstValidTimestamp);
    }
}
