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
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.storm.metricstore.AggLevel;
import org.apache.storm.metricstore.FilterOptions;
import org.apache.storm.metricstore.Metric;
import org.apache.storm.metricstore.MetricException;
import org.apache.storm.metricstore.MetricStore;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.IndexType;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RocksDbStore implements MetricStore {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDbStore.class);
    static final Integer INVALID_METADATA_STRING_ID = 0;
    static final String CONF_KEY_LOCATION = "storm.metricstore.rocksdb.location";
    static final String CONF_KEY_CREATE_IF_MISSING = "storm.metricstore.rocksdb.create_if_missing";
    static final String CONF_KEY_METADATA_STRING_CACHE_SIZE = "storm.metricstore.rocksdb.metadata_string_cache_capacity";
    static final String CONF_KEY_METRIC_RETENTION_HOURS = "storm.metricstore.rocksdb.retention_hours";
    static final String CONF_KEY_METRIC_RETENTION_PERIOD_HOURS = "storm.metricstore.rocksdb.retention_period_hours";
    RocksDB db;
    private ReadOnlyStringMetadataCache readOnlyStringMetadataCache = null;
    ArrayList<AggLevel> aggBuckets = new ArrayList<>();
    private BlockingQueue queue = new PriorityBlockingQueue();
    private RocksDbMetricsWriter metricsWriter = null;
    private MetricsCleaner metricsCleaner = null;
    private Meter failureMeter = null;

    interface RocksDbScanCallback {
        boolean cb(RocksDbKey key, RocksDbValue val);  // return false to stop scan
    }

    /**
     * Create metric store instance using the configurations provided via the config map.
     *
     * @param config Storm config map
     * @param meter  the meter to use for tracking failures
     * @return the store
     * @throws MetricException on preparation error
     */
    public MetricStore prepare(Map config, Meter meter) throws MetricException {
        validateConfig(config);

        this.failureMeter = meter;

        RocksDB.loadLibrary();
        boolean createIfMissing = Boolean.parseBoolean(config.get(CONF_KEY_CREATE_IF_MISSING).toString());
        Options options = new Options().setCreateIfMissing(createIfMissing);

        // use the hash index for prefix searches
        BlockBasedTableConfig tfc = new BlockBasedTableConfig();
        tfc.setIndexType(IndexType.kHashSearch);
        options.setTableFormatConfig(tfc);
        options.useCappedPrefixExtractor(RocksDbKey.KEY_SIZE);

        try {
            String path = config.get(CONF_KEY_LOCATION).toString();
            LOG.info("Opening RocksDB from " + path);
            db = RocksDB.open(options, path);
        } catch (RocksDBException e) {
            String message = "Error opening RockDB database";
            LOG.error(message, e);
            throw new MetricException(message, e);
        }

        aggBuckets.add(AggLevel.AGG_LEVEL_1_MIN);
        aggBuckets.add(AggLevel.AGG_LEVEL_10_MIN);
        aggBuckets.add(AggLevel.AGG_LEVEL_60_MIN);

        // create thread to delete old metrics and metadata
        Long retentionHours = Long.parseLong(config.get(CONF_KEY_METRIC_RETENTION_HOURS).toString());
        Long retentionPeriod = 0L;
        if (config.containsKey(CONF_KEY_METRIC_RETENTION_PERIOD_HOURS)) {
            retentionPeriod = Long.parseLong(config.get(CONF_KEY_METRIC_RETENTION_PERIOD_HOURS).toString());
        }
        metricsCleaner = new MetricsCleaner(this, retentionHours, retentionPeriod);

        // create thread to process insertion of all metrics
        metricsWriter = new RocksDbMetricsWriter(this, this.queue, this.failureMeter);

        Integer cacheCapacity = Integer.parseInt(config.get(CONF_KEY_METADATA_STRING_CACHE_SIZE).toString());
        StringMetadataCache.init(metricsWriter, cacheCapacity);
        readOnlyStringMetadataCache = StringMetadataCache.getReadOnlyStringMetadataCache();
        metricsWriter.init(); // init the writer once the cache is setup

        // start threads after metadata cache created
        Thread thread = new Thread(metricsCleaner);
        thread.start();
        thread = new Thread(metricsWriter);
        thread.start();

        return this;
    }

    /**
     * Implements configuration validation of Metrics Store, validates storm configuration for Metrics Store.
     *
     * @param config Storm config to specify which store type, location of store and creation policy
     * @throws MetricException if there is a missing required configuration or if the store does not exist but
     *                         the config specifies not to create the store
     */
    private void validateConfig(Map config) throws MetricException {
        if (!(config.containsKey(CONF_KEY_LOCATION))) {
            throw new MetricException("Not a vaild RocksDB configuration - Missing store location " + CONF_KEY_LOCATION);
        }

        if (!(config.containsKey(CONF_KEY_CREATE_IF_MISSING))) {
            throw new MetricException("Not a vaild RocksDB configuration - Does not specify creation policy " + CONF_KEY_CREATE_IF_MISSING);
        }

        String createIfMissing = config.get(CONF_KEY_CREATE_IF_MISSING).toString();
        if (!Boolean.parseBoolean(createIfMissing)) {
            String storePath = config.get(CONF_KEY_LOCATION).toString();
            if (!(new File(storePath).exists())) {
                throw new MetricException("Configuration specifies not to create a store but no store currently exists at " + storePath);
            }
        }

        if (!(config.containsKey(CONF_KEY_METADATA_STRING_CACHE_SIZE))) {
            throw new MetricException("Not a valid RocksDB configuration - Missing metadata string cache size "
                    + CONF_KEY_METADATA_STRING_CACHE_SIZE);
        }

        if (!config.containsKey(CONF_KEY_METRIC_RETENTION_HOURS)) {
            throw new MetricException("Not a valid RocksDB configuration - Missing metric retention " + CONF_KEY_METRIC_RETENTION_HOURS);
        }
    }

    /**
     * Stores metrics in the store.
     *
     * @param metric  Metric to store
     * @throws MetricException  if database write fails
     */
    public void insert(Metric metric) throws MetricException {
        try {
            queue.put(metric);
        } catch (Exception e) {
            String message = "Failed to insert metric";
            LOG.error(message, e);
            if (this.failureMeter != null) {
                this.failureMeter.mark();
            }
            throw new MetricException(message, e);
        }
    }

    /**
     * Fill out the numeric values for a metric.
     *
     * @param metric  Metric to populate
     * @return   true if the metric was populated, false otherwise
     * @throws MetricException  if read from database fails
     */
    @Override
    public boolean populateValue(Metric metric) throws MetricException {
        Map<String, Integer> localLookupCache = new HashMap<>(6);

        Integer topologyId = lookupMetadataString(KeyType.TOPOLOGY_STRING, metric.getTopologyId(), localLookupCache);
        if (INVALID_METADATA_STRING_ID.equals(topologyId)) {
            return false;
        }
        Integer metricId = lookupMetadataString(KeyType.METRIC_STRING, metric.getMetricName(), localLookupCache);
        if (INVALID_METADATA_STRING_ID.equals(metricId)) {
            return false;
        }
        Integer componentId = lookupMetadataString(KeyType.COMPONENT_STRING, metric.getComponentId(), localLookupCache);
        if (INVALID_METADATA_STRING_ID.equals(componentId)) {
            return false;
        }
        Integer executorId = lookupMetadataString(KeyType.EXEC_ID_STRING, metric.getExecutorId(), localLookupCache);
        if (INVALID_METADATA_STRING_ID.equals(executorId)) {
            return false;
        }
        Integer hostId = lookupMetadataString(KeyType.HOST_STRING, metric.getHostname(), localLookupCache);
        if (INVALID_METADATA_STRING_ID.equals(hostId)) {
            return false;
        }
        Integer streamId = lookupMetadataString(KeyType.STREAM_ID_STRING, metric.getStreamId(), localLookupCache);
        if (INVALID_METADATA_STRING_ID.equals(streamId)) {
            return false;
        }

        RocksDbKey key = RocksDbKey.createMetricKey(metric.getAggLevel(), topologyId, metric.getTimestamp(), metricId,
                componentId, executorId, hostId, metric.getPort(), streamId);

        return populateFromKey(key, metric);
    }

    // populate metric values using the provided key
    boolean populateFromKey(RocksDbKey key, Metric metric) throws MetricException {
        try {
            byte[] value = db.get(key.getRaw());
            if (value == null) {
                return false;
            }
            RocksDbValue rdbValue = new RocksDbValue(value);
            rdbValue.populateMetric(metric);
        } catch (Exception e) {
            String message = "Failed to populate metric";
            LOG.error(message, e);
            if (this.failureMeter != null) {
                this.failureMeter.mark();
            }
            throw new MetricException(message, e);
        }
        return true;
    }

    // attempts to lookup the unique Id for a string that may not exist yet.  Returns INVALID_METADATA_STRING_ID
    // if it does not exist.
    private Integer lookupMetadataString(KeyType type, String s, Map<String, Integer> lookupCache) throws MetricException {
        if (s == null) {
            if (this.failureMeter != null) {
                this.failureMeter.mark();
            }
            throw new MetricException("No string for metric metadata string type " + type);
        }

        // attempt to find it in the string cache, this will update the LRU
        StringMetadata stringMetadata = readOnlyStringMetadataCache.get(s);
        if (stringMetadata != null) {
            return stringMetadata.getStringId();
        }

        // attempt to find it in callers cache
        Integer id = lookupCache.get(s);
        if (id != null) {
            return id;
        }

        // attempt to find the string in the database
        stringMetadata = rocksDbGetStringMetadata(type, s);
        if (stringMetadata != null) {
            id = stringMetadata.getStringId();

            // add to the callers cache.  We can't add it to the stringMetadataCache, since that could cause an eviction
            // database write, which we want to only occur from the inserting DB thread.
            lookupCache.put(s, id);

            return id;
        }

        // string does not exist
        return INVALID_METADATA_STRING_ID;
    }

    // scans the database to look for a metadata string and returns the metadata info
    StringMetadata rocksDbGetStringMetadata(KeyType type, String s) {
        RocksDbKey firstKey = RocksDbKey.getInitialKey(type);
        RocksDbKey lastKey = RocksDbKey.getLastKey(type);
        final AtomicReference<StringMetadata> reference = new AtomicReference<>();
        scanRange(firstKey, lastKey, (key, value) -> {
            if (s.equals(value.getMetdataString())) {
                reference.set(value.getStringMetadata(key));
                return false;
            } else {
                return true;  // haven't found string, keep searching
            }
        });
        return reference.get();
    }

    // scans from key start to the key before end, calling back until callback indicates not to process further
    void scanRange(RocksDbKey start, RocksDbKey end, RocksDbScanCallback fn) {
        ReadOptions ro = new ReadOptions();
        ro.setTotalOrderSeek(true);
        RocksIterator iterator = db.newIterator(ro);
        for (iterator.seek(start.getRaw()); iterator.isValid(); iterator.next()) {
            RocksDbKey key = new RocksDbKey(iterator.key());
            if (key.compareTo(end) >= 0) { // past limit, quit
                return;
            }

            RocksDbValue val = new RocksDbValue(iterator.value());
            if (!fn.cb(key, val)) {
                // if cb returns false, we are done with this section of rows
                return;
            }
        }
    }

    /**
     * Shutdown the store.
     */
    public void shutdown() {
        metricsWriter.shutdown();
        metricsCleaner.shutdown();
    }

    /**
     *  Scans all metrics in the store and returns the ones matching the specified filtering options.
     *  Callback returns Metric class results.
     *
     * @param filter   options to filter by
     * @param scanCallback  callback for each Metric found
     * @throws MetricException  on error
     */
    public void scan(FilterOptions filter, ScanCallback scanCallback) throws MetricException {
        scanInternal(filter, scanCallback, null);
    }

    /**
     *  Scans all metrics in the store and returns the ones matching the specified filtering options.
     *  Callback returns raw key/value data.
     *
     * @param filter   options to filter by
     * @param rawCallback  callback for each Metric found
     * @throws MetricException  on error
     */
    void scanRaw(FilterOptions filter, RocksDbScanCallback rawCallback) throws MetricException {
        scanInternal(filter, null, rawCallback);
    }

    // perform a scan given filter options, and return results in either Metric or raw data.
    private void scanInternal(FilterOptions filter, ScanCallback scanCallback, RocksDbScanCallback rawCallback) throws MetricException {

        Map<String, Integer> stringToIdCache = new HashMap<>();
        Map<Integer, String> idToStringCache = new HashMap<>();

        Integer startTopologyId = 0;
        Integer endTopologyId = 0xFFFFFFFF;
        String filterTopologyId = filter.getTopologyId();
        if (filterTopologyId != null) {
            Integer topologyId = lookupMetadataString(KeyType.TOPOLOGY_STRING, filterTopologyId, stringToIdCache);
            if (INVALID_METADATA_STRING_ID.equals(topologyId)) {
                return;  // string does not exist in database
            }
            startTopologyId = topologyId;
            endTopologyId = topologyId;
        }

        Long startTime = filter.getStartTime();
        Long endTime = filter.getEndTime();

        Integer startMetricId = 0;
        Integer endMetricId = 0xFFFFFFFF;
        String filterMetricName = filter.getMetricName();
        if (filterMetricName != null) {
            Integer metricId = lookupMetadataString(KeyType.METRIC_STRING, filterMetricName, stringToIdCache);
            if (INVALID_METADATA_STRING_ID.equals(metricId)) {
                return;  // string does not exist in database
            }
            startMetricId = metricId;
            endMetricId = metricId;
        }

        Integer startComponentId = 0;
        Integer endComponentId = 0xFFFFFFFF;
        String filterComponentId = filter.getComponentId();
        if (filterComponentId != null) {
            Integer componentId = lookupMetadataString(KeyType.COMPONENT_STRING, filterComponentId, stringToIdCache);
            if (INVALID_METADATA_STRING_ID.equals(componentId)) {
                return;  // string does not exist in database
            }
            startComponentId = componentId;
            endComponentId = componentId;
        }

        Integer startExecutorId = 0;
        Integer endExecutorId = 0xFFFFFFFF;
        String filterExecutorName = filter.getExecutorId();
        if (filterExecutorName != null) {
            Integer executorId = lookupMetadataString(KeyType.EXEC_ID_STRING, filterExecutorName, stringToIdCache);
            if (INVALID_METADATA_STRING_ID.equals(executorId)) {
                return;  // string does not exist in database
            }
            startExecutorId = executorId;
            endExecutorId = executorId;
        }

        Integer startHostId = 0;
        Integer endHostId = 0xFFFFFFFF;
        String filterHostId = filter.getHostId();
        if (filterHostId != null) {
            Integer hostId = lookupMetadataString(KeyType.HOST_STRING, filterHostId, stringToIdCache);
            if (INVALID_METADATA_STRING_ID.equals(hostId)) {
                return;  // string does not exist in database
            }
            startHostId = hostId;
            endHostId = hostId;
        }

        Integer startPort = 0;
        Integer endPort = 0xFFFFFFFF;
        Integer filterPort = filter.getPort();
        if (filterPort != null) {
            startPort = filterPort;
            endPort = filterPort;
        }

        Integer startStreamId = 0;
        Integer endStreamId = 0xFFFFFFFF;
        String filterStreamId = filter.getStreamId();
        if (filterStreamId != null) {
            Integer streamId = lookupMetadataString(KeyType.HOST_STRING, filterStreamId, stringToIdCache);
            if (INVALID_METADATA_STRING_ID.equals(streamId)) {
                return;  // string does not exist in database
            }
            startStreamId = streamId;
            endStreamId = streamId;
        }

        for (AggLevel aggLevel : filter.getAggLevels()) {

            RocksDbKey startKey = RocksDbKey.createMetricKey(aggLevel, startTopologyId, startTime, startMetricId,
                    startComponentId, startExecutorId, startHostId, startPort, startStreamId);
            RocksDbKey endKey = RocksDbKey.createMetricKey(aggLevel, endTopologyId, endTime, endMetricId,
                    endComponentId, endExecutorId, endHostId, endPort, endStreamId);

            ReadOptions ro = new ReadOptions();
            ro.setTotalOrderSeek(true);
            RocksIterator iterator = db.newIterator(ro);
            for (iterator.seek(startKey.getRaw()); iterator.isValid(); iterator.next()) {
                RocksDbKey key = new RocksDbKey(iterator.key());

                if (key.compareTo(endKey) > 0) { // past limit, quit
                    break;
                }

                if (startTopologyId != 0 && !key.getTopologyId().equals(startTopologyId)) {
                    continue;
                }

                long timestamp = key.getTimestamp();
                if (timestamp < startTime.longValue() || timestamp > endTime.longValue()) {
                    continue;
                }

                if (startMetricId != 0 && !key.getMetricId().equals(startMetricId)) {
                    continue;
                }

                if (startComponentId != 0 && !key.getComponentId().equals(startComponentId)) {
                    continue;
                }

                if (startExecutorId != 0 && !key.getExecutorId().equals(startExecutorId)) {
                    continue;
                }

                if (startHostId != 0 && !key.getHostnameId().equals(startHostId)) {
                    continue;
                }

                if (startPort != 0 && !key.getPort().equals(startPort)) {
                    continue;
                }

                if (startStreamId != 0 && !key.getStreamId().equals(startStreamId)) {
                    continue;
                }

                RocksDbValue val = new RocksDbValue(iterator.value());

                if (scanCallback != null) {
                    // populate a metric
                    String metricName = metadataIdToString(KeyType.METRIC_STRING, key.getMetricId(), idToStringCache);
                    String topologyId = metadataIdToString(KeyType.TOPOLOGY_STRING, key.getTopologyId(), idToStringCache);
                    String componentId = metadataIdToString(KeyType.COMPONENT_STRING, key.getComponentId(), idToStringCache);
                    String executorId = metadataIdToString(KeyType.EXEC_ID_STRING, key.getExecutorId(), idToStringCache);
                    String hostname = metadataIdToString(KeyType.HOST_STRING, key.getHostnameId(), idToStringCache);
                    String streamId = metadataIdToString(KeyType.STREAM_ID_STRING, key.getStreamId(), idToStringCache);

                    Metric metric = new Metric(metricName, timestamp, topologyId, 0.0, componentId, executorId, hostname,
                            streamId, key.getPort(), aggLevel);

                    val.populateMetric(metric);

                    // callback to caller
                    scanCallback.cb(metric);
                } else {
                    if (!rawCallback.cb(key, val)) {
                        return;
                    }
                }
            }
        }
    }

    // Finds the metadata string that matches the string Id and type provided.  The string should exist, as it is
    // referenced from a metric
    String metadataIdToString(KeyType type, Integer id, Map<Integer, String> lookupCache) throws MetricException {
        String s = readOnlyStringMetadataCache.getMetadataString(id);
        if (s != null) {
            return s;
        }
        s = lookupCache.get(id);
        if (s != null) {
            return s;
        }
        // get from DB and add to lookup cache
        RocksDbKey key = new RocksDbKey(type, id);
        try {
            byte[] value = db.get(key.getRaw());
            if (value == null) {
                throw new MetricException("Failed to find metadata string for id " + id);
            }
            RocksDbValue rdbValue = new RocksDbValue(value);
            s = rdbValue.getMetdataString();
            lookupCache.put(id, s);
            return s;
        }  catch (RocksDBException e) {
            if (this.failureMeter != null) {
                this.failureMeter.mark();
            }
            throw new MetricException("Failed to get from RocksDb", e);
        }
    }

    // deletes metrics matching the filter options
    void deleteMetrics(FilterOptions filter) throws MetricException {
        WriteBatch writeBatch = new WriteBatch();
        WriteOptions writeOps = new WriteOptions();

        scanRaw(filter, (RocksDbKey key, RocksDbValue value) -> {
            writeBatch.remove(key.getRaw());
            return true;
        });

        if (writeBatch.count() > 0) {
            LOG.info("Deleting " + writeBatch.count() + " metrics");
            try {
                db.write(writeOps, writeBatch);
            } catch (Exception e) {
                String message = "Failed delete metrics";
                LOG.error(message, e);
                if (this.failureMeter != null) {
                    this.failureMeter.mark();
                }
                throw new MetricException(message, e);
            }
        }
    }

    // deletes metadata strings before the provided timestamp
    void deleteMetadataBefore(long firstValidTimestamp) throws MetricException {
        if (firstValidTimestamp < 1L) {
            if (this.failureMeter != null) {
                this.failureMeter.mark();
            }
            throw new MetricException("Invalid timestamp for deleting metadata: " + firstValidTimestamp);
        }

        WriteBatch writeBatch = new WriteBatch();
        WriteOptions writeOps = new WriteOptions();

        // search all metadata strings
        RocksDbKey topologyMetadataPrefix = RocksDbKey.getPrefix(KeyType.METADATA_STRING_START);
        RocksDbKey lastPrefix = RocksDbKey.getPrefix(KeyType.METADATA_STRING_END);
        scanRange(topologyMetadataPrefix, lastPrefix, (key, value) -> {
            if (value.getLastTimestamp() < firstValidTimestamp) {
                writeBatch.remove(key.getRaw());
            }
            return true;
        });

        if (writeBatch.count() > 0) {
            LOG.info("Deleting " + writeBatch.count() + " metadata strings");
            try {
                db.write(writeOps, writeBatch);
            } catch (Exception e) {
                String message = "Failed delete metadata strings";
                LOG.error(message, e);
                if (this.failureMeter != null) {
                    this.failureMeter.mark();
                }
                throw new MetricException(message, e);
            }
        }
    }
}

