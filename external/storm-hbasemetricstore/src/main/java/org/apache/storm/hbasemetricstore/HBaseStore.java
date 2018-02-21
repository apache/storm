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

package org.apache.storm.hbasemetricstore;

import com.codahale.metrics.Meter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.storm.DaemonConfig;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.metricstore.AggLevel;
import org.apache.storm.metricstore.FilterOptions;
import org.apache.storm.metricstore.Metric;
import org.apache.storm.metricstore.MetricException;
import org.apache.storm.metricstore.MetricStore;
import org.apache.storm.utils.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseStore implements MetricStore {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseStore.class);

    private static final String VALUE_QUALIFIER_NAME = "value";
    private static final String COUNT_QUALIFIER_NAME = "count";
    private static final String SUM_QUALIFIER_NAME = "sum";
    private static final String MIN_QUALIFIER_NAME = "min";
    private static final String MAX_QUALIFIER_NAME = "max";
    private static final String VERSION_QUALIFIER_NAME = "version";
    private static final String METRIC_COLUMN_FAMILY_NAME = "metricdata";
    private static final byte[] VALUE_QUALIFIER = Bytes.toBytes(VALUE_QUALIFIER_NAME);
    private static final byte[] COUNT_QUALIFIER = Bytes.toBytes(COUNT_QUALIFIER_NAME);
    private static final byte[] SUM_QUALIFIER = Bytes.toBytes(SUM_QUALIFIER_NAME);
    private static final byte[] MIN_QUALIFIER = Bytes.toBytes(MIN_QUALIFIER_NAME);
    private static final byte[] MAX_QUALIFIER = Bytes.toBytes(MAX_QUALIFIER_NAME);
    static final byte[] VERSION_QUALIFIER = Bytes.toBytes(VERSION_QUALIFIER_NAME);
    static final byte[] METRIC_COLUMN_FAMILY = Bytes.toBytes(METRIC_COLUMN_FAMILY_NAME);
    static final byte[] METADATA_COLUMN_FAMILY = Bytes.toBytes("metadata");
    static final int INVALID_METADATA_STRING_ID = 0;
    private static final int NUM_TRIES = 4;
    private volatile boolean shutdown = false;

    private Map<HBaseMetadataKeyType, HBaseMetadataCache> metadataCacheMap = new HashMap<>();
    private ArrayList<AggLevel> aggBuckets = new ArrayList<>();
    private BlockingQueue queue = new LinkedBlockingQueue(4000);
    private Meter failureMeter = StormMetricsRegistry.registerMeter("HBaseStore:metric-failures");
    private ExecutorService executor;
    private Configuration configuration;

    /**
     * Create metric store instance using the configurations provided via the config map.
     *
     * @param mapConfig Storm config map
     * @throws MetricException on preparation error
     */
    @Override
    public void prepare(Map mapConfig) throws MetricException {
        this.configuration = this.createConfiguration(mapConfig);
        handleUgi(mapConfig);

        String namespace = ObjectReader.getString(mapConfig.get(DaemonConfig.STORM_HBASESTORE_TABLE_NAMESPACE), "default");
        String tableName = ObjectReader.getString(mapConfig.get(DaemonConfig.STORM_HBASESTORE_TABLE_NAME), "metrics");
        HBaseTablePool.init(this.configuration, namespace, tableName);

        for (HBaseMetadataKeyType type : EnumSet.allOf(HBaseMetadataKeyType.class)) {
            metadataCacheMap.put(type, new HBaseMetadataCache(type, this.failureMeter));
        }

        aggBuckets.add(AggLevel.AGG_LEVEL_1_MIN);
        aggBuckets.add(AggLevel.AGG_LEVEL_10_MIN);
        aggBuckets.add(AggLevel.AGG_LEVEL_60_MIN);

        int numInsertionThreads = ObjectReader.getInt(mapConfig.get(DaemonConfig.STORM_HBASESTORE_INSERTION_THREADS), 1);
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("HBase-metric-inserter-%d").setDaemon(true).build();
        this.executor = Executors.newFixedThreadPool(numInsertionThreads, threadFactory);

        for (int i = 0; i < numInsertionThreads; i++) {
            Runnable processor = new InsertionProcessor(this.queue);
            executor.execute(processor);
        }
    }

    private void handleUgi(Map mapConfig) throws MetricException {
        UserGroupInformation.setConfiguration(this.configuration);
        if (UserGroupInformation.isSecurityEnabled()) {
            String principal = ObjectReader.getString(mapConfig.get(DaemonConfig.STORM_HBASESTORE_PRINCIPAL), null);
            if (principal == null) {
                throw new MetricException(DaemonConfig.STORM_HBASESTORE_PRINCIPAL + " not configured");
            }
            String keytab = ObjectReader.getString(mapConfig.get(DaemonConfig.STORM_HBASESTORE_KEYTAB_FILE), null);
            if (keytab == null) {
                throw new MetricException(DaemonConfig.STORM_HBASESTORE_KEYTAB_FILE + " not configured");
            }

            try {
                LOG.info("Logging in {} using {}", principal, keytab);
                UserGroupInformation.loginUserFromKeytab(principal, keytab);
            } catch (IOException e) {
                throw new MetricException("Failed to login", e);
            }
        }
    }

    protected Configuration createConfiguration(Map mapConfig) throws MetricException {
        Configuration conf = new Configuration();
        String hbaseSiteXmlPath = ObjectReader.getString(mapConfig.get(DaemonConfig.STORM_HBASESTORE_HBASE_SITE), null);
        if (hbaseSiteXmlPath != null) {
            try {
                conf.addResource(new URL("file://" + hbaseSiteXmlPath));
            } catch (MalformedURLException e) {
                throw new MetricException("Failed to create configuration using " + hbaseSiteXmlPath, e);
            }
        }
        return conf;
    }

    /**
     * Class to handle inserting metrics to HBase from a queue.
     */
    private class InsertionProcessor implements Runnable {
        private BlockingQueue queue;

        InsertionProcessor(BlockingQueue queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            while (!shutdown) {
                try {
                    Metric m = (Metric) queue.take();
                    processInsert(m);
                } catch (Throwable t) {
                    LOG.error("Failed to insert metric", t);
                }
            }
        }
    }

    /**
     * Stores a metric in the store.
     *
     * @param metric Metric to store
     * @throws MetricException on error
     */
    @Override
    public void insert(Metric metric) throws MetricException {
        // don't bother blocking on a full queue, just drop metrics in case we can't keep up
        if (queue.remainingCapacity() <= 0) {
            LOG.info("Metrics q full, dropping metric");
            return;
        }
        try {
            queue.put(metric);
        } catch (InterruptedException e) {
            throw new MetricException("Failed to add metric to queue", e);
        }
    }

    private void processInsert(Metric metric) throws MetricException {
        byte[] key = getMetricKeyForInsert(metric);
        byte[] value = Bytes.toBytes(metric.getValue());
        byte[] count = Bytes.toBytes(metric.getCount());

        Map<byte[], byte[]> qualifierMap = new HashMap<>();
        qualifierMap.put(VALUE_QUALIFIER, value);
        qualifierMap.put(COUNT_QUALIFIER, count);

        HBaseTableOperation.putMetric(key, metric.getTimestamp(), qualifierMap);

        // Aggregate matching metrics over bucket timeframes.
        ListIterator li = aggBuckets.listIterator(aggBuckets.size());
        while (li.hasPrevious()) {
            AggLevel bucket = (AggLevel)li.previous();
            retriableInsertAggregatedMetric(bucket, metric, key);
        }
    }

    private void retriableInsertAggregatedMetric(AggLevel bucket, Metric baseMetric, byte[] baseMetricKey) throws MetricException {
        for (int i = 0; i < NUM_TRIES; i++) {
            try {
                insertAggregatedMetric(bucket, baseMetric, baseMetricKey);
                return;
            } catch (HBaseMetricException e) {
                try {
                    Thread.sleep(30L + i * randomLong(10L, 70L)); // hold off in case multiple threads are updating
                } catch (InterruptedException e1) {
                    //
                }
            }
        }
        throw new MetricException("Failed to insert aggregated metric from base " + baseMetric);
    }

    private long randomLong(long min, long max) {
        return ThreadLocalRandom.current().nextLong(min, max + 1L);
    }

    /**
     * Updates and inserts an aggregated metric based on the passed in metric.
     *
     * @param bucket   aggregation level for the new metric
     * @param baseMetric   the metric to aggregate
     * @param baseMetricKey  the HBase key for baseMetric
     *
     * @throws HBaseMetricException  if another thread appears to be modifying the aggregated metric
     * @throws MetricException on other errors
     */
    private void insertAggregatedMetric(AggLevel bucket, Metric baseMetric, byte[] baseMetricKey)
            throws MetricException, HBaseMetricException {
        HBaseAggregatedMetric aggMetric = new HBaseAggregatedMetric(baseMetric);
        aggMetric.setAggLevel(bucket);

        long msToBucket = 1000L * 60L * bucket.getValue();
        long roundedToBucket = msToBucket * (baseMetric.getTimestamp() / msToBucket);
        aggMetric.setTimestamp(roundedToBucket);

        // create new key for Agg bucket - all fields are the same except the first byte is the Agg value
        byte[] aggKey = Arrays.copyOf(baseMetricKey, baseMetricKey.length);
        aggKey[0] = bucket.getValue();

        // retrieve any existing aggregation matching this one and update the values
        if (this.processPopulateValue(aggMetric, aggKey)) {
            aggMetric.addValue(baseMetric.getValue());
        }

        Map<byte[], byte[]> qualifierMap = new HashMap<>();
        qualifierMap.put(VALUE_QUALIFIER, Bytes.toBytes(aggMetric.getValue()));
        qualifierMap.put(COUNT_QUALIFIER, Bytes.toBytes(aggMetric.getCount()));
        qualifierMap.put(SUM_QUALIFIER, Bytes.toBytes(aggMetric.getSum()));
        qualifierMap.put(MIN_QUALIFIER, Bytes.toBytes(aggMetric.getMin()));
        qualifierMap.put(MAX_QUALIFIER, Bytes.toBytes(aggMetric.getMax()));

        // it is possible two threads could be updating the same aggregated metric at the same time.  To prevent this,
        // we will do a checkandput operation validating the version matches when we update the metric.  If the
        // checkandput fails, we will throw an HBaseMetricException and retry the insert.
        qualifierMap.put(VERSION_QUALIFIER, Bytes.toBytes(aggMetric.getVersion() + 1));

        HBaseTableOperation.putAggregatedMetric(aggKey, aggMetric.getTimestamp(), qualifierMap, aggMetric.getVersion());
    }

    /**
     * Get the HBase key for a metric for a insert.  If any of the metric metadata strings do not exist, they will be
     * created and added to HBase.
     *
     * @param metric Metric to insert
     * @throws MetricException on error
     */
    private byte[] getMetricKeyForInsert(Metric metric) throws MetricException {
        int topologyId = this.metadataCacheMap.get(HBaseMetadataKeyType.TOPOLOGY).getOrCreateMetadataStringId(metric.getTopologyId());
        int streamId = this.metadataCacheMap.get(HBaseMetadataKeyType.STREAM_ID).getOrCreateMetadataStringId(metric.getStreamId());
        int hostId = this.metadataCacheMap.get(HBaseMetadataKeyType.HOST_ID).getOrCreateMetadataStringId(metric.getHostname());
        int componentId = this.metadataCacheMap.get(HBaseMetadataKeyType.COMPONENT_ID).getOrCreateMetadataStringId(metric.getComponentId());
        int metricNameId = this.metadataCacheMap.get(HBaseMetadataKeyType.METRIC_NAME).getOrCreateMetadataStringId(metric.getMetricName());
        int executorId = this.metadataCacheMap.get(HBaseMetadataKeyType.EXECUTOR_ID).getOrCreateMetadataStringId(metric.getExecutorId());

        HBaseMetricKey key = new HBaseMetricKey(metric.getAggLevel(), topologyId, metricNameId, componentId, executorId,
                hostId, metric.getPort(), streamId);
        return key.getRawKey();
    }

    /**
     * Get the HBase key for a metric for a populate.  If any of the metric metadata strings do not exist, return null.
     *
     * @param metric Metric to populate
     * @throws MetricException on error
     */
    private byte[] getMetricKeyForPopulate(Metric metric) throws MetricException {
        int topologyId = this.metadataCacheMap.get(HBaseMetadataKeyType.TOPOLOGY).lookupMetadataStringId(metric.getTopologyId());
        if (topologyId == INVALID_METADATA_STRING_ID) {
            return null;
        }
        int streamId = this.metadataCacheMap.get(HBaseMetadataKeyType.STREAM_ID).lookupMetadataStringId(metric.getStreamId());
        if (streamId == INVALID_METADATA_STRING_ID) {
            return null;
        }
        int hostId = this.metadataCacheMap.get(HBaseMetadataKeyType.HOST_ID).lookupMetadataStringId(metric.getHostname());
        if (hostId == INVALID_METADATA_STRING_ID) {
            return null;
        }
        int componentId = this.metadataCacheMap.get(HBaseMetadataKeyType.COMPONENT_ID).lookupMetadataStringId(metric.getComponentId());
        if (componentId == INVALID_METADATA_STRING_ID) {
            return null;
        }
        int metricNameId = this.metadataCacheMap.get(HBaseMetadataKeyType.METRIC_NAME).lookupMetadataStringId(metric.getMetricName());
        if (metricNameId == INVALID_METADATA_STRING_ID) {
            return null;
        }
        int executorId = this.metadataCacheMap.get(HBaseMetadataKeyType.EXECUTOR_ID).lookupMetadataStringId(metric.getExecutorId());
        if (executorId == INVALID_METADATA_STRING_ID) {
            return null;
        }

        HBaseMetricKey key = new HBaseMetricKey(metric.getAggLevel(), topologyId, metricNameId, componentId, executorId, hostId,
                metric.getPort(), streamId);
        return key.getRawKey();
    }

    /**
     * Fill out the numeric values for a metric.
     *
     * @param metric Metric to populate
     * @return true if the metric was populated, false otherwise
     * @throws MetricException on error
     */
    @Override
    public boolean populateValue(Metric metric) throws MetricException {
        return processPopulateValue(metric, null);
    }

    private boolean processPopulateValue(Metric metric, byte[] key) throws MetricException {
        if (key == null) {
            key = getMetricKeyForPopulate(metric);
        }
        if (key == null) {  // metric strings metadata do not exist, so no metric should exist
            return false;
        }

        if (metric instanceof HBaseAggregatedMetric) {
            int version = HBaseTableOperation.getAggregatedMetricVersion(key);
            ((HBaseAggregatedMetric) metric).setVersion(version);
        }

        Result result = HBaseTableOperation.performGet(key, metric.getTimestamp());
        if (result == null) {
            return false;
        }

        byte[] valueBytes = result.getValue(METRIC_COLUMN_FAMILY, VALUE_QUALIFIER);
        byte[] countBytes = result.getValue(METRIC_COLUMN_FAMILY, COUNT_QUALIFIER);
        if (valueBytes == null || countBytes == null) {
            return false;
        }

        double value = Bytes.toDouble(valueBytes);
        long count = Bytes.toLong(countBytes);
        metric.setValue(value);
        metric.setCount(count);

        if (metric.getAggLevel() != AggLevel.AGG_LEVEL_NONE) {
            byte[] sumBytes = result.getValue(METRIC_COLUMN_FAMILY, SUM_QUALIFIER);
            byte[] minBytes = result.getValue(METRIC_COLUMN_FAMILY, MIN_QUALIFIER);
            byte[] maxBytes = result.getValue(METRIC_COLUMN_FAMILY, MAX_QUALIFIER);
            if (sumBytes == null || minBytes == null || maxBytes == null) {
                return false;
            }
            double sum = Bytes.toDouble(sumBytes);
            double min = Bytes.toDouble(minBytes);
            double max = Bytes.toDouble(maxBytes);

            metric.setSum(sum);
            metric.setMin(min);
            metric.setMax(max);
        }
        return true;
    }

    /**
     * Close the metric store.
     */
    @Override
    public void close() {
        shutdown = true;
        HBaseTablePool.shutdown();
        executor.shutdownNow();
    }

    /**
     * Scans all metrics in the store and returns the ones matching the specified filtering options.
     *
     * @param filter       options to filter by
     * @param scanCallback callback for each Metric found
     * @throws MetricException on error
     */
    @Override
    public void scan(FilterOptions filter, ScanCallback scanCallback) throws MetricException {
        if (filter == null || scanCallback == null) {
            return;
        }

        long startTime = filter.getStartTime();
        long endTime = filter.getEndTime();

        int topologyId = INVALID_METADATA_STRING_ID;
        int metricNameId = INVALID_METADATA_STRING_ID;
        int componentId = INVALID_METADATA_STRING_ID;
        int executorId = INVALID_METADATA_STRING_ID;
        int hostId = INVALID_METADATA_STRING_ID;
        int port = 0;
        int streamId = INVALID_METADATA_STRING_ID;

        String filterTopologyId = filter.getTopologyId();
        if (filterTopologyId != null) {
            topologyId = this.metadataCacheMap.get(HBaseMetadataKeyType.TOPOLOGY).lookupMetadataStringId(filterTopologyId);
            if (topologyId == INVALID_METADATA_STRING_ID) {
                return;
            }
        }

        String filterMetricName = filter.getMetricName();
        if (filterMetricName != null) {
            metricNameId = this.metadataCacheMap.get(HBaseMetadataKeyType.METRIC_NAME).lookupMetadataStringId(filterMetricName);
            if (metricNameId == INVALID_METADATA_STRING_ID) {
                return;
            }
        }

        String filterComponentId = filter.getComponentId();
        if (filterComponentId != null) {
            componentId = this.metadataCacheMap.get(HBaseMetadataKeyType.COMPONENT_ID).lookupMetadataStringId(filterComponentId);
            if (componentId == INVALID_METADATA_STRING_ID) {
                return;
            }
        }

        String filterExecutorName = filter.getExecutorId();
        if (filterExecutorName != null) {
            executorId = this.metadataCacheMap.get(HBaseMetadataKeyType.EXECUTOR_ID).lookupMetadataStringId(filterExecutorName);
            if (executorId == INVALID_METADATA_STRING_ID) {
                return;
            }
        }

        String filterHostId = filter.getHostId();
        if (filterHostId != null) {
            hostId = this.metadataCacheMap.get(HBaseMetadataKeyType.HOST_ID).lookupMetadataStringId(filterHostId);
            if (hostId == INVALID_METADATA_STRING_ID) {
                return;
            }
        }

        Integer filterPort = filter.getPort();
        if (filterPort != null) {
            port = filterPort;
        }

        String filterStreamId = filter.getStreamId();
        if (filterStreamId != null) {
            streamId = this.metadataCacheMap.get(HBaseMetadataKeyType.STREAM_ID).lookupMetadataStringId(filterStreamId);
            if (streamId == INVALID_METADATA_STRING_ID) {
                return;
            }
        }

        for (AggLevel aggLevel : filter.getAggLevels()) {
            Scan scan = new Scan();
            scan.setMaxVersions(HConstants.ALL_VERSIONS);
            scan.addColumn(METRIC_COLUMN_FAMILY, VALUE_QUALIFIER);
            scan.addColumn(METRIC_COLUMN_FAMILY, COUNT_QUALIFIER);
            if (aggLevel != AggLevel.AGG_LEVEL_NONE) {
                scan.addColumn(METRIC_COLUMN_FAMILY, SUM_QUALIFIER);
                scan.addColumn(METRIC_COLUMN_FAMILY, MIN_QUALIFIER);
                scan.addColumn(METRIC_COLUMN_FAMILY, MAX_QUALIFIER);
            }

            HBaseMetricKey key = new HBaseMetricKey(aggLevel, topologyId, metricNameId, componentId, executorId, hostId,
                port, streamId);
            HBaseMetricKey fuzzyInfo = key.createScanKey();

            scan.setFilter(new FuzzyRowFilter(Arrays.asList(new Pair<>(key.getRawKey(), fuzzyInfo.getRawKey()))));

            ResultScanner scanner = null;
            try {
                scan.setTimeRange(startTime, endTime);
                scanner = HBaseTableOperation.getScanner(scan);

                for (Result result = scanner.next(); result != null; result = scanner.next()) {
                    try {
                        resultToMetrics(result, scanCallback);
                    } catch (MetricException e) {
                        LOG.error("Failed to callback on scan", e);
                    }
                }
            } catch (IOException e) {
                throw new MetricException("Scan failure", e);
            } finally {
                if (scanner != null) {
                    scanner.close();
                }
            }
        }
    }

    /**
     * Calls back scanned metrics contained within a Result.
     *
     * @param result       Result containing scanned metrics
     * @param scanCallback callback for each Metric found
     * @throws MetricException on error
     */
    private void resultToMetrics(Result result, ScanCallback scanCallback) throws MetricException {
        byte[] key = result.getRow();
        HBaseMetricKey metricKey = new HBaseMetricKey(key);

        Metric baseMetric = metricKey.createMetric(this.metadataCacheMap);

        // map all the metric cell data by timestamp
        Map<Long, Map<String, byte[]>> cellData = new HashMap<>();

        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            long timestamp = cell.getTimestamp();
            Map<String, byte[]> columnData = cellData.get(timestamp);
            if (columnData == null) {
                columnData = new HashMap<>();
                cellData.put(timestamp, columnData);
            }

            String column = new String(CellUtil.cloneQualifier(cell));
            byte[] columnValue = CellUtil.cloneValue(cell);
            columnData.put(column, columnValue);
        }

        for (Long timestamp : cellData.keySet()) {
            Map<String, byte[]> columnData = cellData.get(timestamp);

            byte[] valueBytes = columnData.get(VALUE_QUALIFIER_NAME);
            if (valueBytes == null) {
                LOG.error("key {} timestamp {} is missing value", metricKey, timestamp);
                continue;
            }
            Double value = Bytes.toDouble(valueBytes);

            byte[] countBytes = columnData.get(COUNT_QUALIFIER_NAME);
            if (countBytes == null) {
                LOG.error("key {} timestamp {} is missing count", metricKey, timestamp);
                continue;
            }
            Long count = Bytes.toLong(countBytes);

            Metric metric = new Metric(baseMetric);
            metric.setTimestamp(timestamp);
            metric.setValue(value);
            metric.setCount(count);

            if (baseMetric.getAggLevel() != AggLevel.AGG_LEVEL_NONE) {
                byte[] sumBytes = columnData.get(SUM_QUALIFIER_NAME);
                if (sumBytes == null) {
                    LOG.error("key {} timestamp {} is missing sum", metricKey, timestamp);
                    continue;
                }
                Double sum = Bytes.toDouble(sumBytes);
                metric.setSum(sum);

                byte[] maxBytes = columnData.get(MAX_QUALIFIER_NAME);
                if (maxBytes == null) {
                    LOG.error("key {} timestamp {} is missing max", metricKey, timestamp);
                    continue;
                }
                Double max = Bytes.toDouble(maxBytes);
                metric.setMax(max);

                byte[] minBytes = columnData.get(MIN_QUALIFIER_NAME);
                if (minBytes == null) {
                    LOG.error("key {} timestamp {} is missing min", metricKey, timestamp);
                    continue;
                }
                Double min = Bytes.toDouble(minBytes);
                metric.setMin(min);
            }

            scanCallback.cb(metric);
        }
    }
}
