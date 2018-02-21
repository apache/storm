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

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.metricstore.MetricException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HBaseTableOperation performs various operations on an HBase table for the HBaseStore.  It reuses Table objects
 * from an object pool to reduce the time it takes to create a table connection and to allow operation from any
 * thread.
 */
class HBaseTableOperation {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseTableOperation.class);

    static ResultScanner getScanner(Scan scan) throws MetricException {
        Table table = null;
        try {
            table = HBaseTablePool.allocate();
            ResultScanner scanner;
            try {
                scanner = table.getScanner(scan);
            } catch (IOException e) {
                throw new MetricException("Failed to get scanner", e);
            }
            return scanner;
        } finally {
            HBaseTablePool.release(table);
        }
    }

    static Result performGet(byte[] key, long timestamp) throws MetricException {
        Get g = new Get(key);
        try {
            g.setTimeStamp(timestamp);
        } catch (IOException e) {
            throw new MetricException("Failed to set timestamp on Get", e);
        }
        return performGet(g);
    }

    private static Result performGet(Get g) throws MetricException {
        Table table = null;
        try {
            table = HBaseTablePool.allocate();
            return table.get(g);
        } catch (IOException e) {
            throw new MetricException("Failed Get", e);
        } finally {
            HBaseTablePool.release(table);
        }
    }

    static void putMetric(byte[] key, long timestamp, Map<byte[], byte[]> qualifierMap) throws MetricException {
        Put p = new Put(key, timestamp);
        for (Map.Entry<byte[], byte[]> entry : qualifierMap.entrySet()) {
            p.addColumn(HBaseStore.METRIC_COLUMN_FAMILY, entry.getKey(), entry.getValue());
        }
        put(p);
    }

    static void putMetadata(byte[] key, byte[] value, HBaseMetadataKeyType type) throws MetricException {
        Put p = new Put(key);
        p.addColumn(HBaseStore.METADATA_COLUMN_FAMILY, type.getQualifier(), value);
        put(p);
    }

    private static void put(Put p) throws MetricException {
        Table table = null;
        try {
            table = HBaseTablePool.allocate();
            table.put(p);
        } catch (IOException e) {
            throw new MetricException("Failed to put metric", e);
        } finally {
            HBaseTablePool.release(table);
        }
    }

    /**
     * Attempts to put an aggregated metric with a given version to HBase.  If the put fails (due to a version mismatch),
     * an HBaseMetricException will be thrown.  This indicates another thread has updated the metric, and the put should
     * no longer be considered valid/up-to-date.
     *
     * @param key  HBase metric key
     * @param timestamp metric timestamp
     * @param qualifierMap  HBase column qualifiers for the metric
     * @param existingVersion  version of the metric
     * @throws HBaseMetricException  if the put failed due to a version mismatch
     * @throws MetricException  on any HBase error
     */
    static void putAggregatedMetric(byte[] key, long timestamp, Map<byte[], byte[]> qualifierMap, int existingVersion)
            throws HBaseMetricException, MetricException {
        Put p = new Put(key, timestamp);
        for (Map.Entry<byte[], byte[]> entry : qualifierMap.entrySet()) {
            p.addColumn(HBaseStore.METRIC_COLUMN_FAMILY, entry.getKey(), entry.getValue());
        }
        boolean putAdded;
        Table table = null;
        try {
            table = HBaseTablePool.allocate();
            if (existingVersion == 0) {
                // only do a put for the Agg metric if the version does not exist
                putAdded = table.checkAndPut(key, HBaseStore.METRIC_COLUMN_FAMILY, HBaseStore.VERSION_QUALIFIER, null, p);
            } else {
                putAdded = table.checkAndPut(key, HBaseStore.METRIC_COLUMN_FAMILY, HBaseStore.VERSION_QUALIFIER,
                        Bytes.toBytes(existingVersion), p);
            }
        } catch (IOException e) {
            throw new MetricException("Failed to put aggregated metric", e);
        } finally {
            HBaseTablePool.release(table);
        }
        if (!putAdded) {
            String message = "Failed to match version of aggregated metric with key " + new HBaseMetricKey(key).toString();
            LOG.warn(message);
            throw new HBaseMetricException(message);
        }
    }

    static long incrementMetadataReferenceCounter(HBaseMetadataKeyType type) throws MetricException {
        long columnValue;
        Table table = null;
        try {
            table = HBaseTablePool.allocate();
            columnValue = table.incrementColumnValue(type.getRefCounter(), HBaseStore.METADATA_COLUMN_FAMILY, type.getQualifier(), 1L);
        } catch (IOException e) {
            throw new MetricException("Failed to increment metadata column", e);
        } finally {
            HBaseTablePool.release(table);
        }
        if (columnValue > Integer.MAX_VALUE) {
            throw new MetricException("Unable to create new metadata string of type " + type + ". Ref count is " + columnValue);
        }
        return columnValue;
    }

    /**
     * Attempts to put a new metadata string to HBase.  If the string already exists (which would indicate another thread
     * recently inserted it), the put will fail and this will throw a HBaseMetricException.
     *
     * @param s  The metadata string to add
     * @param value  The unique id the metadata string maps to
     * @param type   The type of metadata string
     * @throws HBaseMetricException  if the put failed due to the string already existing
     * @throws MetricException  on any HBase error
     */
    static void checkAndPutMetadataIfMissing(String s, byte[] value, HBaseMetadataKeyType type)
            throws MetricException, HBaseMetricException {
        byte[] key = Bytes.toBytes(s);
        Put p = new Put(key);
        p.addColumn(HBaseStore.METADATA_COLUMN_FAMILY, type.getQualifier(), value);
        boolean putAdded;
        Table table = null;
        try {
            table = HBaseTablePool.allocate();
            putAdded = table.checkAndPut(key, HBaseStore.METADATA_COLUMN_FAMILY, type.getQualifier(), null, p);
        } catch (IOException e) {
            throw new MetricException("Failed to put new metadata string " + type + "." + s, e);
        } finally {
            HBaseTablePool.release(table);
        }
        if (!putAdded) {
            throw new HBaseMetricException("Failed to put metadata string " + type + "." + s);
        }
    }

    static String getMetadataString(int id, HBaseMetadataKeyType type) throws MetricException {
        byte[] metadataIdKey = Bytes.toBytes(id);
        Get g = new Get(metadataIdKey);
        Result result = performGet(g);
        if (result != null) {
            byte[] valueBytes = result.getValue(HBaseStore.METADATA_COLUMN_FAMILY, type.getQualifier());
            if (valueBytes == null) {
                return null;
            }
            return Bytes.toString(valueBytes);
        } else {
            return null;
        }
    }

    static Integer getHBaseMetadataStringId(String s, HBaseMetadataKeyType type) throws MetricException {
        byte[] metadataStringKey = Bytes.toBytes(s);
        Get g = new Get(metadataStringKey);
        Result result = performGet(g);
        if (result != null) {
            byte[] valueBytes = result.getValue(HBaseStore.METADATA_COLUMN_FAMILY, type.getQualifier());
            if (valueBytes == null) {
                return null;
            }
            return Bytes.toInt(valueBytes);
        } else {
            return null;
        }
    }

    static int getAggregatedMetricVersion(byte[] key) throws MetricException {
        Get g = new Get(key);
        Result r = performGet(g);
        if (r != null) {
            byte[] versionBytes = r.getValue(HBaseStore.METRIC_COLUMN_FAMILY, HBaseStore.VERSION_QUALIFIER);
            if (versionBytes != null) {
                return Bytes.toInt(versionBytes);
            } else {
                return 0;
            }
        }
        return 0;
    }
}
