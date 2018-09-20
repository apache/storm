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

import java.nio.ByteBuffer;
import org.apache.storm.metricstore.Metric;


/**
 * Class representing the data used as a Value in RocksDB.  Values can be used either for metadata or metrics.
 *
 * <p>Formats for Metadata String values are:
 *
 * <pre>
 * Field             Size         Offset
 *
 * Version              1              0      The current metadata version - allows migrating if the format changes in the future
 * Timestamp            8              1      The time when the metadata was last used by a metric.  Allows deleting of old metadata.
 * Metadata String    any              9      The metadata string
 *</pre>
 *
 * <p>Formats for Metric values are:
 *
 * <pre>
 * Field             Size         Offset
 *
 * Version              1              0      The current metric version - allows migrating if the format changes in the future
 * Value                8              1      The metric value
 * Count                8              9      The metric count
 * Min                  8             17      The minimum metric value
 * Max                  8             25      The maximum metric value
 * Sum                  8             33      The sum of the metric values
 * </pre>
 */

class RocksDbValue {
    private static final byte CURRENT_METADATA_VERSION = 0;
    private static final byte CURRENT_METRIC_VERSION = 0;
    private static int METRIC_VALUE_SIZE = 41;
    private static int MIN_METADATA_VALUE_SIZE = 9;
    private byte[] value;

    /**
     * Constructor from raw data.
     *
     * @param value  the raw bytes representing the key
     */
    RocksDbValue(byte[] value) {
        this.value = value;
    }

    /**
     * Constructor for a metadata string.
     *
     * @param lastTimestamp  the last timestamp when the string was used
     * @param metadataString   the metadata string
     */
    RocksDbValue(long lastTimestamp, String metadataString) {
        this.value = new byte[MIN_METADATA_VALUE_SIZE + metadataString.length()];
        ByteBuffer bb = ByteBuffer.wrap(value);
        bb.put(CURRENT_METADATA_VERSION);
        bb.putLong(lastTimestamp);
        bb.put(metadataString.getBytes());
    }

    /**
     * Constructor for a metric.
     *
     * @param m  the metric to create a value from
     */
    RocksDbValue(Metric m) {
        this.value = new byte[METRIC_VALUE_SIZE];
        ByteBuffer bb = ByteBuffer.wrap(value);
        bb.put(CURRENT_METRIC_VERSION);
        bb.putDouble(m.getValue());
        bb.putLong(m.getCount());
        bb.putDouble(m.getMin());
        bb.putDouble(m.getMax());
        bb.putDouble(m.getSum());
    }

    /**
     * Get the metadata string portion of the value.  Assumes the value is metadata.
     *
     * @return the metadata string
     */
    String getMetdataString() {
        if (this.value.length < MIN_METADATA_VALUE_SIZE) {
            throw new RuntimeException("RocksDB value is too small to be a metadata string!");
        }
        return new String(this.value, 9, this.value.length - 9);
    }

    /**
     * Gets StringMetadata associated with the key/value pair.
     */
    StringMetadata getStringMetadata(RocksDbKey key) {
        return new StringMetadata(key.getType(), key.getMetadataStringId(), this.getLastTimestamp());
    }

    /**
     * Gets the last time a metadata string was used.
     */
    long getLastTimestamp() {
        return ByteBuffer.wrap(value, 1, 8).getLong();
    }

    /**
     * get the raw value bytes
     */
    byte[] getRaw() {
        return this.value;
    }

    /**
     * populate metric values from the raw data.
     */
    void populateMetric(Metric metric) {
        ByteBuffer bb = ByteBuffer.wrap(this.value, 0, METRIC_VALUE_SIZE);
        bb.get();  // version
        metric.setValue(bb.getDouble());
        metric.setCount(bb.getLong());
        metric.setMin(bb.getDouble());
        metric.setMax(bb.getDouble());
        metric.setSum(bb.getDouble());
    }

}
