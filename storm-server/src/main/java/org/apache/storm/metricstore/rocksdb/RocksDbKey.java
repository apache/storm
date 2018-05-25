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
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import javax.xml.bind.DatatypeConverter;
import org.apache.storm.metricstore.AggLevel;
import org.apache.storm.shade.com.google.common.primitives.UnsignedBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class representing the data used as a Key in RocksDB.  Keys can be used either for metadata or metrics.
 *
 * <P>Keys are 38 bytes in size.  The fields for a key are:
 * <pre><
 * Field             Size         Offset
 *
 * Type                 1              0      The type maps to the KeyType enum, specifying a metric or various types of metadata
 * Aggregation Level    1              1      The aggregation level for a metric (see AggLevel enum).  0 for metadata.
 * TopologyId           4              2      The metadata string Id representing a topologyId for a metric, or the unique
 *                                                   string Id for a metadata string
 * Timestamp            8              6      The timestamp for a metric, unused for metadata
 * MetricId             4             14      The metadata string Id for the metric name
 * ComponentId          4             18      The metadata string Id for the component Id
 * ExecutorId           4             22      The metadata string Id for the executor Id
 * HostId               4             26      The metadata string Id for the host Id
 * Port                 4             30      The port number
 * StreamId             4             34      The metadata string Id for the stream Id
 * </pre>
 */
public class RocksDbKey implements Comparable<RocksDbKey> {
    static final int KEY_SIZE = 38;
    private static final Logger LOG = LoggerFactory.getLogger(RocksDbKey.class);
    private static Map<Byte, RocksDbKey> PREFIX_MAP = new HashMap<>();

    static {
        // pregenerate commonly used keys for scans
        for (KeyType type : EnumSet.allOf(KeyType.class)) {
            RocksDbKey key = new RocksDbKey(type, 0);
            PREFIX_MAP.put(type.getValue(), key);
        }
        PREFIX_MAP = Collections.unmodifiableMap(PREFIX_MAP);
    }

    private byte[] key;

    /**
     * Constructor for a RocksDB key for a metadata string.
     *
     * @param type  type of metadata string
     * @param metadataStringId  the string Id for the string (stored in the topologyId portion of the key)
     */
    RocksDbKey(KeyType type, int metadataStringId) {
        byte[] key = new byte[KEY_SIZE];
        ByteBuffer bb = ByteBuffer.wrap(key);
        bb.put(type.getValue());
        bb.put(AggLevel.AGG_LEVEL_NONE.getValue());
        bb.putInt(metadataStringId);
        this.key = key;
    }

    /**
     * Constructor for a RocksDB key from raw data.
     *
     * @param raw  the key data
     */
    RocksDbKey(byte[] raw) {
        this.key = raw;
    }


    /**
     * Get a zeroed key of the specified type.
     *
     * @param type  the desired type
     * @return a key of the desired type
     */
    static RocksDbKey getPrefix(KeyType type) {
        return PREFIX_MAP.get(type.getValue());
    }

    /**
     * gets the first possible key value for the desired key type.
     *
     * @return the initial key
     */
    static RocksDbKey getInitialKey(KeyType type) {
        return PREFIX_MAP.get(type.getValue());
    }

    /**
     * gets the key just larger than the last possible key value for the desired key type.
     *
     * @return the last key
     */
    static RocksDbKey getLastKey(KeyType type) {
        byte value = (byte) (type.getValue() + 1);
        return PREFIX_MAP.get(value);
    }

    /**
     * Creates a metric key with the desired properties.
     *
     * @return the generated key
     */
    static RocksDbKey createMetricKey(AggLevel aggLevel, int topologyId, long metricTimestamp, int metricId,
                                      int componentId, int executorId, int hostId, int port,
                                      int streamId) {
        byte[] raw = new byte[KEY_SIZE];
        ByteBuffer bb = ByteBuffer.wrap(raw);
        bb.put(KeyType.METRIC_DATA.getValue());
        bb.put(aggLevel.getValue());
        bb.putInt(topologyId);       // offset 2
        bb.putLong(metricTimestamp); // offset 6
        bb.putInt(metricId);         // offset 14
        bb.putInt(componentId);      // offset 18
        bb.putInt(executorId);       // offset 22
        bb.putInt(hostId);           // offset 26
        bb.putInt(port);             // offset 30
        bb.putInt(streamId);         // offset 34

        RocksDbKey key = new RocksDbKey(raw);
        return key;
    }

    /**
     * get the metadata string Id portion of the key for metadata keys.
     *
     * @return the metadata string Id
     * @throws RuntimeException  if the key is not a metadata type
     */
    int getMetadataStringId() {
        if (this.getType().getValue() < KeyType.METADATA_STRING_END.getValue()) {
            return ByteBuffer.wrap(key, 2, 4).getInt();
        } else {
            throw new RuntimeException("Cannot fetch metadata string for key of type " + this.getType());
        }
    }

    /**
     * get the raw key bytes
     */
    byte[] getRaw() {
        return this.key;
    }

    /**
     * get the type of key.
     *
     * @return the type of key
     */
    KeyType getType() {
        return KeyType.getKeyType(key[0]);
    }

    /**
     * compares to keys on a byte by byte basis.
     *
     * @return comparison of key byte values
     */
    @Override
    public int compareTo(RocksDbKey o) {
        return UnsignedBytes.lexicographicalComparator().compare(this.getRaw(), o.getRaw());
    }

    /**
     * Get the unique string Id for a metric's topologyId.
     */
    int getTopologyId() {
        int val = ByteBuffer.wrap(key, 2, 4).getInt();
        return val;
    }

    long getTimestamp() {
        return ByteBuffer.wrap(key, 6, 8).getLong();
    }

    int getMetricId() {
        return ByteBuffer.wrap(key, 14, 4).getInt();
    }

    int getComponentId() {
        return ByteBuffer.wrap(key, 18, 4).getInt();
    }

    int getExecutorId() {
        return ByteBuffer.wrap(key, 22, 4).getInt();
    }

    int getHostnameId() {
        return ByteBuffer.wrap(key, 26, 4).getInt();
    }

    int getPort() {
        return ByteBuffer.wrap(key, 30, 4).getInt();
    }

    int getStreamId() {
        return ByteBuffer.wrap(key, 34, 4).getInt();
    }

    @Override
    public String toString() {
        return "[0x" + DatatypeConverter.printHexBinary(key) + "]";
    }
}

