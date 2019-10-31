/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.hbase.state;

import static org.apache.storm.hbase.state.HBaseKeyValueState.STATE_QUALIFIER;

import com.google.common.primitives.UnsignedBytes;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.storm.hbase.common.HBaseClient;
import org.apache.storm.state.BaseBinaryStateIterator;
import org.apache.storm.state.DefaultStateEncoder;
import org.apache.storm.state.Serializer;
import org.apache.storm.state.StateEncoder;

/**
 * An iterator over {@link HBaseKeyValueState}.
 */
public class HBaseKeyValueStateIterator<K, V> extends BaseBinaryStateIterator<K, V> {

    private final byte[] keyNamespace;
    private final byte[] endScanKey;
    private final byte[] columnFamily;
    private final HBaseClient hbaseClient;
    private final int chunkSize;
    private final StateEncoder<K, V, byte[], byte[]> encoder;
    private byte[] cursorKey;
    private Iterator<Map.Entry<byte[], byte[]>> cachedResultIterator;

    /**
     * Constructor.
     *
     * @param namespace              The namespace of State
     * @param columnFamily           The column family of state
     * @param hbaseClient            The instance of HBaseClient
     * @param pendingPrepareIterator The iterator of pendingPrepare
     * @param pendingCommitIterator  The iterator of pendingCommit
     * @param chunkSize              The size of chunk to get entries from HBase
     * @param keySerializer          The serializer of key
     * @param valueSerializer        The serializer of value
     */
    public HBaseKeyValueStateIterator(String namespace, byte[] columnFamily, HBaseClient hbaseClient,
                                      Iterator<Map.Entry<byte[], byte[]>> pendingPrepareIterator,
                                      Iterator<Map.Entry<byte[], byte[]>> pendingCommitIterator,
                                      int chunkSize, Serializer<K> keySerializer,
                                      Serializer<V> valueSerializer) {
        super(pendingPrepareIterator, pendingCommitIterator);
        this.columnFamily = columnFamily;
        this.keyNamespace = (namespace + "$key:").getBytes();
        this.cursorKey = (namespace + "$key:").getBytes();

        // this is the end key for whole scan
        this.endScanKey = advanceRow(this.cursorKey);
        this.hbaseClient = hbaseClient;
        this.chunkSize = chunkSize;
        this.encoder = new DefaultStateEncoder<K, V>(keySerializer, valueSerializer);
    }

    @Override
    protected Iterator<Map.Entry<byte[], byte[]>> loadChunkFromStateStorage() {
        loadChunkFromHBase();
        return cachedResultIterator;
    }

    @Override
    protected boolean isEndOfDataFromStorage() {
        if (cachedResultIterator != null && cachedResultIterator.hasNext()) {
            return false;
        }

        try {
            ResultScanner resultScanner = hbaseClient.scan(cursorKey, endScanKey);
            return !(resultScanner.iterator().hasNext());
        } catch (Exception e) {
            throw new RuntimeException("Fail to scan from HBase state storage.");
        }
    }

    @Override
    protected K decodeKey(byte[] key) {
        return encoder.decodeKey(key);
    }

    @Override
    protected V decodeValue(byte[] value) {
        return encoder.decodeValue(value);
    }

    @Override
    protected boolean isTombstoneValue(byte[] value) {
        return Arrays.equals(value, encoder.getTombstoneValue());
    }

    private void loadChunkFromHBase() {
        Map<byte[], byte[]> chunk = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
        try {
            ResultScanner resultScanner = hbaseClient.scan(cursorKey, endScanKey);

            Result[] results = resultScanner.next(chunkSize);

            for (Result result : results) {
                byte[] columnKey = extractStateKeyFromRowKey(result.getRow());
                byte[] columnValue = result.getValue(columnFamily, STATE_QUALIFIER);

                chunk.put(columnKey, columnValue);
            }

            if (results.length > 0) {
                byte[] lastRow = results[results.length - 1].getRow();
                cursorKey = advanceRow(lastRow);
            }

            cachedResultIterator = chunk.entrySet().iterator();
        } catch (Exception e) {
            throw new RuntimeException("Fail to scan from HBase state storage.", e);
        }
    }

    private byte[] advanceRow(byte[] row) {
        byte[] advancedRow = new byte[row.length];
        System.arraycopy(row, 0, advancedRow, 0, row.length);
        advancedRow[row.length - 1]++;
        return advancedRow;
    }

    private byte[] extractStateKeyFromRowKey(byte[] row) {
        byte[] stateKey = new byte[row.length - keyNamespace.length];
        System.arraycopy(row, keyNamespace.length, stateKey, 0, row.length - keyNamespace.length);
        return stateKey;
    }

}
