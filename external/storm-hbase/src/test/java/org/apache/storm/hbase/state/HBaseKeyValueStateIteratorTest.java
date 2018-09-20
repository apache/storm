/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.hbase.state;

import com.google.common.primitives.UnsignedBytes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.storm.hbase.common.ColumnList;
import org.apache.storm.hbase.common.HBaseClient;
import org.apache.storm.state.DefaultStateEncoder;
import org.apache.storm.state.DefaultStateSerializer;
import org.apache.storm.state.Serializer;
import org.junit.Before;
import org.junit.Test;

import static org.apache.storm.hbase.state.HBaseKeyValueState.STATE_QUALIFIER;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for HBaseKeyValueStateIterator.
 */
public class HBaseKeyValueStateIteratorTest {

    private String namespace;
    private byte[] keyNamespace;
    private byte[] columnFamily;
    private HBaseClient mockHBaseClient;
    private int chunkSize = 1000;
    private Serializer<byte[]> keySerializer = new DefaultStateSerializer<>();
    private Serializer<byte[]> valueSerializer = new DefaultStateSerializer<>();
    private DefaultStateEncoder<byte[], byte[]> encoder;

    @Before
    public void setUp() throws Exception {
        namespace = "namespace";
        keyNamespace = (namespace + "$key:").getBytes();
        columnFamily = "cf".getBytes();
        mockHBaseClient = HBaseClientTestUtil.mockedHBaseClient();
        encoder = new DefaultStateEncoder<>(keySerializer, valueSerializer);
    }

    @Test
    public void testGetEntriesInHBase() throws Exception {
        // pendingPrepare has no entries
        final NavigableMap<byte[], byte[]> pendingPrepare = getBinaryTreeMap();

        // pendingCommit has no entries
        final NavigableMap<byte[], byte[]> pendingCommit = getBinaryTreeMap();

        // HBase has some entries
        NavigableMap<byte[], byte[]> chunkMap = getBinaryTreeMap();
        putEncodedKeyValueToMap(chunkMap, "key0".getBytes(), "value0".getBytes());
        putEncodedKeyValueToMap(chunkMap, "key2".getBytes(), "value2".getBytes());

        applyPendingStateToHBase(chunkMap);

        HBaseKeyValueStateIterator<byte[], byte[]> kvIterator =
            new HBaseKeyValueStateIterator<>(namespace, columnFamily, mockHBaseClient, pendingPrepare.entrySet().iterator(),
                                             pendingCommit.entrySet().iterator(), chunkSize, keySerializer, valueSerializer);

        assertNextEntry(kvIterator, "key0".getBytes(), "value0".getBytes());

        // key1 shouldn't in iterator

        assertNextEntry(kvIterator, "key2".getBytes(), "value2".getBytes());

        assertFalse(kvIterator.hasNext());
    }

    @Test
    public void testGetEntriesRemovingDuplicationKeys() throws Exception {
        NavigableMap<byte[], byte[]> pendingPrepare = getBinaryTreeMap();
        putEncodedKeyValueToMap(pendingPrepare, "key0".getBytes(), "value0".getBytes());
        putTombstoneToMap(pendingPrepare, "key1".getBytes());

        NavigableMap<byte[], byte[]> pendingCommit = getBinaryTreeMap();
        putEncodedKeyValueToMap(pendingCommit, "key1".getBytes(), "value1".getBytes());
        putEncodedKeyValueToMap(pendingCommit, "key2".getBytes(), "value2".getBytes());

        NavigableMap<byte[], byte[]> chunkMap = getBinaryTreeMap();
        putEncodedKeyValueToMap(chunkMap, "key2".getBytes(), "value2".getBytes());
        putEncodedKeyValueToMap(chunkMap, "key3".getBytes(), "value3".getBytes());
        putEncodedKeyValueToMap(chunkMap, "key4".getBytes(), "value4".getBytes());

        applyPendingStateToHBase(chunkMap);

        HBaseKeyValueStateIterator<byte[], byte[]> kvIterator =
            new HBaseKeyValueStateIterator<>(namespace, columnFamily, mockHBaseClient, pendingPrepare.entrySet().iterator(),
                                             pendingCommit.entrySet().iterator(), chunkSize, keySerializer, valueSerializer);

        // keys shouldn't appear twice

        assertNextEntry(kvIterator, "key0".getBytes(), "value0".getBytes());

        // key1 shouldn't be in iterator since it's marked as deleted

        assertNextEntry(kvIterator, "key2".getBytes(), "value2".getBytes());
        assertNextEntry(kvIterator, "key3".getBytes(), "value3".getBytes());
        assertNextEntry(kvIterator, "key4".getBytes(), "value4".getBytes());

        assertFalse(kvIterator.hasNext());
    }

    @Test
    public void testGetEntryNotAvailable() {
        NavigableMap<byte[], byte[]> pendingPrepare = getBinaryTreeMap();

        NavigableMap<byte[], byte[]> pendingCommit = getBinaryTreeMap();

        HBaseKeyValueStateIterator<byte[], byte[]> kvIterator =
            new HBaseKeyValueStateIterator<>(namespace, columnFamily, mockHBaseClient, pendingPrepare.entrySet().iterator(),
                                             pendingCommit.entrySet().iterator(), chunkSize, keySerializer, valueSerializer);

        assertFalse(kvIterator.hasNext());
    }

    private void assertNextEntry(HBaseKeyValueStateIterator<byte[], byte[]> kvIterator, byte[] expectedKey,
                                 byte[] expectedValue) {
        assertTrue(kvIterator.hasNext());
        Map.Entry<byte[], byte[]> entry = kvIterator.next();
        assertArrayEquals(expectedKey, entry.getKey());
        assertArrayEquals(expectedValue, entry.getValue());
    }

    private void putEncodedKeyValueToMap(NavigableMap<byte[], byte[]> map, byte[] key, byte[] value) {
        map.put(encoder.encodeKey(key), encoder.encodeValue(value));
    }

    private void putTombstoneToMap(NavigableMap<byte[], byte[]> map, byte[] key) {
        map.put(encoder.encodeKey(key), encoder.getTombstoneValue());
    }

    private TreeMap<byte[], byte[]> getBinaryTreeMap() {
        return new TreeMap<>(UnsignedBytes.lexicographicalComparator());
    }

    private void applyPendingStateToHBase(NavigableMap<byte[], byte[]> pendingMap) throws Exception {
        List<Mutation> mutations = new ArrayList<>();
        for (Map.Entry<byte[], byte[]> entry : pendingMap.entrySet()) {
            byte[] rowKey = entry.getKey();
            byte[] value = entry.getValue();

            if (Arrays.equals(value, encoder.getTombstoneValue())) {
                mutations.add(new Delete(getRowKeyForStateKey(rowKey)));
            } else {
                List<Mutation> mutationsForRow = prepareMutateRow(getRowKeyForStateKey(rowKey), columnFamily,
                                                                  Collections.singletonMap(STATE_QUALIFIER, value));
                mutations.addAll(mutationsForRow);
            }
        }

        mockHBaseClient.batchMutate(mutations);
    }

    private byte[] getRowKeyForStateKey(byte[] columnKey) {
        byte[] rowKey = new byte[keyNamespace.length + columnKey.length];
        System.arraycopy(keyNamespace, 0, rowKey, 0, keyNamespace.length);
        System.arraycopy(columnKey, 0, rowKey, keyNamespace.length, columnKey.length);
        return rowKey;
    }

    private ColumnList buildColumnList(byte[] columnFamily, Map<byte[], byte[]> map) {
        ColumnList columnList = new ColumnList();
        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            columnList.addColumn(columnFamily, entry.getKey(), entry.getValue());
        }
        return columnList;
    }

    private List<Mutation> prepareMutateRow(byte[] rowKey, byte[] columnFamily, Map<byte[], byte[]> map) {
        return prepareMutateRow(rowKey, columnFamily, map, Durability.USE_DEFAULT);
    }

    private List<Mutation> prepareMutateRow(byte[] rowKey, byte[] columnFamily, Map<byte[], byte[]> map,
                                            Durability durability) {
        ColumnList columnList = buildColumnList(columnFamily, map);
        return mockHBaseClient.constructMutationReq(rowKey, columnList, durability);
    }

    private void mutateRow(byte[] rowKey, byte[] columnFamily, Map<byte[], byte[]> map)
        throws Exception {
        mutateRow(rowKey, columnFamily, map, Durability.USE_DEFAULT);
    }

    private void mutateRow(byte[] rowKey, byte[] columnFamily, Map<byte[], byte[]> map,
                           Durability durability) throws Exception {
        mockHBaseClient.batchMutate(prepareMutateRow(rowKey, columnFamily, map, durability));
    }
}