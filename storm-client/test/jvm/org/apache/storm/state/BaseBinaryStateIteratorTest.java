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

package org.apache.storm.state;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.storm.shade.com.google.common.primitives.UnsignedBytes;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link BaseBinaryStateIterator}.
 */
public class BaseBinaryStateIteratorTest {
    private DefaultStateEncoder<byte[], byte[]> encoder;

    @Before
    public void setUp() {
        Serializer<byte[]> keySerializer = new DefaultStateSerializer<>();
        Serializer<byte[]> valueSerializer = new DefaultStateSerializer<>();
        encoder = new DefaultStateEncoder<>(keySerializer, valueSerializer);
    }

    @Test
    public void testGetEntriesFromPendingPrepare() {
        NavigableMap<byte[], byte[]> pendingPrepare = getBinaryTreeMap();
        putEncodedKeyValueToMap(pendingPrepare, "key0".getBytes(), "value0".getBytes());
        putTombstoneToMap(pendingPrepare, "key1".getBytes());
        putEncodedKeyValueToMap(pendingPrepare, "key2".getBytes(), "value2".getBytes());

        NavigableMap<byte[], byte[]> pendingCommit = getBinaryTreeMap();

        MockBinaryStateIterator kvIterator = new MockBinaryStateIterator(
            pendingPrepare.entrySet().iterator(), pendingCommit.entrySet().iterator());

        assertNextEntry(kvIterator, "key0".getBytes(), "value0".getBytes());

        // key1 shouldn't in iterator

        assertNextEntry(kvIterator, "key2".getBytes(), "value2".getBytes());

        assertFalse(kvIterator.hasNext());
    }

    @Test
    public void testGetEntriesFromPendingCommit() {
        NavigableMap<byte[], byte[]> pendingPrepare = getBinaryTreeMap();

        NavigableMap<byte[], byte[]> pendingCommit = getBinaryTreeMap();
        putEncodedKeyValueToMap(pendingCommit, "key0".getBytes(), "value0".getBytes());
        putTombstoneToMap(pendingCommit, "key1".getBytes());
        putEncodedKeyValueToMap(pendingCommit, "key2".getBytes(), "value2".getBytes());

        MockBinaryStateIterator kvIterator = new MockBinaryStateIterator(
            pendingPrepare.entrySet().iterator(), pendingCommit.entrySet().iterator());

        assertNextEntry(kvIterator, "key0".getBytes(), "value0".getBytes());

        // key1 shouldn't in iterator

        assertNextEntry(kvIterator, "key2".getBytes(), "value2".getBytes());

        assertFalse(kvIterator.hasNext());
    }

    @Test
    public void testGetEntriesRemovingDuplicationKeys() {
        NavigableMap<byte[], byte[]> pendingPrepare = getBinaryTreeMap();
        putEncodedKeyValueToMap(pendingPrepare, "key0".getBytes(), "value0".getBytes());
        putTombstoneToMap(pendingPrepare, "key1".getBytes());

        NavigableMap<byte[], byte[]> pendingCommit = getBinaryTreeMap();
        putEncodedKeyValueToMap(pendingCommit, "key1".getBytes(), "value1".getBytes());
        putEncodedKeyValueToMap(pendingCommit, "key2".getBytes(), "value2".getBytes());

        MockBinaryStateIterator kvIterator = new MockBinaryStateIterator(
            pendingPrepare.entrySet().iterator(), pendingCommit.entrySet().iterator());

        // keys shouldn't appear twice

        assertNextEntry(kvIterator, "key0".getBytes(), "value0".getBytes());

        // key1 shouldn't be in iterator since it's marked as deleted

        assertNextEntry(kvIterator, "key2".getBytes(), "value2".getBytes());

        assertFalse(kvIterator.hasNext());
    }

    @Test
    public void testGetEntryNotAvailable() {
        NavigableMap<byte[], byte[]> pendingPrepare = getBinaryTreeMap();

        NavigableMap<byte[], byte[]> pendingCommit = getBinaryTreeMap();

        MockBinaryStateIterator kvIterator = new MockBinaryStateIterator(
            pendingPrepare.entrySet().iterator(), pendingCommit.entrySet().iterator());

        assertFalse(kvIterator.hasNext());
    }

    private void assertNextEntry(BaseBinaryStateIterator<byte[], byte[]> kvIterator, byte[] expectedKey,
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
}

class MockBinaryStateIterator extends BaseBinaryStateIterator<byte[], byte[]> {
    private DefaultStateEncoder<byte[], byte[]> encoder;

    /**
     * Constructor.
     *
     * @param pendingPrepareIterator The iterator of pendingPrepare
     * @param pendingCommitIterator  The iterator of pendingCommit
     */
    public MockBinaryStateIterator(Iterator<Map.Entry<byte[], byte[]>> pendingPrepareIterator,
                                   Iterator<Map.Entry<byte[], byte[]>> pendingCommitIterator) {
        super(pendingPrepareIterator, pendingCommitIterator);
        Serializer<byte[]> keySerializer = new DefaultStateSerializer<>();
        Serializer<byte[]> valueSerializer = new DefaultStateSerializer<>();
        encoder = new DefaultStateEncoder<>(keySerializer, valueSerializer);
    }

    @Override
    protected Iterator<Map.Entry<byte[], byte[]>> loadChunkFromStateStorage() {
        // no data
        return null;
    }

    @Override
    protected boolean isEndOfDataFromStorage() {
        return true;
    }

    @Override
    protected byte[] decodeKey(byte[] key) {
        return encoder.decodeKey(key);
    }

    @Override
    protected byte[] decodeValue(byte[] value) {
        return encoder.decodeValue(value);
    }

    @Override
    protected boolean isTombstoneValue(byte[] value) {
        return Arrays.equals(value, encoder.getTombstoneValue());
    }
}
