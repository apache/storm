/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.redis.state;

import org.apache.storm.redis.common.container.JedisCommandsInstanceContainer;
import org.apache.storm.redis.utils.RedisEncoder;
import org.apache.storm.state.DefaultStateSerializer;
import org.apache.storm.state.Serializer;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for RedisKeyValueStateIterator.
 */
public class RedisKeyValueStateIteratorTest {

    private String namespace;
    private JedisCommandsInstanceContainer mockContainer;
    private Jedis mockJedis;
    private int chunkSize = 1000;
    private Serializer<String> keySerializer = new DefaultStateSerializer<>();
    private Serializer<String> valueSerializer = new DefaultStateSerializer<>();
    private RedisEncoder<String, String> encoder;

    @Before
    public void setUp() {
        namespace = "namespace";
        mockContainer = mock(JedisCommandsInstanceContainer.class);
        mockJedis = mock(Jedis.class);
        when(mockContainer.getInstance()).thenReturn(mockJedis);

        encoder = new RedisEncoder<>(keySerializer, valueSerializer);
    }

    @Test
    public void testGetEntriesFromPendingPrepare() {
        Map<String, String> pendingPrepare = new TreeMap<>();
        putEncodedKeyValueToMap(pendingPrepare, "key0", "value0");
        putTombstoneToMap(pendingPrepare, "key1");
        putEncodedKeyValueToMap(pendingPrepare, "key2", "value2");

        Map<String, String> pendingCommit = new TreeMap<>();

        ScanResult<Map.Entry<String, String>> scanResult = new ScanResult<Map.Entry<String, String>>(
                ScanParams.SCAN_POINTER_START, new ArrayList<Map.Entry<String, String>>());
        when(mockJedis.hscan(eq(namespace), anyString(), any(ScanParams.class))).thenReturn(scanResult);

        RedisKeyValueStateIterator<String, String> kvIterator =
                new RedisKeyValueStateIterator<>(namespace, mockContainer, pendingPrepare.entrySet().iterator(),
                        pendingCommit.entrySet().iterator(), chunkSize, keySerializer, valueSerializer);

        assertNextEntry(kvIterator, "key0", "value0");

        // key1 shouldn't in iterator

        assertNextEntry(kvIterator, "key2", "value2");

        assertFalse(kvIterator.hasNext());
    }

    @Test
    public void testGetEntriesFromPendingCommit() {
        Map<String, String> pendingPrepare = new TreeMap<>();

        Map<String, String> pendingCommit = new TreeMap<>();
        putEncodedKeyValueToMap(pendingCommit, "key0", "value0");
        putTombstoneToMap(pendingCommit, "key1");
        putEncodedKeyValueToMap(pendingCommit, "key2", "value2");

        ScanResult<Map.Entry<String, String>> scanResult = new ScanResult<Map.Entry<String, String>>(
                ScanParams.SCAN_POINTER_START, new ArrayList<Map.Entry<String, String>>());
        when(mockJedis.hscan(eq(namespace), anyString(), any(ScanParams.class))).thenReturn(scanResult);

        RedisKeyValueStateIterator<String, String> kvIterator =
                new RedisKeyValueStateIterator<>(namespace, mockContainer, pendingPrepare.entrySet().iterator(),
                        pendingCommit.entrySet().iterator(), chunkSize, keySerializer, valueSerializer);

        assertNextEntry(kvIterator, "key0", "value0");

        // key1 shouldn't in iterator

        assertNextEntry(kvIterator, "key2", "value2");

        assertFalse(kvIterator.hasNext());
    }

    @Test
    public void testGetEntriesFromFirstPartOfChunkInRedis() {
        // pendingPrepare has no entries
        Map<String, String> pendingPrepare = new TreeMap<>();

        // pendingCommit has no entries
        Map<String, String> pendingCommit = new TreeMap<>();

        // Redis has a chunk but no more
        Map<String, String> chunkMap = new TreeMap<>();
        putEncodedKeyValueToMap(chunkMap, "key0", "value0");
        putEncodedKeyValueToMap(chunkMap, "key2", "value2");

        ScanResult<Map.Entry<String, String>> scanResultFirst = new ScanResult<>(
                "12345", new ArrayList<>(chunkMap.entrySet()));
        ScanResult<Map.Entry<String, String>> scanResultSecond = new ScanResult<>(
                ScanParams.SCAN_POINTER_START, new ArrayList<Map.Entry<String, String>>());
        when(mockJedis.hscan(eq(namespace), anyString(), any(ScanParams.class)))
                .thenReturn(scanResultFirst, scanResultSecond);

        RedisKeyValueStateIterator<String, String> kvIterator =
                new RedisKeyValueStateIterator<>(namespace, mockContainer, pendingPrepare.entrySet().iterator(),
                        pendingCommit.entrySet().iterator(), chunkSize, keySerializer, valueSerializer);

        assertNextEntry(kvIterator, "key0", "value0");

        // key1 shouldn't in iterator

        assertNextEntry(kvIterator, "key2", "value2");

        assertFalse(kvIterator.hasNext());
    }

    @Test
    public void testGetEntriesFromThirdPartOfChunkInRedis() {
        // pendingPrepare has no entries
        Map<String, String> pendingPrepare = new TreeMap<>();

        // pendingCommit has no entries
        Map<String, String> pendingCommit = new TreeMap<>();

        // Redis has three chunks which last chunk only has entries
        Map<String, String> chunkMap = new TreeMap<>();
        putEncodedKeyValueToMap(chunkMap, "key0", "value0");
        putEncodedKeyValueToMap(chunkMap, "key2", "value2");

        ScanResult<Map.Entry<String, String>> scanResultFirst = new ScanResult<>(
                "12345", new ArrayList<Map.Entry<String, String>>());
        ScanResult<Map.Entry<String, String>> scanResultSecond = new ScanResult<>(
                "23456", new ArrayList<Map.Entry<String, String>>());
        ScanResult<Map.Entry<String, String>> scanResultThird = new ScanResult<>(
                ScanParams.SCAN_POINTER_START, new ArrayList<>(chunkMap.entrySet()));
        when(mockJedis.hscan(eq(namespace), anyString(), any(ScanParams.class)))
                .thenReturn(scanResultFirst, scanResultSecond, scanResultThird);

        RedisKeyValueStateIterator<String, String> kvIterator =
                new RedisKeyValueStateIterator<>(namespace, mockContainer, pendingPrepare.entrySet().iterator(),
                        pendingCommit.entrySet().iterator(), chunkSize, keySerializer, valueSerializer);

        assertNextEntry(kvIterator, "key0", "value0");

        // key1 shouldn't in iterator

        assertNextEntry(kvIterator, "key2", "value2");

        assertFalse(kvIterator.hasNext());
    }

    @Test
    public void testGetEntriesRemovingDuplicationKeys() {
        Map<String, String> pendingPrepare = new TreeMap<>();
        putEncodedKeyValueToMap(pendingPrepare, "key0", "value0");
        putTombstoneToMap(pendingPrepare, "key1");

        Map<String, String> pendingCommit = new TreeMap<>();
        putEncodedKeyValueToMap(pendingCommit, "key1", "value1");
        putEncodedKeyValueToMap(pendingCommit, "key2", "value2");

        Map<String, String> chunkMap = new TreeMap<>();
        putEncodedKeyValueToMap(chunkMap, "key2", "value2");
        putEncodedKeyValueToMap(chunkMap, "key3", "value3");

        Map<String, String> chunkMap2 = new TreeMap<>();
        putEncodedKeyValueToMap(chunkMap2, "key3", "value3");
        putEncodedKeyValueToMap(chunkMap2, "key4", "value4");

        ScanResult<Map.Entry<String, String>> scanResultFirst = new ScanResult<>(
                "12345", new ArrayList<>(chunkMap.entrySet()));
        ScanResult<Map.Entry<String, String>> scanResultSecond = new ScanResult<>(
                "23456", new ArrayList<>(chunkMap2.entrySet()));
        ScanResult<Map.Entry<String, String>> scanResultThird = new ScanResult<>(
                ScanParams.SCAN_POINTER_START, new ArrayList<Map.Entry<String, String>>());
        when(mockJedis.hscan(eq(namespace), anyString(), any(ScanParams.class)))
                .thenReturn(scanResultFirst, scanResultSecond, scanResultThird);

        RedisKeyValueStateIterator<String, String> kvIterator =
                new RedisKeyValueStateIterator<>(namespace, mockContainer, pendingPrepare.entrySet().iterator(),
                        pendingCommit.entrySet().iterator(), chunkSize, keySerializer, valueSerializer);

        // keys shouldn't appear twice

        assertNextEntry(kvIterator, "key0", "value0");

        // key1 shouldn't be in iterator since it's marked as deleted

        assertNextEntry(kvIterator, "key2", "value2");
        assertNextEntry(kvIterator, "key3", "value3");
        assertNextEntry(kvIterator, "key4", "value4");

        assertFalse(kvIterator.hasNext());
    }

    @Test
    public void testGetEntryNotAvailable() {
        Map<String, String> pendingPrepare = new TreeMap<>();

        Map<String, String> pendingCommit = new TreeMap<>();

        ScanResult<Map.Entry<String, String>> scanResult = new ScanResult<>(
                ScanParams.SCAN_POINTER_START, new ArrayList<Map.Entry<String, String>>());
        when(mockJedis.hscan(eq(namespace), anyString(), any(ScanParams.class)))
                .thenReturn(scanResult);

        RedisKeyValueStateIterator<String, String> kvIterator =
                new RedisKeyValueStateIterator<>(namespace, mockContainer, pendingPrepare.entrySet().iterator(),
                        pendingCommit.entrySet().iterator(), chunkSize, keySerializer, valueSerializer);

        assertFalse(kvIterator.hasNext());
    }

    private void assertNextEntry(RedisKeyValueStateIterator<String, String> kvIterator, String expectedKey,
                                 String expectedValue) {
        assertTrue(kvIterator.hasNext());
        Map.Entry<String, String> entry = kvIterator.next();
        assertEquals(expectedKey, entry.getKey());
        assertEquals(expectedValue, entry.getValue());
    }

    private void putEncodedKeyValueToMap(Map<String, String> map, String key, String value) {
        map.put(encoder.encodeKey(key), encoder.encodeValue(value));
    }

    private void putTombstoneToMap(Map<String, String> map, String key) {
        map.put(encoder.encodeKey(key), RedisEncoder.TOMBSTONE);
    }
}