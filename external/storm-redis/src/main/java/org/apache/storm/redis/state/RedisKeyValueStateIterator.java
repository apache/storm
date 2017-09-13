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

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.storm.redis.common.container.JedisCommandsInstanceContainer;
import org.apache.storm.redis.utils.RedisEncoder;
import org.apache.storm.state.Serializer;

import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

/**
 * An iterator over {@link RedisKeyValueState}.
 */
public class RedisKeyValueStateIterator<K, V> implements Iterator<Map.Entry<K, V>> {

    private final String namespace;
    private final PeekingIterator<Map.Entry<String, String>> pendingPrepareIterator;
    private final PeekingIterator<Map.Entry<String, String>> pendingCommitIterator;
    private final RedisEncoder<K, V> decoder;
    private final JedisCommandsInstanceContainer jedisContainer;
    private final ScanParams scanParams;
    private final Set<String> providedKeys;

    private PeekingIterator<Map.Entry<String, String>> cachedResultIterator;
    private String cursor;
    private boolean firstLoad = true;
    private PeekingIterator<Map.Entry<String, String>> pendingIterator;

    /**
     * Constructor.
     *
     * @param namespace The namespace of State
     * @param jedisContainer The instance of JedisContainter
     * @param pendingPrepareIterator The iterator of pendingPrepare
     * @param pendingCommitIterator The iterator of pendingCommit
     * @param chunkSize The size of chunk to get entries from Redis
     * @param keySerializer The serializer of key
     * @param valueSerializer The serializer of value
     */
    public RedisKeyValueStateIterator(String namespace, JedisCommandsInstanceContainer jedisContainer,
                                      Iterator<Map.Entry<String, String>> pendingPrepareIterator,
                                      Iterator<Map.Entry<String, String>> pendingCommitIterator,
                                      int chunkSize, Serializer<K> keySerializer,
                                      Serializer<V> valueSerializer) {
        this.namespace = namespace;
        this.pendingPrepareIterator = Iterators.peekingIterator(pendingPrepareIterator);
        this.pendingCommitIterator = Iterators.peekingIterator(pendingCommitIterator);
        this.jedisContainer = jedisContainer;
        this.decoder = new RedisEncoder<K, V>(keySerializer, valueSerializer);
        this.scanParams = new ScanParams().count(chunkSize);
        this.cursor = ScanParams.SCAN_POINTER_START;
        this.providedKeys = new HashSet<>();
    }

    @Override
    public boolean hasNext() {
        if (seekToAvailableEntry(pendingPrepareIterator)) {
            pendingIterator = pendingPrepareIterator;
            return true;
        }

        if (seekToAvailableEntry(pendingCommitIterator)) {
            pendingIterator = pendingCommitIterator;
            return true;
        }

        if (firstLoad) {
            // load the first part of entries
            loadChunkFromRedis();
            firstLoad = false;
        }

        while (true) {
            if (seekToAvailableEntry(cachedResultIterator)) {
                pendingIterator = cachedResultIterator;
                return true;
            }

            if (cursor.equals(ScanParams.SCAN_POINTER_START)) {
                break;
            }

            loadChunkFromRedis();
        }

        pendingIterator = null;
        return false;
    }

    @Override
    public Map.Entry<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Map.Entry<String, String> redisKeyValue = pendingIterator.next();
        K key = decoder.decodeKey(redisKeyValue.getKey());
        V value = decoder.decodeValue(redisKeyValue.getValue());

        providedKeys.add(redisKeyValue.getKey());
        return new AbstractMap.SimpleEntry(key, value);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    private boolean seekToAvailableEntry(PeekingIterator<Map.Entry<String, String>> iterator) {
        if (iterator != null) {
            while (iterator.hasNext()) {
                Map.Entry<String, String> entry = iterator.peek();
                if (!providedKeys.contains(entry.getKey())) {
                    if (entry.getValue().equals(RedisEncoder.TOMBSTONE)) {
                        providedKeys.add(entry.getKey());
                    } else {
                        return true;
                    }
                }

                iterator.next();
            }
        }

        return false;
    }

    private void loadChunkFromRedis() {
        JedisCommands commands = null;
        try {
            commands = jedisContainer.getInstance();
            ScanResult<Map.Entry<String, String>> scanResult = commands.hscan(namespace, cursor, scanParams);
            List<Map.Entry<String, String>> result = scanResult.getResult();
            if (result != null) {
                cachedResultIterator = Iterators.peekingIterator(result.iterator());
            }
            cursor = scanResult.getStringCursor();
        } finally {
            jedisContainer.returnInstance(commands);
        }
    }

}
