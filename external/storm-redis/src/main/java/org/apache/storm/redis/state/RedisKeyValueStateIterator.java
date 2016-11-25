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

import com.google.common.base.Optional;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.storm.redis.common.container.JedisCommandsInstanceContainer;
import org.apache.storm.redis.utils.RedisEncoder;
import org.apache.storm.state.DefaultStateSerializer;
import org.apache.storm.state.Serializer;

import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

/**
 * An iterator over {@link RedisKeyValueState}
 */
public class RedisKeyValueStateIterator<K, V> implements Iterator<Map.Entry<K, V>> {

    private final String namespace;
    private final Iterator<Map.Entry<String, String>> pendingPrepareIterator;
    private final Iterator<Map.Entry<String, String>> pendingCommitIterator;
    private final RedisEncoder<K, V> decoder;
    private final JedisCommandsInstanceContainer jedisContainer;
    private final ScanParams scanParams;
    private Iterator<Map.Entry<String, String>> pendingIterator;
    private String cursor;
    private List<Map.Entry<String, String>> cachedResult;
    private int readPosition;

    public RedisKeyValueStateIterator(String namespace, JedisCommandsInstanceContainer jedisContainer, Iterator<Map.Entry<String, String>> pendingPrepareIterator, Iterator<Map.Entry<String, String>> pendingCommitIterator, int chunkSize, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.namespace = namespace;
        this.pendingPrepareIterator = pendingPrepareIterator;
        this.pendingCommitIterator = pendingCommitIterator;
        this.jedisContainer = jedisContainer;
        this.decoder = new RedisEncoder<K, V>(keySerializer, valueSerializer);
        this.scanParams = new ScanParams().count(chunkSize);
        this.cursor = ScanParams.SCAN_POINTER_START;
    }

    @Override
    public boolean hasNext() {
        if (pendingPrepareIterator != null && pendingPrepareIterator.hasNext()) {
            pendingIterator = pendingPrepareIterator;
            return true;
        } else if (pendingCommitIterator != null && pendingCommitIterator.hasNext()) {
            pendingIterator = pendingCommitIterator;
            return true;
        } else {
            pendingIterator = null;
            return !cursor.equals("0");
        }
    }

    @Override
    public Map.Entry<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Map.Entry<String, String> redisKeyValue = null;
        if (pendingIterator != null) {
            redisKeyValue = pendingIterator.next();
        } else {
            if (cachedResult == null || readPosition >= cachedResult.size()) {
                JedisCommands commands = null;
                try {
                    commands = jedisContainer.getInstance();
                    ScanResult<Map.Entry<String, String>> scanResult = commands.hscan(namespace, cursor, scanParams);
                    cachedResult = scanResult.getResult();
                    cursor = scanResult.getStringCursor();
                    readPosition = 0;
                } finally {
                    jedisContainer.returnInstance(commands);
                }
            }
            redisKeyValue = cachedResult.get(readPosition);
            readPosition += 1;
        }
        K key = decoder.decodeKey(redisKeyValue.getKey());
        V value = decoder.decodeValue(redisKeyValue.getValue());
        return new AbstractMap.SimpleEntry(key, value);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
