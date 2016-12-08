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
    private final RedisEncoder<K, V> decoder;
    private final JedisCommandsInstanceContainer jedisContainer;
    private final ScanParams scanParams;
    private String cursor;
    private List<Map.Entry<String, String>> cachedResult;
    private int readPosition;

    public RedisKeyValueStateIterator(String namespace, JedisCommandsInstanceContainer jedisContainer, int chunkSize, RedisEncoder<K, V> encoder) {
        this(namespace, jedisContainer, chunkSize, encoder.getKeySerializer(), encoder.getValueSerializer());
    }

    public RedisKeyValueStateIterator(String namespace, JedisCommandsInstanceContainer jedisContainer, int chunkSize, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.namespace = namespace;
        this.jedisContainer = jedisContainer;
        this.decoder = new RedisEncoder<K, V>(keySerializer, valueSerializer);
        this.scanParams = new ScanParams().count(chunkSize);
        this.cursor = ScanParams.SCAN_POINTER_START;
    }

    @Override
    public boolean hasNext() {
        return !cursor.equals("0");
    }

    @Override
    public Map.Entry<K, V> next() {
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
        Map.Entry<String, String> redisKeyValue = cachedResult.get(readPosition);
        readPosition += 1;
        K key = decoder.decodeKey(redisKeyValue.getKey());
        V value = decoder.decodeValue(redisKeyValue.getValue());
        return new AbstractMap.SimpleEntry(key, value);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
