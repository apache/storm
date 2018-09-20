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

package org.apache.storm.redis.state;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.storm.redis.common.commands.RedisCommands;
import org.apache.storm.redis.common.container.RedisCommandsInstanceContainer;
import org.apache.storm.state.BaseBinaryStateIterator;
import org.apache.storm.state.DefaultStateEncoder;
import org.apache.storm.state.Serializer;
import org.apache.storm.state.StateEncoder;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

/**
 * An iterator over {@link RedisKeyValueState}.
 */
public class RedisKeyValueStateIterator<K, V> extends BaseBinaryStateIterator<K, V> {

    private final byte[] namespace;
    private final StateEncoder<K, V, byte[], byte[]> encoder;
    private final RedisCommandsInstanceContainer container;
    private final ScanParams scanParams;

    private Iterator<Map.Entry<byte[], byte[]>> cachedResultIterator;
    private byte[] cursor;

    /**
     * Constructor.
     *
     * @param namespace The namespace of State
     * @param container The instance of RedisCommandsInstanceContainer
     * @param pendingPrepareIterator The iterator of pendingPrepare
     * @param pendingCommitIterator The iterator of pendingCommit
     * @param chunkSize The size of chunk to get entries from Redis
     * @param keySerializer The serializer of key
     * @param valueSerializer The serializer of value
     */
    public RedisKeyValueStateIterator(byte[] namespace, RedisCommandsInstanceContainer container,
                                      Iterator<Map.Entry<byte[], byte[]>> pendingPrepareIterator,
                                      Iterator<Map.Entry<byte[], byte[]>> pendingCommitIterator,
                                      int chunkSize, Serializer<K> keySerializer,
                                      Serializer<V> valueSerializer) {
        super(pendingPrepareIterator, pendingCommitIterator);
        this.namespace = namespace;
        this.container = container;
        this.encoder = new DefaultStateEncoder<K, V>(keySerializer, valueSerializer);
        this.scanParams = new ScanParams().count(chunkSize);
        this.cursor = ScanParams.SCAN_POINTER_START_BINARY;
    }

    @Override
    protected Iterator<Map.Entry<byte[], byte[]>> loadChunkFromStateStorage() {
        loadChunkFromRedis();
        return cachedResultIterator;
    }

    @Override
    protected boolean isEndOfDataFromStorage() {
        return (cachedResultIterator == null || !cachedResultIterator.hasNext())
               && Arrays.equals(cursor, ScanParams.SCAN_POINTER_START_BINARY);
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

    private void loadChunkFromRedis() {
        RedisCommands commands = null;
        try {
            commands = container.getInstance();
            ScanResult<Map.Entry<byte[], byte[]>> scanResult = commands.hscan(namespace, cursor, scanParams);
            List<Map.Entry<byte[], byte[]>> result = scanResult.getResult();
            if (result != null) {
                cachedResultIterator = result.iterator();
            }
            cursor = scanResult.getCursorAsBytes();
        } finally {
            container.returnInstance(commands);
        }
    }

}
