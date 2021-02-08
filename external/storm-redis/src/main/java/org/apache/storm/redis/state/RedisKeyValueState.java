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

import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.storm.redis.common.commands.RedisCommands;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.storm.redis.common.container.RedisCommandsInstanceContainer;
import org.apache.storm.state.DefaultStateEncoder;
import org.apache.storm.state.DefaultStateSerializer;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.state.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.util.SafeEncoder;

/**
 * A redis based implementation that persists the state in Redis.
 */
public class RedisKeyValueState<K, V> implements KeyValueState<K, V> {
    public static final int ITERATOR_CHUNK_SIZE = 100;
    public static final NavigableMap<byte[], byte[]> EMPTY_PENDING_COMMIT_MAP = Maps.unmodifiableNavigableMap(
        new TreeMap<byte[], byte[]>(UnsignedBytes.lexicographicalComparator()));
    private static final Logger LOG = LoggerFactory.getLogger(RedisKeyValueState.class);
    private static final String COMMIT_TXID_KEY = "commit";
    private static final String PREPARE_TXID_KEY = "prepare";
    private final byte[] namespace;
    private final byte[] prepareNamespace;

    private final String txidNamespace;
    private final DefaultStateEncoder<K, V> encoder;

    private final RedisCommandsInstanceContainer container;
    private ConcurrentNavigableMap<byte[], byte[]> pendingPrepare;
    private NavigableMap<byte[], byte[]> pendingCommit;

    // the key and value of txIds are guaranteed to be converted to UTF-8 encoded String
    private Map<String, String> txIds;

    public RedisKeyValueState(String namespace) {
        this(namespace, new JedisPoolConfig.Builder().build());
    }

    public RedisKeyValueState(String namespace, JedisPoolConfig poolConfig) {
        this(namespace, poolConfig, new DefaultStateSerializer<K>(), new DefaultStateSerializer<V>());
    }

    public RedisKeyValueState(String namespace, JedisPoolConfig poolConfig, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(namespace, RedisCommandsContainerBuilder.build(poolConfig), keySerializer, valueSerializer);
    }

    public RedisKeyValueState(String namespace, JedisClusterConfig jedisClusterConfig, Serializer<K> keySerializer,
                              Serializer<V> valueSerializer) {
        this(namespace, RedisCommandsContainerBuilder.build(jedisClusterConfig), keySerializer, valueSerializer);
    }

    public RedisKeyValueState(String namespace, RedisCommandsInstanceContainer container,
                              Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.namespace = SafeEncoder.encode(namespace);
        this.prepareNamespace = SafeEncoder.encode(namespace + "$prepare");
        this.txidNamespace = namespace + "$txid";
        this.encoder = new DefaultStateEncoder<K, V>(keySerializer, valueSerializer);
        this.container = container;
        this.pendingPrepare = createPendingPrepareMap();
        initTxids();
        initPendingCommit();
    }

    private void initTxids() {
        RedisCommands commands = null;
        try {
            commands = container.getInstance();
            if (commands.exists(txidNamespace)) {
                txIds = commands.hgetAll(txidNamespace);
            } else {
                txIds = new HashMap<>();
            }
            LOG.debug("initTxids, txIds {}", txIds);
        } finally {
            container.returnInstance(commands);
        }
    }

    private void initPendingCommit() {
        RedisCommands commands = null;
        try {
            commands = container.getInstance();
            if (commands.exists(prepareNamespace)) {
                LOG.debug("Loading previously prepared commit from {}", prepareNamespace);
                NavigableMap<byte[], byte[]> pendingCommitMap = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
                pendingCommitMap.putAll(commands.hgetAll(prepareNamespace));
                pendingCommit = Maps.unmodifiableNavigableMap(pendingCommitMap);
            } else {
                LOG.debug("No previously prepared commits.");
                pendingCommit = EMPTY_PENDING_COMMIT_MAP;
            }
        } finally {
            container.returnInstance(commands);
        }
    }

    @Override
    public void put(K key, V value) {
        LOG.debug("put key '{}', value '{}'", key, value);
        byte[] redisKey = encoder.encodeKey(key);
        byte[] redisValue = encoder.encodeValue(value);
        pendingPrepare.put(redisKey, redisValue);
    }

    @Override
    public V get(K key) {
        LOG.debug("get key '{}'", key);
        byte[] redisKey = encoder.encodeKey(key);
        byte[] redisValue = null;

        if (pendingPrepare.containsKey(redisKey)) {
            redisValue = pendingPrepare.get(redisKey);
        } else if (pendingCommit.containsKey(redisKey)) {
            redisValue = pendingCommit.get(redisKey);
        } else {
            RedisCommands commands = null;
            try {
                commands = container.getInstance();
                redisValue = commands.hget(namespace, redisKey);
            } finally {
                container.returnInstance(commands);
            }
        }
        V value = null;
        if (redisValue != null) {
            value = encoder.decodeValue(redisValue);
        }
        LOG.debug("Value for key '{}' is '{}'", key, value);
        return value;
    }

    @Override
    public V get(K key, V defaultValue) {
        V val = get(key);
        return val != null ? val : defaultValue;
    }

    @Override
    public V delete(K key) {
        LOG.debug("delete key '{}'", key);
        byte[] redisKey = encoder.encodeKey(key);
        V curr = get(key);
        pendingPrepare.put(redisKey, encoder.getTombstoneValue());
        return curr;
    }

    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
        return new RedisKeyValueStateIterator<K, V>(namespace, container, pendingPrepare.entrySet().iterator(),
                                                    pendingCommit.entrySet().iterator(),
                                                    ITERATOR_CHUNK_SIZE, encoder.getKeySerializer(), encoder.getValueSerializer());
    }

    @Override
    public void prepareCommit(long txid) {
        LOG.debug("prepareCommit txid {}", txid);
        validatePrepareTxid(txid);
        RedisCommands commands = null;
        try {
            ConcurrentNavigableMap<byte[], byte[]> currentPending = pendingPrepare;
            pendingPrepare = createPendingPrepareMap();
            commands = container.getInstance();
            if (commands.exists(prepareNamespace)) {
                LOG.debug("Prepared txn already exists, will merge", txid);
                for (Map.Entry<byte[], byte[]> e : pendingCommit.entrySet()) {
                    if (!currentPending.containsKey(e.getKey())) {
                        currentPending.put(e.getKey(), e.getValue());
                    }
                }
            }
            if (!currentPending.isEmpty()) {
                commands.hmset(prepareNamespace, currentPending);
            } else {
                LOG.debug("Nothing to save for prepareCommit, txid {}.", txid);
            }
            txIds.put(PREPARE_TXID_KEY, String.valueOf(txid));

            commands.hmset(txidNamespace, txIds);
            pendingCommit = Maps.unmodifiableNavigableMap(currentPending);
        } finally {
            container.returnInstance(commands);
        }
    }

    @Override
    public void commit(long txid) {
        LOG.debug("commit txid {}", txid);
        validateCommitTxid(txid);
        RedisCommands commands = null;
        try {
            commands = container.getInstance();
            if (!pendingCommit.isEmpty()) {
                List<byte[]> keysToDelete = new ArrayList<>();
                Map<byte[], byte[]> keysToAdd = new HashMap<>();
                for (Map.Entry<byte[], byte[]> entry : pendingCommit.entrySet()) {
                    byte[] key = entry.getKey();
                    byte[] value = entry.getValue();
                    if (Arrays.equals(encoder.getTombstoneValue(), value)) {
                        keysToDelete.add(key);
                    } else {
                        keysToAdd.put(key, value);
                    }
                }
                if (!keysToAdd.isEmpty()) {
                    commands.hmset(namespace, keysToAdd);
                }
                if (!keysToDelete.isEmpty()) {
                    commands.hdel(namespace, keysToDelete.toArray(new byte[0][]));
                }
            } else {
                LOG.debug("Nothing to save for commit, txid {}.", txid);
            }
            txIds.put(COMMIT_TXID_KEY, String.valueOf(txid));
            commands.hmset(txidNamespace, txIds);
            commands.del(prepareNamespace);
            pendingCommit = EMPTY_PENDING_COMMIT_MAP;
        } finally {
            container.returnInstance(commands);
        }
    }

    @Override
    public void commit() {
        RedisCommands commands = null;
        try {
            commands = container.getInstance();
            if (!pendingPrepare.isEmpty()) {
                commands.hmset(namespace, pendingPrepare);
            } else {
                LOG.debug("Nothing to save for commit");
            }
            pendingPrepare = createPendingPrepareMap();
        } finally {
            container.returnInstance(commands);
        }
    }

    @Override
    public void rollback() {
        LOG.debug("rollback");
        RedisCommands commands = null;
        try {
            commands = container.getInstance();
            if (commands.exists(prepareNamespace)) {
                commands.del(prepareNamespace);
            } else {
                LOG.debug("Nothing to rollback, prepared data is empty");
            }
            Long lastCommittedId = lastCommittedTxid();
            if (lastCommittedId != null) {
                txIds.put(PREPARE_TXID_KEY, String.valueOf(lastCommittedId));
            } else {
                txIds.remove(PREPARE_TXID_KEY);
            }
            if (!txIds.isEmpty()) {
                LOG.debug("hmset txidNamespace {}, txIds {}", txidNamespace, txIds);
                commands.hmset(txidNamespace, txIds);
            }
            pendingCommit = EMPTY_PENDING_COMMIT_MAP;
            pendingPrepare = createPendingPrepareMap();
        } finally {
            container.returnInstance(commands);
        }
    }

    /*
     * Same txid can be prepared again, but the next txid cannot be prepared
     * when previous one is not committed yet.
     */
    private void validatePrepareTxid(long txid) {
        Long committedTxid = lastCommittedTxid();
        if (committedTxid != null) {
            if (txid <= committedTxid) {
                throw new RuntimeException("Invalid txid '" + txid + "' for prepare. Txid '" + committedTxid
                        + "' is already committed");
            }
        }
    }

    /*
     * Same txid can be committed again but the
     * txid to be committed must be the last prepared one.
     */
    private void validateCommitTxid(long txid) {
        Long committedTxid = lastCommittedTxid();
        if (committedTxid != null) {
            if (txid < committedTxid) {
                throw new RuntimeException("Invalid txid '" + txid + "' txid '" + committedTxid + "' is already committed");
            }
        }
        Long preparedTxid = lastPreparedTxid();
        if (preparedTxid != null) {
            if (txid != preparedTxid) {
                throw new RuntimeException("Invalid txid '" + txid + "' not same as prepared txid '" + preparedTxid + "'");
            }
        }
    }

    private Long lastCommittedTxid() {
        return lastId(COMMIT_TXID_KEY);
    }

    private Long lastPreparedTxid() {
        return lastId(PREPARE_TXID_KEY);
    }

    private Long lastId(String key) {
        Long lastId = null;
        String txId = txIds.get(key);
        if (txId != null) {
            lastId = Long.valueOf(txId);
        }
        return lastId;
    }

    private ConcurrentNavigableMap<byte[], byte[]> createPendingPrepareMap() {
        return new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());
    }
}
