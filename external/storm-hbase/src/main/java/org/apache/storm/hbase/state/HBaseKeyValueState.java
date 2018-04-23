/*
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

package org.apache.storm.hbase.state;

import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.storm.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.apache.storm.hbase.common.ColumnList;
import org.apache.storm.hbase.common.HBaseClient;
import org.apache.storm.state.DefaultStateEncoder;
import org.apache.storm.state.DefaultStateSerializer;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.state.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Hbase based implementation that persists the state in HBase.
 */
public class HBaseKeyValueState<K, V> implements KeyValueState<K, V> {
    public static final int ITERATOR_CHUNK_SIZE = 1000;
    public static final NavigableMap<byte[], byte[]> EMPTY_PENDING_COMMIT_MAP = Maps.unmodifiableNavigableMap(
        new TreeMap<byte[], byte[]>(UnsignedBytes.lexicographicalComparator()));
    private static final Logger LOG = LoggerFactory.getLogger(HBaseKeyValueState.class);
    public static byte[] STATE_QUALIFIER = "s".getBytes();
    private static byte[] COMMIT_TXID_KEY = "commit".getBytes();
    private static byte[] PREPARE_TXID_KEY = "prepare".getBytes();

    private final byte[] keyNamespace;
    private final byte[] prepareNamespace;
    private final byte[] txidNamespace;
    private final String namespace;
    private final byte[] columnFamily;
    private final DefaultStateEncoder<K, V> encoder;
    private final HBaseClient hbaseClient;

    private ConcurrentNavigableMap<byte[], byte[]> pendingPrepare;
    private NavigableMap<byte[], byte[]> pendingCommit;

    // the key and value of txIds are guaranteed to be converted to UTF-8 encoded String
    private NavigableMap<byte[], byte[]> txIds;

    /**
     * Constructor.
     *
     * @param hbaseClient  HBaseClient instance
     * @param columnFamily column family to store State
     * @param namespace    namespace
     */
    public HBaseKeyValueState(HBaseClient hbaseClient, String columnFamily, String namespace) {
        this(hbaseClient, columnFamily, namespace, new DefaultStateSerializer<K>(),
             new DefaultStateSerializer<V>());
    }

    /**
     * Constructor.
     *
     * @param hbaseClient     HBaseClient instance
     * @param columnFamily    column family to store State
     * @param namespace       namespace
     * @param keySerializer   key serializer
     * @param valueSerializer value serializer
     */
    public HBaseKeyValueState(HBaseClient hbaseClient, String columnFamily, String namespace,
                              Serializer<K> keySerializer, Serializer<V> valueSerializer) {

        this.hbaseClient = hbaseClient;
        this.columnFamily = columnFamily.getBytes();
        this.namespace = namespace;
        this.keyNamespace = (namespace + "$key:").getBytes();
        this.prepareNamespace = (namespace + "$prepare").getBytes();
        this.txidNamespace = (namespace + "$txid").getBytes();
        this.encoder = new DefaultStateEncoder<K, V>(keySerializer, valueSerializer);
        this.pendingPrepare = createPendingPrepareMap();
        initTxids();
        initPendingCommit();
    }

    private void initTxids() {
        HBaseProjectionCriteria criteria = new HBaseProjectionCriteria();
        criteria.addColumnFamily(columnFamily);
        Get get = hbaseClient.constructGetRequests(txidNamespace, criteria);
        try {
            Result[] results = hbaseClient.batchGet(Collections.singletonList(get));
            Result result = results[0];
            if (!result.isEmpty()) {
                NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(columnFamily);
                txIds = new TreeMap<>(familyMap);
            } else {
                txIds = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
            }

            LOG.debug("initTxids, txIds {}", txIds);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void initPendingCommit() {
        HBaseProjectionCriteria criteria = new HBaseProjectionCriteria();
        criteria.addColumnFamily(columnFamily);
        Get get = hbaseClient.constructGetRequests(prepareNamespace, criteria);
        try {
            Result[] results = hbaseClient.batchGet(Collections.singletonList(get));
            Result result = results[0];
            if (!result.isEmpty()) {
                LOG.debug("Loading previously prepared commit from {}", prepareNamespace);
                NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(columnFamily);
                pendingCommit = Maps.unmodifiableNavigableMap(familyMap);
            } else {
                LOG.debug("No previously prepared commits.");
                pendingCommit = EMPTY_PENDING_COMMIT_MAP;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(K key, V value) {
        LOG.debug("put key '{}', value '{}'", key, value);
        byte[] columnKey = encoder.encodeKey(key);
        byte[] columnValue = encoder.encodeValue(value);
        pendingPrepare.put(columnKey, columnValue);
    }

    @Override
    public V get(K key) {
        LOG.debug("get key '{}'", key);
        byte[] columnKey = encoder.encodeKey(key);
        byte[] columnValue = null;

        if (pendingPrepare.containsKey(columnKey)) {
            columnValue = pendingPrepare.get(columnKey);
        } else if (pendingCommit.containsKey(columnKey)) {
            columnValue = pendingCommit.get(columnKey);
        } else {
            HBaseProjectionCriteria criteria = new HBaseProjectionCriteria();
            HBaseProjectionCriteria.ColumnMetaData column = new HBaseProjectionCriteria.ColumnMetaData(columnFamily,
                                                                                                       STATE_QUALIFIER);
            criteria.addColumn(column);
            Get get = hbaseClient.constructGetRequests(getRowKeyForStateKey(columnKey), criteria);
            try {
                Result[] results = hbaseClient.batchGet(Collections.singletonList(get));
                Result result = results[0];
                columnValue = result.getValue(column.getColumnFamily(), column.getQualifier());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        V value = null;
        if (columnValue != null) {
            value = encoder.decodeValue(columnValue);
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
        byte[] columnKey = encoder.encodeKey(key);
        V curr = get(key);
        pendingPrepare.put(columnKey, encoder.getTombstoneValue());
        return curr;
    }

    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
        return new HBaseKeyValueStateIterator<>(namespace, columnFamily, hbaseClient, pendingPrepare.entrySet().iterator(),
                                                pendingCommit.entrySet().iterator(), ITERATOR_CHUNK_SIZE, encoder.getKeySerializer(),
                                                encoder.getValueSerializer());
    }

    @Override
    public void prepareCommit(long txid) {
        LOG.debug("prepareCommit txid {}", txid);
        validatePrepareTxid(txid);

        try {
            ConcurrentNavigableMap<byte[], byte[]> currentPending = pendingPrepare;
            pendingPrepare = createPendingPrepareMap();

            Result result = getColumnFamily(prepareNamespace, columnFamily);
            if (!result.isEmpty()) {
                LOG.debug("Prepared txn already exists, will merge", txid);
                for (Map.Entry<byte[], byte[]> e : pendingCommit.entrySet()) {
                    if (!currentPending.containsKey(e.getKey())) {
                        currentPending.put(e.getKey(), e.getValue());
                    }
                }
            } else {
                LOG.debug("Nothing to save for prepareCommit, txid {}.", txid);
            }

            if (!currentPending.isEmpty()) {
                mutateRow(prepareNamespace, columnFamily, currentPending);
            } else {
                LOG.debug("Nothing to save for prepareCommit, txid {}.", txid);
            }

            txIds.put(PREPARE_TXID_KEY, String.valueOf(txid).getBytes());
            mutateRow(txidNamespace, columnFamily, txIds);
            pendingCommit = Maps.unmodifiableNavigableMap(currentPending);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void commit(long txid) {
        LOG.debug("commit txid {}", txid);
        validateCommitTxid(txid);
        try {
            if (!pendingCommit.isEmpty()) {
                applyPendingStateToHBase(pendingCommit);
            } else {
                LOG.debug("Nothing to save for commit, txid {}.", txid);
            }
            txIds.put(COMMIT_TXID_KEY, String.valueOf(txid).getBytes());
            mutateRow(txidNamespace, columnFamily, txIds);
            deleteRow(prepareNamespace);
            pendingCommit = EMPTY_PENDING_COMMIT_MAP;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void commit() {
        if (!pendingPrepare.isEmpty()) {
            try {
                applyPendingStateToHBase(pendingPrepare);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            LOG.debug("Nothing to save for commit");
        }
        pendingPrepare = createPendingPrepareMap();
    }

    @Override
    public void rollback() {
        LOG.debug("rollback");
        try {
            if (existsRow(prepareNamespace)) {
                deleteRow(prepareNamespace);
            } else {
                LOG.debug("Nothing to rollback, prepared data is empty");
            }
            Long lastCommittedId = lastCommittedTxid();
            if (lastCommittedId != null) {
                txIds.put(PREPARE_TXID_KEY, String.valueOf(lastCommittedId).getBytes());
            } else {
                txIds.remove(PREPARE_TXID_KEY);
            }
            if (!txIds.isEmpty()) {
                LOG.debug("put txidNamespace {}, txIds {}", txidNamespace, txIds);
                mutateRow(txidNamespace, columnFamily, txIds);
            }
            pendingCommit = EMPTY_PENDING_COMMIT_MAP;
            pendingPrepare = createPendingPrepareMap();
        } catch (Exception e) {
            throw new RuntimeException(e);
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

    private Long lastId(byte[] key) {
        Long lastId = null;
        byte[] txId = txIds.get(key);
        if (txId != null) {
            lastId = Long.valueOf(new String(txId));
        }
        return lastId;
    }

    private byte[] getRowKeyForStateKey(byte[] columnKey) {
        byte[] rowKey = new byte[keyNamespace.length + columnKey.length];
        System.arraycopy(keyNamespace, 0, rowKey, 0, keyNamespace.length);
        System.arraycopy(columnKey, 0, rowKey, keyNamespace.length, columnKey.length);
        return rowKey;
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

        hbaseClient.batchMutate(mutations);
    }

    private Result getColumnFamily(byte[] rowKey, byte[] columnFamily) throws Exception {
        HBaseProjectionCriteria criteria = new HBaseProjectionCriteria();
        criteria.addColumnFamily(columnFamily);
        Get get = hbaseClient.constructGetRequests(rowKey, criteria);
        Result[] results = hbaseClient.batchGet(Collections.singletonList(get));
        return results[0];
    }

    private List<Mutation> prepareMutateRow(byte[] rowKey, byte[] columnFamily, Map<byte[], byte[]> map) {
        return prepareMutateRow(rowKey, columnFamily, map, Durability.USE_DEFAULT);
    }

    private List<Mutation> prepareMutateRow(byte[] rowKey, byte[] columnFamily, Map<byte[], byte[]> map,
                                            Durability durability) {
        ColumnList columnList = buildColumnList(columnFamily, map);
        return hbaseClient.constructMutationReq(rowKey, columnList, durability);
    }

    private void mutateRow(byte[] rowKey, byte[] columnFamily, Map<byte[], byte[]> map)
        throws Exception {
        mutateRow(rowKey, columnFamily, map, Durability.USE_DEFAULT);
    }

    private void mutateRow(byte[] rowKey, byte[] columnFamily, Map<byte[], byte[]> map,
                           Durability durability) throws Exception {
        hbaseClient.batchMutate(prepareMutateRow(rowKey, columnFamily, map, durability));
    }

    private boolean existsRow(byte[] rowKey) throws Exception {
        Get get = new Get(rowKey);
        return hbaseClient.exists(get);
    }

    private void deleteRow(byte[] rowKey) throws Exception {
        Delete delete = new Delete(rowKey);
        hbaseClient.batchMutate(Collections.<Mutation>singletonList(delete));
    }

    private ColumnList buildColumnList(byte[] columnFamily, Map<byte[], byte[]> map) {
        ColumnList columnList = new ColumnList();
        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            columnList.addColumn(columnFamily, entry.getKey(), entry.getValue());
        }
        return columnList;
    }

    /**
     * Intended to extract this to separate method since only pendingPrepare uses ConcurrentNavigableMap.
     */
    private ConcurrentNavigableMap<byte[], byte[]> createPendingPrepareMap() {
        return new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());
    }
}
