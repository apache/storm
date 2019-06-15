/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.state;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
import org.apache.storm.shade.com.google.common.collect.Iterators;
import org.apache.storm.shade.com.google.common.primitives.UnsignedBytes;

/**
 * Base implementation of iterator over {@link KeyValueState} which encoded types of key and value are both binary type.
 */
public abstract class BaseBinaryStateIterator<K, V> extends BaseStateIterator<K, V, byte[], byte[]> {

    /**
     * Constructor.
     *
     * @param pendingPrepareIterator The iterator of pendingPrepare
     * @param pendingCommitIterator  The iterator of pendingCommit
     */
    public BaseBinaryStateIterator(Iterator<Map.Entry<byte[], byte[]>> pendingPrepareIterator,
                                   Iterator<Map.Entry<byte[], byte[]>> pendingCommitIterator) {
        super(Iterators.peekingIterator(pendingPrepareIterator), Iterators.peekingIterator(pendingCommitIterator),
              new TreeSet<>(UnsignedBytes.lexicographicalComparator()));
    }

    /**
     * Load some part of state KVs from storage and returns iterator of cached data from storage.
     *
     * @return Iterator of loaded state KVs
     */
    @Override
    protected abstract Iterator<Map.Entry<byte[], byte[]>> loadChunkFromStateStorage();

    /**
     * Check whether end of data is reached from storage state KVs.
     *
     * @return whether end of data is reached from storage state KVs
     */
    @Override
    protected abstract boolean isEndOfDataFromStorage();

    /**
     * Decode key to convert byte array to state key type.
     *
     * @param key byte array encoded key
     * @return Decoded value of key
     */
    @Override
    protected abstract K decodeKey(byte[] key);

    /**
     * Decode value to convert byte array to state value type.
     *
     * @param value byte array encoded value
     * @return Decoded value of value
     */
    @Override
    protected abstract V decodeValue(byte[] value);

    /**
     * Check whether the value is tombstone (deletion mark) value.
     *
     * @param value the value to check
     * @return true if the value is tombstone, false otherwise
     */
    @Override
    protected abstract boolean isTombstoneValue(byte[] value);

}
