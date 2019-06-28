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

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.storm.shade.com.google.common.collect.Iterators;
import org.apache.storm.shade.com.google.common.collect.PeekingIterator;

/**
 * Base implementation of iterator over {@link KeyValueState}. Encoded/Decoded types of key and value are all generic.
 */
public abstract class BaseStateIterator<K, V, KENCODEDT, VENCODEDT> implements Iterator<Map.Entry<K, V>> {

    private final PeekingIterator<Map.Entry<KENCODEDT, VENCODEDT>> pendingPrepareIterator;
    private final PeekingIterator<Map.Entry<KENCODEDT, VENCODEDT>> pendingCommitIterator;
    private final Set<KENCODEDT> providedKeys;

    private boolean firstLoad = true;
    private PeekingIterator<Map.Entry<KENCODEDT, VENCODEDT>> pendingIterator;
    private PeekingIterator<Map.Entry<KENCODEDT, VENCODEDT>> cachedResultIterator;

    /**
     * Constructor.
     *
     * @param pendingPrepareIterator The iterator of pendingPrepare
     * @param pendingCommitIterator  The iterator of pendingCommit
     * @param initialProvidedKeys    The initial value of provided keys
     */
    public BaseStateIterator(Iterator<Map.Entry<KENCODEDT, VENCODEDT>> pendingPrepareIterator,
                             Iterator<Map.Entry<KENCODEDT, VENCODEDT>> pendingCommitIterator,
                             Set<KENCODEDT> initialProvidedKeys) {
        this.pendingPrepareIterator = Iterators.peekingIterator(pendingPrepareIterator);
        this.pendingCommitIterator = Iterators.peekingIterator(pendingCommitIterator);
        this.providedKeys = initialProvidedKeys;
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
            fillCachedResultIterator();
            firstLoad = false;
        }

        while (true) {
            if (seekToAvailableEntry(cachedResultIterator)) {
                pendingIterator = cachedResultIterator;
                return true;
            }

            if (isEndOfDataFromStorage()) {
                break;
            }

            fillCachedResultIterator();
        }

        pendingIterator = null;
        return false;
    }

    private void fillCachedResultIterator() {
        Iterator<Map.Entry<KENCODEDT, VENCODEDT>> iterator = loadChunkFromStateStorage();
        if (iterator != null) {
            cachedResultIterator = Iterators.peekingIterator(iterator);
        } else {
            cachedResultIterator = null;
        }
    }

    @Override
    public Map.Entry<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        Map.Entry<KENCODEDT, VENCODEDT> keyValue = pendingIterator.next();

        K key = decodeKey(keyValue.getKey());
        V value = decodeValue(keyValue.getValue());

        providedKeys.add(keyValue.getKey());
        return new AbstractMap.SimpleEntry(key, value);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    /**
     * Load some part of state KVs from storage and returns iterator of cached data from storage.
     *
     * @return Iterator of loaded state KVs
     */
    protected abstract Iterator<Map.Entry<KENCODEDT, VENCODEDT>> loadChunkFromStateStorage();

    /**
     * Check whether end of data is reached from storage state KVs.
     *
     * @return whether end of data is reached from storage state KVs
     */
    protected abstract boolean isEndOfDataFromStorage();

    /**
     * Decode key to convert encoded type of key to state key type.
     *
     * @param key raw type of encoded key
     * @return Decoded value of key
     */
    protected abstract K decodeKey(KENCODEDT key);

    /**
     * Decode value to convert encoded type of value to state value type.
     *
     * @param value raw type of encoded value
     * @return Decoded value of value
     */
    protected abstract V decodeValue(VENCODEDT value);

    /**
     * Check whether the value is tombstone (deletion mark) value.
     *
     * @param value the value to check
     * @return true if the value is tombstone, false otherwise
     */
    protected abstract boolean isTombstoneValue(VENCODEDT value);

    private boolean seekToAvailableEntry(PeekingIterator<Map.Entry<KENCODEDT, VENCODEDT>> iterator) {
        if (iterator != null) {
            while (iterator.hasNext()) {
                Map.Entry<KENCODEDT, VENCODEDT> entry = iterator.peek();
                if (!providedKeys.contains(entry.getKey())) {
                    if (isTombstoneValue(entry.getValue())) {
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

}
