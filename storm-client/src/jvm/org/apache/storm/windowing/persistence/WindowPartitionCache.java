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

package org.apache.storm.windowing.persistence;

import java.util.concurrent.ConcurrentMap;

/**
 * A loading cache abstraction for caching {@link WindowState.WindowPartition}.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface WindowPartitionCache<K, V> {

    /**
     * Get value from the cache or load the value.
     *
     * @param key the key
     * @return the value
     */
    V get(K key);

    /**
     * Get value from the cache or load the value pinning it so that the entry will never get evicted.
     *
     * @param key the key
     * @return the value
     */
    V pinAndGet(K key);

    /**
     * Unpin an entry from the cache so that it can be a candidate for eviction.
     *
     * @param key the key
     * @return true if the entry was unpinned, false otherwise
     */
    boolean unpin(K key);

    /**
     * Return a {@link ConcurrentMap} view of the current entries in the cache.
     *
     * @return the map of key-values currently cached.
     */
    ConcurrentMap<K, V> asMap();

    /**
     * Invalidate an entry from the cache.
     *
     * @param key the key
     */
    void invalidate(K key);

    /**
     * The reason why an enrty got evicted from the cache.
     */
    enum RemovalCause {
        /**
         * The entry was forcefully invalidated from the cache.
         */
        EXPLICIT,
        /**
         * The entry was evicted from the cache due to overflow.
         */
        REPLACED
    }

    /**
     * A callback interface for handling removal of events from the cache.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    interface RemovalListener<K, V> {
        /**
         * The method that is invoked when an entry is removed from the cache.
         *
         * @param key          the key of the entry that was removed
         * @param val          the value of the entry that was removed
         * @param removalCause the {@link RemovalCause}
         */
        void onRemoval(K key, V val, RemovalCause removalCause);
    }

    /**
     * The interface for loading entires into the cache.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    interface CacheLoader<K, V> {
        V load(K key);
    }

    /**
     * Builder interface for {@link WindowPartitionCache}.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    interface Builder<K, V> {
        /**
         * The maximum cache size. After this limit, entries are evicted from the cache.
         *
         * @param size the size
         * @return the Builder
         */
        Builder<K, V> maximumSize(long size);

        /**
         * The {@link RemovalListener} to be invoked when entries are evicted.
         *
         * @param listener the listener
         * @return the builder
         */
        Builder<K, V> removalListener(RemovalListener<K, V> listener);

        /**
         * Build the cache.
         *
         * @param loader the {@link CacheLoader}
         * @return the {@link WindowPartitionCache}
         */
        WindowPartitionCache<K, V> build(CacheLoader<K, V> loader);
    }
}
