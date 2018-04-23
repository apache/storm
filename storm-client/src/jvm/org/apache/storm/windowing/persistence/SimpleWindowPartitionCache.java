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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple implementation that evicts the largest un-pinned entry from the cache. This works well for caching window partitions since the
 * access pattern is mostly sequential scans.
 */
public class SimpleWindowPartitionCache<K, V> implements WindowPartitionCache<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleWindowPartitionCache.class);

    private final ConcurrentSkipListMap<K, V> map = new ConcurrentSkipListMap<>();
    private final Map<K, Long> pinned = new HashMap<>();
    private final long maximumSize;
    private final RemovalListener<K, V> removalListener;
    private final CacheLoader<K, V> cacheLoader;
    private final ReentrantLock lock = new ReentrantLock(true);
    private int size;

    private SimpleWindowPartitionCache(long maximumSize, RemovalListener<K, V> removalListener, CacheLoader<K, V> cacheLoader) {
        if (maximumSize <= 0) {
            throw new IllegalArgumentException("maximumSize must be greater than 0");
        }
        Objects.requireNonNull(cacheLoader);
        this.maximumSize = maximumSize;
        this.removalListener = removalListener;
        this.cacheLoader = cacheLoader;
    }

    public static <K, V> SimpleWindowPartitionCacheBuilder<K, V> newBuilder() {
        return new SimpleWindowPartitionCacheBuilder<>();
    }

    @Override
    public V get(K key) {
        return getOrLoad(key, false);
    }

    @Override
    public V pinAndGet(K key) {
        return getOrLoad(key, true);
    }

    @Override
    public boolean unpin(K key) {
        LOG.debug("unpin '{}'", key);
        boolean res = false;
        try {
            lock.lock();
            Long val = pinned.computeIfPresent(key, (k, v) -> v - 1);
            if (val != null) {
                if (val <= 0) {
                    pinned.remove(key);
                }
                res = true;
            }
        } finally {
            lock.unlock();
        }
        LOG.debug("pinned '{}'", pinned);
        return res;
    }

    @Override
    public ConcurrentMap<K, V> asMap() {
        return map;
    }

    @Override
    public void invalidate(K key) {
        try {
            lock.lock();
            if (isPinned(key)) {
                LOG.debug("Entry '{}' is pinned, skipping invalidation", key);
            } else {
                LOG.debug("Invalidating entry '{}'", key);
                V val = map.remove(key);
                if (val != null) {
                    --size;
                    pinned.remove(key);
                    if (removalListener != null) {
                        removalListener.onRemoval(key, val, RemovalCause.EXPLICIT);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    // Get or load from the cache optionally pinning the entry
    // so that it wont get evicted from the cache
    private V getOrLoad(K key, boolean shouldPin) {
        V val;
        if (shouldPin) {
            try {
                lock.lock();
                val = load(key);
                pin(key);
            } finally {
                lock.unlock();
            }
        } else {
            val = map.get(key);
            if (val == null) {
                try {
                    lock.lock();
                    val = load(key);
                } finally {
                    lock.unlock();
                }
            }
        }

        return val;
    }

    private V load(K key) {
        V val = map.get(key);
        if (val == null) {
            val = cacheLoader.load(key);
            if (val == null) {
                throw new NullPointerException("Null value for key " + key);
            }
            ensureCapacity();
            map.put(key, val);
            ++size;
        }
        return val;
    }

    private void ensureCapacity() {
        if (size >= maximumSize) {
            Iterator<Map.Entry<K, V>> it = map.descendingMap().entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<K, V> next = it.next();
                if (!isPinned(next.getKey())) {
                    it.remove();
                    if (removalListener != null) {
                        removalListener.onRemoval(next.getKey(), next.getValue(), RemovalCause.REPLACED);
                    }
                    --size;
                    break;
                }
            }
        }
    }

    private void pin(K key) {
        LOG.debug("pin '{}'", key);
        pinned.compute(key, (k, v) -> v == null ? 1L : v + 1);
        LOG.debug("pinned '{}'", pinned);
    }

    private boolean isPinned(K key) {
        return pinned.getOrDefault(key, 0L) > 0;
    }

    public static class SimpleWindowPartitionCacheBuilder<K, V> implements WindowPartitionCache.Builder<K, V> {
        private long maximumSize;
        private RemovalListener<K, V> removalListener;

        public SimpleWindowPartitionCacheBuilder<K, V> maximumSize(long size) {
            maximumSize = size;
            return this;
        }

        public SimpleWindowPartitionCacheBuilder<K, V> removalListener(RemovalListener<K, V> listener) {
            removalListener = listener;
            return this;
        }

        public SimpleWindowPartitionCache<K, V> build(CacheLoader<K, V> loader) {
            return new SimpleWindowPartitionCache<>(maximumSize, removalListener, loader);
        }
    }
}
