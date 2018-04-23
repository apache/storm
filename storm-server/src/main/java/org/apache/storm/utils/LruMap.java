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

package org.apache.storm.utils;

import java.util.LinkedHashMap;
import java.util.Map;

public class LruMap<A, B> extends LinkedHashMap<A, B> {
    private int maxSize;
    private CacheEvictionCallback evCb = null;

    public LruMap(int maxSize) {
        super(maxSize + 1, 1.0f, true);
        this.maxSize = maxSize;
    }

    /**
     * Creates an LRU map that will call back before data is removed from the map.
     *
     * @param maxSize   max capacity for the map
     * @param evictionCallback   callback to be called before removing data
     */
    public LruMap(int maxSize, CacheEvictionCallback evictionCallback) {
        this(maxSize);
        this.evCb = evictionCallback;
    }

    @Override
    protected boolean removeEldestEntry(final Map.Entry<A, B> eldest) {
        boolean evict = size() > this.maxSize;
        if (evict && this.evCb != null) {
            this.evCb.evictionCallback(eldest.getKey(), eldest.getValue());
        }
        return evict;
    }

    public interface CacheEvictionCallback<K, V> {
        void evictionCallback(K key, V val);
    }
}
