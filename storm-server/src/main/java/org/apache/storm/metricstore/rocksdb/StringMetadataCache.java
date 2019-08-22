/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.metricstore.rocksdb;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.storm.metricstore.MetricException;
import org.apache.storm.utils.LruMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to create a use a cache that stores Metadata string information in memory.  It allows searching for a
 * Metadata string's unique id, or looking up the string by the unique id.  The StringMetadata is stored in an
 * LRU map.  When an entry is added to the cache, an older entry may be evicted, which then needs to be
 * immediately stored to the database to provide a consistent view of all the metadata strings.
 *
 * <p>All write operations adding metadata to RocksDB are done by a single thread (a RocksDbMetricsWriter),
 * but multiple threads can read values from the cache. To clarify which permissions are accessible by various
 * threads, the ReadOnlyStringMetadataCache and WritableStringMetadataCache are provided to be used.
 */

public class StringMetadataCache implements LruMap.CacheEvictionCallback<String, StringMetadata>,
                                            WritableStringMetadataCache, ReadOnlyStringMetadataCache {
    private static final Logger LOG = LoggerFactory.getLogger(StringMetadataCache.class);
    private static StringMetadataCache instance = null;
    private Map<String, StringMetadata> lruStringCache;
    private Map<Integer, String> hashToString = new ConcurrentHashMap<>();
    private RocksDbMetricsWriter dbWriter;

    /**
     * Constructor to create a cache.
     *
     * @param dbWriter   The rocks db writer instance the cache should use when evicting data
     * @param capacity  The cache size
     */
    private StringMetadataCache(RocksDbMetricsWriter dbWriter, int capacity) {
        lruStringCache = Collections.synchronizedMap(new LruMap<>(capacity, this));
        this.dbWriter = dbWriter;
    }

    /**
     * Initializes the cache instance.
     *
     * @param dbWriter   the RocksDB writer instance to handle writing evicted cache data
     * @param capacity   the number of StringMetadata instances to hold in memory
     * @throws MetricException   if creating multiple cache instances
     */
    static void init(RocksDbMetricsWriter dbWriter, int capacity) throws MetricException {
        if (instance == null) {
            instance = new StringMetadataCache(dbWriter, capacity);
        } else {
            throw new MetricException("StringMetadataCache already created");
        }
    }

    /**
     * Provides the WritableStringMetadataCache interface to the cache instance.
     *
     * @throws MetricException  if the cache instance was not created
     */
    static WritableStringMetadataCache getWritableStringMetadataCache() throws MetricException {
        if (instance != null) {
            return instance;
        } else {
            throw new MetricException("StringMetadataCache was not initialized");
        }
    }

    /**
     * Provides the ReadOnlyStringMetadataCache interface to the cache instance.
     *
     * @throws MetricException  if the cache instance was not created
     */
    static ReadOnlyStringMetadataCache getReadOnlyStringMetadataCache() throws MetricException {
        if (instance != null) {
            return instance;
        } else {
            throw new MetricException("StringMetadataCache was not initialized");
        }
    }

    static void cleanUp() {
        instance = null;
    }

    /**
     * Get the string metadata from the cache.
     *
     * @param s   The string to look for
     * @return the metadata associated with the string or null if not found
     */
    @Override
    public StringMetadata get(String s) {
        return lruStringCache.get(s);
    }

    /**
     * Add the string metadata to the cache.
     *
     * <p>NOTE: this can cause data to be evicted from the cache when full.  When this occurs, the evictionCallback() method
     * is called to store the metadata back into the RocksDB database.
     *
     * <p>This method is only exposed to the WritableStringMetadataCache interface.
     *
     * @param s   The string to add
     * @param stringMetadata  The string's metadata
     * @param newEntry   Indicates the metadata is being used for the first time and should be written to RocksDB immediately
     * @throws MetricException   when evicted data fails to save to the database or when the database is shutdown
     */
    @Override
    public void put(String s, StringMetadata stringMetadata, boolean newEntry) throws MetricException {
        if (dbWriter.isShutdown()) {
            // another thread could be writing out the metadata cache to the database.
            throw new MetricException("Shutting down");
        }
        try {
            if (newEntry) {
                writeMetadataToDisk(s, stringMetadata);
            }
            lruStringCache.put(s, stringMetadata);
            hashToString.put(stringMetadata.getStringId(), s);
        } catch (Exception e) { // catch any runtime exceptions caused by eviction
            throw new MetricException("Failed to save string in metadata cache", e);
        }
    }

    /**
     * Callback when data is about to be removed from the cache.  This method then
     * immediately writes the metadata to RocksDB.
     *
     * @param key   The evicted string
     * @param val  The evicted string's metadata
     * @throws RuntimeException   when evicted data fails to save to the database
     */
    @Override
    public void evictionCallback(String key, StringMetadata val) {
        writeMetadataToDisk(key, val);
    }

    private void writeMetadataToDisk(String key, StringMetadata val) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Writing {} to RocksDB", key);
        }
        // remove reverse lookup from map
        hashToString.remove(val.getStringId());

        // save the evicted key/value to the database immediately
        RocksDbValue rval = new RocksDbValue(val.getLastTimestamp(), key);

        for (KeyType type : val.getMetadataTypes()) { // save the metadata for all types of strings it matches
            RocksDbKey rkey = new RocksDbKey(type, val.getStringId());
            dbWriter.handleEvictedMetadata(rkey, rval);
        }
    }

    /**
     * Determines if a string Id is contained in the cache.
     *
     * @param stringId   The string Id to check
     * @return true if the Id is in the cache, false otherwise
     */
    @Override
    public boolean contains(Integer stringId) {
        return hashToString.containsKey(stringId);
    }

    /**
     * Returns the string matching the string Id if in the cache.
     *
     * @param stringId   The string Id to check
     * @return the associated string if the Id is in the cache, null otherwise
     */
    @Override
    public String getMetadataString(Integer stringId) {
        return hashToString.get(stringId);
    }

    /**
     * Get the map of the cache contents.  Provided to allow writing the data to RocksDB on shutdown.
     *
     * @return the string metadata map entrySet
     */
    @Override
    public Set<Map.Entry<String, StringMetadata>> entrySet() {
        return lruStringCache.entrySet();
    }

}

