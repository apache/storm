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

import java.util.Map;
import java.util.Set;
import org.apache.storm.metricstore.MetricException;

/**
 * The writable interface to a StringMetadataCache intended to be used by a single RocksDBMetricwWriter instance.
 * This class is not thread safe.
 */
public interface WritableStringMetadataCache extends ReadOnlyStringMetadataCache {

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
    void put(String s, StringMetadata stringMetadata, boolean newEntry) throws MetricException;

    /**
     * Get the map of the cache contents.  Provided to allow writing the data to RocksDB on shutdown.
     *
     * @return the string metadata map entrySet
     */
    Set<Map.Entry<String, StringMetadata>> entrySet();
}
