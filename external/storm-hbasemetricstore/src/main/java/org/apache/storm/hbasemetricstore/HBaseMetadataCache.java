/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.storm.hbasemetricstore;

import com.codahale.metrics.Meter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.annotation.GuardedBy;
import org.apache.http.annotation.ThreadSafe;
import org.apache.storm.metricstore.MetricException;
import org.apache.storm.utils.LruMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache for metadata strings stored in HBase.  An instance is designed to hold strings for a specific metadata type.
 * <p></p>
 * Access to the cache is protected by a lock.  When data is not found in the cache, it is fetched from HBase (and
 * possibly created if missing, depending on the API).
 *
 */
@ThreadSafe
class HBaseMetadataCache {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseMetadataCache.class);
    private static final int NUM_TRIES = 3;
    private final Object lock = new Object();
    private final HBaseMetadataKeyType type;
    private final Meter failureMeter;

    @GuardedBy("lock")
    private final LruMap<String, Integer> intLookupMap;

    @GuardedBy("lock")
    private final LruMap<Integer, String> stringLookupMap;

    HBaseMetadataCache(HBaseMetadataKeyType type, Meter meter) throws MetricException {
        this.type = type;
        this.intLookupMap = new LruMap<>(type.getCacheCapacity());
        this.stringLookupMap = new LruMap<>(type.getCacheCapacity());
        this.failureMeter = meter;
    }

    /**
     * Returns the unique integer id for the metadata string.  If the string does not exist in the cache or HBase,
     * it will be created and added to both.
     *
     * @param s    metadata string to lookup/create
     * @return  the unique id for the string
     * @throws MetricException  on error
     */
    int getOrCreateMetadataStringId(String s) throws MetricException {
        return retriableGetMetadataStringId(s);
    }

    private int retriableGetMetadataStringId(String s) throws MetricException {
        for (int i = 0; i < NUM_TRIES; i++) {
            try {
                return getMetadataStringId(s);
            } catch (HBaseMetricException e) {
                // this handles the case where two hosts are updating the metadata at the same time.
            }
        }
        this.failureMeter.mark();
        String message = "Failed to get metadata string " + this.type + "." + s;
        LOG.error(message);
        throw new MetricException(message);
    }

    private int getMetadataStringId(String s) throws MetricException, HBaseMetricException {
        synchronized (lock) {
            int id = this.lookupMetadataStringId(s);
            if (id != HBaseStore.INVALID_METADATA_STRING_ID) {
                return id;
            }

            // doesn't exist, add it to HBase

            // first get the reference counter for the metadata type and increment it.  This will be the
            // unique id.
            long columnValue = HBaseTableOperation.incrementMetadataReferenceCounter(this.type);
            int intRefCount = (int)columnValue;
            byte[] uniqueIdBytes = Bytes.toBytes(intRefCount);

            // save the string key to HBase with the id.  If another host wrote the value already, this should
            // throw an HBaseMetricException, and we will relook for the string.
            HBaseTableOperation.checkAndPutMetadataIfMissing(s, uniqueIdBytes, this.type);

            // Now add the reverse lookup to hbase.  Since we've added the string, we really would like to
            // make sure this succeeds.
            retriableAddStringReverseLookup(uniqueIdBytes, s);

            // add it to the cache
            intLookupMap.put(s, intRefCount);
            stringLookupMap.put(intRefCount, s);
            return intRefCount;
        }
    }

    private void retriableAddStringReverseLookup(byte[] uniqueIdBytes, String s) throws MetricException {
        for (int i = 0; i < NUM_TRIES; i++) {
            try {
                HBaseTableOperation.putMetadata(uniqueIdBytes, Bytes.toBytes(s), this.type);
                return;
            } catch (MetricException e) {
                // retry
            }
        }
        this.failureMeter.mark();
        String message = "Failed to add reverse lookup for " + this.type + "." + s;
        LOG.error(message);
        throw new MetricException(message);
    }

    /**
     * Returns the unique integer id for the metadata string.  If the string does not exist in the cache, it will check
     * in HBase.
     *
     * @param s    metadata string to lookup
     * @return  the unique id for the string, HBaseStore.INVALID_METADATA_STRING_ID if it does not exist
     * @throws MetricException  on error
     */
    int lookupMetadataStringId(String s) throws MetricException {
        synchronized (lock) {
            // see if it is in the cache
            Integer id = intLookupMap.get(s);
            if (id != null) {
                return id;
            }

            // see if HBase has it
            id = HBaseTableOperation.getHBaseMetadataStringId(s, this.type);
            if (id != null) {
                // add it to the cache
                intLookupMap.put(s, id);
                stringLookupMap.put(id, s);
                return id;
            }

            return HBaseStore.INVALID_METADATA_STRING_ID;
        }
    }

    /**
     * Returns the metadata string for the given unique Id.  If the Id does not exist in the cache, it will check
     * in HBase.
     *
     * @param id    metadata string unique Id to lookup
     * @return  the metadata string, null if it does not exist
     * @throws MetricException  on error
     */
    String lookupMetadataStringId(int id) throws MetricException {
        synchronized (lock) {
            String s = this.stringLookupMap.get(id);
            if (s != null) {
                return s;
            }

            // check HBase
            s  = HBaseTableOperation.getMetadataString(id, this.type);
            if (s == null) {
                return null;
            }
            this.stringLookupMap.put(id, s);
            return s;
        }
    }
}
