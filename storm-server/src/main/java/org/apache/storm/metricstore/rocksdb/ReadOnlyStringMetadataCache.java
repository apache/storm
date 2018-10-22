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

/**
 * The read-only interface to a StringMetadataCache allowed to be used by any thread.
 */
public interface ReadOnlyStringMetadataCache {

    /**
     * Get the string metadata from the cache.
     *
     * @param s   The string to look for
     * @return the metadata associated with the string or null if not found
     */
    StringMetadata get(String s);

    /**
     * Returns the string matching the string Id if in the cache.
     *
     * @param stringId   The string Id to check
     * @return the associated string if the Id is in the cache, null otherwise
     */
    String getMetadataString(Integer stringId);

    /**
     * Determines if a string Id is contained in the cache.
     *
     * @param stringId   The string Id to check
     * @return true if the Id is in the cache, false otherwise
     */
    boolean contains(Integer stringId);
}
