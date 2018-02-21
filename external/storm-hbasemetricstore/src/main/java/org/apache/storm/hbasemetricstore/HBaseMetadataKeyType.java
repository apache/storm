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

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Enumeration for the different types of metadata strings used as a key in an HBase metric store.
 */
public enum HBaseMetadataKeyType {
    TOPOLOGY("TOPOLOGY", 100),
    STREAM_ID("STREAM_ID", 100),
    HOST_ID("HOST_ID", 100),
    COMPONENT_ID("COMPONENT_ID", 100),
    METRIC_NAME("METRIC_NAME", 100),
    EXECUTOR_ID("EXECUTOR_ID", 100);

    private final byte[] qualifier;
    private final byte[] refCounter;
    private final int cacheCapacity;

    HBaseMetadataKeyType(String name, int cacheCapacity) {
        this.qualifier = Bytes.toBytes(name);
        this.refCounter = Bytes.toBytes("REFCOUNTER_" + name);
        this.cacheCapacity = cacheCapacity;
    }

    /**
     * Get the HBase column qualifier for the type.
     */
    byte[] getQualifier() {
        return this.qualifier;
    }

    /**
     * Get the HBase reference counter key for the metadata type.  This is used to track/create unique string Ids for the type.
     */
    byte[] getRefCounter() {
        return this.refCounter;
    }

    /**
     * Get the number of strings to cache in memory for the type.
     */
    int getCacheCapacity() {
        return this.cacheCapacity;
    }
}
