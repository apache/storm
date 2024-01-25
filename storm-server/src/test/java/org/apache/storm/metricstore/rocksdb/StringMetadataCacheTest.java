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

package org.apache.storm.metricstore.rocksdb;

import org.apache.storm.metricstore.MetricException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDB;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class StringMetadataCacheTest {

    @BeforeEach
    public void setUp() {
        // remove any previously created cache instance
        StringMetadataCache.cleanUp();
        RocksDB.loadLibrary();
    }

    private class TestDbWriter extends RocksDbMetricsWriter {
        boolean evictCalled = false;

        TestDbWriter() {
            super(null, null, null);
        }

        @Override
        void handleEvictedMetadata(RocksDbKey key, RocksDbValue val) {
            evictCalled = true;
        }
    }

    @AfterEach
    public void tearDown() {
        StringMetadataCache.cleanUp();
    }

    @Test
    public void validateEviction() throws MetricException {
        TestDbWriter writer = new TestDbWriter();
        StringMetadataCache.init(writer, 2);
        WritableStringMetadataCache wCache = StringMetadataCache.getWritableStringMetadataCache();
        ReadOnlyStringMetadataCache rCache = StringMetadataCache.getReadOnlyStringMetadataCache();

        String s1 = "string1";
        Integer s1Id = 1;
        long s1Timestamp = 1L;
        StringMetadata metadata1 = new StringMetadata(KeyType.STREAM_ID_STRING, s1Id, s1Timestamp);
        wCache.put(s1, metadata1, false);
        assertEquals(metadata1, rCache.get(s1));
        assertTrue(rCache.contains(s1Id));
        assertEquals(s1, rCache.getMetadataString(s1Id));

        String s2 = "string2";
        Integer s2Id = 2;
        long s2Timestamp = 2L;
        StringMetadata metadata2 = new StringMetadata(KeyType.EXEC_ID_STRING, s2Id, s2Timestamp);
        wCache.put(s2, metadata2, false);
        assertEquals(metadata2, rCache.get(s2));
        assertTrue(rCache.contains(s2Id));
        assertEquals(s2, rCache.getMetadataString(s2Id));

        assertEquals(false, writer.evictCalled);

        // read s1 last....  This should cause s2 to be evicted on next put
        rCache.get(s1);

        String s3 = "string3";
        Integer s3Id = 3;
        long s3Timestamp = 3L;
        StringMetadata metadata3 = new StringMetadata(KeyType.TOPOLOGY_STRING, s3Id, s3Timestamp);
        wCache.put(s3, metadata3, false);

        assertEquals(true, writer.evictCalled);
        assertEquals(metadata3, rCache.get(s3));
        assertTrue(rCache.contains(s3Id));
        assertEquals(s3, rCache.getMetadataString(s3Id));

        // since s2 read last, it should be evicted, s1 and s3 should exist
        assertEquals(null, rCache.get(s2));
        assertFalse(rCache.contains(s2Id));
        assertEquals(metadata1, rCache.get(s1));
        assertTrue(rCache.contains(s1Id));
        assertEquals(s1, rCache.getMetadataString(s1Id));

        StringMetadataCache.cleanUp();
    }

    @Test
    public void validateMultipleKeyTypes() throws MetricException {
        TestDbWriter writer = new TestDbWriter();
        StringMetadataCache.init(writer, 2);
        WritableStringMetadataCache wCache = StringMetadataCache.getWritableStringMetadataCache();

        StringMetadata metadata = new StringMetadata(KeyType.STREAM_ID_STRING, 1, 1L);
        wCache.put("default", metadata, false);

        metadata = wCache.get("default");
        metadata.update(3L, KeyType.COMPONENT_STRING);

        metadata = wCache.get("default");
        metadata.update(2L, KeyType.STREAM_ID_STRING);

        metadata = wCache.get("default");
        assertEquals(2, metadata.getMetadataTypes().size());
        assertTrue(metadata.getMetadataTypes().contains(KeyType.STREAM_ID_STRING));
        assertTrue(metadata.getMetadataTypes().contains(KeyType.COMPONENT_STRING));
        assertEquals(3L, metadata.getLastTimestamp());

        StringMetadataCache.cleanUp();
    }
}
