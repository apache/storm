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

package org.apache.storm.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.storm.generated.GlobalStreamId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


public class ZstdBridgeThriftSerializationDelegateTest {
    private SerializationDelegate testDelegate;

    @BeforeEach
    public void setUp() throws Exception {
        // The bridge we are testing
        testDelegate = new ZstdBridgeThriftSerializationDelegate();
        testDelegate.prepare(null);
    }

    /**
     * Verifies that the bridge can deserialize data specifically
     * produced by the ZstdThriftSerializationDelegate.
     */
    @Test
    public void testDeserialize_readingFromZstd() {
        GlobalStreamId id = new GlobalStreamId("component-1", "stream-A");

        // Serialize using the pure Zstd delegate
        byte[] serialized = new ZstdThriftSerializationDelegate().serialize(id);

        GlobalStreamId deserialized = testDelegate.deserialize(serialized, GlobalStreamId.class);

        assertEquals(id.get_componentId(), deserialized.get_componentId());
        assertEquals(id.get_streamId(), deserialized.get_streamId());
    }

    /**
     * Verifies that the bridge writes Zstd and can successfully
     * read its own output.
     */
    @Test
    public void testDeserialize_readingFromZstdBridge() {
        GlobalStreamId id = new GlobalStreamId("bridge-component", "bridge-stream");

        // The bridge's serialize method should be using Zstd
        byte[] serialized = testDelegate.serialize(id);

        GlobalStreamId deserialized = testDelegate.deserialize(serialized, GlobalStreamId.class);

        assertEquals(id.get_componentId(), deserialized.get_componentId());
        assertEquals(id.get_streamId(), deserialized.get_streamId());
    }

    /**
     * Critical for backward compatibility. Tests that if a byte array
     * does NOT have the Zstd magic header, it falls back to raw Thrift.
     */
    @Test
    public void testDeserialize_readingFromDefault() {
        GlobalStreamId id = new GlobalStreamId("legacy-A", "legacy-B");

        // Serialize using the standard uncompressed Thrift delegate
        byte[] serialized = new ThriftSerializationDelegate().serialize(id);

        GlobalStreamId deserialized = testDelegate.deserialize(serialized, GlobalStreamId.class);

        assertEquals(id.get_componentId(), deserialized.get_componentId());
        assertEquals(id.get_streamId(), deserialized.get_streamId());
    }

    @Test
    public void testDeserialize_ComplexNestedObject() {

        StringBuilder largeData = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            largeData.append("StormData-").append(i).append("-");
        }

        GlobalStreamId complexId = new GlobalStreamId(
            largeData.toString(),
            "stream-" + System.currentTimeMillis()
        );

        byte[] serialized = testDelegate.serialize(complexId);

        // Zstd magic: 0x28, 0xB5, 0x2F, 0xFD
        assertEquals((byte) 0x28, serialized[0], "Should have Zstd magic byte 0");
        assertEquals((byte) 0xB5, serialized[1], "Should have Zstd magic byte 1");

        GlobalStreamId deserialized = testDelegate.deserialize(serialized, GlobalStreamId.class);

        assertEquals(complexId.get_componentId(), deserialized.get_componentId());
        assertEquals(complexId.get_streamId(), deserialized.get_streamId());
        assertEquals(complexId, deserialized, "Objects should be deep-equal");
    }

    @Test
    public void testDeserialize_EmptyAndNullFields() {
        // Testing edge cases for Thrift: empty strings and nulls
        GlobalStreamId edgeCaseId = new GlobalStreamId("", "");

        byte[] serialized = testDelegate.serialize(edgeCaseId);
        GlobalStreamId deserialized = testDelegate.deserialize(serialized, GlobalStreamId.class);

        assertEquals("", deserialized.get_componentId());
        assertEquals("", deserialized.get_streamId());
    }

    @Test
    public void testStress_MultipleThreads() throws InterruptedException {
        // Since we used ThreadLocal, we must ensure multi-threaded access works
        int threadCount = 10;
        Thread[] threads = new Thread[threadCount];

        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < 50; j++) {
                    GlobalStreamId id = new GlobalStreamId("comp-" + j, "stream");
                    byte[] bytes = testDelegate.serialize(id);
                    GlobalStreamId out = testDelegate.deserialize(bytes, GlobalStreamId.class);
                    assertEquals(id.get_componentId(), out.get_componentId());
                }
            });
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();
    }
}
