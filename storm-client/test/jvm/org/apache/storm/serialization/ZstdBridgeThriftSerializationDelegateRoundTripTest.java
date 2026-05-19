/*
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

import java.util.Collections;
import org.apache.storm.generated.GlobalStreamId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ZstdBridgeThriftSerializationDelegateRoundTripTest {
    SerializationDelegate bridge;

    @BeforeEach
    public void setUp() {
        bridge = new ZstdBridgeThriftSerializationDelegate();
        bridge.prepare(Collections.emptyMap());
    }

    @Test
    public void testDeserialize_readingFromGzip() {
        GlobalStreamId id = new GlobalStreamId("first", "second");
        byte[] serialized = new GzipThriftSerializationDelegate().serialize(id);

        GlobalStreamId out = bridge.deserialize(serialized, GlobalStreamId.class);

        assertEquals(id.get_componentId(), out.get_componentId());
        assertEquals(id.get_streamId(), out.get_streamId());
    }

    @Test
    public void testDeserialize_readingFromRawThrift() {
        GlobalStreamId id = new GlobalStreamId("A", "B");
        byte[] serialized = new ThriftSerializationDelegate().serialize(id);

        GlobalStreamId out = bridge.deserialize(serialized, GlobalStreamId.class);

        assertEquals(id.get_componentId(), out.get_componentId());
        assertEquals(id.get_streamId(), out.get_streamId());
    }

    @Test
    public void testRoundTrip_throughBridge() {
        GlobalStreamId id = new GlobalStreamId("x", "y");
        byte[] serialized = bridge.serialize(id);

        GlobalStreamId out = bridge.deserialize(serialized, GlobalStreamId.class);

        assertEquals(id.get_componentId(), out.get_componentId());
        assertEquals(id.get_streamId(), out.get_streamId());
    }
}
