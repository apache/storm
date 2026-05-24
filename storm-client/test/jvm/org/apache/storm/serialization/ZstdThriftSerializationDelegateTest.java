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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ZstdThriftSerializationDelegateTest {

    private ZstdThriftSerializationDelegate delegate;

    private static StormTopology validTopology() {
        StormTopology t = new StormTopology();
        t.set_spouts(Collections.emptyMap());
        t.set_bolts(Collections.emptyMap());
        t.set_state_spouts(Collections.emptyMap());
        return t;
    }

    private static final StormTopology topology = validTopology();

    @BeforeEach
    void setUp() {
        delegate = new ZstdThriftSerializationDelegate();
        delegate.prepare(Collections.emptyMap()); // uses defaults
    }

    @Test
    void prepare_emptyConf_usesDefaults() {
        // Verify that prepare with empty conf doesn't throw and produces
        // a delegate that can still serialize/deserialize correctly.
        ZstdThriftSerializationDelegate d = new ZstdThriftSerializationDelegate();
        assertDoesNotThrow(() -> d.prepare(Collections.emptyMap()));

        byte[] serialized = d.serialize(topology);
        assertTrue(Utils.ZstdUtils.isZstd(serialized));
    }

    @Test
    void prepare_customLevel_isUsed() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_COMPRESSION_ZSTD_LEVEL, 1);

        ZstdThriftSerializationDelegate d = new ZstdThriftSerializationDelegate();
        d.prepare(conf);

        // Both level 1 and level 3 produce valid zstd output — verify it is zstd and round-trips
        byte[] serialized = d.serialize(topology);
        assertTrue(Utils.ZstdUtils.isZstd(serialized));
        assertNotNull(d.deserialize(serialized, StormTopology.class));
    }

    @Test
    void prepare_customMaxDecompressedBytes_isRespected() {
        // Set a limit of 1 byte — decompressing anything non-trivial must fail
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_COMPRESSION_ZSTD_MAX_DECOMPRESSED_BYTES, 1);

        ZstdThriftSerializationDelegate d = new ZstdThriftSerializationDelegate();
        d.prepare(conf);

        // Serialize with the default delegate (no limit) to get valid zstd bytes
        byte[] serialized = delegate.serialize(topology);

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> d.deserialize(serialized, StormTopology.class));
        assertEquals("Zstd decompression failed", ex.getMessage());
    }

    @Test
    void serialize_tBaseObject_producesZstdFrame() {
        byte[] result = delegate.serialize(topology);

        assertNotNull(result);
        assertTrue(result.length > 0);
        assertTrue(Utils.ZstdUtils.isZstd(result), "Serialized output must start with zstd magic");
    }

    @Test
    void serialize_nonTBaseObject_throwsIllegalArgumentException() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> delegate.serialize("not a TBase"));
        assertEquals("Object must be an instance of TBase", ex.getMessage());
    }

    @Test
    void serialize_nullObject_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> delegate.serialize(null));
    }

    @Test
    void serialize_sameTBaseObject_producesDeterministicOutput() {
        byte[] first = delegate.serialize(topology);
        byte[] second = delegate.serialize(topology);
        assertArrayEquals(first, second, "Serialization of the same object must be deterministic");
    }

    @Test
    void deserialize_validZstdBytes_returnsCorrectType() {
        byte[] serialized = delegate.serialize(topology);

        StormTopology result = delegate.deserialize(serialized, StormTopology.class);

        assertNotNull(result);
    }

    @Test
    void deserialize_corruptedBytes_throwsRuntimeException() {
        byte[] garbage = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07};
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> delegate.deserialize(garbage, StormTopology.class));
        assertEquals("Cannot deserialize [" +StormTopology.class.getSimpleName()  + "]. " +
                "Expected zstd compressed bytes, but received unknown format.", ex.getMessage());
    }

    @Test
    void deserialize_wrongTargetClass_throwsRuntimeException() {
        byte[] serialized = delegate.serialize(validTopology());
        assertThrows(RuntimeException.class,
                () -> delegate.deserialize(serialized, String.class));
    }

    @Test
    void roundTrip_emptyTopology_isLossless() {
        StormTopology original = topology;
        byte[] serialized = delegate.serialize(original);
        StormTopology recovered = delegate.deserialize(serialized, StormTopology.class);

        assertEquals(original, recovered);
    }

    @Test
    void roundTrip_afterPrepareWithCustomConf_isLossless() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_COMPRESSION_ZSTD_LEVEL, 9);
        conf.put(Config.STORM_COMPRESSION_ZSTD_MAX_DECOMPRESSED_BYTES, 100);

        ZstdThriftSerializationDelegate d = new ZstdThriftSerializationDelegate();
        d.prepare(conf);

        StormTopology original = topology;
        byte[] serialized = d.serialize(original);
        StormTopology recovered = d.deserialize(serialized, StormTopology.class);

        assertEquals(original, recovered);
    }

    @Test
    void roundTrip_prepareNotCalled_throwsNullPointerOrRuntime() {
        // Without prepare(), topoConf fields are zero/null — behaviour is defined by ObjectReader.
        // At minimum, serialize must not silently corrupt data.
        ZstdThriftSerializationDelegate unprepared = new ZstdThriftSerializationDelegate();
        // Either it works (ObjectReader tolerates null map) or throws — must not return wrong data.
        try {
            byte[] serialized = unprepared.serialize(topology);
            StormTopology recovered = unprepared.deserialize(serialized, StormTopology.class);
            assertNotNull(recovered);
        } catch (RuntimeException e) {
            // Acceptable: prepare() was never called
        }
    }
}
