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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;


@ExtendWith(MockitoExtension.class)
class ZstdBridgeThriftSerializationDelegateTest {

    @Mock
    private GzipBridgeThriftSerializationDelegate defaultDelegate;

    @Mock
    private ZstdThriftSerializationDelegate zstdDelegate;

    private ZstdBridgeThriftSerializationDelegate delegate;

    private static final Map<String, Object> TOPO_CONF = Collections.emptyMap();

    private static final byte[] ZSTD_BYTES  = {(byte) 0x28, (byte) 0xB5, (byte) 0x2F, (byte) 0xFD, 0x00};
    private static final byte[] PLAIN_BYTES  = {0x00, 0x01, 0x02, 0x03, 0x04};
    private static final byte[] RESULT_BYTES = {(byte) 0xAA, (byte) 0xBB};


    @BeforeEach
    void setUp() throws Exception {
        delegate = new ZstdBridgeThriftSerializationDelegate();

        Field defaultField = ZstdBridgeThriftSerializationDelegate.class.getDeclaredField("defaultDelegate");
        defaultField.setAccessible(true);
        defaultField.set(delegate, defaultDelegate);

        Field zstdField = ZstdBridgeThriftSerializationDelegate.class.getDeclaredField("zstdDelegate");
        zstdField.setAccessible(true);
        zstdField.set(delegate, zstdDelegate);
    }

    @Test
    void prepare_delegatesToBothDelegates() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("key", "value");

        // user defined conf
        conf.put(Config.STORM_COMPRESSION_ZSTD_LEVEL, 3);
        conf.put(Config.STORM_COMPRESSION_ZSTD_MAX_DECOMPRESSED_BYTES, 2 * 1024 * 1024);
        conf.put(Config.STORM_COMPRESSION_GZIP_MAX_DECOMPRESSED_BYTES, 2 * 1024 * 1024);

        delegate.prepare(conf);

        verify(defaultDelegate).prepare(conf);
        verify(zstdDelegate).prepare(conf);
        verifyNoMoreInteractions(defaultDelegate, zstdDelegate);
    }

    @Test
    void prepare_emptyConf_doesNotThrow() {
        assertDoesNotThrow(() -> delegate.prepare(TOPO_CONF));
        verify(defaultDelegate).prepare(TOPO_CONF);
        verify(zstdDelegate).prepare(TOPO_CONF);
    }

    @Test
    void prepare_nullConf_propagatesToBothDelegates() {
        delegate.prepare(null);
        verify(defaultDelegate).prepare(null);
        verify(zstdDelegate).prepare(null);
    }

    @Test
    void serialize_alwaysUsesZstdDelegate() {
        Object payload = new Object();
        when(zstdDelegate.serialize(payload)).thenReturn(RESULT_BYTES);

        byte[] result = delegate.serialize(payload);

        assertArrayEquals(RESULT_BYTES, result);
        verify(zstdDelegate).serialize(payload);
        verifyNoInteractions(defaultDelegate);
    }

    @Test
    void serialize_nullObject_delegatedToZstd() {
        when(zstdDelegate.serialize(null)).thenReturn(RESULT_BYTES);

        byte[] result = delegate.serialize(null);

        assertArrayEquals(RESULT_BYTES, result);
        verify(zstdDelegate).serialize(null);
        verifyNoInteractions(defaultDelegate);
    }

    @Test
    void serialize_neverUsesDefaultDelegate() {
        when(zstdDelegate.serialize(any())).thenReturn(RESULT_BYTES);

        delegate.serialize("anything");

        verifyNoInteractions(defaultDelegate);
    }

    @Test
    void deserialize_zstdMagic_usesZstdDelegate() {
        try (MockedStatic<Utils.ZstdUtils> mocked = mockStatic(Utils.ZstdUtils.class)) {
            mocked.when(() -> Utils.ZstdUtils.isZstd(ZSTD_BYTES)).thenReturn(true);
            when(zstdDelegate.deserialize(ZSTD_BYTES, String.class)).thenReturn("zstd-result");

            String result = delegate.deserialize(ZSTD_BYTES, String.class);

            assertEquals("zstd-result", result);
            verify(zstdDelegate).deserialize(ZSTD_BYTES, String.class);
            verifyNoInteractions(defaultDelegate);
        }
    }

    @Test
    void deserialize_zstdMagic_doesNotTouchDefaultDelegate() {
        try (MockedStatic<Utils.ZstdUtils> mocked = mockStatic(Utils.ZstdUtils.class)) {
            mocked.when(() -> Utils.ZstdUtils.isZstd(ZSTD_BYTES)).thenReturn(true);
            when(zstdDelegate.deserialize(any(), any())).thenReturn(new Object());

            delegate.deserialize(ZSTD_BYTES, Object.class);

            verifyNoInteractions(defaultDelegate);
        }
    }

    // fallback
    @Test
    void deserialize_noZstdMagic_usesDefaultDelegate() {
        try (MockedStatic<Utils.ZstdUtils> mocked = mockStatic(Utils.ZstdUtils.class)) {
            mocked.when(() -> Utils.ZstdUtils.isZstd(PLAIN_BYTES)).thenReturn(false);
            when(defaultDelegate.deserialize(PLAIN_BYTES, String.class)).thenReturn("plain-result");

            String result = delegate.deserialize(PLAIN_BYTES, String.class);

            assertEquals("plain-result", result);
            verify(defaultDelegate).deserialize(PLAIN_BYTES, String.class);
            verifyNoInteractions(zstdDelegate);
        }
    }

    @Test
    void deserialize_noZstdMagic_doesNotTouchZstdDelegate() {
        try (MockedStatic<Utils.ZstdUtils> mocked = mockStatic(Utils.ZstdUtils.class)) {
            mocked.when(() -> Utils.ZstdUtils.isZstd(PLAIN_BYTES)).thenReturn(false);
            when(defaultDelegate.deserialize(any(), any())).thenReturn(new Object());

            delegate.deserialize(PLAIN_BYTES, Object.class);

            verifyNoInteractions(zstdDelegate);
        }
    }

    @Test
    void deserialize_nullBytes_routedToDefaultDelegate() {
        try (MockedStatic<Utils.ZstdUtils> mocked = mockStatic(Utils.ZstdUtils.class)) {
            mocked.when(() -> Utils.ZstdUtils.isZstd(null)).thenReturn(false);
            when(defaultDelegate.deserialize(null, String.class)).thenReturn("fallback");

            String result = delegate.deserialize(null, String.class);

            assertEquals("fallback", result);
            verify(defaultDelegate).deserialize(null, String.class);
            verifyNoInteractions(zstdDelegate);
        }
    }

    // exceptions propagation
    @Test
    void deserialize_zstdDelegateThrows_exceptionPropagates() {
        try (MockedStatic<Utils.ZstdUtils> mocked = mockStatic(Utils.ZstdUtils.class)) {
            mocked.when(() -> Utils.ZstdUtils.isZstd(ZSTD_BYTES)).thenReturn(true);
            when(zstdDelegate.deserialize(any(), any()))
                    .thenThrow(new RuntimeException("decompression error"));

            RuntimeException ex = assertThrows(RuntimeException.class,
                    () -> delegate.deserialize(ZSTD_BYTES, String.class));
            assertEquals("decompression error", ex.getMessage());
        }
    }

    @Test
    void deserialize_defaultDelegateThrows_exceptionPropagates() {
        try (MockedStatic<Utils.ZstdUtils> mocked = mockStatic(Utils.ZstdUtils.class)) {
            mocked.when(() -> Utils.ZstdUtils.isZstd(PLAIN_BYTES)).thenReturn(false);
            when(defaultDelegate.deserialize(any(), any()))
                    .thenThrow(new RuntimeException("thrift error"));

            RuntimeException ex = assertThrows(RuntimeException.class,
                    () -> delegate.deserialize(PLAIN_BYTES, String.class));
            assertEquals("thrift error", ex.getMessage());
        }
    }

    // delegation chain
    @Test
    void deserialize_isZstdDeterminesRouting_trueThenFalse() {
        try (MockedStatic<Utils.ZstdUtils> mocked = mockStatic(Utils.ZstdUtils.class)) {
            byte[] bytes = PLAIN_BYTES;

            // First call: treated as Zstd
            mocked.when(() -> Utils.ZstdUtils.isZstd(bytes)).thenReturn(true);
            when(zstdDelegate.deserialize(bytes, String.class)).thenReturn("via-zstd");
            assertEquals("via-zstd", delegate.deserialize(bytes, String.class));

            // Second call: treated as non-Zstd
            mocked.when(() -> Utils.ZstdUtils.isZstd(bytes)).thenReturn(false);
            when(defaultDelegate.deserialize(bytes, String.class)).thenReturn("via-default");
            assertEquals("via-default", delegate.deserialize(bytes, String.class));
        }
    }
}
