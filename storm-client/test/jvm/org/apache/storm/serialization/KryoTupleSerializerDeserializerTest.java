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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.shade.net.minidev.json.JSONValue;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link KryoTupleSerializer} and {@link KryoTupleDeserializer}, covering the compressed and
 * uncompressed code paths, round-trip fidelity, mixing both encodings through a single (de)serializer instance,
 * and the negative/error paths.
 */
public class KryoTupleSerializerDeserializerTest {

    private static final String SOURCE_COMPONENT = "1";
    private static final String DEST_COMPONENT = "2";
    private static final int SOURCE_TASK_ID = 1;

    private GeneralTopologyContext context;

    @BeforeEach
    public void setup() {
        StormTopology topology = createStormTopology();
        context = mock(GeneralTopologyContext.class);
        when(context.getRawTopology()).thenReturn(topology);
        when(context.getComponentId(SOURCE_TASK_ID)).thenReturn(SOURCE_COMPONENT);
        when(context.doSanityCheck()).thenReturn(false);
    }

    private StormTopology createStormTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SOURCE_COMPONENT, new TestWordSpout(true), 1);
        builder.setBolt(DEST_COMPONENT, new TestWordCounter(), 1).fieldsGrouping(SOURCE_COMPONENT, new Fields("word"));
        return builder.createTopology();
    }

    private Map<String, Object> baseConf() {
        return new HashMap<>(Utils.readStormConfig());
    }

    private Map<String, Object> compressionEnabledConf(int threshold) {
        Map<String, Object> conf = baseConf();
        conf.put(Config.TOPOLOGY_TUPLE_COMPRESSION_ENABLE, true);
        conf.put(Config.TOPOLOGY_TUPLE_COMPRESSION_THRESHOLD, threshold);
        conf.put(Config.STORM_COMPRESSION_ZSTD_LEVEL, 3);
        return conf;
    }

    private void enableComponentLevelCompression(String componentId) {
        enableComponentLevelCompression(context, componentId);
    }

    private void enableComponentLevelCompression(GeneralTopologyContext ctx, String componentId) {
        Map<String, Object> componentConf = new HashMap<>();
        componentConf.put(Config.TOPOLOGY_TUPLE_COMPRESSION_ENABLE, true);
        ComponentCommon common = mock(ComponentCommon.class);
        when(common.get_json_conf()).thenReturn(JSONValue.toJSONString(componentConf));
        when(ctx.getComponentIds()).thenReturn(Collections.singleton(componentId));
        when(ctx.getComponentCommon(componentId)).thenReturn(common);
    }

    private GeneralTopologyContext newContext() {
        GeneralTopologyContext ctx = mock(GeneralTopologyContext.class);
        when(ctx.getRawTopology()).thenReturn(createStormTopology());
        when(ctx.getComponentId(SOURCE_TASK_ID)).thenReturn(SOURCE_COMPONENT);
        when(ctx.doSanityCheck()).thenReturn(false);
        return ctx;
    }

    @Test
    public void testRoundTripUncompressedWhenCompressionDisabled() {
        Map<String, Object> conf = baseConf(); // compression disabled by default
        KryoTupleSerializer serializer = new KryoTupleSerializer(conf, context);
        KryoTupleDeserializer deserializer = new KryoTupleDeserializer(conf, context);

        TupleImpl original = tuple(new Values("hello", 42, bigString(8192)), MessageId.makeRootId(7L, 99L));
        byte[] bytes = serializer.serialize(original);

        assertFalse(Utils.ZstdUtils.isZstd(bytes), "compression disabled must never emit a zstd frame");
        assertSameTuple(original, deserializer.deserialize(bytes));
    }

    @Test
    public void testRoundTripUncompressedWhenBelowThreshold() {
        Map<String, Object> conf = compressionEnabledConf(1460);
        KryoTupleSerializer serializer = new KryoTupleSerializer(conf, context);
        KryoTupleDeserializer deserializer = new KryoTupleDeserializer(conf, context);

        TupleImpl original = tuple(new Values("small", 1), MessageId.makeUnanchored());
        byte[] bytes = serializer.serialize(original);

        assertFalse(Utils.ZstdUtils.isZstd(bytes), "payload below threshold must not be compressed");
        assertSameTuple(original, deserializer.deserialize(bytes));
    }

    @Test
    public void testRoundTripCompressedWhenAboveThreshold() {
        Map<String, Object> conf = compressionEnabledConf(1460);
        KryoTupleSerializer serializer = new KryoTupleSerializer(conf, context);
        KryoTupleDeserializer deserializer = new KryoTupleDeserializer(conf, context);

        TupleImpl original = tuple(new Values(bigString(64 * 1024)), MessageId.makeRootId(5L, 11L));
        byte[] bytes = serializer.serialize(original);

        assertTrue(Utils.ZstdUtils.isZstd(bytes), "payload above threshold must be compressed");
        assertSameTuple(original, deserializer.deserialize(bytes));
    }

    @Test
    public void testCompressionAtExactThreshold() {
        Map<String, Object> conf = compressionEnabledConf(0);
        KryoTupleSerializer serializer = new KryoTupleSerializer(conf, context);
        KryoTupleDeserializer deserializer = new KryoTupleDeserializer(conf, context);

        TupleImpl original = tuple(new Values("x"), MessageId.makeUnanchored());
        byte[] bytes = serializer.serialize(original);

        assertTrue(Utils.ZstdUtils.isZstd(bytes), "threshold 0 must compress every non-empty payload");
        assertSameTuple(original, deserializer.deserialize(bytes));
    }

    @Test
    public void testMixedCompressedAndUncompressedSameInstances() {
        Map<String, Object> conf = compressionEnabledConf(1460);
        KryoTupleSerializer serializer = new KryoTupleSerializer(conf, context);
        KryoTupleDeserializer deserializer = new KryoTupleDeserializer(conf, context);

        TupleImpl small = tuple(new Values("tiny", 1), MessageId.makeRootId(1L, 2L));
        TupleImpl large = tuple(new Values(bigString(32 * 1024), "tail"), MessageId.makeRootId(3L, 4L));

        // Buffer reuse across compressed and uncompressed payloads.
        byte[] smallBytes1 = serializer.serialize(small);
        byte[] largeBytes1 = serializer.serialize(large);
        byte[] smallBytes2 = serializer.serialize(small);
        byte[] largeBytes2 = serializer.serialize(large);

        assertFalse(Utils.ZstdUtils.isZstd(smallBytes1));
        assertTrue(Utils.ZstdUtils.isZstd(largeBytes1));
        assertFalse(Utils.ZstdUtils.isZstd(smallBytes2));
        assertTrue(Utils.ZstdUtils.isZstd(largeBytes2));

        // deserializer must transparently handle both encodings, in any order.
        assertSameTuple(large, deserializer.deserialize(largeBytes1));
        assertSameTuple(small, deserializer.deserialize(smallBytes1));
        assertSameTuple(small, deserializer.deserialize(smallBytes2));
        assertSameTuple(large, deserializer.deserialize(largeBytes2));
    }

    @Test
    public void testComponentLevelCompressionEnablesDecompressPath() {
        enableComponentLevelCompression(SOURCE_COMPONENT);
        KryoTupleSerializer serializer = new KryoTupleSerializer(compressionEnabledConf(0), context);
        KryoTupleDeserializer deserializer = new KryoTupleDeserializer(baseConf(), context);

        TupleImpl original = tuple(new Values(bigString(16 * 1024)), MessageId.makeRootId(8L, 9L));
        byte[] bytes = serializer.serialize(original);

        assertTrue(Utils.ZstdUtils.isZstd(bytes));
        assertSameTuple(original, deserializer.deserialize(bytes));
    }

    @Test
    public void testDecompressPathGatedPerTopology() {
        // Two distinct topologies sharing the exact same compressed frame on the wire. The gating decision is
        // per-topology (workers are per-topology), so the same bytes must be decompressed by one and not the other.
        GeneralTopologyContext compressingTopology = newContext();
        enableComponentLevelCompression(compressingTopology, SOURCE_COMPONENT); // at least one component compresses
        GeneralTopologyContext plainTopology = newContext();                    // no component enables compression

        KryoTupleSerializer serializer = new KryoTupleSerializer(compressionEnabledConf(0), compressingTopology);
        TupleImpl original = tuple(new Values(bigString(16 * 1024)), MessageId.makeRootId(8L, 9L));
        byte[] bytes = serializer.serialize(original);
        assertTrue(Utils.ZstdUtils.isZstd(bytes), "precondition: serializer produced a compressed frame");

        KryoTupleDeserializer compressingDeser = new KryoTupleDeserializer(baseConf(), compressingTopology);
        KryoTupleDeserializer plainDeser = new KryoTupleDeserializer(baseConf(), plainTopology);

        // Compressing topology: decompress path is taken, the frame round-trips.
        assertSameTuple(original, compressingDeser.deserialize(bytes));
        // Plain topology: decompress path is skipped, the frame is treated as a raw kryo tuple and fails to parse.
        assertThrows(RuntimeException.class, () -> plainDeser.deserialize(bytes));
    }

    @Test
    public void testNoComponentCompressionSkipsDecompressPath() {
        KryoTupleSerializer serializer = new KryoTupleSerializer(compressionEnabledConf(0), context);
        KryoTupleDeserializer deserializer = new KryoTupleDeserializer(baseConf(), context);

        byte[] bytes = serializer.serialize(tuple(new Values(bigString(16 * 1024)), MessageId.makeRootId(8L, 9L)));
        assertTrue(Utils.ZstdUtils.isZstd(bytes), "precondition: serializer produced a compressed frame");
        assertThrows(RuntimeException.class, () -> deserializer.deserialize(bytes));
    }

    @Test
    public void testDecompressFailureFallsBackToRawTupleParsing() {
        // Exercises the false-positive fallback in KryoTupleDeserializer#deserialize: compression is enabled and
        // isZstd() reports a match, but the decompress path surfaces a "Failed to deserialize tuple" error.
        enableComponentLevelCompression(SOURCE_COMPONENT); // anyTupleCompressionEnabled == true
        KryoTupleSerializer serializer = new KryoTupleSerializer(baseConf(), context);
        KryoTupleDeserializer deserializer = new KryoTupleDeserializer(baseConf(), context);

        TupleImpl original = tuple(new Values("hello", 42), MessageId.makeRootId(5L, 11L));
        byte[] raw = serializer.serialize(original);
        assertFalse(Utils.ZstdUtils.isZstd(raw), "precondition: serializer produced a raw, uncompressed tuple");

        try (MockedStatic<Utils.ZstdUtils> mocked = mockStatic(Utils.ZstdUtils.class)) {
            // Force entry into the decompress branch, then make decompression report a tuple-deserialization failure.
            mocked.when(() -> Utils.ZstdUtils.isZstd(raw)).thenReturn(true);
            mocked.when(() -> Utils.ZstdUtils.decompress(eq(raw), anyInt()))
                  .thenThrow(new RuntimeException(KryoTupleDeserializer.FAILED_TO_DESERIALIZE_TUPLE));

            TupleImpl result = deserializer.deserialize(raw);
            assertSameTuple(original, result); // fallback re-parsed the raw bytes and recovered the tuple
        }
    }

    @Test
    public void testDecompressFailureFallbackRethrowsWhenRawBytesAlsoInvalid() {
        // the raw bytes are not a valid kryo tuple either: the fallback parse
        // must surface a RuntimeException rather than silently returning a bogus tuple.
        enableComponentLevelCompression(SOURCE_COMPONENT);
        KryoTupleDeserializer deserializer = new KryoTupleDeserializer(baseConf(), context);

        byte[] raw = {1, 2, 3, 4, 5, 6, 7, 8};
        try (MockedStatic<Utils.ZstdUtils> mocked = mockStatic(Utils.ZstdUtils.class)) {
            mocked.when(() -> Utils.ZstdUtils.isZstd(raw)).thenReturn(true);
            mocked.when(() -> Utils.ZstdUtils.decompress(eq(raw), anyInt()))
                  .thenThrow(new RuntimeException(KryoTupleDeserializer.FAILED_TO_DESERIALIZE_TUPLE));

            assertThrows(RuntimeException.class, () -> deserializer.deserialize(raw));
        }
    }

    // corner cases

    @Test
    public void testRoundTripEmptyValues() {
        Map<String, Object> conf = baseConf();
        KryoTupleSerializer serializer = new KryoTupleSerializer(conf, context);
        KryoTupleDeserializer deserializer = new KryoTupleDeserializer(conf, context);

        TupleImpl original = tuple(new Values(), MessageId.makeUnanchored());
        TupleImpl result = deserializer.deserialize(serializer.serialize(original));
        assertSameTuple(original, result);
        assertTrue(result.getValues().isEmpty());
    }

    @Test
    public void testRoundTripNullValue() {
        Map<String, Object> conf = baseConf();
        KryoTupleSerializer serializer = new KryoTupleSerializer(conf, context);
        KryoTupleDeserializer deserializer = new KryoTupleDeserializer(conf, context);

        TupleImpl original = tuple(new Values("a", null, "b"), MessageId.makeUnanchored());
        assertSameTuple(original, deserializer.deserialize(serializer.serialize(original)));
    }

    // negative tests

    @Test
    public void testDeserializeGarbageBytesThrows() {
        KryoTupleDeserializer deserializer = new KryoTupleDeserializer(baseConf(), context);
        // Not a zstd frame (length < 4 keeps isZstd false) and not a valid kryo tuple.
        assertThrows(RuntimeException.class, () -> deserializer.deserialize(new byte[]{1, 2, 3}));
    }

    @Test
    public void testDeserializeEmptyBytesThrows() {
        KryoTupleDeserializer deserializer = new KryoTupleDeserializer(baseConf(), context);
        assertThrows(RuntimeException.class, () -> deserializer.deserialize(new byte[0]));
    }

    @Test
    public void testDeserializeZstdMagicButInvalidFrameThrows() {
        enableComponentLevelCompression(SOURCE_COMPONENT);
        KryoTupleDeserializer deserializer = new KryoTupleDeserializer(baseConf(), context);
        // First 4 bytes are the little-endian zstd magic 0xFD2FB528 so isZstd() is true, but the rest is
        // not a valid zstd frame. decompress() throws, the false-positive fallback re-parses the raw bytes,
        // which are also not a valid kryo tuple -> RuntimeException.
        byte[] fakeZstd = new byte[]{(byte) 0x28, (byte) 0xB5, (byte) 0x2F, (byte) 0xFD, 0x00, 0x01, 0x02, 0x03};
        assertTrue(Utils.ZstdUtils.isZstd(fakeZstd));
        assertThrows(RuntimeException.class, () -> deserializer.deserialize(fakeZstd));
    }

    @Test
    public void testDeserializeCompressedExceedingMaxDecompressedBytesThrows() {
        // A genuinely compressed tuple, but the deserializer is configured with a tiny decompression cap.
        // decompress() throws ("threshold exceeded"), the fallback re-parses the still-compressed raw bytes,
        // which are not a valid kryo tuple -> RuntimeException.
        enableComponentLevelCompression(SOURCE_COMPONENT);
        KryoTupleSerializer serializer = new KryoTupleSerializer(compressionEnabledConf(0), context);
        Map<String, Object> deserConf = baseConf();
        deserConf.put(Config.TOPOLOGY_TUPLE_COMPRESSION_MAX_DECOMPRESSED_BYTES, 1);
        KryoTupleDeserializer deserializer = new KryoTupleDeserializer(deserConf, context);

        byte[] bytes = serializer.serialize(tuple(new Values(bigString(8192)), MessageId.makeUnanchored()));
        assertTrue(Utils.ZstdUtils.isZstd(bytes));
        assertThrows(RuntimeException.class, () -> deserializer.deserialize(bytes));
    }

    @Test
    public void testSerializeUnregisteredTypeWithoutJavaFallbackThrows() {
        Map<String, Object> conf = baseConf();
        conf.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, false);
        KryoTupleSerializer serializer = new KryoTupleSerializer(conf, context);

        TupleImpl original = tuple(new Values(new UnregisteredType()), MessageId.makeUnanchored());
        assertThrows(RuntimeException.class, () -> serializer.serialize(original));
    }

    private static class UnregisteredType {
        @SuppressWarnings("unused")
        private final int field = 1;
    }

    private TupleImpl tuple(List<Object> values, MessageId id) {
        return new TupleImpl(context, values, SOURCE_COMPONENT, SOURCE_TASK_ID, Utils.DEFAULT_STREAM_ID, id);
    }

    private void assertSameTuple(TupleImpl expected, TupleImpl actual) {
        assertEquals(expected.getValues(), actual.getValues());
        assertEquals(expected.getSourceTask(), actual.getSourceTask());
        assertEquals(expected.getSourceComponent(), actual.getSourceComponent());
        assertEquals(expected.getSourceStreamId(), actual.getSourceStreamId());
        assertEquals(expected.getMessageId(), actual.getMessageId());
    }

    private String bigString(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            sb.append((char) ('a' + (i % 26)));
        }
        return sb.toString();
    }
}
