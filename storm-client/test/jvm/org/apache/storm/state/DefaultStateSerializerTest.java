/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.state;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.spout.CheckPointState;
import org.junit.jupiter.api.Test;
import org.objenesis.strategy.StdInstantiatorStrategy;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link DefaultStateSerializer}
 */
public class DefaultStateSerializerTest {

    @Test
    public void testSerializeDeserialize() {
        Serializer<Long> s1 = new DefaultStateSerializer<>();
        byte[] bytes;
        long val = 100;
        bytes = s1.serialize(val);
        assertEquals(val, (long) s1.deserialize(bytes));

        CheckPointState cs = new CheckPointState(100, CheckPointState.State.COMMITTED);

        Serializer<CheckPointState> s2 = new DefaultStateSerializer<>();
        bytes = s2.serialize(cs);
        assertEquals(cs, s2.deserialize(bytes));

        List<Class<?>> classesToRegister = new ArrayList<>();
        classesToRegister.add(CheckPointState.class);
        classesToRegister.add(CheckPointState.State.class);
        Serializer<CheckPointState> s3 = new DefaultStateSerializer<>(Collections.emptyMap(), null, classesToRegister);
        bytes = s3.serialize(cs);
        assertEquals(cs, s3.deserialize(bytes));

    }

    /**
     * The encoder wraps every value as Optional&lt;byte[]&gt;, so byte[] must round-trip even though
     * no caller registers it explicitly.
     */
    @Test
    public void testDefaultStateEncoderRoundTrip() {
        DefaultStateEncoder<String, byte[]> encoder =
            new DefaultStateEncoder<>(new DefaultStateSerializer<>(), new DefaultStateSerializer<>());
        byte[] value = new byte[]{ 1, 2, 3 };

        assertEquals("k", encoder.decodeKey(encoder.encodeKey("k")));
        assertArrayEquals(value, encoder.decodeValue(encoder.encodeValue(value)));
        // the tombstone is produced by the same static serializer at class-init time
        assertNull(encoder.decodeValue(encoder.getTombstoneValue()));
    }

    @Test
    public void testDeserializeRejectsUnregisteredClasses() {
        // a Kryo stream naming a class the topology never registered, as an earlier release
        // or an unrelated writer could have left in the store
        Kryo permissive = new Kryo();
        permissive.setRegistrationRequired(false);
        permissive.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
        Output out = new Output(4096);
        permissive.writeClassAndObject(out, new UnregisteredPojo());
        byte[] unregisteredClassBytes = out.toBytes();

        Serializer<Object> serializer = new DefaultStateSerializer<>();
        assertThrows(IllegalArgumentException.class, () -> serializer.deserialize(unregisteredClassBytes));
    }

    @Test
    public void testSerializeRejectsUnregisteredClasses() {
        Serializer<Object> serializer = new DefaultStateSerializer<>();
        assertThrows(IllegalArgumentException.class, () -> serializer.serialize(new UnregisteredPojo()));
    }

    @Test
    public void testFallBackOnJavaSerializationAllowsUnregisteredClasses() {
        Map<String, Object> topoConf = new HashMap<>();
        topoConf.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, true);
        Serializer<Object> serializer = new DefaultStateSerializer<>(topoConf, null);
        byte[] bytes = serializer.serialize(new UnregisteredPojo());
        assertEquals(UnregisteredPojo.class, serializer.deserialize(bytes).getClass());
    }

    public static class UnregisteredPojo {
        private long value;
    }

    /**
     * Replica of the serializer as it behaved before registration was required: unregistered classes
     * are written by name. State persisted by earlier releases is in this format.
     */
    private static Kryo legacyKryo() {
        Kryo k = new Kryo();
        k.setRegistrationRequired(false);
        org.apache.storm.serialization.SerializationFactory.register(
            k, Collections.singletonList(java.util.Optional.class.getName()));
        k.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
        return k;
    }

    private static byte[] legacyWrite(Object obj) {
        Output out = new Output(4096);
        legacyKryo().writeClassAndObject(out, obj);
        out.flush();
        return out.toBytes();
    }

    /** Exactly what the pre-registration DefaultStateEncoder.encodeValue produced. */
    private static byte[] legacyEncodeValue(Object value) {
        return legacyWrite(java.util.Optional.of(legacyWrite(value)));
    }

    /**
     * State written before registration was required is name-encoded. Kryo skips the class name for
     * a name id it has already seen, so without clearing that cache between operations the second
     * such read desynchronises from the stream.
     */
    @Test
    public void testSequentialReadsOfLegacyEncodedState() {
        DefaultStateEncoder<String, Object> encoder =
            new DefaultStateEncoder<>(new DefaultStateSerializer<>(), new DefaultStateSerializer<>());

        assertEquals("first", encoder.decodeValue(legacyEncodeValue("first")));
        assertEquals("second", encoder.decodeValue(legacyEncodeValue("second")));
        assertEquals("third", encoder.decodeValue(legacyEncodeValue("third")));
    }

    /**
     * A rejected payload must not affect later reads. The serializers are shared, so a failure that
     * left the class resolver dirty would let one planted value deny service to the rest.
     */
    @Test
    public void testRejectedPayloadDoesNotBreakLaterReads() {
        DefaultStateEncoder<String, Object> encoder =
            new DefaultStateEncoder<>(new DefaultStateSerializer<>(), new DefaultStateSerializer<>());

        assertThrows(IllegalArgumentException.class,
            () -> encoder.decodeValue(legacyWrite(new UnregisteredPojo())));

        // a legitimate value, in both the current and the legacy encoding
        assertEquals("still-works", encoder.decodeValue(encoder.encodeValue("still-works")));
        assertEquals("legacy-still-works", encoder.decodeValue(legacyEncodeValue("legacy-still-works")));
    }
}
