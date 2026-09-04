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

import java.io.InvalidClassException;
import java.io.ObjectInputFilter;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.commons.collections.functors.SimulatedGadget;
import org.apache.storm.Config;
import org.apache.storm.serialization.types.ListDelegateSerializer;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the JEP-290 serial filter ({@link Config#TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION_FILTER}) protecting the
 * java-serialization fallback bridge. Every allow and deny case exercises an actual round-trip through the bridge, not the
 * filter API in isolation. All round-trips go through KryoValuesSerializer and KryoValuesDeserializer end to end.
 */
public class SerializableSerializerFilterTest {

    /** The maxbytes limit set in conf/defaults.yaml. */
    private static final long DEFAULT_MAX_BYTES = 10485760L;

    /**
     * Minimal conf that routes unregistered classes through the java-serialization fallback bridge. {@code filterSpec == null}
     * means the filter key is absent from the conf entirely (the pre-existing behavior).
     */
    private Map<String, Object> bridgeConf(String filterSpec) {
        Map<String, Object> conf = new Config();
        conf.put(Config.TOPOLOGY_KRYO_FACTORY, DefaultKryoFactory.class.getName());
        conf.put(Config.TOPOLOGY_TUPLE_SERIALIZER, ListDelegateSerializer.class.getName());
        conf.put(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS, false);
        conf.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, true);
        if (filterSpec != null) {
            conf.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION_FILTER, filterSpec);
        }
        return conf;
    }

    /** conf assembled exactly like a worker's would be: defaults.yaml + topology-level overrides. */
    private Map<String, Object> defaultsBridgeConf() {
        Map<String, Object> conf = new Config();
        conf.putAll(Utils.readDefaultConfig());
        conf.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, true);
        return conf;
    }

    private Object roundTrip(Map<String, Object> conf, Object value) {
        KryoValuesSerializer serializer = new KryoValuesSerializer(conf);
        KryoValuesDeserializer deserializer = new KryoValuesDeserializer(conf);
        return deserializer.deserialize(serializer.serialize(Collections.singletonList(value))).get(0);
    }

    /** Serializes {@code value} and asserts that reading it back fails with a JEP-290 rejection in the cause chain. */
    private void assertRejectedOnRead(Map<String, Object> conf, Object value) {
        KryoValuesSerializer serializer = new KryoValuesSerializer(conf);
        KryoValuesDeserializer deserializer = new KryoValuesDeserializer(conf);
        // Writing is plain java serialization (filters apply to deserialization only), so this must succeed.
        byte[] bytes = serializer.serialize(Collections.singletonList(value));
        RuntimeException ex = assertThrows(RuntimeException.class, () -> deserializer.deserialize(bytes));
        assertTrue(hasCause(ex, InvalidClassException.class),
                "expected the JEP-290 filter rejection in the cause chain, got: " + ex);
    }

    private static boolean hasCause(Throwable throwable, Class<? extends Throwable> type) {
        for (Throwable t = throwable; t != null; t = t.getCause()) {
            if (type.isInstance(t)) {
                return true;
            }
        }
        return false;
    }

    /** ArrayDeque and PriorityQueue do not override equals, so round-trips are compared by iteration content. */
    private static void assertSameContent(Iterable<?> expected, Iterable<?> actual) {
        assertIterableEquals(expected, actual);
    }

    @Test
    public void testFilterRejectsDeniedClassOnDeserialization() {
        Map<String, Object> conf = bridgeConf("!java.util.PriorityQueue");

        PriorityQueue<Integer> original = new PriorityQueue<>(Arrays.asList(3, 1, 2));
        assertRejectedOnRead(conf, original);
    }

    @Test
    public void testFilterAllowsNonDeniedClassesRoundTrip() {
        Map<String, Object> conf = bridgeConf("!java.util.PriorityQueue");

        // HashMap has a dedicated kryo serializer: ordinary payloads must keep round-tripping.
        HashMap<String, Integer> hashMap = new HashMap<>(Collections.singletonMap("one", 1));
        assertEquals(hashMap, roundTrip(conf, hashMap));

        // ArrayDeque is unregistered and Serializable, so it travels through the java-serialization bridge itself.
        ArrayDeque<String> deque = new ArrayDeque<>(Arrays.asList("a", "b", "c"));
        assertSameContent(deque, (ArrayDeque<String>) roundTrip(conf, deque));
    }

    @Test
    public void testUnsetFilterKeyKeepsUnfilteredBehavior() {
        // No filter key in the conf at all: PriorityQueue must round-trip like it did before the filter existed.
        PriorityQueue<Integer> original = new PriorityQueue<>(Arrays.asList(5, 4, 6));
        assertSameContent(original, (PriorityQueue<Integer>) roundTrip(bridgeConf(null), original));
    }

    @Test
    public void testInvalidPatternFailsFastAtKryoConstruction() {
        // The parser only rejects a few inputs: '!' (no pattern) and a non-numeric maxbytes; malformed class patterns
        // are ignored, not rejected.
        for (String invalid : Arrays.asList("!", "maxbytes=not-a-number")) {
            Map<String, Object> conf = bridgeConf(invalid);
            RuntimeException ex = assertThrows(RuntimeException.class, () -> new KryoValuesSerializer(conf));
            assertTrue(ex.getMessage().contains(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION_FILTER),
                    "error must name the offending config key: " + ex.getMessage());
        }
    }

    @Test
    public void testDefaultsYamlFilterParsesAndDeniesGadgetPackage() {
        Map<String, Object> defaults = Utils.readDefaultConfig();
        Object filterSpec = defaults.get(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION_FILTER);
        assertNotNull(filterSpec, "conf/defaults.yaml must define a default "
                + Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION_FILTER);
        // The default pattern must parse (bad patterns throw IllegalArgumentException here).
        ObjectInputFilter.Config.createFilter((String) filterSpec);

        // Through an actual round-trip: a class in a deny-listed package is rejected on read...
        assertRejectedOnRead(defaultsBridgeConf(), new SimulatedGadget());
        // ...while ordinary JDK collections keep round-tripping under the same default.
        PriorityQueue<Integer> queue = new PriorityQueue<>(Arrays.asList(5, 4, 6));
        assertSameContent(queue, (PriorityQueue<Integer>) roundTrip(defaultsBridgeConf(), queue));
    }

    @Test
    public void testDefaultFilterRejectsCommonsCollections3Comparators() {
        // CC3's TransformingComparator gadget chain lives in the comparators package (the functors pair alone is not enough).
        assertRejectedOnRead(defaultsBridgeConf(), new org.apache.commons.collections.comparators.SimulatedGadget());
    }

    @Test
    public void testDefaultFilterRejectsSubpackagesOfRecursiveWildcardEntries() {
        // Wildcard depth matters: 'pkg.**' denies subpackages too; 'pkg.*' does not (c3p0.impl sits under the
        // shipped '!com.mchange.v2.c3p0.**' entry).
        assertRejectedOnRead(defaultsBridgeConf(), new com.mchange.v2.c3p0.impl.SimulatedGadget());
    }

    @Test
    public void testDefaultFilterEnforcesMaxBytesLimit() {
        Map<String, Object> conf = defaultsBridgeConf();
        // Every array read re-invokes the filter, so a stream built from many small arrays makes the cumulative
        // streamBytes() limit bite mid-deserialization (one huge primitive payload would not re-invoke the filter).
        ArrayDeque<byte[]> big = new ArrayDeque<>();
        for (int i = 0; i < 11000; i++) {
            big.add(new byte[1024]);
        }
        KryoValuesSerializer serializer = new KryoValuesSerializer(conf);
        KryoValuesDeserializer deserializer = new KryoValuesDeserializer(conf);
        byte[] bytes = serializer.serialize(Collections.singletonList(big));
        assertTrue(bytes.length > DEFAULT_MAX_BYTES, "payload must exceed the default limit, was " + bytes.length);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> deserializer.deserialize(bytes));
        assertTrue(hasCause(ex, InvalidClassException.class),
                "expected the maxbytes rejection in the cause chain, got: " + ex);
    }
}
