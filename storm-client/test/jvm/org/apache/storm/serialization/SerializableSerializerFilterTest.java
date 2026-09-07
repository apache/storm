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

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputFilter;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.logging.Level;
import java.util.regex.Pattern;
import javax.management.BadAttributeValueExpException;
import org.apache.storm.Config;
import org.apache.storm.serialization.types.ListDelegateSerializer;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the JEP-290 serial filter ({@link Config#TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION_FILTER}) protecting the
 * java-serialization fallback bridge. Round-trip cases exercise an actual pass through the bridge via KryoValuesSerializer and
 * KryoValuesDeserializer end to end, using JDK classes only (no fixtures under third-party package names). Filter semantics
 * that a round-trip cannot express (merging, limit tightening) are asserted through checkInput with synthetic FilterInfos
 * (rejections only).
 */
public class SerializableSerializerFilterTest {

    /** The maxbytes limit carried by {@link #SAMPLE_PATTERN}. */
    private static final long SAMPLE_MAX_BYTES = 10485760L;

    /**
     * The sample filter pattern documented in docs/SECURITY.md: a deny-list of well-known gadget namespaces plus
     * depth/reference/array/byte limits. Only entries whose classes exist on a plain JDK classpath are asserted against real
     * Class objects; the rest are covered by the parse (createFilter) and by the doc-sync test below.
     */
    private static final String SAMPLE_PATTERN = "!org.apache.commons.collections.functors.*;!org.apache.commons.collections.comparators.*;!org.apache.commons.collections4.functors.*;!org.apache.commons.collections4.comparators.*;!org.apache.commons.beanutils.*;!org.apache.xalan.xsltc.trax.*;!com.sun.org.apache.xalan.internal.**;!com.sun.rowset.*;!com.sun.org.apache.rowset.internal.*;!com.mchange.v2.c3p0.**;!org.codehaus.groovy.runtime.ConvertedClosure;!org.codehaus.groovy.runtime.MethodClosure;!javax.management.BadAttributeValueExpException;!sun.reflect.annotation.AnnotationInvocationHandler;!com.sun.jndi.**;!java.rmi.**;!clojure.**;!org.apache.commons.fileupload.**;!bsh.**;!org.python.**;!org.jboss.**;maxdepth=64;maxrefs=2097152;maxarray=1048576;maxbytes=10485760";

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
        assertTrue(Utils.exceptionCauseIsInstanceOf(InvalidClassException.class, ex),
                "expected the JEP-290 filter rejection in the cause chain, got: " + ex);
    }

    /** A FilterInfo describing only the candidate class; depth/references/streamBytes stay in-range so they cannot reject on their own. */
    private static ObjectInputFilter.FilterInfo info(Class<?> serialClass) {
        return info(serialClass, -1);
    }

    /** A FilterInfo describing an array class of the given length. */
    private static ObjectInputFilter.FilterInfo info(Class<?> serialClass, long arrayLength) {
        return new ObjectInputFilter.FilterInfo() {
            @Override
            public Class<?> serialClass() {
                return serialClass;
            }

            @Override
            public long arrayLength() {
                return arrayLength;
            }

            @Override
            public long depth() {
                return 1;
            }

            @Override
            public long references() {
                return 1;
            }

            @Override
            public long streamBytes() {
                return 0;
            }
        };
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
        assertIterableEquals(deque, (Iterable<String>) roundTrip(conf, deque));
    }

    @Test
    public void testUnsetFilterKeyKeepsUnfilteredBehavior() {
        // No filter key in the conf at all: PriorityQueue must round-trip like it did before the filter existed.
        PriorityQueue<Integer> original = new PriorityQueue<>(Arrays.asList(5, 4, 6));
        assertIterableEquals(original, (Iterable<Integer>) roundTrip(bridgeConf(null), original));
    }

    @Test
    public void testWildcardDepthCoversDirectMembersAndSubpackages() {
        // '.*' denies direct package members only: PriorityQueue (member of java.util) is rejected...
        Map<String, Object> shallow = bridgeConf("!java.util.*");
        assertRejectedOnRead(shallow, new PriorityQueue<>(Arrays.asList(3, 1, 2)));

        // ...while classes in subpackages keep round-tripping through the bridge: java.util.regex.Pattern and
        // java.util.logging.Level are unregistered, non-trivial, Serializable, and their java-serialized graphs stay
        // inside java.lang for fields, so the pass/fail outcome is decided by their own package.
        Pattern compiled = (Pattern) roundTrip(shallow, Pattern.compile("bridge-wildcard-probe"));
        assertEquals("bridge-wildcard-probe", compiled.pattern());
        assertEquals(Level.WARNING, roundTrip(shallow, Level.WARNING));

        // '**' also covers subpackages, so both depths are rejected.
        Map<String, Object> recursive = bridgeConf("!java.util.**");
        assertRejectedOnRead(recursive, new PriorityQueue<>(Arrays.asList(3, 1, 2)));
        assertRejectedOnRead(recursive, Pattern.compile("bridge-wildcard-probe"));
    }

    @Test
    public void testInvalidPatternFailsFastAtKryoConstruction() {
        // The parser only rejects a few inputs: '!' (no pattern) and a non-numeric maxbytes; malformed class patterns
        // are ignored, not rejected.
        for (String invalid : Arrays.asList("!", "maxbytes=not-a-number")) {
            Map<String, Object> conf = bridgeConf(invalid);
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> new KryoValuesSerializer(conf));
            assertTrue(ex.getMessage().contains(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION_FILTER),
                    "error must name the offending config key: " + ex.getMessage());
        }
    }

    @Test
    public void testSamplePatternRejectsDenyListedJdkClasses() {
        // Parsing the full sample also syntax-checks the third-party gadget entries, whose classes are not on the
        // classpath and thus cannot be asserted as Class objects.
        ObjectInputFilter sample = ObjectInputFilter.Config.createFilter(SAMPLE_PATTERN);
        assertEquals(ObjectInputFilter.Status.REJECTED, sample.checkInput(info(BadAttributeValueExpException.class)));
        assertEquals(ObjectInputFilter.Status.REJECTED, sample.checkInput(info(java.rmi.MarshalledObject.class)));
    }

    @Test
    public void testSecurityDocCarriesTheSamplePattern() throws IOException {
        // Surefire runs with the module directory as working directory, so the repo-root docs are one level up.
        Path securityDoc = Paths.get("..", "docs", "SECURITY.md").toAbsolutePath().normalize();
        assertTrue(Files.exists(securityDoc), "docs/SECURITY.md not found at " + securityDoc);
        String doc = Files.readString(securityDoc, StandardCharsets.UTF_8);
        assertTrue(doc.contains(SAMPLE_PATTERN), "docs/SECURITY.md must carry this test's sample pattern verbatim");
    }

    @Test
    public void testSamplePatternEnforcesMaxBytesLimit() {
        // ~11MB of heap churn per run: the payload must exceed the pattern's maxbytes for the cumulative limit to bite
        // mid-deserialization (every array read re-invokes the filter, so many small arrays make streamBytes add up).
        Map<String, Object> conf = bridgeConf(SAMPLE_PATTERN);
        ArrayDeque<byte[]> big = new ArrayDeque<>();
        for (int i = 0; i < 11000; i++) {
            big.add(new byte[1024]);
        }
        KryoValuesSerializer serializer = new KryoValuesSerializer(conf);
        KryoValuesDeserializer deserializer = new KryoValuesDeserializer(conf);
        byte[] bytes = serializer.serialize(Collections.singletonList(big));
        assertTrue(bytes.length > SAMPLE_MAX_BYTES, "payload must exceed the maxbytes limit, was " + bytes.length);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> deserializer.deserialize(bytes));
        assertTrue(Utils.exceptionCauseIsInstanceOf(InvalidClassException.class, ex),
                "expected the maxbytes rejection in the cause chain, got: " + ex);
    }

    @Test
    public void testMaxArrayLimitRejectsOversizedArray() {
        // One big array can ride past the byte cap, because an array passes the filter before its contents are read;
        // maxarray is what bounds the allocation itself.
        Map<String, Object> conf = bridgeConf("maxarray=1024");
        ArrayDeque<byte[]> payload = new ArrayDeque<>();
        payload.add(new byte[4096]);
        assertRejectedOnRead(conf, payload);
    }

    @Test
    public void testOversizedDeclaredLengthRejectedOnBufferedInput() {
        // A fixed-width prefix (Output.writeInt and Input.readInt are symmetric 4-byte reads) declares a thousand
        // bytes where only one follows; the mismatch fails up front, before the new byte[len] allocation can balloon.
        Output out = new Output(16);
        out.writeInt(1000);
        out.writeByte(0);
        SerializableSerializer serializer = new SerializableSerializer();
        KryoException ex = assertThrows(KryoException.class,
                () -> serializer.read(null, new Input(out.toBytes()), Object.class));
        assertTrue(ex.getMessage().contains("declared: 1000"),
                "error should name the declared length, got: " + ex.getMessage());
    }

    @Test
    public void testStreamBackedInputIsExemptFromUpperBoundGuard() throws IOException {
        // Stream-backed input is not length-checked: at prefix-read time the stream may have delivered only part of
        // the value, with the rest still arriving, so an upper bound there would reject well-formed input and only
        // the negative-length check applies.
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(new byte[100000]);
        }
        byte[] payload = bos.toByteArray();

        Output out = new Output(4096, Integer.MAX_VALUE);
        out.writeInt(payload.length);
        out.writeBytes(payload);
        // Small buffer on purpose: after the 4-byte prefix the buffer holds fewer bytes than declared.
        Input streamBacked = new Input(new ByteArrayInputStream(out.toBytes()), 1024);
        byte[] result = (byte[]) new SerializableSerializer().read(null, streamBacked, Object.class);
        // The declared length counted the java-serialization framing; the object that comes back is the original array.
        assertEquals(100000, result.length);
    }

    @Test
    public void testNegativeDeclaredLengthRejected() {
        // An all-ones 4-byte length prefix decodes as -1: no legitimate writer produces a negative length, buffered or streamed.
        SerializableSerializer serializer = new SerializableSerializer();
        KryoException ex = assertThrows(KryoException.class,
                () -> serializer.read(null, new Input(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}),
                        Object.class));
        assertTrue(ex.getMessage().contains("-1"), "error should name the negative length, got: " + ex.getMessage());
    }

    @Test
    public void testMergeWithExistingReturnsConfiguredWhenNoExistingFilter() {
        ObjectInputFilter configured = ObjectInputFilter.Config.createFilter("!java.util.PriorityQueue");
        assertSame(configured, SerializableSerializer.mergeWithExisting(configured, null));
    }

    @Test
    public void testMergeWithExistingRejectsClassDeniedByEitherFilter() {
        ObjectInputFilter configured = ObjectInputFilter.Config.createFilter("!java.util.PriorityQueue");
        ObjectInputFilter existing = ObjectInputFilter.Config.createFilter("!java.util.ArrayDeque");
        ObjectInputFilter merged = SerializableSerializer.mergeWithExisting(configured, existing);
        // The configured filter's denial survives the merge...
        assertEquals(ObjectInputFilter.Status.REJECTED, merged.checkInput(info(PriorityQueue.class)));
        // ...and the existing (e.g. JVM-wide) filter's denial is not replaced by it.
        assertEquals(ObjectInputFilter.Status.REJECTED, merged.checkInput(info(ArrayDeque.class)));
    }

    @Test
    public void testMergeWithExistingEnforcesTighterLimit() {
        ObjectInputFilter configured = ObjectInputFilter.Config.createFilter("maxarray=1000");
        ObjectInputFilter existing = ObjectInputFilter.Config.createFilter("maxarray=10");
        ObjectInputFilter merged = SerializableSerializer.mergeWithExisting(configured, existing);
        // 500 fits the configured limit but exceeds the existing one; the merge keeps the tighter bound.
        assertEquals(ObjectInputFilter.Status.REJECTED, merged.checkInput(info(byte[].class, 500)));
    }
}
