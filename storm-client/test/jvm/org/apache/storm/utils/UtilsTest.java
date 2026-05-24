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

package org.apache.storm.utils;

import java.io.IOException;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.shade.com.google.common.collect.ImmutableMap;
import org.apache.storm.shade.com.google.common.collect.ImmutableSet;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.storm.utils.Utils.handleUncaughtException;
import static org.junit.jupiter.api.Assertions.*;

public class UtilsTest {
    public static final Logger LOG = LoggerFactory.getLogger(UtilsTest.class);

    @Test
    public void isZkAuthenticationConfiguredTopologyTest() {
        assertFalse(
            Utils.isZkAuthenticationConfiguredTopology(null),
            "Returns null if given null config");

        assertFalse(
            Utils.isZkAuthenticationConfiguredTopology(emptyMockMap()),
            "Returns false if scheme key is missing");

        assertFalse(
            Utils.isZkAuthenticationConfiguredTopology(topologyMockMap(null)),
            "Returns false if scheme value is null");

        assertTrue(
            Utils.isZkAuthenticationConfiguredTopology(topologyMockMap("foobar")),
            "Returns true if scheme value is string");
    }

    private Map<String, Object> topologyMockMap(String value) {
        return mockMap(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME, value);
    }

    private Map<String, Object> mockMap(String key, String value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    private Map<String, Object> serverMockMap(String value) {
        return mockMap(Config.STORM_ZOOKEEPER_AUTH_SCHEME, value);
    }

    private Map<String, Object> emptyMockMap() {
        return new HashMap<>();
    }

    private void doParseJvmHeapMemByChildOptsTest(String message, String opt, double expected) {
        doParseJvmHeapMemByChildOptsTest(message, Collections.singletonList(opt), expected);
    }

    private void doParseJvmHeapMemByChildOptsTest(String message, List<String> opts, double expected) {
        assertEquals(expected, Utils.parseJvmHeapMemByChildOpts(opts, 123.0), 0, message);
    }

    @Test
    public void parseJvmHeapMemByChildOptsTestK() {
        doParseJvmHeapMemByChildOptsTest("Xmx1024k results in 1 MB", "Xmx1024k", 1.0);
        doParseJvmHeapMemByChildOptsTest("Xmx1024K results in 1 MB", "Xmx1024K", 1.0);
        doParseJvmHeapMemByChildOptsTest("-Xmx1024k results in 1 MB", "-Xmx1024k", 1.0);
    }

    @Test
    public void parseJvmHeapMemByChildOptsTestM() {
        doParseJvmHeapMemByChildOptsTest("Xmx100M results in 100 MB", "Xmx100m", 100.0);
        doParseJvmHeapMemByChildOptsTest("Xmx100m results in 100 MB", "Xmx100M", 100.0);
        doParseJvmHeapMemByChildOptsTest("-Xmx100M results in 100 MB", "-Xmx100m", 100.0);
        doParseJvmHeapMemByChildOptsTest("-Xmx2048M results in 2048 MB", "-Xmx2048m", 2048.0);
    }

    @Test
    public void parseJvmHeapMemByChildOptsTestG() {
        doParseJvmHeapMemByChildOptsTest("Xmx1g results in 1024 MB", "Xmx1g", 1024.0);
        doParseJvmHeapMemByChildOptsTest("Xmx1G results in 1024 MB", "Xmx1G", 1024.0);
        doParseJvmHeapMemByChildOptsTest("-Xmx1g results in 1024 MB", "-Xmx1g", 1024.0);
        doParseJvmHeapMemByChildOptsTest("-Xmx2g results in 2048 MB", "-Xmx2g", 2048.0);
    }

    @Test
    public void parseJvmHeapMemByChildOptsTestNoMatch() {
        doParseJvmHeapMemByChildOptsTest("Unmatched unit results in default", "Xmx1t", 123.0);
        doParseJvmHeapMemByChildOptsTest("Unmatched option results in default", "Xms1g", 123.0);
    }

    @Test
    public void parseJvmHeapMemByChildOptsTestNulls() {
        doParseJvmHeapMemByChildOptsTest("Null value results in default", (String) null, 123.0);
        doParseJvmHeapMemByChildOptsTest("Null list results in default", (List<String>) null, 123.0);
    }

    @Test
    public void parseJvmHeapMemByChildOptsTestExtraChars() {
        doParseJvmHeapMemByChildOptsTest("Leading characters are ignored", "---Xmx1g", 1024.0);
        doParseJvmHeapMemByChildOptsTest("Trailing characters are ignored", "Xmx1ggggg", 1024.0);
    }

    @Test
    public void parseJvmHeapMemByChildOptsTestFirstMatch() {
        doParseJvmHeapMemByChildOptsTest("First valid match is used",
                                         Arrays.asList(null, "Xmx1t", "Xmx1g", "Xms1024k Xmx1024k", "Xmx100m"),
                                         1024.0);
    }

    @Test
    public void isZkAuthenticationConfiguredStormServerTest() {
        assertFalse(
            Utils.isZkAuthenticationConfiguredStormServer(null),
            "Returns false if given null config");

        assertFalse(
            Utils.isZkAuthenticationConfiguredStormServer(emptyMockMap()),
            "Returns false if scheme key is missing");

        assertFalse(
            Utils.isZkAuthenticationConfiguredStormServer(serverMockMap(null)),
            "Returns false if scheme value is null");

        assertTrue(
            Utils.isZkAuthenticationConfiguredStormServer(serverMockMap("foobar")),
            "Returns true if scheme value is string");
    }

    @Test
    public void isZkAuthenticationConfiguredStormServerWithPropertyTest() {
        String key = "java.security.auth.login.config";
        String oldValue = System.getProperty(key);
        try {
            System.setProperty("java.security.auth.login.config", "anything");
            assertTrue(Utils.isZkAuthenticationConfiguredStormServer(emptyMockMap()));
        } finally {
            // reset property
            if (oldValue == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, oldValue);
            }
        }
    }

    @Test
    public void testIsValidConfEmpty() {
        Map<String, Object> map0 = ImmutableMap.of();
        assertTrue(Utils.isValidConf(map0, map0));
    }

    @Test
    public void testIsValidConfIdentical() {
        Map<String, Object> map1 = ImmutableMap.of("k0", ImmutableList.of(1L, 2L), "k1", ImmutableSet.of('s', 'f'),
                                                   "k2", "as");
        assertTrue(Utils.isValidConf(map1, map1));
    }

    @Test
    public void testIsValidConfEqual() {
        Map<String, Object> map1 = ImmutableMap.of("k0", ImmutableList.of(1L, 2L), "k1", ImmutableSet.of('s', 'f'),
                                                   "k2", "as");
        Map<String, Object> map2 = ImmutableMap.of("k0", ImmutableList.of(1L, 2L), "k1", ImmutableSet.of('s', 'f'),
                                                   "k2", "as");
        assertTrue(Utils.isValidConf(map1, map2)); // test deep equal
    }

    @Test
    public void testIsValidConfNotEqual() {
        Map<String, Object> map1 = ImmutableMap.of("k0", ImmutableList.of(1L, 2L), "k1", ImmutableSet.of('s', 'f'),
                                                   "k2", "as");
        Map<String, Object> map3 = ImmutableMap.of("k0", ImmutableList.of(1L, 2L), "k1", ImmutableSet.of('s', 't'),
                                                   "k2", "as");
        assertFalse(Utils.isValidConf(map1, map3));
    }

    @Test
    public void testIsValidConfPrimitiveNotEqual() {
        Map<String, Object> map4 = ImmutableMap.of("k0", 2L);
        Map<String, Object> map5 = ImmutableMap.of("k0", 3L);
        assertFalse(Utils.isValidConf(map4, map5));
    }

    @Test
    public void testIsValidConfEmptyNotEqual() {
        Map<String, Object> map0 = ImmutableMap.of();
        Map<String, Object> map5 = ImmutableMap.of("k0", 3L);
        assertFalse(Utils.isValidConf(map0, map5));
    }

    @Test
    public void checkVersionInfo() {
        Map<String, String> versions = new HashMap<>();
        String key = VersionInfo.getVersion();
        assertNotEquals("Unknown", key, "Looks like we don't know what version of storm we are");
        versions.put(key, System.getProperty("java.class.path"));
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.SUPERVISOR_WORKER_VERSION_CLASSPATH_MAP, versions);
        NavigableMap<String, IVersionInfo> alternativeVersions = Utils.getAlternativeVersionsMap(conf);
        assertEquals(1, alternativeVersions.size());
        IVersionInfo found = alternativeVersions.get(key);
        assertNotNull(found);
        assertEquals(key, found.getVersion());
    }

    @Test
    public void testFindComponentCycles() {
        class CycleDetectionScenario {
            final String testName;
            final String testDescription;
            final StormTopology topology;
            final int expectedCycles;

            CycleDetectionScenario() {
                testName = "dummy";
                testDescription = "dummy test";
                topology = null;
                expectedCycles = 0;
            }

            CycleDetectionScenario(String testName, String testDescription, StormTopology topology, int expectedCycles) {
                this.testName = testName.replace(' ', '-');
                this.testDescription = testDescription;
                this.topology = topology;
                this.expectedCycles = expectedCycles;
            }

            public List<CycleDetectionScenario> createTestScenarios() {
                List<CycleDetectionScenario> ret = new ArrayList<>();
                int testNo = 0;
                CycleDetectionScenario s;
                TopologyBuilder tb;

                // Base case
                {
                    testNo++;
                    tb = new TopologyBuilder();
                    tb.setSpout("spout1", new TestWordSpout(), 10);
                    tb.setBolt("bolt1", new TestWordCounter(), 10).shuffleGrouping("spout1");
                    tb.setBolt("bolt2", new TestWordCounter(), 10).shuffleGrouping("spout1");
                    tb.setBolt("bolt11", new TestWordCounter(), 10).shuffleGrouping("bolt1");
                    tb.setBolt("bolt12", new TestWordCounter(), 10).shuffleGrouping("bolt1");
                    tb.setBolt("bolt21", new TestWordCounter(), 10).shuffleGrouping("bolt2");
                    tb.setBolt("bolt22", new TestWordCounter(), 10).shuffleGrouping("bolt2");
                    s = new CycleDetectionScenario(String.format("(%d) Base", testNo),
                            "Three level component hierarchy with no loops",
                            tb.createTopology(),
                            0);
                    ret.add(s);
                }

                // single loop with one bolt
                {
                    testNo++;
                    tb = new TopologyBuilder();
                    tb.setSpout("spout1", new TestWordSpout(), 10);
                    tb.setBolt("bolt1", new TestWordCounter(), 10).shuffleGrouping("spout1");
                    tb.setBolt("bolt2", new TestWordCounter(), 10).shuffleGrouping("spout1");
                    tb.setBolt("bolt11", new TestWordCounter(), 10).shuffleGrouping("bolt1");
                    tb.setBolt("bolt12", new TestWordCounter(), 10).shuffleGrouping("bolt1");
                    tb.setBolt("bolt21", new TestWordCounter(), 10).shuffleGrouping("bolt2");
                    tb.setBolt("bolt22", new TestWordCounter(), 10).shuffleGrouping("bolt2");
                    // loop bolt 3  (also connect bolt3 to spout 1)
                    tb.setBolt("bolt3", new TestWordCounter(), 10).shuffleGrouping("spout1").shuffleGrouping("bolt3");
                    ret.add(new CycleDetectionScenario(String.format("(%d) One Loop", testNo),
                            "Three level component hierarchy with 1 cycle in bolt3",
                            tb.createTopology(),
                            1));
                }

                // single loop with three bolts
                {
                    testNo++;
                    tb = new TopologyBuilder();
                    tb.setSpout("spout1", new TestWordSpout(), 10);
                    tb.setBolt("bolt1", new TestWordCounter(), 10).shuffleGrouping("spout1");
                    tb.setBolt("bolt2", new TestWordCounter(), 10).shuffleGrouping("spout1");
                    tb.setBolt("bolt11", new TestWordCounter(), 10).shuffleGrouping("bolt1");
                    tb.setBolt("bolt12", new TestWordCounter(), 10).shuffleGrouping("bolt1");
                    tb.setBolt("bolt21", new TestWordCounter(), 10).shuffleGrouping("bolt2");
                    tb.setBolt("bolt22", new TestWordCounter(), 10).shuffleGrouping("bolt2");
                    // loop bolt 3 -> 4 -> 5 -> 3 (also connect bolt3 to spout1)
                    tb.setBolt("bolt3", new TestWordCounter(), 10).shuffleGrouping("spout1").shuffleGrouping("bolt5");
                    tb.setBolt("bolt4", new TestWordCounter(), 10).shuffleGrouping("bolt3");
                    tb.setBolt("bolt5", new TestWordCounter(), 10).shuffleGrouping("bolt4");
                    ret.add(new CycleDetectionScenario(String.format("(%d) One Loop", testNo),
                            "Four level component hierarchy with 1 cycle in bolt3,bolt4,bolt5",
                            tb.createTopology(),
                            1));
                }

                // two loops with three bolts, and one bolt
                {
                    testNo++;
                    tb = new TopologyBuilder();
                    tb.setSpout("spout1", new TestWordSpout(), 10);
                    tb.setBolt("bolt1", new TestWordCounter(), 10).shuffleGrouping("spout1");
                    tb.setBolt("bolt2", new TestWordCounter(), 10).shuffleGrouping("spout1");
                    tb.setBolt("bolt11", new TestWordCounter(), 10).shuffleGrouping("bolt1");
                    tb.setBolt("bolt12", new TestWordCounter(), 10).shuffleGrouping("bolt1");
                    tb.setBolt("bolt21", new TestWordCounter(), 10).shuffleGrouping("bolt2");
                    tb.setBolt("bolt22", new TestWordCounter(), 10).shuffleGrouping("bolt2");
                    // loop bolt 3 -> 4 -> 5 -> 3 (also connect bolt3 to spout1)
                    tb.setBolt("bolt3", new TestWordCounter(), 10).shuffleGrouping("spout1").shuffleGrouping("bolt5");
                    tb.setBolt("bolt4", new TestWordCounter(), 10).shuffleGrouping("bolt3");
                    tb.setBolt("bolt5", new TestWordCounter(), 10).shuffleGrouping("bolt4");
                    // loop bolt 6  (also connect bolt6 to spout 1)
                    tb.setBolt("bolt6", new TestWordCounter(), 10).shuffleGrouping("spout1").shuffleGrouping("bolt6");
                    ret.add(new CycleDetectionScenario(String.format("(%d) Two Loops", testNo),
                            "Four level component hierarchy with 2 cycles in bolt3,bolt4,bolt5 and bolt6",
                            tb.createTopology(),
                            2));
                }

                // complex cycle
                {
                    // (S1 -> B1 -> B2 -> B3 -> B4 <- S2), (B4 -> B3), (B4 -> B1)
                    testNo++;
                    tb = new TopologyBuilder();
                    tb.setSpout("spout1", new TestWordSpout(), 10);
                    tb.setSpout("spout2", new TestWordSpout(), 10);
                    tb.setBolt("bolt1", new TestWordCounter(), 10).shuffleGrouping("spout1").shuffleGrouping("bolt4");
                    tb.setBolt("bolt2", new TestWordCounter(), 10).shuffleGrouping("bolt1");
                    tb.setBolt("bolt3", new TestWordCounter(), 10).shuffleGrouping("bolt2").shuffleGrouping("bolt4");
                    tb.setBolt("bolt4", new TestWordCounter(), 10).shuffleGrouping("bolt3").shuffleGrouping("spout2");
                    ret.add(new CycleDetectionScenario(String.format("(%d) Complex Loops#1", testNo),
                            "Complex cycle (S1 -> B1 -> B2 -> B3 -> B4 <- S2), (B4 -> B3), (B4 -> B1)",
                            tb.createTopology(),
                            1));
                }

                // another complex
                {
                    testNo++;
                    tb = new TopologyBuilder();
                    tb.setSpout("spout1", new TestWordSpout(), 10);
                    tb.setSpout("spout2", new TestWordSpout(), 10);
                    tb.setBolt("bolt1", new TestWordCounter(), 10).shuffleGrouping("spout1").shuffleGrouping("bolt4").shuffleGrouping("bolt2");
                    tb.setBolt("bolt2", new TestWordCounter(), 10).shuffleGrouping("bolt1");
                    tb.setBolt("bolt3", new TestWordCounter(), 10).shuffleGrouping("bolt2").shuffleGrouping("bolt4");
                    tb.setBolt("bolt4", new TestWordCounter(), 10).shuffleGrouping("spout2");
                    ret.add(new CycleDetectionScenario(String.format("(%d) Complex Loops#2", testNo),
                            "Complex cycle 2 (S1 -> B1 <-> B2 -> B3 ), (S2 -> B4 -> B3), (B4 -> B1)",
                            tb.createTopology(),
                            1));
                }

                // no spouts but with loops; the loops wont be detected
                {
                    testNo++;
                    tb = new TopologyBuilder();
                    tb.setBolt("bolt1", new TestWordCounter(), 10).shuffleGrouping("bolt4").shuffleGrouping("bolt2");
                    tb.setBolt("bolt2", new TestWordCounter(), 10).shuffleGrouping("bolt1");
                    tb.setBolt("bolt3", new TestWordCounter(), 10).shuffleGrouping("bolt2").shuffleGrouping("bolt4");
                    tb.setBolt("bolt4", new TestWordCounter(), 10);
                    ret.add(new CycleDetectionScenario(String.format("(%d) No spout complex loops", testNo),
                            "No Spouts, but with cycles (B1 <-> B2 -> B3 ), (B4 -> B3), (B4 -> B1)",
                            tb.createTopology(),
                            0));
                }

                // now some randomly generated topologies
                int maxSpouts = 10;
                int maxBolts = 30;
                int randomTopoCnt = 100;
                for (int iRandTest = 0; iRandTest < randomTopoCnt; iRandTest++) {
                    testNo++;
                    tb = new TopologyBuilder();

                    // topology component and connection counts
                    int spoutCnt = ThreadLocalRandom.current().nextInt(0, maxSpouts) + 1;
                    int boltCnt = ThreadLocalRandom.current().nextInt(0, maxBolts) + 1;
                    int spoutToBoltConnectionCnt = ThreadLocalRandom.current().nextInt(spoutCnt * boltCnt) + 1;
                    int boltToBoltConnectionCnt =  ThreadLocalRandom.current().nextInt(boltCnt * boltCnt) + 1;

                    Map<Integer, BoltDeclarer> boltDeclarers = new HashMap<>();
                    for (int iSpout = 0 ; iSpout  < spoutCnt ; iSpout++) {
                        tb.setSpout("spout" + iSpout, new TestWordSpout(), 10);
                    }
                    for (int iBolt = 0 ; iBolt  < boltCnt ; iBolt++) {
                        boltDeclarers.put(iBolt, tb.setBolt("bolt" + iBolt, new TestWordCounter(), 10));
                    }
                    // spout to bolt connections
                    for (int i = 0 ; i < spoutToBoltConnectionCnt ; i++) {
                        int iSpout = ThreadLocalRandom.current().nextInt(0, spoutCnt);
                        int iBolt = ThreadLocalRandom.current().nextInt(0, boltCnt);
                        boltDeclarers.get(iBolt).shuffleGrouping("spout" + iSpout);
                    }
                    // bolt to bolt connections
                    for (int i = 0 ; i < boltToBoltConnectionCnt ; i++) {
                        int iBolt1 = ThreadLocalRandom.current().nextInt(0, boltCnt);
                        int iBolt2 = ThreadLocalRandom.current().nextInt(0, boltCnt);
                        boltDeclarers.get(iBolt2).shuffleGrouping("bolt" + iBolt1);
                    }
                    ret.add(new CycleDetectionScenario(String.format("(%d) Random Topo#%d", testNo, iRandTest),
                            String.format("Random topology #%d, spouts=%d, bolts=%d, connections: fromSpouts=%d/fromBolts=%d",
                                    iRandTest, spoutCnt, boltCnt, spoutToBoltConnectionCnt, boltToBoltConnectionCnt),
                            tb.createTopology(),
                            -1));
                }

                return ret;
            }
        }
        List<String> testFailures = new ArrayList<>();

        new CycleDetectionScenario().createTestScenarios().forEach(x -> {
            LOG.info("==================== Running Test Scenario: {} =======================", x.testName);
            LOG.info("{}: {}", x.testName, x.testDescription);

            List<List<String>> loops = Utils.findComponentCycles(x.topology, x.testName);
            if (x.expectedCycles >= 0) {
                if (!loops.isEmpty()) {
                    LOG.info("{} detected loops are \"{}\"", x.testName,
                            loops.stream()
                                    .map(y -> String.join(",", y))
                                    .collect(Collectors.joining(" ; "))
                    );
                }
                if (loops.size() != x.expectedCycles) {
                    testFailures.add(
                            String.format("Test \"%s\" failed, detected cycles=%d does not match expected=%d for \"%s\"",
                                    x.testName, loops.size(), x.expectedCycles, x.testDescription));
                    if (!loops.isEmpty()) {
                        testFailures.add(
                                String.format("\t\tdetected loops are \"%s\"",
                                        loops.stream()
                                                .map(y -> String.join(",", y))
                                                .collect(Collectors.joining(" ; "))
                                )
                        );
                    }
                }
            } else {
                // these are random topologies, with indeterminate number of loops
                LOG.info("{} detected loop count is \"{}\"", x.testName, loops.size());
            }
        });
        if (!testFailures.isEmpty()) {
            fail(String.join("\n", testFailures));
        }
    }

    @Test
    public void testHandleUncaughtExceptionSwallowsCausedAndDerivedExceptions() {
        Set<Class<?>> allowedExceptions = new HashSet<>(Arrays.asList(new Class<?>[]{ IOException.class }));
        try {
            handleUncaughtException(new IOException(), allowedExceptions, false);
        } catch(Throwable unexpected) {
            fail("Should have swallowed IOException!", unexpected);
        }

        try {
            handleUncaughtException(new SocketException(), allowedExceptions, false);
        } catch(Throwable unexpected) {
            fail("Should have swallowed Throwable derived from IOException!", unexpected);
        }

        try {
            handleUncaughtException(new TTransportException(new IOException()), allowedExceptions, false);
        } catch(Throwable unexpected) {
            fail("Should have swallowed Throwable caused by an IOException!", unexpected);
        }

        try {
            handleUncaughtException(new TTransportException(new SocketException()), allowedExceptions, false);
        } catch(Throwable unexpected) {
            fail("Should have swallowed Throwable caused by a Throwable derived from IOException!", unexpected);
        }

        Throwable t = new NullPointerException();
        String expectationMessage = "Should have thrown an Error() with a cause of NullPointerException";
        try {
            handleUncaughtException(t, allowedExceptions, false);
            fail(expectationMessage);
        } catch(Error expected) {
            assertEquals(expected.getCause(), t, expectationMessage);
        } catch(Throwable unexpected) {
            fail(expectationMessage, unexpected);
        }
    }

    @Test
    void compress_nullInput_returnsNull() {
        assertNull(Utils.ZstdUtils.compress(null, 3));
    }

    @Test
    void compress_emptyInput_returnsEmptyArray() {
        byte[] result = Utils.ZstdUtils.compress(new byte[0], 3);
        assertNotNull(result);
        assertEquals(0, result.length);
    }

    @Test
    void compress_singleByte_producesZstdFrame() {
        byte[] input = {0x42};
        byte[] compressed = Utils.ZstdUtils.compress(input, 3);
        assertTrue(Utils.ZstdUtils.isZstd(compressed),
                "Compressed output should start with the Zstd magic number");
    }

    @Test
    void compress_smallPayload_roundtrips() {
        byte[] input = "Hello, ZSTD!".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = Utils.ZstdUtils.compress(input, 3);
        byte[] decompressed = Utils.ZstdUtils.decompress(compressed, 1024 * 1024);
        assertArrayEquals(input, decompressed);
    }

    @Test
    void compress_largeRepetitivePayload_isSmallerThanOriginal() {
        // Highly compressible: 64 KB of zeros
        byte[] input = new byte[64 * 1024];
        byte[] compressed = Utils.ZstdUtils.compress(input, 3);
        assertTrue(compressed.length < input.length,
                "Compressed size should be smaller than original for repetitive data");
    }

    void compress_variousLevels_roundtrip() {
        byte[] input = zstdSampleData(4096);
        for (int level : Set.of(1, 3, 9, 19)) {
            byte[] compressed = Utils.ZstdUtils.compress(input, level);
            byte[] decompressed = Utils.ZstdUtils.decompress(compressed, input.length * 2);
            assertArrayEquals(input, decompressed,
                    "Round-trip must be lossless at compression level " + level);
        }
    }

    @Test
    void compress_highEntropData_doesNotThrow() {
        // Random-ish bytes — Zstd may not shrink them, but must not fail
        byte[] input = zstdSampleData(8192);
        assertDoesNotThrow(() -> Utils.ZstdUtils.compress(input, 3));
    }

    @Test
    void decompress_nullInput_returnsNull() {
        assertNull(Utils.ZstdUtils.decompress(null, 1024 * 1024));
    }

    @Test
    void decompress_emptyInput_returnsEmptyArray() {
        byte[] result = Utils.ZstdUtils.decompress(new byte[0], 1024 * 1024);
        assertNotNull(result);
        assertEquals(0, result.length);
    }

    @Test
    void decompress_validFrame_recoversOriginal() {
        byte[] original = "Unit test payload".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = Utils.ZstdUtils.compress(original, 3);
        byte[] recovered = Utils.ZstdUtils.decompress(compressed, 1024 * 1024);
        assertArrayEquals(original, recovered);
    }

    @Test
    void decompress_corruptedData_throwsRuntimeException() {
        byte[] garbage = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07};
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> Utils.ZstdUtils.decompress(garbage, 1024 * 1024));
        assertTrue(ex.getMessage().contains("Zstd decompression failed"),
                "Exception message should describe the failure");
    }

    @Test
    void decompress_exceedsMaxBytes_throwsRuntimeException() {
        // Compress a 4 KB payload but only allow 10 bytes out
        byte[] input = zstdSampleData(4096);
        byte[] compressed = Utils.ZstdUtils.compress(input, 3);

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> Utils.ZstdUtils.decompress(compressed, 10));
        assertTrue(ex.getMessage().contains("Zstd decompression failed"),
                "Should throw when decompressed size exceeds the limit");
        assertTrue(ex.getCause().getMessage().contains("Decompression threshold exceeded"),
                "Root cause should describe the threshold breach");
    }

    @Test
    void decompress_exactlyAtLimit_succeeds() {
        byte[] input = "abc".getBytes(StandardCharsets.UTF_8); // 3 bytes
        byte[] compressed = Utils.ZstdUtils.compress(input, 3);
        // Allow exactly 3 bytes — should succeed
        byte[] result = Utils.ZstdUtils.decompress(compressed, 3);
        assertArrayEquals(input, result);
    }

    @Test
    void decompress_truncatedFrame_throwsRuntimeException() {
        byte[] input = zstdSampleData(256);
        byte[] compressed = Utils.ZstdUtils.compress(input, 3);
        // Slice off the last third to simulate truncation
        byte[] truncated = new byte[compressed.length * 2 / 3];
        System.arraycopy(compressed, 0, truncated, 0, truncated.length);

        assertThrows(RuntimeException.class,
                () -> Utils.ZstdUtils.decompress(truncated, 1024 * 1024));
    }

    @Test
    void isZstd_nullInput_returnsFalse() {
        assertFalse(Utils.ZstdUtils.isZstd(null));
    }

    @Test
    void isZstd_emptyArray_returnsFalse() {
        assertFalse(Utils.ZstdUtils.isZstd(new byte[0]));
    }

    @Test
    void isZstd_arrayTooShort_returnsFalse() {
        // Only 3 bytes — not enough for the 4-byte magic
        assertFalse(Utils.ZstdUtils.isZstd(new byte[]{(byte) 0x28, (byte) 0xB5, (byte) 0x2F}));
    }

    @Test
    void isZstd_validMagicBytes_returnsTrue() {
        // Pad with enough trailing bytes so INT_HANDLE.get(bytes, 0) does not throw
        byte[] magic = new byte[]{(byte) 0x28, (byte) 0xB5, (byte) 0x2F, (byte) 0xFD, 0x00};
        assertTrue(Utils.ZstdUtils.isZstd(magic));
    }

    @Test
    void isZstd_validCompressedOutput_returnsTrue() {
        byte[] compressed = Utils.ZstdUtils.compress("test".getBytes(StandardCharsets.UTF_8), 3);
        assertTrue(Utils.ZstdUtils.isZstd(compressed),
                "Output of compress() must be recognised as Zstd");
    }

    @Test
    void isZstd_plainText_returnsFalse() {
        byte[] plain = "Hello, world!".getBytes(StandardCharsets.UTF_8);
        assertFalse(Utils.ZstdUtils.isZstd(plain));
    }

    @Test
    void isZstd_wrongMagicOrder_returnsFalse() {
        // Big-endian order of the same bytes — should NOT match
        byte[] wrongEndian = {(byte) 0xFD, (byte) 0x2F, (byte) 0xB5, (byte) 0x28, 0x00};
        assertFalse(Utils.ZstdUtils.isZstd(wrongEndian));
    }

    @Test
    void isZstd_almostCorrectMagic_returnsFalse() {
        // One byte off
        byte[] close = {(byte) 0x28, (byte) 0xB5, (byte) 0x2F, (byte) 0xFE, 0x00};
        assertFalse(Utils.ZstdUtils.isZstd(close));
    }

    @Test
    void roundTrip_emptyStringBytes_succeeds() {
        byte[] input = "".getBytes(StandardCharsets.UTF_8);
        // compress() returns the original array for empty input
        byte[] compressed = Utils.ZstdUtils.compress(input, 3);
        assertArrayEquals(input, compressed); // empty → empty, no compression
    }

    @Test
    void roundTrip_binaryData_succeeds() {
        byte[] input = zstdSampleData(1024);
        byte[] compressed = Utils.ZstdUtils.compress(input, 3);
        byte[] decompressed = Utils.ZstdUtils.decompress(compressed, input.length * 2);
        assertArrayEquals(input, decompressed);
    }

    @Test
    void roundTrip_unicodePayload_succeeds() {
        byte[] input = "apache storm - storm - apache".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = Utils.ZstdUtils.compress(input, 3);
        byte[] decompressed = Utils.ZstdUtils.decompress(compressed, input.length * 4);
        assertArrayEquals(input, decompressed);
    }

    private static byte[] zstdSampleData(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i % 256);
        }
        return data;
    }
}
