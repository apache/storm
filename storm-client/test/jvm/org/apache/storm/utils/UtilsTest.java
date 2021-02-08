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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
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
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class UtilsTest {
    public static final Logger LOG = LoggerFactory.getLogger(UtilsTest.class);

    @Test
    public void isZkAuthenticationConfiguredTopologyTest() {
        Assert.assertFalse(
            "Returns null if given null config",
            Utils.isZkAuthenticationConfiguredTopology(null));

        Assert.assertFalse(
            "Returns false if scheme key is missing",
            Utils.isZkAuthenticationConfiguredTopology(emptyMockMap()));

        Assert.assertFalse(
            "Returns false if scheme value is null",
            Utils.isZkAuthenticationConfiguredTopology(topologyMockMap(null)));

        Assert.assertTrue(
            "Returns true if scheme value is string",
            Utils.isZkAuthenticationConfiguredTopology(topologyMockMap("foobar")));
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
        doParseJvmHeapMemByChildOptsTest(message, Arrays.asList(opt), expected);
    }

    private void doParseJvmHeapMemByChildOptsTest(String message, List<String> opts, double expected) {
        Assert.assertEquals(message, expected, Utils.parseJvmHeapMemByChildOpts(opts, 123.0), 0);
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

    public void getConfiguredClientThrowsRuntimeExceptionOnBadArgsTest() throws TTransportException {
        Map<String, Object> config = ConfigUtils.readStormConfig();
        config.put(Config.STORM_NIMBUS_RETRY_TIMES, 0);

        try {
            new NimbusClient(config, "", 65535);
            Assert.fail("Expected exception to be thrown");
        } catch (RuntimeException e) {
            Assert.assertTrue(
                "Cause is not TTransportException " + e,
                Utils.exceptionCauseIsInstanceOf(TTransportException.class, e));
        }
    }

    @Test
    public void isZkAuthenticationConfiguredStormServerTest() {
        Assert.assertFalse(
            "Returns false if given null config",
            Utils.isZkAuthenticationConfiguredStormServer(null));

        Assert.assertFalse(
            "Returns false if scheme key is missing",
            Utils.isZkAuthenticationConfiguredStormServer(emptyMockMap()));

        Assert.assertFalse(
            "Returns false if scheme value is null",
            Utils.isZkAuthenticationConfiguredStormServer(serverMockMap(null)));

        Assert.assertTrue(
            "Returns true if scheme value is string",
            Utils.isZkAuthenticationConfiguredStormServer(serverMockMap("foobar")));
    }

    @Test
    public void isZkAuthenticationConfiguredStormServerWithPropertyTest() {
        String key = "java.security.auth.login.config";
        String oldValue = System.getProperty(key);
        try {
            System.setProperty("java.security.auth.login.config", "anything");
            Assert.assertTrue(Utils.isZkAuthenticationConfiguredStormServer(emptyMockMap()));
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
        Assert.assertTrue(Utils.isValidConf(map0, map0));
    }

    @Test
    public void testIsValidConfIdentical() {
        Map<String, Object> map1 = ImmutableMap.of("k0", ImmutableList.of(1L, 2L), "k1", ImmutableSet.of('s', 'f'),
                                                   "k2", "as");
        Assert.assertTrue(Utils.isValidConf(map1, map1));
    }

    @Test
    public void testIsValidConfEqual() {
        Map<String, Object> map1 = ImmutableMap.of("k0", ImmutableList.of(1L, 2L), "k1", ImmutableSet.of('s', 'f'),
                                                   "k2", "as");
        Map<String, Object> map2 = ImmutableMap.of("k0", ImmutableList.of(1L, 2L), "k1", ImmutableSet.of('s', 'f'),
                                                   "k2", "as");
        Assert.assertTrue(Utils.isValidConf(map1, map2)); // test deep equal
    }

    @Test
    public void testIsValidConfNotEqual() {
        Map<String, Object> map1 = ImmutableMap.of("k0", ImmutableList.of(1L, 2L), "k1", ImmutableSet.of('s', 'f'),
                                                   "k2", "as");
        Map<String, Object> map3 = ImmutableMap.of("k0", ImmutableList.of(1L, 2L), "k1", ImmutableSet.of('s', 't'),
                                                   "k2", "as");
        Assert.assertFalse(Utils.isValidConf(map1, map3));
    }

    @Test
    public void testIsValidConfPrimitiveNotEqual() {
        Map<String, Object> map4 = ImmutableMap.of("k0", 2L);
        Map<String, Object> map5 = ImmutableMap.of("k0", 3L);
        Assert.assertFalse(Utils.isValidConf(map4, map5));
    }

    @Test
    public void testIsValidConfEmptyNotEqual() {
        Map<String, Object> map0 = ImmutableMap.of();
        Map<String, Object> map5 = ImmutableMap.of("k0", 3L);
        Assert.assertFalse(Utils.isValidConf(map0, map5));
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
}
