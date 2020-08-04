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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.storm.Config;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.shade.com.google.common.collect.ImmutableMap;
import org.apache.storm.shade.com.google.common.collect.ImmutableSet;
import org.apache.storm.shade.com.google.common.collect.Sets;
import org.apache.storm.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;

public class UtilsTest {

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
        Assert.assertEquals(
            message,
            Utils.parseJvmHeapMemByChildOpts(opts, 123.0).doubleValue(), expected, 0);
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
    }

    @Test
    public void parseJvmHeapMemByChildOptsTestG() {
        doParseJvmHeapMemByChildOptsTest("Xmx1g results in 1024 MB", "Xmx1g", 1024.0);
        doParseJvmHeapMemByChildOptsTest("Xmx1G results in 1024 MB", "Xmx1G", 1024.0);
        doParseJvmHeapMemByChildOptsTest("-Xmx1g results in 1024 MB", "-Xmx1g", 1024.0);
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
        Assert.assertNotEquals("Unknown", key, "Looks like we don't know what version of storm we are");
        versions.put(key, System.getProperty("java.class.path"));
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.SUPERVISOR_WORKER_VERSION_CLASSPATH_MAP, versions);
        NavigableMap<String, IVersionInfo> alternativeVersions = Utils.getAlternativeVersionsMap(conf);
        Assert.assertEquals(1, alternativeVersions.size());
        IVersionInfo found = alternativeVersions.get(key);
        Assert.assertNotNull(found);
        Assert.assertEquals(key, found.getVersion());
    }
    @Test
    public void testFindSubstitutableVarNames() {
        Map<String, Set<String>> testCases = new LinkedHashMap<>();
        testCases.put("(1) this is a test %THIS_ISTEST-8% this is point %INValid-var% test %VALI_D-VAR% %%",
                new TreeSet<>(Arrays.asList("THIS_ISTEST-8", "VALI_D-VAR")));
        testCases.put("(2) this is a test %T1HIS_ISTEST-8% this is point %INValid-var% test %__VALI_D-VAR% %% %_1%",
                new TreeSet<>(Arrays.asList("T1HIS_ISTEST-8", "__VALI_D-VAR", "_1")));

        testCases.forEach((key, expected) -> {
            Set<String> foundVars = Utils.findSubstitutableVarNames(key);
            Set<String> disjunction = Sets.symmetricDifference(foundVars, expected);
            Assert.assertEquals(String.format("ERROR: In \"%s\" found != expected, differences=\"%s\"", key, disjunction),
                    expected, foundVars);
        });
    }

    @Test
    public void testGetDummySubstitutions() {
        Map<String, Object> dummySubs = Utils.WellKnownRuntimeSubstitutionVars.getDummySubstitutions();
        int expectedSize = Utils.WellKnownRuntimeSubstitutionVars.values().length;
        Set<String> expectedVars = Stream.of(Utils.WellKnownRuntimeSubstitutionVars.values())
                .map(Utils.WellKnownRuntimeSubstitutionVars::getVarName)
                .collect(Collectors.toSet());
        int foundSize = dummySubs.size();
        Set<String> foundVars = new HashSet<>(dummySubs.keySet());
        Collection<String> missingVars = Sets.difference(expectedVars, foundVars);
        if (!missingVars.isEmpty()) {
            String msg = String.format("Expected %d variables, found %d, missing values for \"%s\"", expectedSize, foundSize, missingVars);
            Assert.fail(msg);
        }
        Collection<String> extraVars = Sets.difference(foundVars, expectedVars);
        if (!extraVars.isEmpty()) {
            String msg = String.format("Expected %d variables, found %d, extra values for \"%s\"", expectedSize, foundSize, extraVars);
            Assert.fail(msg);
        }
    }

    @Test
    public void testValidateWorkerLaunchOptions() throws InvalidTopologyException {
        Map<String, Object> supervisorConf = new HashMap<>();
        Map<String, Object> topoConf = new HashMap<>();

        // both should be active
        supervisorConf.put(Config.WORKER_CHILDOPTS, "-DchildOpts1=val1");
        topoConf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-DchildOpts2=val2");

        // topoConf will override
        supervisorConf.put(Config.WORKER_GC_CHILDOPTS, "-DGcOpts=val1");
        topoConf.put(Config.TOPOLOGY_WORKER_GC_CHILDOPTS, "-DGcOpts=val2");

        supervisorConf.put("worker.profiler.childopts", "-DprofilerOpts=val1");
        topoConf.put(Config.TOPOLOGY_WORKER_LOGWRITER_CHILDOPTS, "-Df1=f2");

        Assert.assertTrue(Utils.validateWorkerLaunchOptions(supervisorConf, topoConf, null, false));

        topoConf.put(Config.TOPOLOGY_WORKER_GC_CHILDOPTS, "--XX:+UseG1GC"); // invalid syntax
        Assert.assertFalse(Utils.validateWorkerLaunchOptions(supervisorConf, topoConf, null, false));
    }
}
