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

import org.apache.storm.Config;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

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

    private Map topologyMockMap(String value) {
        return mockMap(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME, value);
    }

    private Map mockMap(String key, String value) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(key, value);
        return map;
    }

    private Map serverMockMap(String value) {
        return mockMap(Config.STORM_ZOOKEEPER_AUTH_SCHEME, value);
    }

    private Map emptyMockMap() {
        return new HashMap<String, Object>();
    }

    @Test
    public void parseJvmHeapMemByChildOptsTest() {
        Assert.assertEquals(
                "1024K results in 1 MB",
                Utils.parseJvmHeapMemByChildOpts("Xmx1024K", 0.0).doubleValue(), 1.0, 0);

        Assert.assertEquals(
                "100M results in 100 MB",
                Utils.parseJvmHeapMemByChildOpts("Xmx100M", 0.0).doubleValue(), 100.0, 0);

        Assert.assertEquals(
                "1G results in 1024 MB",
                Utils.parseJvmHeapMemByChildOpts("Xmx1G", 0.0).doubleValue(), 1024.0, 0);

        Assert.assertEquals(
                "Unmatched value results in default",
                Utils.parseJvmHeapMemByChildOpts("Xmx1T", 123.0).doubleValue(), 123.0, 0);

        Assert.assertEquals(
                "Null value results in default",
                Utils.parseJvmHeapMemByChildOpts(null, 123.0).doubleValue(), 123.0, 0);
    }

    public void getConfiguredClientThrowsRuntimeExceptionOnBadArgsTest () throws TTransportException {
        Map config = ConfigUtils.readStormConfig();
        config.put(Config.STORM_NIMBUS_RETRY_TIMES, 0);

        try {
            new NimbusClient(config, "", 65535);
            Assert.fail("Expected exception to be thrown");
        } catch (RuntimeException e){
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

}
