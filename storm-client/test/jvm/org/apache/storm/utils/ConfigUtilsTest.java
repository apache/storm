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

package org.apache.storm.utils;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConfigUtilsTest {

    private Map<String, Object> mockMap(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    @Test
    public void getValueAsList_nullKeySupported() {
        String key = null;
        List<String> value = Collections.singletonList("test");
        Map<String, Object> map = mockMap(key, value);
        assertEquals(value, ConfigUtils.getValueAsList(key, map));
    }

    @Test
    public void getValueAsList_nullKeyNotSupported() {
        assertThrows(NullPointerException.class, () -> {
            String key = null;
            Map<String, Object> map = new Hashtable<>();
            ConfigUtils.getValueAsList(key, map);
        });
    }

    @Test
    public void getValueAsList_nullConfig() {
        assertThrows(IllegalArgumentException.class, () -> ConfigUtils.getValueAsList(Config.WORKER_CHILDOPTS, null));
    }

    @Test
    public void getValueAsList_nullValue() {
        String key = Config.WORKER_CHILDOPTS;
        Map<String, Object> map = mockMap(key, null);
        assertNull(ConfigUtils.getValueAsList(key, map));
    }

    @Test
    public void getValueAsList_nonStringValue() {
        String key = Config.WORKER_CHILDOPTS;
        List<String> expectedValue = Collections.singletonList("1");
        Map<String, Object> map = mockMap(key, 1);
        assertEquals(expectedValue, ConfigUtils.getValueAsList(key, map));
    }

    @Test
    public void getValueAsList_spaceSeparatedString() {
        String key = Config.WORKER_CHILDOPTS;
        String value = "-Xms1024m -Xmx1024m";
        List<String> expectedValue = Arrays.asList("-Xms1024m", "-Xmx1024m");
        Map<String, Object> map = mockMap(key, value);
        assertEquals(expectedValue, ConfigUtils.getValueAsList(key, map));
    }

    @Test
    public void getValueAsList_stringList() {
        String key = Config.WORKER_CHILDOPTS;
        List<String> values = Arrays.asList("-Xms1024m", "-Xmx1024m");
        Map<String, Object> map = mockMap(key, values);
        assertEquals(values, ConfigUtils.getValueAsList(key, map));
    }

    @Test
    public void getValueAsList_nonStringList() {
        String key = Config.WORKER_CHILDOPTS;
        List<Object> values = Arrays.asList(1, 2);
        List<String> expectedValue = Arrays.asList("1", "2");
        Map<String, Object> map = mockMap(key, values);
        assertEquals(expectedValue, ConfigUtils.getValueAsList(key, map));
    }

    @Deprecated
    @Test
    public void getBlobstoreHDFSPrincipal() throws UnknownHostException {
        Map<String, Object> conf = mockMap(Config.BLOBSTORE_HDFS_PRINCIPAL, "primary/_HOST@EXAMPLE.COM");
        assertEquals(Config.getBlobstoreHDFSPrincipal(conf), "primary/" +  Utils.localHostname() + "@EXAMPLE.COM");

        String principal = "primary/_HOST_HOST@EXAMPLE.COM";
        conf.put(Config.BLOBSTORE_HDFS_PRINCIPAL, principal);
        assertEquals(Config.getBlobstoreHDFSPrincipal(conf), principal);

        principal = "primary/_HOST2@EXAMPLE.COM";
        conf.put(Config.BLOBSTORE_HDFS_PRINCIPAL, principal);
        assertEquals(Config.getBlobstoreHDFSPrincipal(conf), principal);

        principal = "_HOST/instance@EXAMPLE.COM";
        conf.put(Config.BLOBSTORE_HDFS_PRINCIPAL, principal);
        assertEquals(Config.getBlobstoreHDFSPrincipal(conf), principal);

        principal = "primary/instance@_HOST.COM";
        conf.put(Config.BLOBSTORE_HDFS_PRINCIPAL, principal);
        assertEquals(Config.getBlobstoreHDFSPrincipal(conf), principal);

        principal = "_HOST@EXAMPLE.COM";
        conf.put(Config.BLOBSTORE_HDFS_PRINCIPAL, principal);
        assertEquals(Config.getBlobstoreHDFSPrincipal(conf), principal);

        principal = "primary/instance@EXAMPLE.COM";
        conf.put(Config.BLOBSTORE_HDFS_PRINCIPAL, principal);
        assertEquals(Config.getBlobstoreHDFSPrincipal(conf), principal);
    }

    @Test
    public void getHfdsPrincipal() throws UnknownHostException {
        Map<String, Object> conf = mockMap(Config.STORM_HDFS_LOGIN_PRINCIPAL, "primary/_HOST@EXAMPLE.COM");
        assertEquals(Config.getHdfsPrincipal(conf), "primary/" +  Utils.localHostname() + "@EXAMPLE.COM");

        String principal = "primary/_HOST_HOST@EXAMPLE.COM";
        conf.put(Config.STORM_HDFS_LOGIN_PRINCIPAL, principal);
        assertEquals(Config.getHdfsPrincipal(conf), principal);

        principal = "primary/_HOST2@EXAMPLE.COM";
        conf.put(Config.STORM_HDFS_LOGIN_PRINCIPAL, principal);
        assertEquals(Config.getHdfsPrincipal(conf), principal);

        principal = "_HOST/instance@EXAMPLE.COM";
        conf.put(Config.STORM_HDFS_LOGIN_PRINCIPAL, principal);
        assertEquals(Config.getHdfsPrincipal(conf), principal);

        principal = "primary/instance@_HOST.COM";
        conf.put(Config.STORM_HDFS_LOGIN_PRINCIPAL, principal);
        assertEquals(Config.getHdfsPrincipal(conf), principal);

        principal = "_HOST@EXAMPLE.COM";
        conf.put(Config.STORM_HDFS_LOGIN_PRINCIPAL, principal);
        assertEquals(Config.getHdfsPrincipal(conf), principal);

        principal = "primary/instance@EXAMPLE.COM";
        conf.put(Config.STORM_HDFS_LOGIN_PRINCIPAL, principal);
        assertEquals(Config.getHdfsPrincipal(conf), principal);
    }

    @Test
    public void upstreamFeedbackEnable_defaultsFalseWhenAbsent() {
        assertFalse(ConfigUtils.upstreamFeedbackEnable(new HashMap<>()));
    }

    @Test
    public void upstreamFeedbackEnable_readsConfiguredValue() {
        assertTrue(ConfigUtils.upstreamFeedbackEnable(
            mockMap(Config.TOPOLOGY_UPSTREAM_FEEDBACK_ENABLE, true)));
        assertFalse(ConfigUtils.upstreamFeedbackEnable(
            mockMap(Config.TOPOLOGY_UPSTREAM_FEEDBACK_ENABLE, false)));
    }
    
    @Test
    public void upstreamFeedbackFreqSecs_defaultsToTenWhenAbsent() {
        assertEquals(10, ConfigUtils.upstreamFeedbackFreqSecs(new HashMap<>()));
    }

    @Test
    public void upstreamFeedbackFreqSecs_returnsConfiguredPositiveValue() {
        assertEquals(5, ConfigUtils.upstreamFeedbackFreqSecs(
            mockMap(Config.TOPOLOGY_UPSTREAM_FEEDBACK_FREQ_SECS, 5)));
    }

    @Test
    public void upstreamFeedbackFreqSecs_rejectsZero() {
        assertThrows(IllegalArgumentException.class, () -> ConfigUtils.upstreamFeedbackFreqSecs(
            mockMap(Config.TOPOLOGY_UPSTREAM_FEEDBACK_FREQ_SECS, 0)));
    }

    @Test
    public void upstreamFeedbackFreqSecs_rejectsNegative() {
        assertThrows(IllegalArgumentException.class, () -> ConfigUtils.upstreamFeedbackFreqSecs(
            mockMap(Config.TOPOLOGY_UPSTREAM_FEEDBACK_FREQ_SECS, -1)));
    }

    @Test
    public void maskPasswords_masksClusterZookeeperCredentials() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_AUTH_PAYLOAD, "zk-user:zk-secret");
        conf.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD, "topo-user:topo-secret");

        Map<String, Object> masked = ConfigUtils.maskPasswords(conf);

        assertEquals("*****", masked.get(Config.STORM_ZOOKEEPER_AUTH_PAYLOAD));
        assertEquals("*****", masked.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD));
    }

    @Test
    public void maskPasswords_masksThriftTlsStorePasswords() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.NIMBUS_THRIFT_TLS_SERVER_KEYSTORE_PASSWORD, "nimbus-ks");
        conf.put(Config.NIMBUS_THRIFT_TLS_SERVER_TRUSTSTORE_PASSWORD, "nimbus-ts");
        conf.put(Config.NIMBUS_THRIFT_TLS_CLIENT_KEYSTORE_PASSWORD, "client-ks");
        conf.put(Config.NIMBUS_THRIFT_TLS_CLIENT_TRUSTSTORE_PASSWORD, "client-ts");
        conf.put(Config.SUPERVISOR_THRIFT_TLS_SERVER_KEYSTORE_PASSWORD, "sup-ks");
        conf.put(Config.SUPERVISOR_THRIFT_TLS_SERVER_TRUSTSTORE_PASSWORD, "sup-ts");

        Map<String, Object> masked = ConfigUtils.maskPasswords(conf);

        assertEquals("*****", masked.get(Config.NIMBUS_THRIFT_TLS_SERVER_KEYSTORE_PASSWORD));
        assertEquals("*****", masked.get(Config.NIMBUS_THRIFT_TLS_SERVER_TRUSTSTORE_PASSWORD));
        assertEquals("*****", masked.get(Config.NIMBUS_THRIFT_TLS_CLIENT_KEYSTORE_PASSWORD));
        assertEquals("*****", masked.get(Config.NIMBUS_THRIFT_TLS_CLIENT_TRUSTSTORE_PASSWORD));
        assertEquals("*****", masked.get(Config.SUPERVISOR_THRIFT_TLS_SERVER_KEYSTORE_PASSWORD));
        assertEquals("*****", masked.get(Config.SUPERVISOR_THRIFT_TLS_SERVER_TRUSTSTORE_PASSWORD));
    }

    @Test
    public void maskPasswords_masksZookeeperAndNettyTlsStorePasswords() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_SSL_KEYSTORE_PASSWORD, "zk-ks");
        conf.put(Config.STORM_ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD, "zk-ts");
        conf.put(Config.STORM_MESSAGING_NETTY_TLS_KEYSTORE_PASSWORD, "netty-ks");
        conf.put(Config.STORM_MESSAGING_NETTY_TLS_TRUSTSTORE_PASSWORD, "netty-ts");

        Map<String, Object> masked = ConfigUtils.maskPasswords(conf);

        assertEquals("*****", masked.get(Config.STORM_ZOOKEEPER_SSL_KEYSTORE_PASSWORD));
        assertEquals("*****", masked.get(Config.STORM_ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD));
        assertEquals("*****", masked.get(Config.STORM_MESSAGING_NETTY_TLS_KEYSTORE_PASSWORD));
        assertEquals("*****", masked.get(Config.STORM_MESSAGING_NETTY_TLS_TRUSTSTORE_PASSWORD));
    }

    @Test
    public void maskCredentials_masksKeysThatOnlyPluginsDeclare() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("storm.daemon.metrics.reporter.plugin.prometheus.basic_auth_password", "plugin-secret");
        conf.put("storm.zookeeper.auth.password", "zk-pass");
        conf.put("some.plugin.shared_secret", "shared");

        Map<String, Object> masked = ConfigUtils.maskCredentials(conf);

        assertEquals("*****", masked.get("storm.daemon.metrics.reporter.plugin.prometheus.basic_auth_password"));
        assertEquals("*****", masked.get("storm.zookeeper.auth.password"));
        assertEquals("*****", masked.get("some.plugin.shared_secret"));
    }

    @Test
    public void maskCredentials_masksTheAnnotatedKeysAsWell() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_AUTH_PAYLOAD, "zk-user:zk-secret");

        assertEquals("*****", ConfigUtils.maskCredentials(conf).get(Config.STORM_ZOOKEEPER_AUTH_PAYLOAD));
    }

    @Test
    public void maskCredentials_leavesNonStringValuesAlone() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("task.credentials.poll.secs", 30);
        conf.put("nimbus.credential.renewers.freq.secs", 600);
        conf.put("topology.auto-credentials", Collections.singletonList("org.example.AutoCreds"));
        conf.put("nimbus.seeds", Collections.singletonList("nimbus1"));

        Map<String, Object> masked = ConfigUtils.maskCredentials(conf);

        assertEquals(30, masked.get("task.credentials.poll.secs"));
        assertEquals(600, masked.get("nimbus.credential.renewers.freq.secs"));
        assertEquals(Collections.singletonList("org.example.AutoCreds"), masked.get("topology.auto-credentials"));
        assertEquals(Collections.singletonList("nimbus1"), masked.get("nimbus.seeds"));
    }

    @Test
    public void isCredentialKey_recognisesAnnotatedAndPluginDeclaredKeys() {
        assertTrue(ConfigUtils.isCredentialKey(Config.STORM_ZOOKEEPER_AUTH_PAYLOAD));
        assertTrue(ConfigUtils.isCredentialKey(Config.NIMBUS_THRIFT_TLS_CLIENT_KEYSTORE_PASSWORD));
        assertTrue(ConfigUtils.isCredentialKey("storm.daemon.metrics.reporter.plugin.prometheus.basic_auth_password"));
        assertTrue(ConfigUtils.isCredentialKey("some.plugin.shared_secret"));
    }

    @Test
    public void isCredentialKey_ignoresKeysThatOnlyMentionCredentials() {
        assertFalse(ConfigUtils.isCredentialKey("task.credentials.poll.secs"));
        assertFalse(ConfigUtils.isCredentialKey(Config.TOPOLOGY_AUTO_CREDENTIALS));
        assertFalse(ConfigUtils.isCredentialKey(Config.NIMBUS_THRIFT_TLS_CLIENT_KEYSTORE_PATH));
        assertFalse(ConfigUtils.isCredentialKey(Config.TOPOLOGY_NAME));
    }

    @Test
    public void maskPasswords_keepsOrdinaryValues() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_SERVERS, Collections.singletonList("zk1"));
        conf.put(Config.NIMBUS_THRIFT_TLS_SERVER_KEYSTORE_PATH, "/etc/storm/nimbus.jks");

        Map<String, Object> masked = ConfigUtils.maskPasswords(conf);

        assertEquals(Collections.singletonList("zk1"), masked.get(Config.STORM_ZOOKEEPER_SERVERS));
        assertEquals("/etc/storm/nimbus.jks", masked.get(Config.NIMBUS_THRIFT_TLS_SERVER_KEYSTORE_PATH));
    }
}
