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
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.junit.Assert;
import org.junit.Test;

public class ConfigUtilsTest {

    private Map<String, Object> mockMap(String key, Object value) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(key, value);
        return map;
    }

    @Test
    public void getValueAsList_nullKeySupported() {
        String key = null;
        List<String> value = Arrays.asList("test");
        Map<String, Object> map = mockMap(key, value);
        Assert.assertEquals(value, ConfigUtils.getValueAsList(key, map));
    }

    @Test(expected = NullPointerException.class)
    public void getValueAsList_nullKeyNotSupported() {
        String key = null;
        Map<String, Object> map = new Hashtable<>();
        ConfigUtils.getValueAsList(key, map);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getValueAsList_nullConfig() {
        ConfigUtils.getValueAsList(Config.WORKER_CHILDOPTS, null);
    }

    @Test
    public void getValueAsList_nullValue() {
        String key = Config.WORKER_CHILDOPTS;
        Map<String, Object> map = mockMap(key, null);
        Assert.assertNull(ConfigUtils.getValueAsList(key, map));
    }

    @Test
    public void getValueAsList_nonStringValue() {
        String key = Config.WORKER_CHILDOPTS;
        List<String> expectedValue = Arrays.asList("1");
        Map<String, Object> map = mockMap(key, 1);
        Assert.assertEquals(expectedValue, ConfigUtils.getValueAsList(key, map));
    }

    @Test
    public void getValueAsList_spaceSeparatedString() {
        String key = Config.WORKER_CHILDOPTS;
        String value = "-Xms1024m -Xmx1024m";
        List<String> expectedValue = Arrays.asList("-Xms1024m", "-Xmx1024m");
        Map<String, Object> map = mockMap(key, value);
        Assert.assertEquals(expectedValue, ConfigUtils.getValueAsList(key, map));
    }

    @Test
    public void getValueAsList_stringList() {
        String key = Config.WORKER_CHILDOPTS;
        List<String> values = Arrays.asList("-Xms1024m", "-Xmx1024m");
        Map<String, Object> map = mockMap(key, values);
        Assert.assertEquals(values, ConfigUtils.getValueAsList(key, map));
    }

    @Test
    public void getValueAsList_nonStringList() {
        String key = Config.WORKER_CHILDOPTS;
        List<Object> values = Arrays.asList(1, 2);
        List<String> expectedValue = Arrays.asList("1", "2");
        Map<String, Object> map = mockMap(key, values);
        Assert.assertEquals(expectedValue, ConfigUtils.getValueAsList(key, map));
    }

    @Deprecated
    @Test
    public void getBlobstoreHDFSPrincipal() throws UnknownHostException {
        Map<String, Object> conf = mockMap(Config.BLOBSTORE_HDFS_PRINCIPAL, "primary/_HOST@EXAMPLE.COM");
        Assert.assertEquals(Config.getBlobstoreHDFSPrincipal(conf), "primary/" +  Utils.localHostname() + "@EXAMPLE.COM");

        String principal = "primary/_HOST_HOST@EXAMPLE.COM";
        conf.put(Config.BLOBSTORE_HDFS_PRINCIPAL, principal);
        Assert.assertEquals(Config.getBlobstoreHDFSPrincipal(conf), principal);

        principal = "primary/_HOST2@EXAMPLE.COM";
        conf.put(Config.BLOBSTORE_HDFS_PRINCIPAL, principal);
        Assert.assertEquals(Config.getBlobstoreHDFSPrincipal(conf), principal);

        principal = "_HOST/instance@EXAMPLE.COM";
        conf.put(Config.BLOBSTORE_HDFS_PRINCIPAL, principal);
        Assert.assertEquals(Config.getBlobstoreHDFSPrincipal(conf), principal);

        principal = "primary/instance@_HOST.COM";
        conf.put(Config.BLOBSTORE_HDFS_PRINCIPAL, principal);
        Assert.assertEquals(Config.getBlobstoreHDFSPrincipal(conf), principal);

        principal = "_HOST@EXAMPLE.COM";
        conf.put(Config.BLOBSTORE_HDFS_PRINCIPAL, principal);
        Assert.assertEquals(Config.getBlobstoreHDFSPrincipal(conf), principal);

        principal = "primary/instance@EXAMPLE.COM";
        conf.put(Config.BLOBSTORE_HDFS_PRINCIPAL, principal);
        Assert.assertEquals(Config.getBlobstoreHDFSPrincipal(conf), principal);
    }

    @Test
    public void getHfdsPrincipal() throws UnknownHostException {
        Map<String, Object> conf = mockMap(Config.STORM_HDFS_LOGIN_PRINCIPAL, "primary/_HOST@EXAMPLE.COM");
        Assert.assertEquals(Config.getHdfsPrincipal(conf), "primary/" +  Utils.localHostname() + "@EXAMPLE.COM");

        String principal = "primary/_HOST_HOST@EXAMPLE.COM";
        conf.put(Config.STORM_HDFS_LOGIN_PRINCIPAL, principal);
        Assert.assertEquals(Config.getHdfsPrincipal(conf), principal);

        principal = "primary/_HOST2@EXAMPLE.COM";
        conf.put(Config.STORM_HDFS_LOGIN_PRINCIPAL, principal);
        Assert.assertEquals(Config.getHdfsPrincipal(conf), principal);

        principal = "_HOST/instance@EXAMPLE.COM";
        conf.put(Config.STORM_HDFS_LOGIN_PRINCIPAL, principal);
        Assert.assertEquals(Config.getHdfsPrincipal(conf), principal);

        principal = "primary/instance@_HOST.COM";
        conf.put(Config.STORM_HDFS_LOGIN_PRINCIPAL, principal);
        Assert.assertEquals(Config.getHdfsPrincipal(conf), principal);

        principal = "_HOST@EXAMPLE.COM";
        conf.put(Config.STORM_HDFS_LOGIN_PRINCIPAL, principal);
        Assert.assertEquals(Config.getHdfsPrincipal(conf), principal);

        principal = "primary/instance@EXAMPLE.COM";
        conf.put(Config.STORM_HDFS_LOGIN_PRINCIPAL, principal);
        Assert.assertEquals(Config.getHdfsPrincipal(conf), principal);
    }
}
