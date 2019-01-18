/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.scheduler.utils;

import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

public class FileConfigLoaderTest {

    private static final Logger LOG = LoggerFactory.getLogger(FileConfigLoaderTest.class);

    private static final String FILE_SCHEME_PREFIX = "file://";

    @Test
    public void testFileNotThere() {
        Config conf = new Config();
        conf.put(DaemonConfig.SCHEDULER_CONFIG_LOADER_URI, FILE_SCHEME_PREFIX + "/file/not/exist/");
        FileConfigLoader testLoader = new FileConfigLoader(conf);
        Map<String, Object> result = testLoader.load(DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS);
        Assert.assertNull("Unexpectedly returned a map", result);
    }

    @Test
    public void testInvalidConfig() throws Exception {
        Config conf = new Config();
        FileConfigLoader testLoader = new FileConfigLoader(conf);
        Map<String, Object> result = testLoader.load(DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS);
        Assert.assertNull("Unexpectedly returned a map", result);
    }

    @Test
    public void testMalformedYaml() throws Exception {

        File temp = File.createTempFile("FileLoader", ".yaml");
        temp.deleteOnExit();

        FileWriter fw = new FileWriter(temp);
        String outputData = "ThisIsNotValidYaml";
        fw.write(outputData, 0, outputData.length());
        fw.flush();
        fw.close();

        Config conf = new Config();
        conf.put(DaemonConfig.SCHEDULER_CONFIG_LOADER_URI, FILE_SCHEME_PREFIX + temp.getCanonicalPath());

        FileConfigLoader testLoader = new FileConfigLoader(conf);
        Map<String, Object> result = testLoader.load(DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS);
        Assert.assertNull("Unexpectedly returned a map", result);
    }

    @Test
    public void testValidFile() throws Exception {

        File temp = File.createTempFile("FileLoader", ".yaml");
        temp.deleteOnExit();

        Map<String, Integer> testMap = new HashMap<>();
        testMap.put("a", 1);
        testMap.put("b", 2);
        testMap.put("c", 3);
        testMap.put("d", 4);
        testMap.put("e", 5);

        Map<String, Map<String, Integer>> confMap = new HashMap<>();
        confMap.put(DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS, testMap);

        Yaml yaml = new Yaml();
        FileWriter fw = new FileWriter(temp);
        yaml.dump(confMap, fw);
        fw.flush();
        fw.close();

        Config conf = new Config();
        conf.put(DaemonConfig.SCHEDULER_CONFIG_LOADER_URI, FILE_SCHEME_PREFIX + temp.getCanonicalPath());
        FileConfigLoader loader = new FileConfigLoader(conf);

        Map<String, Object> result = loader.load(DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS);

        Assert.assertNotNull("Unexpectedly returned null", result);

        Assert.assertEquals("Maps are a different size", testMap.keySet().size(), result.keySet().size());

        for (String key : testMap.keySet()) {
            Integer expectedValue = testMap.get(key);
            Integer returnedValue = (Integer) result.get(key);
            Assert.assertEquals("Bad value for key=" + key, expectedValue, returnedValue);
        }
    }
}