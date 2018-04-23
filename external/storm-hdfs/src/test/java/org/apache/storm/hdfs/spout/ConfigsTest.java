/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.hdfs.spout;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.validation.ConfigValidation;
import org.junit.Test;

import static org.junit.Assert.fail;

public class ConfigsTest {

    public static void verifyBad(String key, Object value) {
        Map<String, Object> conf = new HashMap<>();
        conf.put(key, value);
        try {
            ConfigValidation.validateFields(conf);
            fail("Expected " + key + " = " + value + " to throw Exception, but it didn't");
        } catch (IllegalArgumentException e) {
            //good
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testGood() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Configs.READER_TYPE, Configs.TEXT);
        ConfigValidation.validateFields(conf);
        conf.put(Configs.READER_TYPE, Configs.SEQ);
        ConfigValidation.validateFields(conf);
        conf.put(Configs.READER_TYPE, TextFileReader.class.getName());
        ConfigValidation.validateFields(conf);
        conf.put(Configs.HDFS_URI, "hdfs://namenode/");
        ConfigValidation.validateFields(conf);
        conf.put(Configs.SOURCE_DIR, "/input/source");
        ConfigValidation.validateFields(conf);
        conf.put(Configs.ARCHIVE_DIR, "/input/done");
        ConfigValidation.validateFields(conf);
        conf.put(Configs.BAD_DIR, "/input/bad");
        ConfigValidation.validateFields(conf);
        conf.put(Configs.LOCK_DIR, "/topology/lock");
        ConfigValidation.validateFields(conf);
        conf.put(Configs.COMMIT_FREQ_COUNT, 0);
        ConfigValidation.validateFields(conf);
        conf.put(Configs.COMMIT_FREQ_COUNT, 100);
        ConfigValidation.validateFields(conf);
        conf.put(Configs.COMMIT_FREQ_SEC, 100);
        ConfigValidation.validateFields(conf);
        conf.put(Configs.MAX_OUTSTANDING, 500);
        ConfigValidation.validateFields(conf);
        conf.put(Configs.LOCK_TIMEOUT, 100);
        ConfigValidation.validateFields(conf);
        conf.put(Configs.CLOCKS_INSYNC, true);
        ConfigValidation.validateFields(conf);
        conf.put(Configs.IGNORE_SUFFIX, ".writing");
        ConfigValidation.validateFields(conf);
        Map<String, String> hdfsConf = new HashMap<>();
        hdfsConf.put("A", "B");
        conf.put(Configs.DEFAULT_HDFS_CONFIG_KEY, hdfsConf);
        ConfigValidation.validateFields(conf);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testBad() {
        verifyBad(Configs.READER_TYPE, "SomeString");
        verifyBad(Configs.HDFS_URI, 100);
        verifyBad(Configs.COMMIT_FREQ_COUNT, -10);
    }
}
