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

package org.apache.storm;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.validation.ConfigValidation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class DaemonConfigTest {

    private void stringOrStringListTest(String key) {
        Map<String, Object> conf = new HashMap<>();
        Collection<Object> passCases = new LinkedList<>();
        Collection<Object> failCases = new LinkedList<>();

        passCases.add(null);
        passCases.add("some string");
        String[] stuff = { "some", "string", "list" };
        passCases.add(Arrays.asList(stuff));

        failCases.add(42);
        Integer[] wrongStuff = { 1, 2, 3 };
        failCases.add(Arrays.asList(wrongStuff));

        //worker.childopts validates
        for (Object value : passCases) {
            conf.put(key, value);
            ConfigValidation.validateFields(conf);
        }

        for (Object value : failCases) {
            conf.put(key, value);
            assertThrows(IllegalArgumentException.class, () -> ConfigValidation.validateFields(conf));
        }
    }

    @Test
    public void testNimbusChildoptsIsStringOrStringList() {
        stringOrStringListTest(DaemonConfig.NIMBUS_CHILDOPTS);
    }

    @Test
    public void testLogviewerChildoptsIsStringOrStringList() {
        stringOrStringListTest(DaemonConfig.LOGVIEWER_CHILDOPTS);
    }

    @Test
    public void testUiChildoptsIsStringOrStringList() {
        stringOrStringListTest(DaemonConfig.UI_CHILDOPTS);
    }

    @Test
    public void testPacemakerChildoptsIsStringOrStringList() {
        stringOrStringListTest(DaemonConfig.PACEMAKER_CHILDOPTS);
    }

    @Test
    public void testDrpcChildoptsIsStringOrStringList() {
        stringOrStringListTest(DaemonConfig.DRPC_CHILDOPTS);
    }

    @Test
    public void testSupervisorChildoptsIsStringOrStringList() {
        stringOrStringListTest(DaemonConfig.SUPERVISOR_CHILDOPTS);
    }

    @Test
    public void testMaskPasswords() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(DaemonConfig.LOGVIEWER_HTTPS_KEY_PASSWORD, "pass1");
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 100);
        Map<String, Object> result = ConfigUtils.maskPasswords(conf);
        assertEquals("*****", result.get(DaemonConfig.LOGVIEWER_HTTPS_KEY_PASSWORD));
        assertEquals(100, result.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS));
    }
}
