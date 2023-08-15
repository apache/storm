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

package org.apache.storm;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import org.apache.storm.validation.ConfigValidation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestDaemonConfigValidate {

    @Test
    public void testSupervisorSchedulerMetaIsStringMap() {
        Map<String, Object> conf = new HashMap<>();
        Map<String, Object> schedulerMeta = new HashMap<>();
        conf.put(DaemonConfig.SUPERVISOR_SCHEDULER_META, schedulerMeta);
        ConfigValidation.validateFields(conf);

        schedulerMeta.put("foo", "bar");

        conf.put(DaemonConfig.SUPERVISOR_SCHEDULER_META, schedulerMeta);
        ConfigValidation.validateFields(conf);

        schedulerMeta.put("baz", true);
        assertThrows(IllegalArgumentException.class, () -> ConfigValidation.validateFields(conf),
            "Expected Exception not Thrown");
    }

    @Test
    public void testIsolationSchedulerMachinesIsMap() {
        Map<String, Object> conf = new HashMap<>();
        Map<String, Integer> isolationMap = new HashMap<>();
        conf.put(DaemonConfig.ISOLATION_SCHEDULER_MACHINES, isolationMap);
        ConfigValidation.validateFields(conf);

        isolationMap.put("host0", 1);
        isolationMap.put("host1", 2);

        conf.put(DaemonConfig.ISOLATION_SCHEDULER_MACHINES, isolationMap);
        ConfigValidation.validateFields(conf);

        conf.put(DaemonConfig.ISOLATION_SCHEDULER_MACHINES, 42);
        assertThrows(IllegalArgumentException.class, () -> ConfigValidation.validateFields(conf),
            "Expected Exception not Thrown");
    }

    @Test
    public void testSupervisorSlotsPorts() {
        Map<String, Object> conf = new HashMap<>();
        Collection<Object> passCases = new LinkedList<>();
        Collection<Object> failCases = new LinkedList<>();

        Integer[] test1 = { 1233, 1234, 1235 };
        Integer[] test2 = { 1233 };
        passCases.add(Arrays.asList(test1));
        passCases.add(Arrays.asList(test2));

        String[] test3 = { "1233", "1234", "1235" };
        //duplicate case
        Integer[] test4 = { 1233, 1233, 1235 };
        failCases.add(test3);
        failCases.add(test4);
        failCases.add(null);
        failCases.add("1234");
        failCases.add(1234);

        for (Object value : passCases) {
            conf.put(DaemonConfig.SUPERVISOR_SLOTS_PORTS, value);
            ConfigValidation.validateFields(conf);
        }

        for (Object value : failCases) {
            assertThrows(IllegalArgumentException.class, () -> {
                conf.put(DaemonConfig.SUPERVISOR_SLOTS_PORTS, value);
                ConfigValidation.validateFields(conf);
            },
                "Expected Exception not Thrown for value: " + value);
        }
    }

}
