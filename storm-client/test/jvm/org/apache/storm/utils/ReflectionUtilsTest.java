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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.junit.jupiter.api.Test;

public class ReflectionUtilsTest {

    @Test
    public void testSchedulerStrategyWithoutWhitelistOnlyAllowsShippedStrategies() {
        Map<String, Object> conf = new HashMap<>();
        DisallowedStrategyException e = assertThrows(DisallowedStrategyException.class,
            () -> ReflectionUtils.newSchedulerStrategyInstance("java.util.HashMap", conf));
        assertEquals("java.util.HashMap", e.getAttemptedClass());
        assertEquals(ReflectionUtils.DEFAULT_SCHEDULER_STRATEGIES, e.getAllowedStrategies());

        for (String strategy : ReflectionUtils.DEFAULT_SCHEDULER_STRATEGIES) {
            // The strategies themselves live in storm-server, so they cannot be loaded here, but they must pass the whitelist check.
            RuntimeException notFound = assertThrows(RuntimeException.class,
                () -> ReflectionUtils.newSchedulerStrategyInstance(strategy, conf));
            assertTrue(notFound.getCause() instanceof ClassNotFoundException, "unexpected failure for " + strategy);
        }
    }

    @Test
    public void testSchedulerStrategyMessageNamesTheClassAndTheConfigToChange() {
        DisallowedStrategyException e = assertThrows(DisallowedStrategyException.class,
            () -> ReflectionUtils.newSchedulerStrategyInstance("com.example.CustomStrategy", new HashMap<>()));
        assertTrue(e.getMessage().contains("com.example.CustomStrategy"), e.getMessage());
        assertTrue(e.getMessage().contains(Config.NIMBUS_SCHEDULER_STRATEGY_CLASS_WHITELIST), e.getMessage());
    }

    @Test
    public void testSchedulerStrategyWithWhitelist() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.NIMBUS_SCHEDULER_STRATEGY_CLASS_WHITELIST, Collections.singletonList("java.util.HashMap"));
        Object instance = ReflectionUtils.newSchedulerStrategyInstance("java.util.HashMap", conf);
        assertEquals(HashMap.class, instance.getClass());

        conf.put(Config.NIMBUS_SCHEDULER_STRATEGY_CLASS_WHITELIST, Collections.emptyList());
        assertThrows(DisallowedStrategyException.class, () -> ReflectionUtils.newSchedulerStrategyInstance("java.util.HashMap", conf));
    }

    @Test
    public void testDefaultsYamlMatchesTheShippedStrategies() {
        List<String> fromDefaults =
            (List<String>) Utils.readDefaultConfig().get(Config.NIMBUS_SCHEDULER_STRATEGY_CLASS_WHITELIST);
        assertEquals(ReflectionUtils.DEFAULT_SCHEDULER_STRATEGIES, fromDefaults);
    }
}
