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
package org.apache.storm.flux;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.flux.model.TopologyDef;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FluxBuilderTest {

    @Test
    public void testIsPrimitiveNumber() {
        assertTrue(FluxBuilder.isPrimitiveNumber(int.class));
        assertFalse(FluxBuilder.isPrimitiveNumber(boolean.class));
        assertFalse(FluxBuilder.isPrimitiveNumber(String.class));
    }

    @Test
    public void testBuildConfigAcceptsValidTopologyConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(Config.TOPOLOGY_WORKERS, 4);
        TopologyDef topologyDef = new TopologyDef();
        topologyDef.setConfig(config);

        Config result = FluxBuilder.buildConfig(topologyDef);
        assertEquals(4, result.get(Config.TOPOLOGY_WORKERS));
    }

    @Test
    public void testBuildConfigRejectsInvalidTopologyConfig() {
        // topology.workers must be a positive integer; a String value is invalid
        Map<String, Object> config = new HashMap<>();
        config.put(Config.TOPOLOGY_WORKERS, "not-a-number");
        TopologyDef topologyDef = new TopologyDef();
        topologyDef.setConfig(config);

        assertThrows(IllegalArgumentException.class, () -> FluxBuilder.buildConfig(topologyDef));
    }
}
