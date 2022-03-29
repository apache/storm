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

import java.nio.ByteBuffer;
import java.util.Set;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StateSpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hooks.BaseWorkerHook;
import org.apache.storm.shade.com.google.common.collect.ImmutableMap;
import org.apache.storm.shade.com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ThriftTopologyUtilsTest {
    @Test
    public void testIsWorkerHook() {
        assertFalse(ThriftTopologyUtils.isWorkerHook(StormTopology._Fields.BOLTS));
        assertFalse(ThriftTopologyUtils.isWorkerHook(StormTopology._Fields.SPOUTS));
        assertFalse(ThriftTopologyUtils.isWorkerHook(StormTopology._Fields.STATE_SPOUTS));
        assertFalse(ThriftTopologyUtils.isWorkerHook(StormTopology._Fields.DEPENDENCY_JARS));
        assertFalse(ThriftTopologyUtils.isWorkerHook(StormTopology._Fields.DEPENDENCY_ARTIFACTS));
        assertTrue(ThriftTopologyUtils.isWorkerHook(StormTopology._Fields.WORKER_HOOKS));
    }

    @Test
    public void testIsDependencies() {
        assertFalse(ThriftTopologyUtils.isDependencies(StormTopology._Fields.BOLTS));
        assertFalse(ThriftTopologyUtils.isDependencies(StormTopology._Fields.SPOUTS));
        assertFalse(ThriftTopologyUtils.isDependencies(StormTopology._Fields.STATE_SPOUTS));
        assertFalse(ThriftTopologyUtils.isDependencies(StormTopology._Fields.WORKER_HOOKS));
        assertTrue(ThriftTopologyUtils.isDependencies(StormTopology._Fields.DEPENDENCY_JARS));
        assertTrue(ThriftTopologyUtils.isDependencies(StormTopology._Fields.DEPENDENCY_ARTIFACTS));
    }

    @Test
    public void testGetComponentIdsWithWorkerHook() {
        StormTopology stormTopology = genereateStormTopology(true);
        Set<String> componentIds = ThriftTopologyUtils.getComponentIds(stormTopology);
        assertEquals(
            ImmutableSet.of("bolt-1", "spout-1"),
            componentIds,
            "We expect to get the IDs of the components sans the Worker Hook"
        );
    }

    @Test
    public void testGetComponentIdsWithoutWorkerHook() {
        StormTopology stormTopology = genereateStormTopology(false);
        Set<String> componentIds = ThriftTopologyUtils.getComponentIds(stormTopology);
        assertEquals(
            ImmutableSet.of("bolt-1", "spout-1"),
            componentIds,
            "We expect to get the IDs of the components sans the Worker Hook"
            );
    }

    @Test
    public void testGetComponentCommonWithWorkerHook() {
        StormTopology stormTopology = genereateStormTopology(true);
        ComponentCommon componentCommon = ThriftTopologyUtils.getComponentCommon(stormTopology, "bolt-1");
        assertEquals(
            new Bolt().get_common(),
            componentCommon,
            "We expect to get bolt-1's common");
    }

    @Test
    public void testGetComponentCommonWithoutWorkerHook() {
        StormTopology stormTopology = genereateStormTopology(false);
        ComponentCommon componentCommon = ThriftTopologyUtils.getComponentCommon(stormTopology, "bolt-1");
        assertEquals(
            new Bolt().get_common(),
            componentCommon,
            "We expect to get bolt-1's common"
            );
    }

    private StormTopology genereateStormTopology(boolean withWorkerHook) {
        ImmutableMap<String, SpoutSpec> spouts = ImmutableMap.of("spout-1", new SpoutSpec());
        ImmutableMap<String, Bolt> bolts = ImmutableMap.of("bolt-1", new Bolt());
        ImmutableMap<String, StateSpoutSpec> state_spouts = ImmutableMap.of();

        StormTopology stormTopology = new StormTopology(spouts, bolts, state_spouts);

        if (withWorkerHook) {
            BaseWorkerHook workerHook = new BaseWorkerHook();
            stormTopology.add_to_worker_hooks(ByteBuffer.wrap(Utils.javaSerialize(workerHook)));
        }

        return stormTopology;
    }
}
