/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.daemon.worker;

import org.apache.storm.Config;
import org.apache.storm.daemon.supervisor.AdvancedFSOps;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkerStateTest {

    @Test
    public void testWorkerHooksLifecycle() throws TException, IOException {
        ConfigUtils mockedConfigUtils = mock(ConfigUtils.class);
        ConfigUtils previousConfigUtils = ConfigUtils.setInstance(mockedConfigUtils);

        try {
            Map<String, Object> conf = new HashMap<>();
            conf.put(Config.TOPOLOGY_WORKER_SHARED_THREAD_POOL_SIZE, 1);

            List<ByteBuffer> workerHookBuffers = Collections.singletonList(ByteBuffer.wrap(Utils.javaSerialize(new TestUtilsForWorkerState.StateTrackingWorkerHook())));
            String topologyId = "1";
            StormTopology topology = mock(StormTopology.class);
            when(topology.deepCopy()).thenReturn(topology);
            when(topology.is_set_worker_hooks()).thenReturn(true);
            when(topology.get_worker_hooks()).thenReturn(workerHookBuffers);
            when(mockedConfigUtils.readSupervisorTopologyImpl(eq(conf), eq(topologyId), any(AdvancedFSOps.class))).thenReturn(topology);

            WorkerState workerState = TestUtilsForWorkerState.getWorkerState(conf, topologyId);
            TestUtilsForWorkerState.StateTrackingWorkerHook workerHook = workerState.getDeserializedWorkerHooks().stream()
                    .filter(iwh -> iwh instanceof TestUtilsForWorkerState.StateTrackingWorkerHook)
                    .map(iwh -> (TestUtilsForWorkerState.StateTrackingWorkerHook) iwh)
                    .findFirst()
                    .get();
            assertFalse(workerHook.isStartCalled());
            assertFalse(workerHook.isShutdownCalled());

            workerState.runWorkerStartHooks();
            assertTrue(workerHook.isStartCalled());

            workerState.runWorkerShutdownHooks();
            assertTrue(workerHook.isShutdownCalled());
        } finally {
            ConfigUtils.setInstance(previousConfigUtils);
        }
    }

    @Test
    public void testVisibilityOfUserResource() throws IOException, TException {
        ConfigUtils mockedConfigUtils = mock(ConfigUtils.class);
        ConfigUtils previousConfigUtils = ConfigUtils.setInstance(mockedConfigUtils);

        try {
            Map<String, Object> conf = new HashMap<>();
            conf.put(Config.TOPOLOGY_WORKER_SHARED_THREAD_POOL_SIZE, 1);

            TestUtilsForWorkerState.ResourceInitializingWorkerHook workerHook = new TestUtilsForWorkerState.ResourceInitializingWorkerHook();
            List<ByteBuffer> workerHookBuffers = Collections.singletonList(ByteBuffer.wrap(Utils.javaSerialize(workerHook)));
            String topologyId = "1";
            StormTopology topology = mock(StormTopology.class);
            when(topology.deepCopy()).thenReturn(topology);
            when(topology.is_set_worker_hooks()).thenReturn(true);
            when(topology.get_worker_hooks()).thenReturn(workerHookBuffers);
            when(mockedConfigUtils.readSupervisorTopologyImpl(eq(conf), eq(topologyId), any(AdvancedFSOps.class))).thenReturn(topology);

            WorkerState workerState = TestUtilsForWorkerState.getWorkerState(conf, topologyId);
            assertNull(workerState.getWorkerTopologyContext().getResource(TestUtilsForWorkerState.RESOURCE_KEY));

            workerState.runWorkerStartHooks();
            assertEquals(TestUtilsForWorkerState.RESOURCE_VALUE, workerState.getWorkerTopologyContext().getResource(TestUtilsForWorkerState.RESOURCE_KEY));

            workerState.runWorkerShutdownHooks();
        } finally {
            ConfigUtils.setInstance(previousConfigUtils);
        }
    }
}
