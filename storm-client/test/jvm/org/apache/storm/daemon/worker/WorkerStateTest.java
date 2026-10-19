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
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.daemon.supervisor.AdvancedFSOps;
import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
    public void testTopologyEnablesFeedbackWhenClusterDisablesIt() throws IOException, TException {
        assertOutboundTasksUseTopologyFeedbackConfiguration(true);
    }

    @Test
    public void testTopologyDisablesFeedbackWhenClusterEnablesIt() throws IOException, TException {
        assertOutboundTasksUseTopologyFeedbackConfiguration(false);
    }

    private void assertOutboundTasksUseTopologyFeedbackConfiguration(boolean topologyFeedbackEnabled) throws IOException, TException {
        Map<String, Object> conf = Utils.readStormConfig();
        conf.put(Config.TOPOLOGY_UPSTREAM_FEEDBACK_ENABLE, !topologyFeedbackEnabled);
        Map<String, Object> topologyConf = new HashMap<>(conf);
        topologyConf.put(Config.TOPOLOGY_UPSTREAM_FEEDBACK_ENABLE, topologyFeedbackEnabled);
        topologyConf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 1);
        topologyConf.put(Config.TOPOLOGY_WORKERS, 1);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new TestWordSpout(true)).setNumTasks(1);
        builder.setBolt("bolt1", new TestWordCounter()).setNumTasks(1).shuffleGrouping("spout");
        StormTopology topology = builder.createTopology();
        Map<String, List<Integer>> componentTasks = Utils.reverseMap(StormCommon.stormTaskInfo(topology, topologyConf));
        int localTask = componentTasks.get("bolt1").getFirst();
        String assignmentId = "supervisor";
        Assignment assignment = new Assignment();
        assignment.set_executor_node_port(Collections.singletonMap(
                Arrays.asList((long) localTask, (long) localTask),
                new NodeInfo(assignmentId, Collections.singleton(0L))));

        ConfigUtils mockedConfigUtils = mock(ConfigUtils.class);
        ConfigUtils previousConfigUtils = ConfigUtils.setInstance(mockedConfigUtils);
        try {
            String topologyId = "feedback-config";
            when(mockedConfigUtils.readSupervisorTopologyImpl(eq(conf), eq(topologyId), any(AdvancedFSOps.class)))
                    .thenReturn(topology);
            WorkerState workerState = TestUtilsForWorkerState.getWorkerState(
                    conf, topologyId, topologyConf, assignmentId, assignment);
            try {
                Set<Integer> outbound = workerState.getOutboundTasks();
                componentTasks.get("spout").forEach(t ->
                        assertEquals(outbound.contains(t), topologyFeedbackEnabled, "upstream routing to spout task " + t + " must follow topology configuration"));
            } finally {
                workerState.closeResources();
            }
        } finally {
            ConfigUtils.setInstance(previousConfigUtils);
        }
    }

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

    @Test
    public void testTransferLocalBatchDropsTuplesForUnknownTasks() throws TException, IOException {
        ConfigUtils mockedConfigUtils = mock(ConfigUtils.class);
        ConfigUtils previousConfigUtils = ConfigUtils.setInstance(mockedConfigUtils);

        try {
            Map<String, Object> conf = new HashMap<>();
            conf.put(Config.TOPOLOGY_WORKER_SHARED_THREAD_POOL_SIZE, 1);

            String topologyId = "1";
            StormTopology topology = mock(StormTopology.class);
            when(topology.deepCopy()).thenReturn(topology);
            when(mockedConfigUtils.readSupervisorTopologyImpl(eq(conf), eq(topologyId), any(AdvancedFSOps.class))).thenReturn(topology);

            WorkerState workerState = TestUtilsForWorkerState.getWorkerState(conf, topologyId);

            // the mocked assignment contains no executors, so no task id is known to this worker;
            // a remote peer with a stale assignment may still address tuples to it (STORM-3751)
            AddressedTuple tupleForUnknownTask = new AddressedTuple(42, mock(Tuple.class));

            assertDoesNotThrow(() ->
                workerState.transferLocalBatch(new ArrayList<>(Collections.singletonList(tupleForUnknownTask))));
        } finally {
            ConfigUtils.setInstance(previousConfigUtils);
        }
    }
}
