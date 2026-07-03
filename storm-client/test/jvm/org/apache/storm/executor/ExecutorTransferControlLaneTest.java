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
package org.apache.storm.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.apache.storm.Constants;
import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.apache.storm.policy.WaitStrategyPark;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.JCQueue;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies the control-lane routing in {@link ExecutorTransfer#tryTransferLocal}: whitelisted control streams
 * (see {@link Constants#SYSTEM_CONTROL_STREAM_IDS}) must reach the destination's control lane regardless of
 * pendingEmits ordering and data-queue saturation, while data tuples and the lane-disabled path must keep the
 * original semantics.
 */
public class ExecutorTransferControlLaneTest {

    private static final int DEST_TASK_ID = 1;

    private Map<String, Object> topoConf;
    private WorkerState workerState;
    private GeneralTopologyContext generalTopologyContext;

    @BeforeEach
    public void setup() {
        topoConf = Utils.readStormConfig();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("1", new TestWordSpout(true), 1);
        builder.setBolt("2", new TestWordCounter(), 1).fieldsGrouping("1", new Fields("word"));
        StormTopology stormTopology = builder.createTopology();

        WorkerTopologyContext workerTopologyContext = mock(WorkerTopologyContext.class);
        when(workerTopologyContext.getRawTopology()).thenReturn(stormTopology);

        workerState = mock(WorkerState.class);
        when(workerState.getWorkerTopologyContext()).thenReturn(workerTopologyContext);
        generalTopologyContext = mock(GeneralTopologyContext.class);
    }

    private ExecutorTransfer mkExecutorTransfer(JCQueue localQueue) {
        Map<Integer, JCQueue> receiveQMap = new HashMap<>();
        receiveQMap.put(DEST_TASK_ID, localQueue);
        when(workerState.getLocalReceiveQueues()).thenReturn(receiveQMap);

        ExecutorTransfer executorTransfer = new ExecutorTransfer(workerState, topoConf);
        executorTransfer.initLocalRecvQueues();
        return executorTransfer;
    }

    private JCQueue mkQueue(String name, int size, int controlQueueSize) {
        return new JCQueue(name, name, size, 0, 1, new WaitStrategyPark(100), "test", "test",
            Collections.singletonList(DEST_TASK_ID), 6701, new StormMetricRegistry(), false, controlQueueSize);
    }

    private AddressedTuple mkTuple(String streamId) {
        TupleImpl tuple = new TupleImpl(generalTopologyContext, new Values("v"), Constants.SYSTEM_COMPONENT_ID,
            (int) Constants.SYSTEM_TASK_ID, streamId);
        return new AddressedTuple(DEST_TASK_ID, tuple);
    }

    private static List<Object> drain(JCQueue queue) {
        List<Object> drained = new ArrayList<>();
        queue.consume(new JCQueue.Consumer() {
            @Override
            public void accept(Object event) {
                drained.add(event);
            }

            @Override
            public void flush() {
            }
        });
        return drained;
    }

    @Test
    public void testControlTupleBypassesPendingEmits() {
        JCQueue queue = mkQueue("bypassPending", 16, 4);
        ExecutorTransfer executorTransfer = mkExecutorTransfer(queue);

        Queue<AddressedTuple> pendingEmits = new ArrayDeque<>();
        pendingEmits.add(mkTuple("default")); // non-empty: the data path would reject and append here

        AddressedTuple controlTuple = mkTuple(Constants.SYSTEM_FLUSH_STREAM_ID);
        assertTrue(executorTransfer.tryTransferLocal(controlTuple, queue, pendingEmits),
            "control tuple must be reported as handled");
        assertEquals(1, pendingEmits.size(), "control tuple must not be queued behind pendingEmits");
        assertEquals(Collections.singletonList(controlTuple), drain(queue));
    }

    @Test
    public void testControlTupleDeliveredWhenDataQueueFull() {
        JCQueue queue = mkQueue("fullDataQueue", 16, 4);
        ExecutorTransfer executorTransfer = mkExecutorTransfer(queue);

        while (queue.tryPublishDirect("DATA")) { // saturate the data plane
        }

        AddressedTuple controlTuple = mkTuple(Constants.SYSTEM_TICK_STREAM_ID);
        assertTrue(executorTransfer.tryTransferLocal(controlTuple, queue, null));
        assertEquals(controlTuple, drain(queue).get(0), "control tuple must be drained ahead of the data backlog");
    }

    @Test
    public void testControlTupleDroppedOnFullLaneStillReportedHandled() {
        JCQueue queue = mkQueue("fullLane", 16, 2);
        ExecutorTransfer executorTransfer = mkExecutorTransfer(queue);

        int accepted = 0;
        while (accepted < 64 && queue.tryPublishControl("CTRL")) { // saturate the control lane
            accepted++;
        }
        assertTrue(accepted > 0 && accepted < 64);

        Queue<AddressedTuple> pendingEmits = new ArrayDeque<>();
        assertTrue(executorTransfer.tryTransferLocal(mkTuple(Constants.SYSTEM_FLUSH_STREAM_ID), queue, pendingEmits),
            "a dropped control tuple is self-healing and must be reported as handled");
        assertTrue(pendingEmits.isEmpty(), "a dropped control tuple must not fall back to pendingEmits");
        assertEquals(accepted, drain(queue).size());
    }

    @Test
    public void testDataTupleKeepsPendingEmitsSemantics() {
        JCQueue queue = mkQueue("dataPath", 16, 4);
        ExecutorTransfer executorTransfer = mkExecutorTransfer(queue);

        Queue<AddressedTuple> pendingEmits = new ArrayDeque<>();
        pendingEmits.add(mkTuple("default"));

        AddressedTuple dataTuple = mkTuple("default");
        assertFalse(executorTransfer.tryTransferLocal(dataTuple, queue, pendingEmits),
            "data tuples must keep the original ordering semantics");
        assertEquals(2, pendingEmits.size());
        assertTrue(drain(queue).isEmpty());
    }

    @Test
    public void testControlTupleFollowsDataPathWhenLaneDisabled() {
        JCQueue queue = mkQueue("laneDisabled", 16, 0);
        ExecutorTransfer executorTransfer = mkExecutorTransfer(queue);

        Queue<AddressedTuple> pendingEmits = new ArrayDeque<>();
        pendingEmits.add(mkTuple("default"));

        assertFalse(executorTransfer.tryTransferLocal(mkTuple(Constants.SYSTEM_FLUSH_STREAM_ID), queue, pendingEmits),
            "with the lane disabled, control tuples must keep the original data-path semantics");
        assertEquals(2, pendingEmits.size());
    }
}
