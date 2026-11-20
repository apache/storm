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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.storm.Constants;
import org.apache.storm.StormTimer;
import org.apache.storm.cluster.IStateStorage;
import org.apache.storm.daemon.Task;
import org.apache.storm.daemon.worker.InstrumentedMessageTimeoutReporter;
import org.apache.storm.daemon.worker.MessageTimeoutReporter;
import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.executor.spout.SpoutExecutor;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.metrics2.RateCounter;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.apache.storm.metrics2.TaskMetrics;
import org.apache.storm.spout.ISpout;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.JCQueue;
import org.apache.storm.utils.KeyStreamRandom;
import org.apache.storm.utils.RotatingMap;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.ReflectionUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class SpoutExecutorTest {

    private static final int TASK_ID = 1;

    @Test
    public void testPendingTuplesRotateShouldBeCalledOnlyOnce() throws Exception {

        RateCounter rateCounter = Mockito.mock(RateCounter.class);

        StormMetricRegistry stormMetricRegistry = Mockito.mock(StormMetricRegistry.class);
        Mockito.when(stormMetricRegistry.rateCounter(anyString(), anyString(), anyInt()))
                .thenReturn(rateCounter);

        Map<String, Object> hashmap = Utils.readDefaultConfig();

        IStateStorage stateStorage = Mockito.mock(IStateStorage.class);

        ComponentCommon componentCommon = Mockito.mock(ComponentCommon.class);
        Mockito.when(componentCommon.get_json_conf()).thenReturn(null);

        WorkerTopologyContext workerTopologyContext = Mockito.mock(WorkerTopologyContext.class);
        Mockito.when(workerTopologyContext.getComponentId(anyInt())).thenReturn("1");
        Mockito.when(workerTopologyContext.getComponentCommon(anyString()))
                .thenReturn(componentCommon);

        WorkerState workerState = Mockito.mock(WorkerState.class);
        Mockito.when(workerState.getWorkerTopologyContext()).thenReturn(workerTopologyContext);
        Mockito.when(workerState.getStateStorage()).thenReturn(stateStorage);
        Mockito.when(workerState.getTopologyConf()).thenReturn(hashmap);
        Mockito.when(workerState.getMetricRegistry()).thenReturn(stormMetricRegistry);

        SpoutExecutor spoutExecutor = new SpoutExecutor(workerState, List.of(1L, 5L),
                new HashMap<>());

        TupleImpl tuple = Mockito.mock(TupleImpl.class);
        Mockito.when(tuple.getSourceStreamId()).thenReturn(Constants.SYSTEM_TICK_STREAM_ID);
        AddressedTuple addressedTuple = Mockito.mock(AddressedTuple.class);
        Mockito.when(addressedTuple.getDest()).thenReturn(AddressedTuple.BROADCAST_DEST);
        Mockito.when(addressedTuple.getTuple()).thenReturn(tuple);

        RotatingMap rotatingMap = Mockito.mock(RotatingMap.class);
        Field fieldRotatingMap = ReflectionUtils
                .findFields(SpoutExecutor.class, f -> f.getName().equals("pending"),
                        ReflectionUtils.HierarchyTraversalMode.TOP_DOWN)
                .get(0);
        fieldRotatingMap.setAccessible(true);
        fieldRotatingMap.set(spoutExecutor, rotatingMap);

        spoutExecutor.accept(addressedTuple);

        Mockito.verify(rotatingMap, Mockito.times(1)).rotate();
    }

    @Test
    public void testTupleTreeIdsComeFromAnUnguessableGenerator() {

        RateCounter rateCounter = Mockito.mock(RateCounter.class);

        StormMetricRegistry stormMetricRegistry = Mockito.mock(StormMetricRegistry.class);
        Mockito.when(stormMetricRegistry.rateCounter(anyString(), anyString(), anyInt()))
                .thenReturn(rateCounter);

        ComponentCommon componentCommon = Mockito.mock(ComponentCommon.class);
        Mockito.when(componentCommon.get_json_conf()).thenReturn(null);

        WorkerTopologyContext workerTopologyContext = Mockito.mock(WorkerTopologyContext.class);
        Mockito.when(workerTopologyContext.getComponentId(anyInt())).thenReturn("1");
        Mockito.when(workerTopologyContext.getComponentCommon(anyString()))
                .thenReturn(componentCommon);

        WorkerState workerState = Mockito.mock(WorkerState.class);
        Mockito.when(workerState.getWorkerTopologyContext()).thenReturn(workerTopologyContext);
        Mockito.when(workerState.getStateStorage()).thenReturn(Mockito.mock(IStateStorage.class));
        Mockito.when(workerState.getTopologyConf()).thenReturn(Utils.readDefaultConfig());
        Mockito.when(workerState.getMetricRegistry()).thenReturn(stormMetricRegistry);

        SpoutExecutor spoutExecutor = new SpoutExecutor(workerState, List.of(1L, 5L),
                new HashMap<>());

        assertInstanceOf(KeyStreamRandom.class, spoutExecutor.rand);
    }

    @Test
    public void testATimeoutStillFailsTheSpoutAfterTheReporterTurnedItselfOff() throws Exception {
        InstrumentedMessageTimeoutReporter reporter = new InstrumentedMessageTimeoutReporter();
        ISpout spout = Mockito.mock(ISpout.class);
        SpoutExecutor executor = mkOpenedExecutor(reporter, mockTask(spout));
        SpoutOutputCollector collector = collectorOf(spout);

        collector.emit(new Values("value"), "before");
        expirePending(executor);
        assertEquals(1, reporter.queuedEvents());

        reporter.failNextCycle(new IllegalStateException("injected"));
        reporter.start();
        assertTrue(reporter.awaitThreadEnd());
        collector.emit(new Values("value"), "after");
        expirePending(executor);

        Mockito.verify(spout).fail("after");
        assertEquals(1, reporter.queuedEvents());
    }

    private static Task mockTask(ISpout spout) {
        Task task = Mockito.mock(Task.class);
        Mockito.when(task.getTaskId()).thenReturn(TASK_ID);
        Mockito.when(task.getTaskObject()).thenReturn(spout);
        Mockito.when(task.getUserContext()).thenReturn(Mockito.mock(TopologyContext.class));
        Mockito.when(task.getTaskMetrics()).thenReturn(Mockito.mock(TaskMetrics.class));
        return task;
    }

    /** A spout executor of one task on a mocked worker, with its spout opened. */
    private static SpoutExecutor mkOpenedExecutor(MessageTimeoutReporter reporter, Task task) throws Exception {
        StormMetricRegistry stormMetricRegistry = Mockito.mock(StormMetricRegistry.class);
        Mockito.when(stormMetricRegistry.rateCounter(anyString(), anyString(), anyInt()))
                .thenReturn(Mockito.mock(RateCounter.class));

        WorkerTopologyContext workerTopologyContext = Mockito.mock(WorkerTopologyContext.class);
        Mockito.when(workerTopologyContext.getComponentId(anyInt())).thenReturn("spout");
        Mockito.when(workerTopologyContext.getComponentCommon(anyString()))
                .thenReturn(Mockito.mock(ComponentCommon.class));

        WorkerState workerState = Mockito.mock(WorkerState.class);
        Mockito.when(workerState.getWorkerTopologyContext()).thenReturn(workerTopologyContext);
        Mockito.when(workerState.getStateStorage()).thenReturn(Mockito.mock(IStateStorage.class));
        Mockito.when(workerState.getTopologyConf()).thenReturn(Utils.readDefaultConfig());
        Mockito.when(workerState.getMetricRegistry()).thenReturn(stormMetricRegistry);
        Mockito.when(workerState.getLocalReceiveQueues()).thenReturn(Map.of(TASK_ID, Mockito.mock(JCQueue.class)));
        Mockito.when(workerState.getIsWorkerActive()).thenReturn(new CountDownLatch(0));
        Mockito.when(workerState.getIsTopologyActive()).thenReturn(new AtomicBoolean(true));
        Mockito.when(workerState.getUserTimer()).thenReturn(Mockito.mock(StormTimer.class));
        Mockito.when(workerState.getMessageTimeoutReporter()).thenReturn(reporter);

        SpoutExecutor executor = new SpoutExecutor(workerState, List.of((long) TASK_ID, (long) TASK_ID), new HashMap<>());
        executor.init(new ArrayList<>(List.of(task)), TASK_ID);
        return executor;
    }

    private static SpoutOutputCollector collectorOf(ISpout spout) {
        ArgumentCaptor<SpoutOutputCollector> collector = ArgumentCaptor.forClass(SpoutOutputCollector.class);
        Mockito.verify(spout).open(any(), any(), collector.capture());
        return collector.getValue();
    }

    /** Two ticks move a tuple through the two buckets of the pending map and out of it. */
    private static void expirePending(SpoutExecutor executor) {
        TupleImpl tick = Mockito.mock(TupleImpl.class);
        Mockito.when(tick.getSourceStreamId()).thenReturn(Constants.SYSTEM_TICK_STREAM_ID);
        AddressedTuple addressedTick = new AddressedTuple(AddressedTuple.BROADCAST_DEST, tick);
        executor.accept(addressedTick);
        executor.accept(addressedTick);
    }
}
