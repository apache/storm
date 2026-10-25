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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.cluster.IStateStorage;
import org.apache.storm.daemon.Task;
import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.executor.bolt.BoltExecutor;
import org.apache.storm.executor.bolt.BoltOutputCollectorImpl;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.hooks.BaseTaskHook;
import org.apache.storm.hooks.info.BoltAckInfo;
import org.apache.storm.hooks.info.BoltExecuteInfo;
import org.apache.storm.metrics2.RateCounter;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.apache.storm.metrics2.TaskMetrics;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.task.IBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tracing.TupleTracer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Time.SimulatedTime;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BoltExecutorTest {

    private static final int TASK_ID = 1;
    private static final long EXECUTE_BEFORE_ACK_MS = 5;
    private static final long EXECUTE_AFTER_ACK_MS = 2;
    private static final long START_EXECUTE_MS = 100;
    private static final long SCOPE_CONTEXT_MS = 20;
    private static final long SCOPE_CLOSE_MS = 3000;

    private final List<Long> executeLatencies = new ArrayList<>();
    private final List<Long> processLatencies = new ArrayList<>();
    private SimulatedTime simulatedTime;

    @BeforeEach
    public void startSimulatedTime() {
        simulatedTime = new SimulatedTime();
    }

    @AfterEach
    public void stopSimulatedTime() {
        simulatedTime.close();
    }

    @Test
    public void testExecuteAndProcessLatencyWithoutTracer() throws Exception {
        BoltExecutor executor = mkExecutor(null, new AckingBolt());

        executor.tupleActionFn(TASK_ID, tuple(null));

        assertEquals(List.of(EXECUTE_BEFORE_ACK_MS + EXECUTE_AFTER_ACK_MS), executeLatencies);
        assertEquals(List.of(EXECUTE_BEFORE_ACK_MS), processLatencies);
    }

    @Test
    public void testExecuteLatencyLeavesOutTheExecuteScopeAndProcessLatencyDoesNot() throws Exception {
        BoltExecutor executor = mkExecutor(slowTracer(slowScope()), new AckingBolt());

        executor.tupleActionFn(TASK_ID, tuple("received context"));

        assertEquals(List.of(EXECUTE_BEFORE_ACK_MS + EXECUTE_AFTER_ACK_MS), executeLatencies);
        assertEquals(List.of(START_EXECUTE_MS + SCOPE_CONTEXT_MS + EXECUTE_BEFORE_ACK_MS), processLatencies);
    }

    @Test
    public void testExecuteLatencyLeavesOutStartExecuteThatReturnsNoScope() throws Exception {
        BoltExecutor executor = mkExecutor(slowTracer(null), new AckingBolt());

        executor.tupleActionFn(TASK_ID, tuple("received context"));

        assertEquals(List.of(EXECUTE_BEFORE_ACK_MS + EXECUTE_AFTER_ACK_MS), executeLatencies);
    }

    @Test
    public void testBoltExceptionPropagatesAndTheExecuteScopeIsClosed() throws Exception {
        TupleTracer.ExecuteScope scope = slowScope();
        RuntimeException failure = new RuntimeException("bolt failed");
        IBolt bolt = mock(IBolt.class);
        doThrow(failure).when(bolt).execute(any());
        BoltExecutor executor = mkExecutor(slowTracer(scope), bolt);

        RuntimeException thrown = assertThrows(RuntimeException.class,
            () -> executor.tupleActionFn(TASK_ID, tuple("received context")));

        assertSame(failure, thrown);
        verify(scope).close();
        assertEquals(List.of(), executeLatencies);
    }

    private static TupleTracer slowTracer(TupleTracer.ExecuteScope scope) {
        TupleTracer tracer = mock(TupleTracer.class);
        when(tracer.startExecute(anyInt(), any(), any())).thenAnswer(invocation -> {
            Time.advanceTime(START_EXECUTE_MS);
            return scope;
        });
        return tracer;
    }

    private static TupleTracer.ExecuteScope slowScope() {
        TupleTracer.ExecuteScope scope = mock(TupleTracer.ExecuteScope.class);
        when(scope.context()).thenAnswer(invocation -> {
            Time.advanceTime(SCOPE_CONTEXT_MS);
            return "execute context";
        });
        doAnswer(invocation -> {
            Time.advanceTime(SCOPE_CLOSE_MS);
            return null;
        }).when(scope).close();
        return scope;
    }

    private BoltExecutor mkExecutor(TupleTracer tracer, IBolt bolt) {
        Map<String, Object> topoConf = Utils.readDefaultConfig();
        topoConf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 1.0);

        WorkerTopologyContext workerTopologyContext = mock(WorkerTopologyContext.class);
        when(workerTopologyContext.getComponentId(anyInt())).thenReturn("bolt");
        when(workerTopologyContext.getComponentCommon(anyString())).thenReturn(mock(ComponentCommon.class));

        StormMetricRegistry metricRegistry = mock(StormMetricRegistry.class);
        when(metricRegistry.rateCounter(anyString(), anyString(), anyInt())).thenReturn(mock(RateCounter.class));

        WorkerState workerState = mock(WorkerState.class);
        when(workerState.getWorkerTopologyContext()).thenReturn(workerTopologyContext);
        when(workerState.getStateStorage()).thenReturn(mock(IStateStorage.class));
        when(workerState.getTopologyConf()).thenReturn(topoConf);
        when(workerState.getMetricRegistry()).thenReturn(metricRegistry);
        when(workerState.getUptime()).thenReturn(new Utils.UptimeComputer());
        when(workerState.getTupleTracer()).thenReturn(tracer);

        TopologyContext userContext = mock(TopologyContext.class);
        when(userContext.getHooks()).thenReturn(List.of(new BaseTaskHook() {
            @Override
            public void boltExecute(BoltExecuteInfo info) {
                executeLatencies.add(info.executeLatencyMs);
            }

            @Override
            public void boltAck(BoltAckInfo info) {
                processLatencies.add(info.processLatencyMs);
            }
        }));
        Task task = mock(Task.class);
        when(task.getTaskId()).thenReturn(TASK_ID);
        when(task.getTaskObject()).thenReturn(bolt);
        when(task.getUserContext()).thenReturn(userContext);
        when(task.getTaskMetrics()).thenReturn(mock(TaskMetrics.class));

        BoltExecutor executor = new BoltExecutor(workerState, List.of((long) TASK_ID, (long) TASK_ID), new HashMap<>());
        executor.idToTask = new ArrayList<>(List.of(task));
        executor.idToTaskBase = TASK_ID;
        bolt.prepare(topoConf, userContext,
            new OutputCollector(new BoltOutputCollectorImpl(executor, task, executor.rand, false, true, false)));
        return executor;
    }

    private static TupleImpl tuple(Object traceContext) {
        TupleImpl tuple = new TupleImpl(mock(GeneralTopologyContext.class), List.of("value"), "spout", 2, Utils.DEFAULT_STREAM_ID);
        tuple.setTraceContext(traceContext);
        return tuple;
    }

    private static class AckingBolt implements IBolt {
        private OutputCollector collector;

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            Time.advanceTime(EXECUTE_BEFORE_ACK_MS);
            collector.ack(input);
            Time.advanceTime(EXECUTE_AFTER_ACK_MS);
        }

        @Override
        public void cleanup() {
        }
    }
}
