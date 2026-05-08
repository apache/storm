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

package org.apache.storm.metrics2;

import com.codahale.metrics.Gauge;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.utils.ConfigUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TaskMetricsTest {

    private static final String TOPOLOGY_ID   = "test-topology-1";
    private static final String COMPONENT_ID  = "test-bolt";
    private static final Integer TASK_ID      = 42;
    private static final Integer WORKER_PORT  = 6700;
    private static final String STREAM_ID     = "default";
    private static final String SOURCE_COMP   = "source-spout";
    private static final int    SAMPLING_RATE = 1;
    private static final double EWMA_FACTOR   = 0.3;

    @Mock private WorkerTopologyContext context;
    @Mock private StormMetricRegistry   metricRegistry;
    @Mock private RateCounter           rateCounter;

    private Map<String, Object> topoConf;

    private TaskMetrics buildTaskMetrics(boolean ewmaEnabled) {
        try (MockedStatic<ConfigUtils> cfgUtils = mockStatic(ConfigUtils.class)) {
            cfgUtils.when(() -> ConfigUtils.samplingRate(topoConf)).thenReturn(SAMPLING_RATE);
            cfgUtils.when(() -> ConfigUtils.ewmaSmoothingFactor(topoConf)).thenReturn(EWMA_FACTOR);
            cfgUtils.when(() -> ConfigUtils.ewmaEnable(topoConf)).thenReturn(ewmaEnabled);

            return new TaskMetrics(context, COMPONENT_ID, TASK_ID, metricRegistry, topoConf);
        }
    }

    @BeforeEach
    void setUp() {
        when(context.getStormId()).thenReturn(TOPOLOGY_ID);
        when(context.getThisWorkerPort()).thenReturn(WORKER_PORT);

        topoConf = new HashMap<>();

        when(metricRegistry.rateCounter(anyString(), anyString(), anyString(),
                anyInt(), anyInt(), anyString())).thenReturn(rateCounter);
    }

    @Test
    void spoutAckedTuple_incrementsAckCounter() {
        when(metricRegistry.gauge(anyString(), any(Gauge.class), anyString(),
                anyString(), anyString(), anyInt(), anyInt())).thenReturn(null);
        TaskMetrics tm = buildTaskMetrics(false);

        tm.spoutAckedTuple(STREAM_ID, 100L);

        verify(rateCounter).inc(SAMPLING_RATE);
    }

    @Test
    void spoutAckedTuple_registersCompleteLatencyGauge() {
        when(metricRegistry.gauge(anyString(), any(Gauge.class), anyString(),
                anyString(), anyString(), anyInt(), anyInt())).thenReturn(null);
        TaskMetrics tm = buildTaskMetrics(false);

        tm.spoutAckedTuple(STREAM_ID, 200L);

        verify(metricRegistry, atLeastOnce()).gauge(
                contains("__complete-latency"), any(Gauge.class),
                anyString(), anyString(), anyString(), anyInt(), anyInt());
    }

    @Test
    void spoutAckedTuple_withEwmaEnabled_registersJitterGauge() {
        when(metricRegistry.gauge(anyString(), any(Gauge.class), anyString(),
                anyString(), anyString(), anyInt(), anyInt())).thenReturn(null);
        TaskMetrics tm = buildTaskMetrics(true);

        tm.spoutAckedTuple(STREAM_ID, 150L);

        verify(metricRegistry, atLeastOnce()).gauge(
                contains("__complete-jitter"), any(Gauge.class),
                anyString(), anyString(), anyString(), anyInt(), anyInt());
    }

    @Test
    void spoutAckedTuple_withEwmaDisabled_doesNotRegisterJitterGauge() {
        when(metricRegistry.gauge(anyString(), any(Gauge.class), anyString(),
                anyString(), anyString(), anyInt(), anyInt())).thenReturn(null);
        TaskMetrics tm = buildTaskMetrics(false);

        tm.spoutAckedTuple(STREAM_ID, 150L);

        verify(metricRegistry, never()).gauge(
                contains("__complete-rfc1889a-jitter"), any(Gauge.class),
                anyString(), anyString(), anyString(), anyInt(), anyInt());
    }

    @Test
    void boltAckedTuple_incrementsAckCounter() {
        when(metricRegistry.gauge(anyString(), any(Gauge.class), anyString(),
                anyString(), anyString(), anyInt(), anyInt())).thenReturn(null);
        TaskMetrics tm = buildTaskMetrics(false);

        tm.boltAckedTuple(SOURCE_COMP, STREAM_ID, 50L);

        verify(rateCounter).inc(SAMPLING_RATE);
    }

    @Test
    void boltAckedTuple_registersProcessLatencyGauge() {
        when(metricRegistry.gauge(anyString(), any(Gauge.class), anyString(),
                anyString(), anyString(), anyInt(), anyInt())).thenReturn(null);
        TaskMetrics tm = buildTaskMetrics(false);

        tm.boltAckedTuple(SOURCE_COMP, STREAM_ID, 50L);

        verify(metricRegistry, atLeastOnce()).gauge(
                contains("__process-latency"), any(Gauge.class),
                anyString(), anyString(), anyString(), anyInt(), anyInt());
    }

    @Test
    void boltAckedTuple_withEwmaEnabled_registersJitterGauge() {
        when(metricRegistry.gauge(anyString(), any(Gauge.class), anyString(),
                anyString(), anyString(), anyInt(), anyInt())).thenReturn(null);
        TaskMetrics tm = buildTaskMetrics(true);

        tm.boltAckedTuple(SOURCE_COMP, STREAM_ID, 75L);

        verify(metricRegistry, atLeastOnce()).gauge(
                contains("__process-jitter"), any(Gauge.class),
                anyString(), anyString(), anyString(), anyInt(), anyInt());
    }

    @Test
    void boltAckedTuple_metricKeyIncludesSourceComponentAndStream() {
        when(metricRegistry.gauge(anyString(), any(Gauge.class), anyString(),
                anyString(), anyString(), anyInt(), anyInt())).thenReturn(null);
        TaskMetrics tm = buildTaskMetrics(false);

        tm.boltAckedTuple(SOURCE_COMP, STREAM_ID, 50L);

        verify(metricRegistry).rateCounter(
                contains(SOURCE_COMP + ":" + STREAM_ID),
                anyString(), anyString(), anyInt(), anyInt(), anyString());
    }

    @Test
    void spoutFailedTuple_incrementsFailCounter() {
        TaskMetrics tm = buildTaskMetrics(false);

        tm.spoutFailedTuple(STREAM_ID);

        verify(rateCounter).inc(SAMPLING_RATE);
    }

    @Test
    void spoutFailedTuple_usesCorrectMetricName() {
        TaskMetrics tm = buildTaskMetrics(false);

        tm.spoutFailedTuple(STREAM_ID);

        verify(metricRegistry).rateCounter(
                eq("__fail-count-" + STREAM_ID),
                eq(TOPOLOGY_ID), eq(COMPONENT_ID), eq(TASK_ID), eq(WORKER_PORT), eq(STREAM_ID));
    }

    @Test
    void boltFailedTuple_incrementsFailCounter() {
        TaskMetrics tm = buildTaskMetrics(false);

        tm.boltFailedTuple(SOURCE_COMP, STREAM_ID);

        verify(rateCounter).inc(SAMPLING_RATE);
    }

    @Test
    void boltFailedTuple_metricKeyIncludesSourceComponentAndStream() {
        TaskMetrics tm = buildTaskMetrics(false);

        tm.boltFailedTuple(SOURCE_COMP, STREAM_ID);

        verify(metricRegistry).rateCounter(
                eq("__fail-count-" + SOURCE_COMP + ":" + STREAM_ID),
                anyString(), anyString(), anyInt(), anyInt(), anyString());
    }

    @Test
    void emittedTuple_incrementsEmitCounter() {
        TaskMetrics tm = buildTaskMetrics(false);

        tm.emittedTuple(STREAM_ID);

        verify(rateCounter).inc(SAMPLING_RATE);
    }

    @Test
    void emittedTuple_usesCorrectMetricName() {
        TaskMetrics tm = buildTaskMetrics(false);

        tm.emittedTuple(STREAM_ID);

        verify(metricRegistry).rateCounter(
                eq("__emit-count-" + STREAM_ID),
                eq(TOPOLOGY_ID), eq(COMPONENT_ID), eq(TASK_ID), eq(WORKER_PORT), eq(STREAM_ID));
    }

    @Test
    void transferredTuples_incrementsByAmountTimesSamplingRate() {
        TaskMetrics tm = buildTaskMetrics(false);
        int amount = 5;

        tm.transferredTuples(STREAM_ID, amount);

        verify(rateCounter).inc(amount * SAMPLING_RATE);
    }

    @Test
    void transferredTuples_usesCorrectMetricName() {
        TaskMetrics tm = buildTaskMetrics(false);

        tm.transferredTuples(STREAM_ID, 3);

        verify(metricRegistry).rateCounter(
                eq("__transfer-count-" + STREAM_ID),
                anyString(), anyString(), anyInt(), anyInt(), anyString());
    }

    @Test
    void boltExecuteTuple_incrementsExecuteCounter() {
        when(metricRegistry.gauge(anyString(), any(Gauge.class), anyString(),
                anyString(), anyString(), anyInt(), anyInt())).thenReturn(null);
        TaskMetrics tm = buildTaskMetrics(false);

        tm.boltExecuteTuple(SOURCE_COMP, STREAM_ID, 30L);

        verify(rateCounter).inc(SAMPLING_RATE);
    }

    @Test
    void boltExecuteTuple_registersExecuteLatencyGauge() {
        when(metricRegistry.gauge(anyString(), any(Gauge.class), anyString(),
                anyString(), anyString(), anyInt(), anyInt())).thenReturn(null);
        TaskMetrics tm = buildTaskMetrics(false);

        tm.boltExecuteTuple(SOURCE_COMP, STREAM_ID, 30L);

        verify(metricRegistry, atLeastOnce()).gauge(
                contains("__execute-latency"), any(Gauge.class),
                anyString(), anyString(), anyString(), anyInt(), anyInt());
    }

    @Test
    void boltExecuteTuple_withEwmaEnabled_registersJitterGauge() {
        when(metricRegistry.gauge(anyString(), any(Gauge.class), anyString(),
                anyString(), anyString(), anyInt(), anyInt())).thenReturn(null);
        TaskMetrics tm = buildTaskMetrics(true);

        tm.boltExecuteTuple(SOURCE_COMP, STREAM_ID, 30L);

        verify(metricRegistry, atLeastOnce()).gauge(
                contains("__execute-jitter"), any(Gauge.class),
                anyString(), anyString(), anyString(), anyInt(), anyInt());
    }

    @Test
    void boltExecuteTuple_withEwmaDisabled_doesNotRegisterJitterGauge() {
        when(metricRegistry.gauge(anyString(), any(Gauge.class), anyString(),
                anyString(), anyString(), anyInt(), anyInt())).thenReturn(null);
        TaskMetrics tm = buildTaskMetrics(false);

        tm.boltExecuteTuple(SOURCE_COMP, STREAM_ID, 30L);

        verify(metricRegistry, never()).gauge(
                contains("__execute-rfc1889a-jitter"), any(Gauge.class),
                anyString(), anyString(), anyString(), anyInt(), anyInt());
    }

    @Test
    void differentStreams_produceSeparateRateCounters() {
        TaskMetrics tm = buildTaskMetrics(false);

        tm.emittedTuple("stream-A");
        tm.emittedTuple("stream-B");

        verify(metricRegistry).rateCounter(
                eq("__emit-count-stream-A"),
                anyString(), anyString(), anyInt(), anyInt(), eq("stream-A"));
        verify(metricRegistry).rateCounter(
                eq("__emit-count-stream-B"),
                anyString(), anyString(), anyInt(), anyInt(), eq("stream-B"));
    }

    @Test
    void rateCounter_registeredOnlyOnceForSameMetricName() {
        TaskMetrics tm = buildTaskMetrics(false);

        tm.emittedTuple(STREAM_ID);
        tm.emittedTuple(STREAM_ID);
        tm.emittedTuple(STREAM_ID);

        verify(metricRegistry, times(1)).rateCounter(
                eq("__emit-count-" + STREAM_ID),
                anyString(), anyString(), anyInt(), anyInt(), anyString());
    }

    @Test
    void gauge_registeredOnlyOnceForSameMetricName() {
        when(metricRegistry.gauge(anyString(), any(Gauge.class), anyString(),
                anyString(), anyString(), anyInt(), anyInt())).thenReturn(null);
        TaskMetrics tm = buildTaskMetrics(false);

        tm.spoutAckedTuple(STREAM_ID, 10L);
        tm.spoutAckedTuple(STREAM_ID, 20L);
        tm.spoutAckedTuple(STREAM_ID, 30L);

        verify(metricRegistry, times(1)).gauge(
                contains("__complete-latency"), any(Gauge.class),
                anyString(), anyString(), anyString(), anyInt(), anyInt());
    }

    @Test
    void concurrentEmittedTuple_registersRateCounterExactlyOnce() throws InterruptedException {
        TaskMetrics tm = buildTaskMetrics(false);
        int threadCount = 20;
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done  = new CountDownLatch(threadCount);

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            pool.submit(() -> {
                ready.countDown();
                try { start.await(); } catch (InterruptedException ignored) {}
                tm.emittedTuple(STREAM_ID);
                done.countDown();
            });
        }

        ready.await();
        start.countDown();
        assertTrue(done.await(5, TimeUnit.SECONDS));
        pool.shutdown();

        verify(metricRegistry, times(1)).rateCounter(
                eq("__emit-count-" + STREAM_ID),
                anyString(), anyString(), anyInt(), anyInt(), anyString());
    }

    @Test
    void concurrentSpoutAckedTuple_registersGaugeExactlyOnce() throws InterruptedException {
        when(metricRegistry.gauge(anyString(), any(Gauge.class), anyString(),
                anyString(), anyString(), anyInt(), anyInt())).thenReturn(null);
        TaskMetrics tm = buildTaskMetrics(false);
        int threadCount = 20;
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done  = new CountDownLatch(threadCount);

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            pool.submit(() -> {
                ready.countDown();
                try { start.await(); } catch (InterruptedException ignored) {}
                tm.spoutAckedTuple(STREAM_ID, 100L);
                done.countDown();
            });
        }

        ready.await();
        start.countDown();
        assertTrue(done.await(5, TimeUnit.SECONDS));
        pool.shutdown();

        verify(metricRegistry, times(1)).gauge(
                contains("__complete-latency"), any(Gauge.class),
                anyString(), anyString(), anyString(), anyInt(), anyInt());
    }

    @Test
    void getOrCreateGauge_sameTypeReusedWithoutThrowing() {
        when(metricRegistry.gauge(anyString(), any(Gauge.class), anyString(),
                anyString(), anyString(), anyInt(), anyInt())).thenReturn(null);
        TaskMetrics tm = buildTaskMetrics(false);

        tm.spoutAckedTuple(STREAM_ID, 10L);
        tm.spoutAckedTuple(STREAM_ID, 20L);

        verify(metricRegistry, times(1)).gauge(
                contains("__complete-latency"), any(Gauge.class),
                anyString(), anyString(), anyString(), anyInt(), anyInt());
    }

    @Test
    void boltAckedTuple_metricNameFormat() {
        when(metricRegistry.gauge(anyString(), any(Gauge.class), anyString(),
                anyString(), anyString(), anyInt(), anyInt())).thenReturn(null);
        TaskMetrics tm = buildTaskMetrics(false);
        String expectedKey = SOURCE_COMP + ":" + STREAM_ID;

        tm.boltAckedTuple(SOURCE_COMP, STREAM_ID, 10L);

        verify(metricRegistry).rateCounter(
                eq("__ack-count-" + expectedKey),
                eq(TOPOLOGY_ID), eq(COMPONENT_ID), eq(TASK_ID), eq(WORKER_PORT), eq(STREAM_ID));
    }

    @Test
    void boltExecuteTuple_metricNameFormat() {
        when(metricRegistry.gauge(anyString(), any(Gauge.class), anyString(),
                anyString(), anyString(), anyInt(), anyInt())).thenReturn(null);
        TaskMetrics tm = buildTaskMetrics(false);
        String expectedKey = SOURCE_COMP + ":" + STREAM_ID;

        tm.boltExecuteTuple(SOURCE_COMP, STREAM_ID, 10L);

        verify(metricRegistry).rateCounter(
                eq("__execute-count-" + expectedKey),
                eq(TOPOLOGY_ID), eq(COMPONENT_ID), eq(TASK_ID), eq(WORKER_PORT), eq(STREAM_ID));
    }

    @Test
    void boltFailedTuple_metricNameFormat() {
        TaskMetrics tm = buildTaskMetrics(false);
        String expectedKey = SOURCE_COMP + ":" + STREAM_ID;

        tm.boltFailedTuple(SOURCE_COMP, STREAM_ID);

        verify(metricRegistry).rateCounter(
                eq("__fail-count-" + expectedKey),
                eq(TOPOLOGY_ID), eq(COMPONENT_ID), eq(TASK_ID), eq(WORKER_PORT), eq(STREAM_ID));
    }

    @Test
    void contextFields_propagatedCorrectlyToRegistry() {
        TaskMetrics tm = buildTaskMetrics(false);

        tm.emittedTuple(STREAM_ID);

        ArgumentCaptor<String> topoCaptor  = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> compCaptor  = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> taskCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Integer> portCaptor = ArgumentCaptor.forClass(Integer.class);

        verify(metricRegistry).rateCounter(
                anyString(),
                topoCaptor.capture(), compCaptor.capture(),
                taskCaptor.capture(), portCaptor.capture(),
                anyString());

        assertEquals(TOPOLOGY_ID, topoCaptor.getValue());
        assertEquals(COMPONENT_ID, compCaptor.getValue());
        assertEquals(TASK_ID, taskCaptor.getValue());
        assertEquals(WORKER_PORT, portCaptor.getValue());
    }
}
