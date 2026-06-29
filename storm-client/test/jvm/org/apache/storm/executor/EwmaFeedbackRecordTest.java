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

import com.codahale.metrics.Gauge;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.apache.storm.metrics2.TaskMetrics;
import org.apache.storm.serialization.KryoValuesDeserializer;
import org.apache.storm.serialization.KryoValuesSerializer;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link EwmaFeedbackRecord}, covering three concerns:
 * <ul>
 *   <li>the {@link EwmaFeedbackRecord#forEachMetric} contract, including the VOID sentinel that
 *       suppresses absent metrics;</li>
 *   <li>Kryo wire round-trip — feedback tuples are emitted to upstream tasks that may live in a
 *       different worker JVM, so the record is serialized across the network exactly as registered in
 *       {@link org.apache.storm.serialization.SerializationFactory};</li>
 *   <li>{@link EwmaFeedbackRecord#fromWorkerState} against the real {@link TaskMetrics} /
 *       {@link StormMetricRegistry} registration path. {@code TaskMetrics} registers jitter gauges under
 *       <em>suffixed</em> names (one per {@code sourceComponent:sourceStream}), e.g.
 *       {@code __execute-jitter-splitter:default}, so a bare {@code get("__execute-jitter")} never matches.
 *       These tests assert the signal is actually discovered and aggregated — without it
 *       {@link org.apache.storm.grouping.JitterAwareStreamGrouping} silently degrades to round-robin.</li>
 * </ul>
 */
public class EwmaFeedbackRecordTest {

    private static final int TASK_ID = 7;
    private static final double VOID = -1.0;

    private Map<String, Object> conf() {
        return Utils.readStormConfig();
    }

    private Map<String, Object> ewmaConf(boolean enabled) {
        Map<String, Object> conf = Utils.readStormConfig();
        conf.put(Config.TOPOLOGY_STATS_EWMA_ENABLE, enabled);
        return conf;
    }

    private TaskMetrics newTaskMetrics(StormMetricRegistry registry, Map<String, Object> conf) {
        WorkerTopologyContext context = mock(WorkerTopologyContext.class);
        when(context.getStormId()).thenReturn("test-topo");
        when(context.getThisWorkerPort()).thenReturn(6700);
        return new TaskMetrics(context, "worker", TASK_ID, registry, conf);
    }

    private WorkerState workerStateFor(StormMetricRegistry registry) {
        WorkerState workerState = mock(WorkerState.class);
        when(workerState.getMetricRegistry()).thenReturn(registry);
        return workerState;
    }

    // ---- forEachMetric ----

    @Test
    public void forEachMetric_emitsAllPresentMetricsWithCorrectNames() {
        EwmaFeedbackRecord record = new EwmaFeedbackRecord(1.0, 2.0, 3.0);

        Map<String, Double> collected = new LinkedHashMap<>();
        record.forEachMetric(collected::put);

        assertEquals(3, collected.size());
        assertEquals(1.0, collected.get(TaskMetrics.METRIC_NAME_PROCESS_JITTER));
        assertEquals(2.0, collected.get(TaskMetrics.METRIC_NAME_COMPLETE_JITTER));
        assertEquals(3.0, collected.get(TaskMetrics.METRIC_NAME_EXECUTE_JITTER));
    }

    @Test
    public void forEachMetric_skipsAbsentMetrics() {
        // -1 is the VOID sentinel: an absent gauge must not be reported as a real (negative) measurement.
        EwmaFeedbackRecord record = new EwmaFeedbackRecord(-1, 2.0, -1);

        Map<String, Double> collected = new LinkedHashMap<>();
        record.forEachMetric(collected::put);

        assertEquals(1, collected.size());
        assertEquals(2.0, collected.get(TaskMetrics.METRIC_NAME_COMPLETE_JITTER));
    }

    @Test
    public void forEachMetric_emitsZeroJitter() {
        // Zero is a legitimate value (jitter decays to 0 under stable latency) and must be distinct from VOID.
        EwmaFeedbackRecord record = new EwmaFeedbackRecord(0.0, 0.0, 0.0);

        Map<String, Double> collected = new LinkedHashMap<>();
        record.forEachMetric(collected::put);

        assertEquals(3, collected.size());
        assertEquals(0.0, collected.get(TaskMetrics.METRIC_NAME_EXECUTE_JITTER));
    }

    // ---- Kryo round-trip ----

    @Test
    public void kryoRoundTrip_preservesRecord() {
        Map<String, Object> conf = conf();
        KryoValuesSerializer serializer = new KryoValuesSerializer(conf);
        KryoValuesDeserializer deserializer = new KryoValuesDeserializer(conf);

        EwmaFeedbackRecord original = new EwmaFeedbackRecord(1.5, 2.5, 3.5);
        Object restored = deserializer.deserializeObject(serializer.serializeObject(original));

        assertInstanceOf(EwmaFeedbackRecord.class, restored);
        assertEquals(original, restored);
    }

    @Test
    public void kryoRoundTrip_insideFeedbackTuple() {
        // Mirrors Executor#buildUpstreamFeedbackTuple: [TaskInfo, EwmaFeedbackRecord]. Both elements
        // must survive the wire so the receiving task can rebuild its child stats.
        Map<String, Object> conf = conf();
        KryoValuesSerializer serializer = new KryoValuesSerializer(conf);
        KryoValuesDeserializer deserializer = new KryoValuesDeserializer(conf);

        IMetricsConsumer.TaskInfo taskInfo =
            new IMetricsConsumer.TaskInfo("host", 6700, "comp", 7, 123456, -1);
        EwmaFeedbackRecord feedback = new EwmaFeedbackRecord(-1, 9.0, 4.0);
        Values original = new Values(taskInfo, feedback);

        List<Object> restored = deserializer.deserialize(serializer.serialize(original));

        assertEquals(2, restored.size());
        assertInstanceOf(IMetricsConsumer.TaskInfo.class, restored.get(0));
        assertInstanceOf(EwmaFeedbackRecord.class, restored.get(1));
        assertEquals(7, ((IMetricsConsumer.TaskInfo) restored.get(0)).srcTaskId);
        assertEquals(feedback, restored.get(1));
        assertTrue(((EwmaFeedbackRecord) restored.get(1)).processJitter() < 0, "VOID sentinel preserved");
    }

    @Test
    public void fromWorkerState_discoversSuffixedExecuteJitterGauge() {
        StormMetricRegistry registry = new StormMetricRegistry();
        TaskMetrics taskMetrics = newTaskMetrics(registry, ewmaConf(true));

        // Real path: registers "__execute-jitter-splitter:default" and feeds the RFC-1889 EWMA estimator.
        // Varying latencies make the jitter estimate strictly positive.
        for (long latency : new long[]{10, 60, 15, 90, 20}) {
            taskMetrics.boltExecuteTuple("splitter", "default", latency);
        }

        EwmaFeedbackRecord record = EwmaFeedbackRecord.fromWorkerState(workerStateFor(registry), TASK_ID);

        // Regression guard: a bare get("__execute-jitter") misses the suffixed key and leaves this VOID (-1).
        assertTrue(record.executeJitter() > 0,
            "execute-jitter must be discovered from the suffixed gauge, got " + record.executeJitter());

        // Metrics that were never driven must stay VOID — proving the prefix filter does not cross-match
        // (e.g. __execute-jitter must not be picked up when asking for __process-jitter).
        assertEquals(VOID, record.processJitter());
        assertEquals(VOID, record.completeJitter());

        // forEachMetric must surface the value under the canonical bare name consumed by ChildEwmaStats.
        Map<String, Double> emitted = new HashMap<>();
        record.forEachMetric(emitted::put);
        assertEquals(1, emitted.size());
        assertEquals(record.executeJitter(), emitted.get(TaskMetrics.METRIC_NAME_EXECUTE_JITTER));
    }

    @Test
    public void fromWorkerState_aggregatesMaxAcrossMultipleSources() {
        StormMetricRegistry registry = new StormMetricRegistry();
        TaskMetrics taskMetrics = newTaskMetrics(registry, ewmaConf(true));

        // Same task consuming from two upstream sources => two distinct __execute-jitter-<src> gauges.
        for (long latency : new long[]{10, 20, 12, 18}) {
            taskMetrics.boltExecuteTuple("srcA", "default", latency);
        }
        for (long latency : new long[]{10, 200, 5, 300}) {   // far jumpier => higher jitter
            taskMetrics.boltExecuteTuple("srcB", "default", latency);
        }

        // Compute the expected max directly from the registry, independent of EWMA arithmetic.
        Map<String, Gauge> gauges = registry.getTaskGauges(TASK_ID);
        double expectedMax = gauges.entrySet().stream()
            .filter(e -> e.getKey().startsWith(TaskMetrics.METRIC_NAME_EXECUTE_JITTER + "-"))
            .mapToDouble(e -> ((Number) e.getValue().getValue()).doubleValue())
            .max()
            .orElseThrow(() -> new AssertionError("no execute-jitter gauges registered"));
        assertTrue(expectedMax > 0, "precondition: at least one source produced positive jitter");

        EwmaFeedbackRecord record = EwmaFeedbackRecord.fromWorkerState(workerStateFor(registry), TASK_ID);
        assertEquals(expectedMax, record.executeJitter(),
            "fromWorkerState must report the max jitter across all of the task's source gauges");
    }

    @Test
    public void fromWorkerState_allVoidWhenNoJitterGaugesRegistered() {
        // EWMA disabled => boltExecuteTuple registers latency gauges but no jitter gauges at all.
        StormMetricRegistry registry = new StormMetricRegistry();
        TaskMetrics taskMetrics = newTaskMetrics(registry, ewmaConf(false));

        taskMetrics.boltExecuteTuple("splitter", "default", 42);

        EwmaFeedbackRecord record = EwmaFeedbackRecord.fromWorkerState(workerStateFor(registry), TASK_ID);
        assertEquals(VOID, record.executeJitter());
        assertEquals(VOID, record.processJitter());
        assertEquals(VOID, record.completeJitter());
    }
}
