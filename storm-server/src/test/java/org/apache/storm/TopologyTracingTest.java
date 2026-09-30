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

package org.apache.storm;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.storm.ILocalCluster.ILocalTopology;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.AckFailMapTracker;
import org.apache.storm.testing.FeederSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs topologies on a two-worker local cluster with tracing on or off and checks the spans
 * Storm exports.
 */
public class TopologyTracingTest {

    @RegisterExtension
    static final OpenTelemetryExtension OTEL = OpenTelemetryExtension.create();

    private static final Set<String> RECEIVED_TRACE_IDS = ConcurrentHashMap.newKeySet();
    /** Span ids that were current on the sink's thread while its execute() ran. */
    private static final Set<String> CURRENT_IN_EXECUTE = ConcurrentHashMap.newKeySet();
    private static final AtomicInteger SINK_TUPLES_RECEIVED = new AtomicInteger();
    private static final AtomicInteger UNSAMPLED_CONTEXTS_RECEIVED = new AtomicInteger();
    private static final Set<String> MIDDLE_TRACE_IDS = ConcurrentHashMap.newKeySet();
    /** Tuple value to the id of the span current while middle, then sink, handled it. */
    private static final Map<Object, String> MIDDLE_SPAN_BY_VALUE = new ConcurrentHashMap<>();
    private static final Map<Object, String> SINK_SPAN_BY_VALUE = new ConcurrentHashMap<>();
    private static final Map<String, Integer> WORKER_PORT_BY_COMPONENT = new ConcurrentHashMap<>();
    private static final AtomicReference<Throwable> EMITTER_THREAD_FAILURE =
        new AtomicReference<>();
    private static final AtomicInteger TICK_TUPLES_RECEIVED = new AtomicInteger();
    private static final AtomicBoolean SPAN_CURRENT_DURING_TICK = new AtomicBoolean();

    private static ILocalCluster cluster;
    private static int topologyCount;

    @BeforeAll
    public static void startCluster() throws Exception {
        cluster = new LocalCluster();
    }

    @AfterAll
    public static void stopCluster() throws Exception {
        cluster.close();
    }

    @Test
    public void testEachSpoutEmitStartsARootSpan() throws Exception {
        List<SpanData> spans = runSpoutToSink(true, 3, 1, 0); // 3 tuples, 1 sink task, no ticks

        List<SpanData> emits = named(spans, "spout emit");
        assertEquals(3, emits.size());
        for (SpanData emit : emits) {
            assertFalse(emit.getParentSpanContext().isValid(), "a spout emit starts a new trace");
        }
        Set<String> emitTraceIds =
            emits.stream().map(SpanData::getTraceId).collect(Collectors.toSet());
        assertEquals(3, emitTraceIds.size());
        assertEquals(emitTraceIds, RECEIVED_TRACE_IDS, "each tuple carries its emit context");
    }

    @Test
    public void testNoSpansWhenTracingIsOff() throws Exception {
        assertTrue(runSpoutToSink(false, 1, 1, 0).isEmpty()); // 1 tuple, 1 sink task, no ticks
        assertTrue(RECEIVED_TRACE_IDS.isEmpty(), "tuples carry no context");
    }

    @Test
    public void testExecuteSpanIsChildOfTheEmitAndCurrentDuringExecute() throws Exception {
        // two sink tasks with all grouping: on two workers, a copy of each tuple crosses workers
        List<SpanData> spans = runSpoutToSink(true, 2, 2, 0); // 2 tuples, 2 sink tasks, no ticks

        Map<String, SpanData> emits = byId(named(spans, "spout emit"));
        List<SpanData> executes = named(spans, "sink execute");
        assertEquals(2, emits.size());
        assertEquals(4, executes.size());
        for (SpanData execute : executes) {
            SpanData emit = emits.get(execute.getParentSpanId());
            assertNotNull(emit, "an execute span is a child of the emit that produced its tuple");
            assertEquals(emit.getTraceId(), execute.getTraceId());
        }
        assertTrue(executes.stream().anyMatch(s -> s.getParentSpanContext().isRemote()),
            "at least one tuple crossed workers, so its context went through the serializer");
        Set<String> executeIds =
            executes.stream().map(SpanData::getSpanId).collect(Collectors.toSet());
        assertEquals(executeIds, CURRENT_IN_EXECUTE, "the execute span is current in the bolt");
    }

    @Test
    public void testTickTuplesGetNoSpanAndSeeNoLeftoverContext() throws Exception {
        List<SpanData> spans = runSpoutToSink(true, 1, 1, 1); // 1 tuple, 1 sink task, 1 s ticks

        assertEquals(1, named(spans, "sink execute").size());
        assertFalse(SPAN_CURRENT_DURING_TICK.get(), "the execute span's scope was closed");
    }

    @Test
    public void testAckRecordsAnOutcomeSpanUnderTheRoot() throws Exception {
        // spout emit, sink execute, spout ack
        List<SpanData> spans = runWithSink(SinkOutcome.ACK, conf(true), 3);

        SpanData emit = named(spans, "spout emit").get(0);
        SpanData ack = named(spans, "spout ack").get(0);
        assertEquals(emit.getSpanId(), ack.getParentSpanId());
        assertEquals(StatusCode.UNSET, ack.getStatus().getStatusCode());
    }

    @Test
    public void testFailRecordsErrorSpansInTheBoltAndAtTheSpout() throws Exception {
        // spout emit, sink execute, sink fail, spout fail
        List<SpanData> spans = runWithSink(SinkOutcome.FAIL, conf(true), 4);

        SpanData emit = named(spans, "spout emit").get(0);
        SpanData execute = named(spans, "sink execute").get(0);
        assertEquals(1, named(spans, "sink fail").size());
        SpanData sinkFail = named(spans, "sink fail").get(0);
        SpanData spoutFail = named(spans, "spout fail").get(0);
        assertEquals(execute.getSpanId(), sinkFail.getParentSpanId());
        assertEquals(emit.getSpanId(), spoutFail.getParentSpanId());
        assertEquals(StatusCode.ERROR, sinkFail.getStatus().getStatusCode());
        assertEquals(StatusCode.ERROR, spoutFail.getStatus().getStatusCode());
    }

    @Test
    public void testBoltFailIsRecordedWithoutAckers() throws Exception {
        Config conf = conf(true);
        conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
        // spout emit, sink execute, sink fail; without ackers the spout records no outcome
        List<SpanData> spans = runWithSink(SinkOutcome.FAIL, conf, 3);

        SpanData sinkFail = named(spans, "sink fail").get(0);
        assertEquals(StatusCode.ERROR, sinkFail.getStatus().getStatusCode());
        assertTrue(named(spans, "spout ack").isEmpty());
    }

    @Test
    public void testTimeoutRecordsAnErrorSpanAtTheSpout() throws Exception {
        Config conf = conf(true);
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 2);
        // spout emit, sink execute, spout timeout
        List<SpanData> spans = runWithSink(SinkOutcome.HOLD, conf, 3);

        SpanData emit = named(spans, "spout emit").get(0);
        SpanData timeout = named(spans, "spout timeout").get(0);
        assertEquals(emit.getSpanId(), timeout.getParentSpanId());
        assertEquals(StatusCode.ERROR, timeout.getStatus().getStatusCode());
        assertTrue(named(spans, "spout fail").isEmpty());
    }

    @Test
    public void testAnchoredEmitContinuesTheTrace() throws Exception {
        // per tuple: spout emit, middle execute, sink execute
        List<SpanData> spans = runThroughMiddle(EmitMode.ANCHORED, 2, 6, 2);

        assertSinkExecutesAreChildrenOfMiddleExecutes(spans, 2);
    }

    @Test
    public void testAnchorsCarryingTheSameSpanAddNoMergeSpan() throws Exception {
        // per tuple: spout emit, middle execute, sink execute; the emit anchors the input twice
        List<SpanData> spans = runThroughMiddle(EmitMode.ANCHORED_TWICE, 2, 6, 2);

        assertTrue(named(spans, "middle emit").isEmpty());
        assertSinkExecutesAreChildrenOfMiddleExecutes(spans, 2);
    }

    @Test
    public void testEmitAnchoredToTwoTracedTuplesStartsARootWithTwoLinks() throws Exception {
        // 2 spout emits, 2 middle executes, 1 merge span, 1 sink execute
        List<SpanData> spans = runThroughMiddle(EmitMode.JOIN, 2, 6, 1);

        List<SpanData> merges = named(spans, "middle emit");
        assertEquals(1, merges.size());
        SpanData merge = merges.get(0);
        assertFalse(merge.getParentSpanContext().isValid(), "a merge starts a new trace");
        Set<String> linked = merge.getLinks().stream()
            .map(link -> link.getSpanContext().getSpanId()).collect(Collectors.toSet());
        assertEquals(byId(named(spans, "middle execute")).keySet(), linked);
        List<SpanData> sinks = named(spans, "sink execute");
        assertEquals(1, sinks.size());
        assertEquals(merge.getSpanId(), sinks.get(0).getParentSpanId());
    }

    @Test
    public void testUnanchoredEmitCarriesNoContext() throws Exception {
        // per tuple: spout emit, middle execute; the sink gets untraced tuples
        List<SpanData> spans = runThroughMiddle(EmitMode.UNANCHORED, 2, 4, 2);

        assertEquals(2, named(spans, "middle execute").size());
        assertTrue(named(spans, "sink execute").isEmpty());
        assertTrue(RECEIVED_TRACE_IDS.isEmpty(), "the sink's tuples carry no context");
    }

    @Test
    public void testDelayedEmitsFromAnotherThreadKeepTheirOwnParents() throws Exception {
        // per tuple: spout emit, middle execute, sink execute
        List<SpanData> spans = runThroughMiddle(EmitMode.ASYNC_REVERSED, 2, 6, 2);

        assertNull(EMITTER_THREAD_FAILURE.get());
        Map<String, SpanData> byId = byId(spans);
        for (Object value : MIDDLE_SPAN_BY_VALUE.keySet()) {
            SpanData sink = byId.get(SINK_SPAN_BY_VALUE.get(value));
            assertEquals(MIDDLE_SPAN_BY_VALUE.get(value), sink.getParentSpanId(),
                "the sink span of " + value + " is a child of the middle span of " + value);
        }
    }

    @Test
    public void testUnsampledContextsPropagateAndNothingIsExported() throws Exception {
        // parent-based: a sampled flag flipped on the way would export the middle or sink span
        InMemorySpanExporter exporter = InMemorySpanExporter.create();
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
            .setSampler(Sampler.parentBased(Sampler.alwaysOff()))
            .addSpanProcessor(SimpleSpanProcessor.create(exporter))
            .build();
        try (OpenTelemetrySdk sdk =
                 OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build()) {
            GlobalOpenTelemetry.resetForTest();
            GlobalOpenTelemetry.set(sdk);
            // spans go to this SDK, not OTEL: wait for the sink only
            runThroughMiddle(EmitMode.ANCHORED, 2, 0, 2);
        } finally {
            GlobalOpenTelemetry.resetForTest();
            GlobalOpenTelemetry.set(OTEL.getOpenTelemetry());
        }

        assertTrue(exporter.getFinishedSpanItems().isEmpty());
        assertEquals(2, UNSAMPLED_CONTEXTS_RECEIVED.get(), "the sink got unsampled contexts");
        assertEquals(MIDDLE_TRACE_IDS, RECEIVED_TRACE_IDS, "the traces continue to the sink");
        // different workers: the contexts went through the serializer
        assertNotEquals(WORKER_PORT_BY_COMPONENT.get("middle"),
            WORKER_PORT_BY_COMPONENT.get("sink"));
    }

    private static void assertSinkExecutesAreChildrenOfMiddleExecutes(List<SpanData> spans,
        int count) {
        Map<String, SpanData> middles = byId(named(spans, "middle execute"));
        List<SpanData> sinks = named(spans, "sink execute");
        assertEquals(count, middles.size());
        assertEquals(count, sinks.size());
        for (SpanData sink : sinks) {
            SpanData middle = middles.get(sink.getParentSpanId());
            assertNotNull(middle, "the emit carries the middle execute span as parent");
            assertEquals(middle.getTraceId(), sink.getTraceId());
        }
    }

    /**
     * Spout to sink (all grouping). With {@code tickSecs} positive, also waits for two ticks.
     */
    private List<SpanData> runSpoutToSink(boolean tracing, int count, int sinkTasks, int tickSecs)
        throws Exception {
        Config conf = conf(tracing);
        if (tickSecs > 0) {
            conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickSecs);
        }
        // one emit span per tuple and one execute span per tuple and sink task
        int expectedSpans = tracing ? count * (1 + sinkTasks) : 0;
        return runTopology(conf, count,
            builder -> builder.setBolt("sink", new SinkBolt(), sinkTasks).allGrouping("spout"),
            () -> OTEL.getSpans().size() >= expectedSpans
                && (tickSecs == 0 || TICK_TUPLES_RECEIVED.get() >= 2));
    }

    /**
     * Spout to middle (one task, which JOIN and ASYNC_REVERSED need) to sink.
     */
    private List<SpanData> runThroughMiddle(EmitMode mode, int count, int expectedSpans,
        int sinkTuples) throws Exception {
        return runTopology(conf(true), count,
            builder -> {
                builder.setBolt("middle", new MiddleBolt(mode)).shuffleGrouping("spout");
                builder.setBolt("sink", new SinkBolt()).shuffleGrouping("middle");
            },
            () -> OTEL.getSpans().size() >= expectedSpans
                && SINK_TUPLES_RECEIVED.get() >= sinkTuples);
    }

    /** One tuple from spout "spout" to a sink that acks, fails or holds it. */
    private List<SpanData> runWithSink(SinkOutcome outcome, Config conf, int expectedSpans)
        throws Exception {
        return runTopology(conf, 1,
            builder -> builder.setBolt("sink", new SinkBolt(outcome)).shuffleGrouping("spout"),
            () -> OTEL.getSpans().size() >= expectedSpans);
    }

    private static Config conf(boolean tracing) {
        Config conf = new Config();
        conf.setNumWorkers(2);
        conf.put(Config.TOPOLOGY_TRACING_ENABLED, tracing);
        return conf;
    }

    /**
     * Feeds {@code count} tuples to spout "spout", waits until each is acked or failed and
     * {@code done} holds, and returns the exported spans.
     */
    private List<SpanData> runTopology(Config conf, int count, Consumer<TopologyBuilder> bolts,
        BooleanSupplier done) throws Exception {
        FeederSpout spout = new FeederSpout(new Fields("value"));
        AckFailMapTracker tracker = new AckFailMapTracker();
        spout.setAckFailDelegate(tracker);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", spout);
        bolts.accept(builder);

        OTEL.clearSpans();
        RECEIVED_TRACE_IDS.clear();
        CURRENT_IN_EXECUTE.clear();
        SINK_TUPLES_RECEIVED.set(0);
        UNSAMPLED_CONTEXTS_RECEIVED.set(0);
        MIDDLE_TRACE_IDS.clear();
        MIDDLE_SPAN_BY_VALUE.clear();
        SINK_SPAN_BY_VALUE.clear();
        WORKER_PORT_BY_COMPONENT.clear();
        EMITTER_THREAD_FAILURE.set(null);
        TICK_TUPLES_RECEIVED.set(0);
        SPAN_CURRENT_DURING_TICK.set(false);
        String name = "tracing-" + topologyCount++;
        StormTopology topology = builder.createTopology();
        try (ILocalTopology ignored = cluster.submitTopology(name, conf, topology)) {
            Object[] ids = new Object[count];
            for (int i = 0; i < count; i++) {
                ids[i] = i;
                spout.feed(new Values("v" + i), i);
            }
            AssertLoop.assertLoop(id -> tracker.isAcked(id) || tracker.isFailed(id), ids);
            // spans end after the bolt acked, and unanchored tuples are not tracked by the acks
            Awaitility.await().atMost(Testing.TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .until(done::getAsBoolean);
            return OTEL.getSpans();
        }
    }

    private static List<SpanData> named(List<SpanData> spans, String name) {
        return spans.stream().filter(s -> s.getName().equals(name)).collect(Collectors.toList());
    }

    private static Map<String, SpanData> byId(List<SpanData> spans) {
        return spans.stream().collect(Collectors.toMap(SpanData::getSpanId, Function.identity()));
    }

    private enum EmitMode {
        ANCHORED,
        /** Anchored to the input twice: both anchors carry the same span. */
        ANCHORED_TWICE,
        UNANCHORED,
        /** Holds the first input, then emits anchored to both. */
        JOIN,
        /**
         * Holds the first input; on the second, another thread emits the second then the first,
         * each anchored to itself, with the first one's span current. The reverse order rules out
         * pairing emits with executes by arrival order.
         */
        ASYNC_REVERSED
    }

    private static class MiddleBolt extends BaseRichBolt {
        private final EmitMode mode;
        private transient OutputCollector collector;
        private transient Tuple held;

        MiddleBolt(EmitMode mode) {
            this.mode = mode;
        }

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context,
            OutputCollector collector) {
            this.collector = collector;
            WORKER_PORT_BY_COMPONENT.put("middle", context.getThisWorkerPort());
        }

        @Override
        public void execute(Tuple input) {
            SpanContext current = Span.current().getSpanContext();
            MIDDLE_SPAN_BY_VALUE.put(input.getValue(0), current.getSpanId());
            MIDDLE_TRACE_IDS.add(current.getTraceId());
            if ((mode == EmitMode.JOIN || mode == EmitMode.ASYNC_REVERSED) && held == null) {
                held = input;
                return;
            }
            Values values = new Values(input.getValue(0));
            switch (mode) {
                case ANCHORED:
                    collector.emit(input, values);
                    break;
                case ANCHORED_TWICE:
                    collector.emit(Arrays.asList(input, input), values);
                    break;
                case UNANCHORED:
                    collector.emit(values);
                    break;
                case JOIN:
                    collector.emit(Arrays.asList(held, input), values);
                    collector.ack(held);
                    held = null;
                    break;
                case ASYNC_REVERSED:
                    Tuple first = held;
                    held = null;
                    new Thread(() -> emitReversed(first, input)).start();
                    return;
                default:
                    throw new IllegalStateException("unknown mode " + mode);
            }
            collector.ack(input);
        }

        private void emitReversed(Tuple first, Tuple second) {
            try {
                Context firstContext = ((TupleImpl) first).getTraceContext();
                try (Scope ignored = firstContext.makeCurrent()) {
                    collector.emit(second, new Values(second.getValue(0)));
                    collector.emit(first, new Values(first.getValue(0)));
                }
                collector.ack(second);
                collector.ack(first);
            } catch (Throwable t) {
                EMITTER_THREAD_FAILURE.set(t);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("value"));
        }
    }

    private enum SinkOutcome {
        ACK,
        FAIL,
        /** Neither acks nor fails, so the tree times out. */
        HOLD
    }

    private static class SinkBolt extends BaseRichBolt {
        private final SinkOutcome outcome;
        private OutputCollector collector;

        SinkBolt() {
            this(SinkOutcome.ACK);
        }

        SinkBolt(SinkOutcome outcome) {
            this.outcome = outcome;
        }

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context,
            OutputCollector collector) {
            this.collector = collector;
            WORKER_PORT_BY_COMPONENT.put("sink", context.getThisWorkerPort());
        }

        @Override
        public void execute(Tuple input) {
            if (TupleUtils.isTick(input)) {
                if (Span.current().getSpanContext().isValid()) {
                    SPAN_CURRENT_DURING_TICK.set(true);
                }
                TICK_TUPLES_RECEIVED.incrementAndGet();
                return;
            }
            if (((TupleImpl) input).getTraceContext() != null) {
                SpanContext received = Span.fromContext(((TupleImpl) input).getTraceContext())
                    .getSpanContext();
                RECEIVED_TRACE_IDS.add(received.getTraceId());
                if (received.isValid() && !received.isSampled()) {
                    UNSAMPLED_CONTEXTS_RECEIVED.incrementAndGet();
                }
            }
            SpanContext current = Span.current().getSpanContext();
            if (current.isValid()) {
                CURRENT_IN_EXECUTE.add(current.getSpanId());
                SINK_SPAN_BY_VALUE.put(input.getValue(0), current.getSpanId());
            }
            SINK_TUPLES_RECEIVED.incrementAndGet();
            if (outcome == SinkOutcome.ACK) {
                collector.ack(input);
            } else if (outcome == SinkOutcome.FAIL) {
                collector.fail(input);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }
}
