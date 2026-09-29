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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
     * Spout to middle (one task, which JOIN needs; emitting as {@code mode} says) to sink.
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

    private static Config conf(boolean tracing) {
        Config conf = new Config();
        conf.setNumWorkers(2);
        conf.put(Config.TOPOLOGY_TRACING_ENABLED, tracing);
        return conf;
    }

    /**
     * Feeds {@code count} tuples to spout "spout", waits for the acks and {@code done}, and
     * returns the exported spans.
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
            AssertLoop.assertAcked(tracker, ids);
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
        JOIN
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
        }

        @Override
        public void execute(Tuple input) {
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
                default:
                    if (held == null) {
                        held = input;
                        return;
                    }
                    collector.emit(Arrays.asList(held, input), values);
                    collector.ack(held);
                    held = null;
            }
            collector.ack(input);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("value"));
        }
    }

    private static class SinkBolt extends BaseRichBolt {
        private OutputCollector collector;

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context,
            OutputCollector collector) {
            this.collector = collector;
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
                Span span = Span.fromContext(((TupleImpl) input).getTraceContext());
                RECEIVED_TRACE_IDS.add(span.getSpanContext().getTraceId());
            }
            SpanContext current = Span.current().getSpanContext();
            if (current.isValid()) {
                CURRENT_IN_EXECUTE.add(current.getSpanId());
            }
            SINK_TUPLES_RECEIVED.incrementAndGet();
            collector.ack(input);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }
}
