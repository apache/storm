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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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

    /** Trace ids of the contexts carried by the tuples the sink received. */
    private static final Set<String> RECEIVED_TRACE_IDS = ConcurrentHashMap.newKeySet();
    /** Span ids that were current on the bolt thread while execute() ran. */
    private static final Set<String> CURRENT_IN_EXECUTE = ConcurrentHashMap.newKeySet();
    /** Tick tuples the sink received. */
    private static final AtomicInteger TICK_TUPLES_RECEIVED = new AtomicInteger();
    /** Set when a span was still current on the bolt thread while a tick tuple was handled. */
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

        Map<String, SpanData> emits = named(spans, "spout emit").stream()
            .collect(Collectors.toMap(SpanData::getSpanId, Function.identity()));
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

    /**
     * Feeds {@code count} tuples from spout "spout" to bolt "sink" (all grouping) on two workers,
     * waits until all are acked, all expected spans are exported and, when {@code tickSecs} is
     * positive, until the sink got two tick tuples. Returns the exported spans.
     */
    private List<SpanData> runSpoutToSink(boolean tracing, int count, int sinkTasks, int tickSecs)
        throws Exception {
        FeederSpout spout = new FeederSpout(new Fields("value"));
        AckFailMapTracker tracker = new AckFailMapTracker();
        spout.setAckFailDelegate(tracker);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", spout);
        builder.setBolt("sink", new SinkBolt(), sinkTasks).allGrouping("spout");

        Config conf = new Config();
        conf.setNumWorkers(2);
        conf.put(Config.TOPOLOGY_TRACING_ENABLED, tracing);
        if (tickSecs > 0) {
            conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickSecs);
        }
        OTEL.clearSpans();
        RECEIVED_TRACE_IDS.clear();
        CURRENT_IN_EXECUTE.clear();
        TICK_TUPLES_RECEIVED.set(0);
        SPAN_CURRENT_DURING_TICK.set(false);
        // one emit span per tuple and one execute span per tuple and sink task
        int expectedSpans = tracing ? count * (1 + sinkTasks) : 0;
        String name = "tracing-" + topologyCount++;
        StormTopology topology = builder.createTopology();
        try (ILocalTopology ignored = cluster.submitTopology(name, conf, topology)) {
            Object[] ids = new Object[count];
            for (int i = 0; i < count; i++) {
                ids[i] = i;
                spout.feed(new Values("v" + i), i);
            }
            AssertLoop.assertAcked(tracker, ids);
            // an execute span ends after the bolt acked, so the ack can arrive before the span
            Awaitility.await().atMost(Testing.TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .until(() -> OTEL.getSpans().size() >= expectedSpans);
            if (tickSecs > 0) {
                Awaitility.await().atMost(Testing.TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                    .until(() -> TICK_TUPLES_RECEIVED.get() >= 2);
            }
            return OTEL.getSpans();
        }
    }

    private static List<SpanData> named(List<SpanData> spans, String name) {
        return spans.stream().filter(s -> s.getName().equals(name)).collect(Collectors.toList());
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
            collector.ack(input);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }
}
