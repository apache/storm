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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.storm.ILocalCluster.ILocalTopology;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.testing.FeederSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tracing.TupleTracer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs topologies on a two-worker local cluster with a tracer that records the calls Storm makes to it.
 */
public class TupleTracerTest {

    /** Tracer calls and execute() calls, in the order they happened on each thread. */
    private static final Queue<String> EVENTS = new ConcurrentLinkedQueue<>();
    private static final Map<Object, Long> LATENCY_BY_CONTEXT = new ConcurrentHashMap<>();
    private static final AtomicInteger NEXT_TRACE = new AtomicInteger();
    private static final AtomicInteger DECODES = new AtomicInteger();
    private static final Map<String, Integer> WORKER_PORT_BY_COMPONENT = new ConcurrentHashMap<>();

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
    public void testTracerCallsFollowTheTupleTree() throws Exception {
        // close() runs after the sink acked, so the acks alone do not mean all scopes are closed
        runThroughMiddle(EmitMode.ANCHORED, SinkOutcome.ACK, conf(), 2,
            () -> count("ACK ") == 2 && count("close ") == 4);

        List<String> events = new ArrayList<>(EVENTS);
        List<String> traces = contexts(events, "spoutEmit ");
        assertEquals(2, traces.size());
        for (String trace : traces) {
            String middle = trace + ">middle";
            String sink = middle + ">sink";
            assertInOrder(events, "start " + middle, "execute " + middle, "boltEmit middle [" + middle + "]",
                "close " + middle);
            assertInOrder(events, "start " + sink, "execute " + sink, "close " + sink);
            assertInOrder(events, "spoutEmit " + trace, "ACK " + trace);
            assertTrue(LATENCY_BY_CONTEXT.getOrDefault(trace, -1L) >= 0, "latency of " + trace);
        }
        assertNotEquals(WORKER_PORT_BY_COMPONENT.get("middle"), WORKER_PORT_BY_COMPONENT.get("sink"));
        assertTrue(DECODES.get() > 0, "contexts crossed workers");
    }

    @Test
    public void testFailIsReportedByTheBoltAndTheSpout() throws Exception {
        runSpoutToSink(SinkOutcome.FAIL, conf(), () -> count("FAIL ") == 1);

        List<String> events = new ArrayList<>(EVENTS);
        String trace = contexts(events, "spoutEmit ").get(0);
        assertInOrder(events, "start " + trace + ">sink", "boltFail " + trace + ">sink", "FAIL " + trace);
    }

    @Test
    public void testTimeoutIsReportedWithTheTimeSinceTheEmit() throws Exception {
        Config conf = conf();
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 2);
        runSpoutToSink(SinkOutcome.HOLD, conf, () -> count("TIMEOUT ") == 1);

        List<String> events = new ArrayList<>(EVENTS);
        String trace = contexts(events, "spoutEmit ").get(0);
        assertInOrder(events, "spoutEmit " + trace, "TIMEOUT " + trace);
        assertTrue(LATENCY_BY_CONTEXT.getOrDefault(trace, -1L) >= TimeUnit.SECONDS.toMillis(1));
    }

    @Test
    public void testBoltFailIsReportedWithoutAckers() throws Exception {
        Config conf = conf();
        conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
        runSpoutToSink(SinkOutcome.FAIL, conf, () -> count("boltFail ") == 1);

        String trace = contexts(new ArrayList<>(EVENTS), "spoutEmit ").get(0);
        assertEquals(1, count("boltFail " + trace + ">sink"));
        assertEquals(0, count("FAIL "), "without ackers the spout reports no outcome");
    }

    @Test
    public void testEmitGetsTheContextsOfItsAnchorsInOrder() throws Exception {
        runThroughMiddle(EmitMode.JOIN, SinkOutcome.ACK, conf(), 2, () -> count("close ") == 3);

        List<String> events = new ArrayList<>(EVENTS);
        List<String> middles = contexts(events, "execute ").stream().filter(c -> c.endsWith(">middle"))
            .collect(Collectors.toList());
        assertEquals(2, middles.size());
        String joined = middles.get(0) + "+" + middles.get(1);
        assertInOrder(events, "boltEmit middle [" + middles.get(0) + ", " + middles.get(1) + "]",
            "start " + joined + ">sink");
    }

    @Test
    public void testUnanchoredEmitCarriesNoContext() throws Exception {
        runThroughMiddle(EmitMode.UNANCHORED, SinkOutcome.ACK, conf(), 2, () -> count("execute null") == 2);

        assertEquals(2, count("boltEmit middle []"));
        assertEquals(2, count("start "), "only the middle bolt gets traced tuples");
    }

    @Test
    public void testTuplesAreNotTracedWithoutATracerClass() throws Exception {
        runThroughMiddle(EmitMode.ANCHORED, SinkOutcome.ACK, new Config(), 2, () -> count("execute null") == 4);

        assertEquals(EVENTS.size(), count("execute null"), "only execute() calls, all without a context");
    }

    private static Config conf() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TRACING_TRACER, RecordingTracer.class.getName());
        return conf;
    }

    private void runSpoutToSink(SinkOutcome outcome, Config conf, BooleanSupplier done) throws Exception {
        runTopology(conf, 1, builder -> builder.setBolt("sink", new SinkBolt(outcome)).shuffleGrouping("spout"), done);
    }

    private void runThroughMiddle(EmitMode mode, SinkOutcome outcome, Config conf, int count, BooleanSupplier done)
        throws Exception {
        runTopology(conf, count, builder -> {
            // one middle task, which JOIN needs
            builder.setBolt("middle", new MiddleBolt(mode)).shuffleGrouping("spout");
            builder.setBolt("sink", new SinkBolt(outcome)).shuffleGrouping("middle");
        }, done);
    }

    /**
     * Feeds {@code count} tuples to spout "spout" and waits until {@code done} holds.
     */
    private void runTopology(Config conf, int count, Consumer<TopologyBuilder> bolts, BooleanSupplier done)
        throws Exception {
        conf.setNumWorkers(2);
        FeederSpout spout = new FeederSpout(new Fields("value"));
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", spout);
        bolts.accept(builder);

        EVENTS.clear();
        LATENCY_BY_CONTEXT.clear();
        DECODES.set(0);
        WORKER_PORT_BY_COMPONENT.clear();
        String name = "tracer-" + topologyCount++;
        try (ILocalTopology ignored = cluster.submitTopology(name, conf, builder.createTopology())) {
            for (int i = 0; i < count; i++) {
                spout.feed(new Values("v" + i), i);
            }
            Awaitility.await().atMost(Testing.TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS).until(done::getAsBoolean);
        }
    }

    private static long count(String prefix) {
        return EVENTS.stream().filter(e -> e.startsWith(prefix)).count();
    }

    private static List<String> contexts(List<String> events, String prefix) {
        return events.stream().filter(e -> e.startsWith(prefix)).map(e -> e.substring(prefix.length()))
            .collect(Collectors.toList());
    }

    private static void assertInOrder(List<String> events, String... expected) {
        int previous = -1;
        for (String event : expected) {
            int index = events.indexOf(event);
            assertTrue(index > previous, event + " after " + String.join(", ", expected) + " in " + events);
            previous = index;
        }
    }

    /**
     * Spout contexts are "t1", "t2", ...; an execute appends "&gt;component" to the context it gets; a bolt emit
     * joins its anchor contexts with "+".
     */
    public static class RecordingTracer implements TupleTracer {
        private WorkerTopologyContext context;

        @Override
        public void prepare(Map<String, Object> topoConf, WorkerTopologyContext context) {
            this.context = context;
        }

        @Override
        public Object spoutEmit(int taskId, String streamId) {
            String trace = "t" + NEXT_TRACE.incrementAndGet();
            EVENTS.add("spoutEmit " + trace);
            return trace;
        }

        @Override
        public Object boltEmit(int taskId, String streamId, List<Object> anchorContexts) {
            EVENTS.add("boltEmit " + context.getComponentId(taskId) + " " + anchorContexts);
            return anchorContexts.isEmpty() ? null
                : anchorContexts.stream().map(String::valueOf).collect(Collectors.joining("+"));
        }

        @Override
        public ExecuteScope startExecute(int taskId, Tuple tuple, Object received) {
            String executeContext = received + ">" + context.getComponentId(taskId);
            EVENTS.add("start " + executeContext);
            return new ExecuteScope() {
                @Override
                public Object context() {
                    return executeContext;
                }

                @Override
                public void close() {
                    EVENTS.add("close " + executeContext);
                }
            };
        }

        @Override
        public void spoutOutcome(int taskId, Object context, Outcome outcome, long latencyMs) {
            LATENCY_BY_CONTEXT.put(context, latencyMs);
            EVENTS.add(outcome + " " + context);
        }

        @Override
        public void boltFail(int taskId, Object context) {
            EVENTS.add("boltFail " + context);
        }

        @Override
        public byte[] encode(Object context) {
            return ((String) context).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public Object decode(byte[] bytes) {
            DECODES.incrementAndGet();
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    private enum EmitMode {
        ANCHORED,
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
        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
            WORKER_PORT_BY_COMPONENT.put("middle", context.getThisWorkerPort());
        }

        @Override
        public void execute(Tuple input) {
            EVENTS.add("execute " + ((TupleImpl) input).getTraceContext());
            Values values = new Values(input.getValue(0));
            switch (mode) {
                case ANCHORED:
                    collector.emit(input, values);
                    break;
                case UNANCHORED:
                    collector.emit(values);
                    break;
                case JOIN:
                    if (held == null) {
                        held = input;
                        return;
                    }
                    collector.emit(Arrays.asList(held, input), values);
                    collector.ack(held);
                    break;
                default:
                    throw new IllegalStateException("unknown mode " + mode);
            }
            collector.ack(input);
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
        private transient OutputCollector collector;

        SinkBolt(SinkOutcome outcome) {
            this.outcome = outcome;
        }

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
            WORKER_PORT_BY_COMPONENT.put("sink", context.getThisWorkerPort());
        }

        @Override
        public void execute(Tuple input) {
            EVENTS.add("execute " + ((TupleImpl) input).getTraceContext());
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
