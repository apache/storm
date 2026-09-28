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
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
        List<SpanData> spans = runSpoutToSink(true, 3);

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
        assertTrue(runSpoutToSink(false, 1).isEmpty());
    }

    /**
     * Feeds {@code count} tuples from spout "spout" to bolt "sink" on two workers, waits until all
     * are acked and returns the spans exported meanwhile.
     */
    private List<SpanData> runSpoutToSink(boolean tracing, int count) throws Exception {
        FeederSpout spout = new FeederSpout(new Fields("value"));
        AckFailMapTracker tracker = new AckFailMapTracker();
        spout.setAckFailDelegate(tracker);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", spout);
        builder.setBolt("sink", new SinkBolt()).shuffleGrouping("spout");

        Config conf = new Config();
        conf.setNumWorkers(2);
        conf.put(Config.TOPOLOGY_TRACING_ENABLED, tracing);
        OTEL.clearSpans();
        RECEIVED_TRACE_IDS.clear();
        String name = "tracing-" + topologyCount++;
        StormTopology topology = builder.createTopology();
        try (ILocalTopology ignored = cluster.submitTopology(name, conf, topology)) {
            Object[] ids = new Object[count];
            for (int i = 0; i < count; i++) {
                ids[i] = i;
                spout.feed(new Values("v" + i), i);
            }
            AssertLoop.assertAcked(tracker, ids);
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
            if (((TupleImpl) input).getTraceContext() != null) {
                Span span = Span.fromContext(((TupleImpl) input).getTraceContext());
                RECEIVED_TRACE_IDS.add(span.getSpanContext().getTraceId());
            }
            collector.ack(input);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }
}
