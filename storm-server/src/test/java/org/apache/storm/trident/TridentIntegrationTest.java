/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.trident;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalCluster.LocalTopology;
import org.apache.storm.LocalDRPC;
import org.apache.storm.Testing;
import org.apache.storm.generated.Bolt;
import org.apache.storm.trident.operation.DefaultResourceDeclarer;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.operation.builtin.TupleCollectionGet;
import org.apache.storm.trident.operation.impl.CombinerAggStateUpdater;
import org.apache.storm.trident.state.StateSpec;
import org.apache.storm.trident.testing.CountAsAggregator;
import org.apache.storm.trident.testing.FeederBatchSpout;
import org.apache.storm.trident.testing.FeederCommitterBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.trident.testing.StringLength;
import org.apache.storm.trident.testing.TrueFilter;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import net.minidev.json.JSONValue;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Ported from storm-core/test/clj/integration/org/apache/storm/trident/integration_test.clj
 */
public class TridentIntegrationTest {

    /**
     * A simple Function that appends "!" to the first field.
     */
    static class AddBangFunction implements Function, java.io.Serializable {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            collector.emit(new Values(tuple.getString(0) + "!"));
        }

        @Override
        public void prepare(Map<String, Object> conf, org.apache.storm.trident.operation.TridentOperationContext context) {
        }

        @Override
        public void cleanup() {
        }
    }

    @SuppressWarnings("unchecked")
    private static List<List<Object>> execDrpc(LocalDRPC drpc, String functionName, String args) throws Exception {
        String res = drpc.execute(functionName, args);
        if (res == null) {
            return null;
        }
        List<?> parsed = (List<?>) JSONValue.parse(res);
        List<List<Object>> result = new java.util.ArrayList<>();
        for (Object item : parsed) {
            List<?> innerList = (List<?>) item;
            List<Object> converted = new java.util.ArrayList<>();
            for (Object val : innerList) {
                // JSON-smart returns Integer for small numbers; normalize to Long for consistency
                if (val instanceof Integer) {
                    converted.add(((Integer) val).longValue());
                } else {
                    converted.add(val);
                }
            }
            result.add(converted);
        }
        return result;
    }

    private static Set<List<Object>> execDrpcAsSet(LocalDRPC drpc, String functionName, String args) throws Exception {
        List<List<Object>> result = execDrpc(drpc, functionName, args);
        return new HashSet<>(result);
    }

    @Test
    public void testMemoryMapGetTuples() throws Exception {
        try (LocalCluster cluster = new LocalCluster()) {
            try (LocalDRPC drpc = new LocalDRPC(cluster.getMetricRegistry())) {
                TridentTopology topo = new TridentTopology();
                FeederBatchSpout feeder = new FeederBatchSpout(Arrays.asList("sentence"));

                TridentState wordCounts = topo
                    .newStream("tester", feeder)
                    .each(new Fields("sentence"), new Split(), new Fields("word"))
                    .groupBy(new Fields("word"))
                    .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                    .parallelismHint(6);

                topo.newDRPCStream("all-tuples", drpc)
                    .broadcast()
                    .stateQuery(wordCounts, new Fields("args"), new TupleCollectionGet(), new Fields("word", "count"))
                    .project(new Fields("word", "count"));

                try (LocalTopology stormTopo = cluster.submitTopology("testing", Map.of(), topo.build())) {
                    feeder.feed(Arrays.asList(new Values("hello the man said"), new Values("the")));

                    Set<List<Object>> expected1 = Set.of(
                        Arrays.asList("hello", 1L),
                        Arrays.asList("said", 1L),
                        Arrays.asList("the", 2L),
                        Arrays.asList("man", 1L)
                    );
                    assertEquals(expected1, execDrpcAsSet(drpc, "all-tuples", "man"));

                    feeder.feed(Arrays.asList(new Values("the foo")));

                    Set<List<Object>> expected2 = Set.of(
                        Arrays.asList("hello", 1L),
                        Arrays.asList("said", 1L),
                        Arrays.asList("the", 3L),
                        Arrays.asList("man", 1L),
                        Arrays.asList("foo", 1L)
                    );
                    assertEquals(expected2, execDrpcAsSet(drpc, "all-tuples", "man"));
                }
            }
        }
    }

    @Test
    public void testWordCount() throws Exception {
        try (LocalCluster cluster = new LocalCluster()) {
            try (LocalDRPC drpc = new LocalDRPC(cluster.getMetricRegistry())) {
                TridentTopology topo = new TridentTopology();
                FeederBatchSpout feeder = new FeederBatchSpout(Arrays.asList("sentence"));

                TridentState wordCounts = topo
                    .newStream("tester", feeder)
                    .each(new Fields("sentence"), new Split(), new Fields("word"))
                    .groupBy(new Fields("word"))
                    .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                    .parallelismHint(6);

                topo.newDRPCStream("words", drpc)
                    .each(new Fields("args"), new Split(), new Fields("word"))
                    .groupBy(new Fields("word"))
                    .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
                    .aggregate(new Fields("count"), new Sum(), new Fields("sum"))
                    .project(new Fields("sum"));

                try (LocalTopology stormTopo = cluster.submitTopology("testing", Map.of(), topo.build())) {
                    feeder.feed(Arrays.asList(new Values("hello the man said"), new Values("the")));

                    assertEquals(List.of(List.of(2L)), execDrpc(drpc, "words", "the"));
                    assertEquals(List.of(List.of(1L)), execDrpc(drpc, "words", "hello"));

                    feeder.feed(Arrays.asList(new Values("the man on the moon"), new Values("where are you")));

                    assertEquals(List.of(List.of(4L)), execDrpc(drpc, "words", "the"));
                    assertEquals(List.of(List.of(2L)), execDrpc(drpc, "words", "man"));
                    assertEquals(List.of(List.of(8L)), execDrpc(drpc, "words", "man where you the"));
                }
            }
        }
    }

    /**
     * This test reproduces a bug where committer spouts freeze processing when
     * there's at least one repartitioning after the spout.
     */
    @Test
    public void testWordCountCommitterSpout() throws Exception {
        try (LocalCluster cluster = new LocalCluster()) {
            try (LocalDRPC drpc = new LocalDRPC(cluster.getMetricRegistry())) {
                TridentTopology topo = new TridentTopology();
                FeederCommitterBatchSpout feeder = new FeederCommitterBatchSpout(Arrays.asList("sentence"));
                feeder.setWaitToEmit(false); // this causes lots of empty batches

                TridentState wordCounts = topo
                    .newStream("tester", feeder)
                    .parallelismHint(2)
                    .each(new Fields("sentence"), new Split(), new Fields("word"))
                    .groupBy(new Fields("word"))
                    .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                    .parallelismHint(6);

                topo.newDRPCStream("words", drpc)
                    .each(new Fields("args"), new Split(), new Fields("word"))
                    .groupBy(new Fields("word"))
                    .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
                    .aggregate(new Fields("count"), new Sum(), new Fields("sum"))
                    .project(new Fields("sum"));

                try (LocalTopology stormTopo = cluster.submitTopology("testing", Map.of(), topo.build())) {
                    feeder.feed(Arrays.asList(new Values("hello the man said"), new Values("the")));

                    assertEquals(List.of(List.of(2L)), execDrpc(drpc, "words", "the"));
                    assertEquals(List.of(List.of(1L)), execDrpc(drpc, "words", "hello"));

                    // Wait for empty batches to cycle through — reproduces bug where
                    // committer spouts freeze processing after repartitioning
                    await().atMost(10, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
                        .pollDelay(500, TimeUnit.MILLISECONDS)
                        .untilAsserted(() -> {
                            // Just verify the system is still responsive after some empty batches
                            List<List<Object>> result = execDrpc(drpc, "words", "the");
                            assertEquals(List.of(List.of(2L)), result);
                        });

                    feeder.feed(Arrays.asList(new Values("the man on the moon"), new Values("where are you")));

                    assertEquals(List.of(List.of(4L)), execDrpc(drpc, "words", "the"));
                    assertEquals(List.of(List.of(2L)), execDrpc(drpc, "words", "man"));
                    assertEquals(List.of(List.of(8L)), execDrpc(drpc, "words", "man where you the"));

                    feeder.feed(Arrays.asList(new Values("the the")));
                    assertEquals(List.of(List.of(6L)), execDrpc(drpc, "words", "the"));

                    feeder.feed(Arrays.asList(new Values("the")));
                    assertEquals(List.of(List.of(7L)), execDrpc(drpc, "words", "the"));
                }
            }
        }
    }

    @Test
    public void testCountAgg() throws Exception {
        try (LocalCluster cluster = new LocalCluster()) {
            try (LocalDRPC drpc = new LocalDRPC(cluster.getMetricRegistry())) {
                TridentTopology topo = new TridentTopology();

                topo.newDRPCStream("numwords", drpc)
                    .each(new Fields("args"), new Split(), new Fields("word"))
                    .aggregate(new CountAsAggregator(), new Fields("count"))
                    .parallelismHint(2) // this makes sure batchGlobal is working correctly
                    .project(new Fields("count"));

                try (LocalTopology stormTopo = cluster.submitTopology("testing", Map.of(), topo.build())) {
                    for (int i = 0; i < 100; i++) {
                        assertEquals(List.of(List.of(1L)), execDrpc(drpc, "numwords", "the"));
                    }
                    assertEquals(List.of(List.of(0L)), execDrpc(drpc, "numwords", ""));
                    assertEquals(List.of(List.of(8L)), execDrpc(drpc, "numwords", "1 2 3 4 5 6 7 8"));
                }
            }
        }
    }

    @Test
    public void testSplitMerge() throws Exception {
        try (LocalCluster cluster = new LocalCluster()) {
            try (LocalDRPC drpc = new LocalDRPC(cluster.getMetricRegistry())) {
                TridentTopology topo = new TridentTopology();
                Stream drpcStream = topo.newDRPCStream("splitter", drpc);

                Stream s1 = drpcStream
                    .each(new Fields("args"), new Split(), new Fields("word"))
                    .project(new Fields("word"));

                Stream s2 = drpcStream
                    .each(new Fields("args"), new StringLength(), new Fields("len"))
                    .project(new Fields("len"));

                topo.merge(s1, s2);

                try (LocalTopology stormTopo = cluster.submitTopology("testing", Map.of(), topo.build())) {
                    assertTrue(Testing.multiseteq(
                        Arrays.asList(Arrays.asList(7L), Arrays.asList("the"), Arrays.asList("man")),
                        execDrpc(drpc, "splitter", "the man")));
                    assertTrue(Testing.multiseteq(
                        Arrays.asList(Arrays.asList(5L), Arrays.asList("hello")),
                        execDrpc(drpc, "splitter", "hello")));
                }
            }
        }
    }

    @Test
    public void testMultipleGroupingsSameStream() throws Exception {
        try (LocalCluster cluster = new LocalCluster()) {
            try (LocalDRPC drpc = new LocalDRPC(cluster.getMetricRegistry())) {
                TridentTopology topo = new TridentTopology();
                Stream drpcStream = topo.newDRPCStream("tester", drpc)
                    .each(new Fields("args"), new TrueFilter());

                Stream s1 = drpcStream
                    .groupBy(new Fields("args"))
                    .aggregate(new CountAsAggregator(), new Fields("count"));

                Stream s2 = drpcStream
                    .groupBy(new Fields("args"))
                    .aggregate(new CountAsAggregator(), new Fields("count"));

                topo.merge(s1, s2);

                try (LocalTopology stormTopo = cluster.submitTopology("testing", Map.of(), topo.build())) {
                    assertTrue(Testing.multiseteq(
                        Arrays.asList(Arrays.asList("the", 1L), Arrays.asList("the", 1L)),
                        execDrpc(drpc, "tester", "the")));
                    assertTrue(Testing.multiseteq(
                        Arrays.asList(Arrays.asList("aaaaa", 1L), Arrays.asList("aaaaa", 1L)),
                        execDrpc(drpc, "tester", "aaaaa")));
                }
            }
        }
    }

    @Test
    public void testMultiRepartition() throws Exception {
        try (LocalCluster cluster = new LocalCluster()) {
            try (LocalDRPC drpc = new LocalDRPC(cluster.getMetricRegistry())) {
                TridentTopology topo = new TridentTopology();
                topo.newDRPCStream("tester", drpc)
                    .each(new Fields("args"), new Split(), new Fields("word"))
                    .localOrShuffle()
                    .shuffle()
                    .aggregate(new CountAsAggregator(), new Fields("count"));

                try (LocalTopology stormTopo = cluster.submitTopology("testing", Map.of(), topo.build())) {
                    assertTrue(Testing.multiseteq(
                        List.of(List.of(2L)),
                        execDrpc(drpc, "tester", "the man")));
                    assertTrue(Testing.multiseteq(
                        List.of(List.of(1L)),
                        execDrpc(drpc, "tester", "aaa")));
                }
            }
        }
    }

    @Test
    public void testStreamProjectionValidation() throws Exception {
        try (LocalCluster cluster = new LocalCluster()) {
            FeederCommitterBatchSpout feeder = new FeederCommitterBatchSpout(Arrays.asList("sentence"));
            TridentTopology topo = new TridentTopology();

            // valid projection fields will not throw exceptions
            TridentState wordCounts = topo
                .newStream("tester", feeder)
                .each(new Fields("sentence"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(6);

            Stream stream = topo.newStream("tester", feeder);

            // test .each
            assertThrows(IllegalArgumentException.class, () ->
                stream.each(new Fields("sentence1"), new Split(), new Fields("word")));

            // test .groupBy
            assertThrows(IllegalArgumentException.class, () ->
                stream.each(new Fields("sentence"), new Split(), new Fields("word"))
                      .groupBy(new Fields("word1")));

            // test .aggregate
            assertThrows(IllegalArgumentException.class, () ->
                stream.each(new Fields("sentence"), new Split(), new Fields("word"))
                      .groupBy(new Fields("word"))
                      .aggregate(new Fields("word1"), new Count(), new Fields("count")));

            // test .project
            assertThrows(IllegalArgumentException.class, () ->
                stream.project(new Fields("sentence1")));

            // test .partitionBy
            assertThrows(IllegalArgumentException.class, () ->
                stream.partitionBy(new Fields("sentence1")));

            // test .partitionAggregate
            assertThrows(IllegalArgumentException.class, () ->
                stream.each(new Fields("sentence"), new Split(), new Fields("word"))
                      .partitionAggregate(new Fields("word1"), new Count(), new Fields("count")));

            // test .persistentAggregate
            assertThrows(IllegalArgumentException.class, () ->
                stream.each(new Fields("sentence"), new Split(), new Fields("word"))
                      .groupBy(new Fields("word"))
                      .persistentAggregate(new StateSpec(new MemoryMapState.Factory()),
                                           new Fields("non-existent"), new Count(), new Fields("count")));

            // test .partitionPersist
            assertThrows(IllegalArgumentException.class, () ->
                stream.each(new Fields("sentence"), new Split(), new Fields("word"))
                      .partitionPersist(new StateSpec(new MemoryMapState.Factory()),
                                        new Fields("non-existent"),
                                        new CombinerAggStateUpdater(new Count()),
                                        new Fields("count")));

            // test .stateQuery
            try (LocalDRPC drpc = new LocalDRPC(cluster.getMetricRegistry())) {
                assertThrows(IllegalArgumentException.class, () ->
                    topo.newDRPCStream("words", drpc)
                        .each(new Fields("args"), new Split(), new Fields("word"))
                        .groupBy(new Fields("word"))
                        .stateQuery(wordCounts, new Fields("word1"), new MapGet(), new Fields("count")));
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSetComponentResources() throws Exception {
        try (LocalCluster cluster = new LocalCluster()) {
            try (LocalDRPC drpc = new LocalDRPC(cluster.getMetricRegistry())) {
                TridentTopology topo = new TridentTopology();
                FeederBatchSpout feeder = new FeederBatchSpout(Arrays.asList("sentence"));

                Function addBang = new AddBangFunction();

                DefaultResourceDeclarer defaults = new DefaultResourceDeclarer();
                defaults.setMemoryLoad(0, 0);
                defaults.setCPULoad(0);
                topo.setResourceDefaults(defaults);

                topo.newStream("words", feeder)
                    .parallelismHint(5)
                    .setCPULoad(20)
                    .setMemoryLoad(512, 256)
                    .each(new Fields("sentence"), new Split(), new Fields("word"))
                    .setCPULoad(10)
                    .setMemoryLoad(512)
                    .each(new Fields("word"), addBang, new Fields("word!"))
                    .parallelismHint(10)
                    .setCPULoad(50)
                    .setMemoryLoad(1024)
                    .groupBy(new Fields("word!"))
                    .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                    .setCPULoad(100)
                    .setMemoryLoad(2048);

                Map<String, Object> conf = Map.of(
                    Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 4096.0
                );

                try (LocalTopology stormTopo = cluster.submitTopology("testing", conf, topo.build())) {
                    Map<String, Bolt> bolts = stormTopo.get_bolts();

                    JSONParser parser = new JSONParser();

                    // Helper to get JSON conf for a bolt
                    java.util.function.Function<String, Map<String, Object>> getJsonConf = (boltId) -> {
                        try {
                            Bolt bolt = bolts.get(boltId);
                            return (Map<String, Object>) parser.parse(bolt.get_common().get_json_conf());
                        } catch (ParseException e) {
                            throw new RuntimeException(e);
                        }
                    };

                    // spout memory
                    Map<String, Object> spoutConf = getJsonConf.apply("spout-words");
                    assertEquals(512.0, spoutConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB));
                    assertEquals(256.0, spoutConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB));

                    Map<String, Object> spoutCoordConf = getJsonConf.apply("$spoutcoord-spout-words");
                    assertEquals(512.0, spoutCoordConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB));
                    assertEquals(256.0, spoutCoordConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB));

                    // spout CPU
                    assertEquals(20.0, spoutConf.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT));
                    assertEquals(20.0, spoutCoordConf.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT));

                    // bolt combinations (b-1 combines Split + addBang: 10+50 CPU, 512+1024 memory)
                    Map<String, Object> b1Conf = getJsonConf.apply("b-1");
                    assertEquals(1024.0 + 512.0, b1Conf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB));
                    assertEquals(60.0, b1Conf.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT));

                    // aggregations after partition (b-0 = persistentAggregate)
                    Map<String, Object> b0Conf = getJsonConf.apply("b-0");
                    assertEquals(2048.0, b0Conf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB));
                    assertEquals(100.0, b0Conf.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT));
                }
            }
        }
    }
}
