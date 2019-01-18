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

package org.apache.storm.streams;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.NullStruct;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.streams.operations.aggregators.Count;
import org.apache.storm.streams.operations.mappers.PairValueMapper;
import org.apache.storm.streams.operations.mappers.ValueMapper;
import org.apache.storm.streams.processors.BranchProcessor;
import org.apache.storm.streams.windowing.TumblingWindows;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link StreamBuilder}
 */
public class StreamBuilderTest {
    StreamBuilder streamBuilder;

    private static IRichSpout newSpout(final String os) {
        return new BaseRichSpout() {

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
                declarer.declareStream(os, new Fields("value"));
            }

            @Override
            public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {

            }

            @Override
            public void nextTuple() {

            }
        };
    }

    private static IRichBolt newBolt() {
        return new BaseRichBolt() {

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {

            }

            @Override
            public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {

            }

            @Override
            public void execute(Tuple input) {

            }
        };
    }

    @Before
    public void setUp() throws Exception {
        streamBuilder = new StreamBuilder();
        UniqueIdGen.getInstance().reset();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSpoutNoDefaultStream() throws Exception {
        Stream<Tuple> stream = streamBuilder.newStream(newSpout("test"));
        stream.filter(x -> true);
        streamBuilder.build();
    }

    @Test
    public void testSpoutToBolt() throws Exception {
        Stream<Tuple> stream = streamBuilder.newStream(newSpout(Utils.DEFAULT_STREAM_ID));
        stream.to(newBolt());
        StormTopology topology = streamBuilder.build();
        assertEquals(1, topology.get_spouts_size());
        assertEquals(1, topology.get_bolts_size());
        String spoutId = topology.get_spouts().keySet().iterator().next();
        Map<GlobalStreamId, Grouping> expected = new HashMap<>();
        expected.put(new GlobalStreamId(spoutId, "default"), Grouping.shuffle(new NullStruct()));
        assertEquals(expected, topology.get_bolts().values().iterator().next().get_common().get_inputs());
    }

    @Test
    public void testBranch() throws Exception {
        Stream<Tuple> stream = streamBuilder.newStream(newSpout(Utils.DEFAULT_STREAM_ID));
        Stream<Tuple>[] streams = stream.branch(x -> true);
        StormTopology topology = streamBuilder.build();
        assertEquals(1, topology.get_spouts_size());
        assertEquals(1, topology.get_bolts_size());
        Map<GlobalStreamId, Grouping> expected = new HashMap<>();
        String spoutId = topology.get_spouts().keySet().iterator().next();
        expected.put(new GlobalStreamId(spoutId, "default"), Grouping.shuffle(new NullStruct()));
        assertEquals(expected, topology.get_bolts().values().iterator().next().get_common().get_inputs());
        assertEquals(1, streams.length);
        assertEquals(1, streams[0].node.getOutputStreams().size());
        String parentStream = streams[0].node.getOutputStreams().iterator().next() + "-branch";
        assertEquals(1, streams[0].node.getParents(parentStream).size());
        Node processorNdoe = streams[0].node.getParents(parentStream).iterator().next();
        assertTrue(processorNdoe instanceof ProcessorNode);
        assertTrue(((ProcessorNode) processorNdoe).getProcessor() instanceof BranchProcessor);
        assertTrue(processorNdoe.getParents("default").iterator().next() instanceof SpoutNode);
    }

    @Test
    public void testJoin() throws Exception {
        Stream<Integer> stream = streamBuilder.newStream(newSpout(Utils.DEFAULT_STREAM_ID), new ValueMapper<>(0));
        Stream<Integer>[] streams = stream.branch(x -> x % 2 == 0, x -> x % 3 == 0);
        PairStream<Integer, Integer> s1 = streams[0].mapToPair(x -> Pair.of(x, 1));
        PairStream<Integer, Integer> s2 = streams[1].mapToPair(x -> Pair.of(x, 1));
        PairStream<Integer, Pair<Integer, Integer>> sj = s1.join(s2);
        assertEquals(Collections.singleton(s1.node), sj.node.getParents(s1.stream));
        assertEquals(Collections.singleton(s2.node), sj.node.getParents(s2.stream));
    }

    @Test
    public void testGroupBy() throws Exception {
        PairStream<String, String> stream = streamBuilder.newStream(newSpout(Utils.DEFAULT_STREAM_ID), new PairValueMapper<>(0, 1), 2);

        stream.window(TumblingWindows.of(BaseWindowedBolt.Count.of(10))).aggregateByKey(new Count<>());

        StormTopology topology = streamBuilder.build();
        assertEquals(2, topology.get_bolts_size());
        Bolt bolt1 = topology.get_bolts().get("bolt1");
        Bolt bolt2 = topology.get_bolts().get("bolt2");
        assertEquals(Grouping.shuffle(new NullStruct()), bolt1.get_common().get_inputs().values().iterator().next());
        assertEquals(Grouping.fields(Collections.singletonList("key")), bolt2.get_common().get_inputs().values().iterator().next());
    }

    @Test
    public void testGlobalAggregate() throws Exception {
        Stream<String> stream = streamBuilder.newStream(newSpout(Utils.DEFAULT_STREAM_ID), new ValueMapper<>(0), 2);

        stream.aggregate(new Count<>());

        StormTopology topology = streamBuilder.build();
        assertEquals(2, topology.get_bolts_size());
        Bolt bolt1 = topology.get_bolts().get("bolt1");
        Bolt bolt2 = topology.get_bolts().get("bolt2");
        String spoutId = topology.get_spouts().keySet().iterator().next();
        Map<GlobalStreamId, Grouping> expected1 = new HashMap<>();
        expected1.put(new GlobalStreamId(spoutId, "default"), Grouping.shuffle(new NullStruct()));
        Map<GlobalStreamId, Grouping> expected2 = new HashMap<>();
        expected2.put(new GlobalStreamId("bolt1", "s1"), Grouping.fields(Collections.emptyList()));
        expected2.put(new GlobalStreamId("bolt1", "s1__punctuation"), Grouping.all(new NullStruct()));
        assertEquals(expected1, bolt1.get_common().get_inputs());
        assertEquals(expected2, bolt2.get_common().get_inputs());
    }

    @Test
    public void testRepartition() throws Exception {
        Stream<String> stream = streamBuilder.newStream(newSpout(Utils.DEFAULT_STREAM_ID), new ValueMapper<>(0));
        stream.repartition(3).filter(x -> true).repartition(2).filter(x -> true).aggregate(new Count<>());
        StormTopology topology = streamBuilder.build();
        assertEquals(1, topology.get_spouts_size());
        SpoutSpec spout = topology.get_spouts().get("spout1");
        assertEquals(4, topology.get_bolts_size());
        Bolt bolt1 = topology.get_bolts().get("bolt1");
        Bolt bolt2 = topology.get_bolts().get("bolt2");
        Bolt bolt3 = topology.get_bolts().get("bolt3");
        Bolt bolt4 = topology.get_bolts().get("bolt4");
        assertEquals(1, spout.get_common().get_parallelism_hint());
        assertEquals(1, bolt1.get_common().get_parallelism_hint());
        assertEquals(3, bolt2.get_common().get_parallelism_hint());
        assertEquals(2, bolt3.get_common().get_parallelism_hint());
        assertEquals(2, bolt4.get_common().get_parallelism_hint());
    }

    @Test
    public void testBranchAndJoin() throws Exception {
        TopologyContext mockContext = Mockito.mock(TopologyContext.class);
        OutputCollector mockCollector = Mockito.mock(OutputCollector.class);
        Stream<Integer> stream = streamBuilder.newStream(newSpout(Utils.DEFAULT_STREAM_ID), new ValueMapper<>(0), 2);
        Stream<Integer>[] streams = stream.branch(x -> x % 2 == 0, x -> x % 2 == 1);
        PairStream<Integer, Pair<Integer, Integer>> joined =
            streams[0].mapToPair(x -> Pair.of(x, 1)).join(streams[1].mapToPair(x -> Pair.of(x, 1)));
        assertTrue(joined.getNode() instanceof ProcessorNode);
        StormTopology topology = streamBuilder.build();
        assertEquals(2, topology.get_bolts_size());
    }

    @Test
    public void testMultiPartitionByKey() {
        TopologyContext mockContext = Mockito.mock(TopologyContext.class);
        OutputCollector mockCollector = Mockito.mock(OutputCollector.class);
        Stream<Integer> stream = streamBuilder.newStream(newSpout(Utils.DEFAULT_STREAM_ID), new ValueMapper<>(0));
        stream.mapToPair(x -> Pair.of(x, x))
              .window(TumblingWindows.of(BaseWindowedBolt.Count.of(10)))
              .reduceByKey((x, y) -> x + y)
              .reduceByKey((x, y) -> 0)
              .print();
        StormTopology topology = streamBuilder.build();
        assertEquals(2, topology.get_bolts_size());
    }

    @Test
    public void testMultiPartitionByKeyWithRepartition() {
        TopologyContext mockContext = Mockito.mock(TopologyContext.class);
        OutputCollector mockCollector = Mockito.mock(OutputCollector.class);
        Map<GlobalStreamId, Grouping> expected = new HashMap<>();
        expected.put(new GlobalStreamId("bolt2", "s3"), Grouping.fields(Collections.singletonList("key")));
        expected.put(new GlobalStreamId("bolt2", "s3__punctuation"), Grouping.all(new NullStruct()));
        Stream<Integer> stream = streamBuilder.newStream(newSpout(Utils.DEFAULT_STREAM_ID), new ValueMapper<>(0));
        stream.mapToPair(x -> Pair.of(x, x))
              .window(TumblingWindows.of(BaseWindowedBolt.Count.of(10)))
              .reduceByKey((x, y) -> x + y)
              .repartition(10)
              .reduceByKey((x, y) -> 0)
              .print();
        StormTopology topology = streamBuilder.build();
        assertEquals(3, topology.get_bolts_size());
        assertEquals(expected, topology.get_bolts().get("bolt3").get_common().get_inputs());

    }

    @Test
    public void testPartitionByKeySinglePartition() {
        TopologyContext mockContext = Mockito.mock(TopologyContext.class);
        OutputCollector mockCollector = Mockito.mock(OutputCollector.class);
        Stream<Integer> stream = streamBuilder.newStream(newSpout(Utils.DEFAULT_STREAM_ID), new ValueMapper<>(0));
        stream.mapToPair(x -> Pair.of(x, x))
              .reduceByKey((x, y) -> x + y)
              .print();
        StormTopology topology = streamBuilder.build();
        assertEquals(1, topology.get_bolts_size());
    }
}
