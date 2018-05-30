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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.shade.com.google.common.collect.Multimap;
import org.apache.storm.shade.org.jgrapht.DirectedGraph;
import org.apache.storm.shade.org.jgrapht.graph.DefaultDirectedGraph;
import org.apache.storm.streams.operations.aggregators.LongSum;
import org.apache.storm.streams.processors.AggregateProcessor;
import org.apache.storm.streams.processors.FilterProcessor;
import org.apache.storm.streams.processors.Processor;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link ProcessorBolt}
 */
public class ProcessorBoltTest {
    TopologyContext mockTopologyContext;
    OutputCollector mockOutputCollector;
    ProcessorBolt bolt;
    Tuple mockTuple1;
    Tuple mockTuple2;
    Tuple mockTuple3;
    Tuple punctuation;
    Multimap<String, ProcessorNode> mockStreamToProcessors;
    DirectedGraph<Node, Edge> graph;

    @Before
    public void setUp() throws Exception {
        mockTopologyContext = Mockito.mock(TopologyContext.class);
        mockOutputCollector = Mockito.mock(OutputCollector.class);
        mockTuple1 = Mockito.mock(Tuple.class);
        mockTuple2 = Mockito.mock(Tuple.class);
        mockTuple3 = Mockito.mock(Tuple.class);
        setUpMockTuples(mockTuple1, mockTuple2, mockTuple3);
        punctuation = Mockito.mock(Tuple.class);
        setUpPunctuation(punctuation);
        mockStreamToProcessors = Mockito.mock(Multimap.class);
        graph = new DefaultDirectedGraph(new StreamsEdgeFactory());

    }

    @Test
    public void testEmitAndAck() throws Exception {
        setUpProcessorBolt(new FilterProcessor<Integer>(x -> true));
        bolt.execute(mockTuple1);
        ArgumentCaptor<Collection> anchor = ArgumentCaptor.forClass(Collection.class);
        ArgumentCaptor<Values> values = ArgumentCaptor.forClass(Values.class);
        ArgumentCaptor<String> os = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockOutputCollector).emit(os.capture(), anchor.capture(), values.capture());
        assertEquals("outputstream", os.getValue());
        assertArrayEquals(new Object[]{ mockTuple1 }, anchor.getValue().toArray());
        assertEquals(new Values(100), values.getValue());
        Mockito.verify(mockOutputCollector, Mockito.times(1)).ack(mockTuple1);
    }

    @Test
    public void testAggResultAndAck() throws Exception {
        setUpProcessorBolt(new AggregateProcessor<>(new LongSum()), Collections.singleton("inputstream"), true, null);
        bolt.execute(mockTuple2);
        bolt.execute(mockTuple3);
        bolt.execute(punctuation);
        ArgumentCaptor<Collection> anchor = ArgumentCaptor.forClass(Collection.class);
        ArgumentCaptor<Values> values = ArgumentCaptor.forClass(Values.class);
        ArgumentCaptor<String> os = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockOutputCollector, Mockito.times(2)).emit(os.capture(), anchor.capture(), values.capture());
        assertArrayEquals(new Object[]{ mockTuple2, mockTuple3, punctuation }, anchor.getAllValues().get(0).toArray());
        assertArrayEquals(new Object[]{ mockTuple2, mockTuple3, punctuation }, anchor.getAllValues().get(1).toArray());
        assertArrayEquals(new Object[]{ new Values(200L), new Values("__punctuation") }, values.getAllValues().toArray());
        assertArrayEquals(new Object[]{ "outputstream", "outputstream__punctuation" }, os.getAllValues().toArray());
        Mockito.verify(mockOutputCollector).ack(mockTuple2);
        Mockito.verify(mockOutputCollector).ack(mockTuple3);
        Mockito.verify(mockOutputCollector).ack(punctuation);
    }

    @Test
    public void testEmitTs() throws Exception {
        Tuple tupleWithTs = Mockito.mock(Tuple.class);
        setUpMockTuples(tupleWithTs);
        Mockito.when(tupleWithTs.getLongByField("ts")).thenReturn(12345L);
        setUpProcessorBolt(new FilterProcessor(x -> true), "ts");
        bolt.execute(tupleWithTs);
        ArgumentCaptor<Collection> anchor = ArgumentCaptor.forClass(Collection.class);
        ArgumentCaptor<Values> values = ArgumentCaptor.forClass(Values.class);
        ArgumentCaptor<String> os = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockOutputCollector).emit(os.capture(), anchor.capture(), values.capture());
        assertEquals("outputstream", os.getValue());
        assertArrayEquals(new Object[]{ tupleWithTs }, anchor.getValue().toArray());
        assertEquals(new Values(100, 12345L), values.getValue());
        Mockito.verify(mockOutputCollector, Mockito.times(1)).ack(tupleWithTs);
    }

    private void setUpProcessorBolt(Processor<?> processor) {
        setUpProcessorBolt(processor, Collections.emptySet(), false, null);
    }

    private void setUpProcessorBolt(Processor<?> processor, String tsFieldName) {
        setUpProcessorBolt(processor, Collections.emptySet(), false, tsFieldName);
    }

    private void setUpProcessorBolt(Processor<?> processor,
                                    Set<String> windowedParentStreams,
                                    boolean isWindowed,
                                    String tsFieldName) {
        ProcessorNode node = new ProcessorNode(processor, "outputstream", new Fields("value"));
        node.setWindowedParentStreams(windowedParentStreams);
        node.setWindowed(isWindowed);
        Mockito.when(mockStreamToProcessors.get(Mockito.anyString())).thenReturn(Collections.singletonList(node));
        Mockito.when(mockStreamToProcessors.keySet()).thenReturn(Collections.singleton("inputstream"));
        Map<GlobalStreamId, Grouping> mockSources = Mockito.mock(Map.class);
        GlobalStreamId mockGlobalStreamId = Mockito.mock(GlobalStreamId.class);
        Mockito.when(mockTopologyContext.getThisSources()).thenReturn(mockSources);
        Mockito.when(mockSources.keySet()).thenReturn(Collections.singleton(mockGlobalStreamId));
        Mockito.when(mockGlobalStreamId.get_streamId()).thenReturn("inputstream");
        Mockito.when(mockGlobalStreamId.get_componentId()).thenReturn("bolt0");
        Mockito.when(mockTopologyContext.getComponentTasks(Mockito.anyString())).thenReturn(Collections.singletonList(1));
        graph.addVertex(node);
        bolt = new ProcessorBolt("bolt1", graph, Collections.singletonList(node));
        if (tsFieldName != null && !tsFieldName.isEmpty()) {
            bolt.setTimestampField(tsFieldName);
        }
        bolt.setStreamToInitialProcessors(mockStreamToProcessors);
        bolt.prepare(new HashMap<>(), mockTopologyContext, mockOutputCollector);
    }

    private void setUpMockTuples(Tuple... tuples) {
        for (Tuple tuple : tuples) {
            Mockito.when(tuple.size()).thenReturn(1);
            Mockito.when(tuple.getValue(0)).thenReturn(100);
            Mockito.when(tuple.getSourceComponent()).thenReturn("bolt0");
            Mockito.when(tuple.getSourceStreamId()).thenReturn("inputstream");
        }
    }

    private void setUpPunctuation(Tuple punctuation) {
        Mockito.when(punctuation.size()).thenReturn(1);
        Mockito.when(punctuation.getValue(0)).thenReturn(WindowNode.PUNCTUATION);
        Mockito.when(punctuation.getSourceComponent()).thenReturn("bolt0");
        Mockito.when(punctuation.getSourceStreamId()).thenReturn("inputstream");
    }
}
