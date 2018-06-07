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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import org.apache.storm.shade.com.google.common.collect.Multimap;
import org.apache.storm.shade.org.jgrapht.DirectedGraph;
import org.apache.storm.shade.org.jgrapht.graph.DefaultDirectedGraph;
import org.apache.storm.streams.operations.aggregators.Count;
import org.apache.storm.streams.processors.AggregateProcessor;
import org.apache.storm.streams.processors.Processor;
import org.apache.storm.streams.windowing.TumblingWindows;
import org.apache.storm.streams.windowing.Window;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for {@link WindowedProcessorBolt}
 */
public class WindowedProcessorBoltTest {
    TopologyContext mockTopologyContext;
    OutputCollector mockOutputCollector;
    WindowedProcessorBolt bolt;
    Tuple mockTuple1;
    Tuple mockTuple2;
    Tuple mockTuple3;
    DirectedGraph<Node, Edge> graph;
    Multimap<String, ProcessorNode> mockStreamToProcessors;

    @Before
    public void setUp() throws Exception {
        mockTopologyContext = Mockito.mock(TopologyContext.class);
        mockOutputCollector = Mockito.mock(OutputCollector.class);
        mockTuple1 = Mockito.mock(Tuple.class);
        mockTuple2 = Mockito.mock(Tuple.class);
        mockTuple3 = Mockito.mock(Tuple.class);
        setUpMockTuples(mockTuple1, mockTuple2, mockTuple3);
        mockStreamToProcessors = Mockito.mock(Multimap.class);
    }

    @Test
    public void testEmit() throws Exception {
        Window<?, ?> window = TumblingWindows.of(BaseWindowedBolt.Count.of(2));
        setUpWindowedProcessorBolt(new AggregateProcessor<>(new Count<>()), window);
        bolt.execute(getMockTupleWindow(mockTuple1, mockTuple2, mockTuple3));
        ArgumentCaptor<Values> values = ArgumentCaptor.forClass(Values.class);
        ArgumentCaptor<String> os = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockOutputCollector, Mockito.times(2)).emit(os.capture(), values.capture());
        assertEquals("outputstream", os.getAllValues().get(0));
        assertEquals(new Values(3L), values.getAllValues().get(0));
        assertEquals("outputstream__punctuation", os.getAllValues().get(1));
        assertEquals(new Values(WindowNode.PUNCTUATION), values.getAllValues().get(1));
    }

    private void setUpWindowedProcessorBolt(Processor<?> processor, Window<?, ?> window) {
        ProcessorNode node = new ProcessorNode(processor, "outputstream", new Fields("value"));
        node.setWindowed(true);
        Mockito.when(mockStreamToProcessors.get(Mockito.anyString())).thenReturn(Collections.singletonList(node));
        Mockito.when(mockStreamToProcessors.keySet()).thenReturn(Collections.singleton("inputstream"));
        graph = new DefaultDirectedGraph<>(new StreamsEdgeFactory());
        graph.addVertex(node);
        bolt = new WindowedProcessorBolt("bolt1", graph, Collections.singletonList(node), window);
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

    private TupleWindow getMockTupleWindow(Tuple... tuples) {
        TupleWindow tupleWindow = Mockito.mock(TupleWindow.class);
        Mockito.when(tupleWindow.get()).thenReturn(Arrays.asList(tuples));
        return tupleWindow;
    }
}
