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
import org.apache.storm.shade.com.google.common.collect.Multimap;
import org.apache.storm.shade.org.jgrapht.DirectedGraph;
import org.apache.storm.shade.org.jgrapht.graph.DefaultDirectedGraph;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.streams.operations.StateUpdater;
import org.apache.storm.streams.processors.Processor;
import org.apache.storm.streams.processors.UpdateStateByKeyProcessor;
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
 * Unit tests for {@link StatefulProcessorBolt}
 */
public class StatefulProcessorBoltTest {
    TopologyContext mockTopologyContext;
    OutputCollector mockOutputCollector;
    StatefulProcessorBolt<String, Long> bolt;
    Tuple mockTuple1;
    DirectedGraph<Node, Edge> graph;
    Multimap<String, ProcessorNode> mockStreamToProcessors;
    KeyValueState<String, Long> mockKeyValueState;

    @Before
    public void setUp() throws Exception {
        mockTopologyContext = Mockito.mock(TopologyContext.class);
        mockOutputCollector = Mockito.mock(OutputCollector.class);
        mockTuple1 = Mockito.mock(Tuple.class);
        mockStreamToProcessors = Mockito.mock(Multimap.class);
        mockKeyValueState = Mockito.mock(KeyValueState.class);
        setUpMockTuples(mockTuple1);
    }

    @Test
    public void testEmitAndAck() throws Exception {
        setUpStatefulProcessorBolt(new UpdateStateByKeyProcessor<>(new StateUpdater<Object, Long>() {
            @Override
            public Long init() {
                return 0L;
            }

            @Override
            public Long apply(Long state, Object value) {
                return state + 1;
            }
        }));
        bolt.execute(mockTuple1);
        ArgumentCaptor<Collection> anchor = ArgumentCaptor.forClass(Collection.class);
        ArgumentCaptor<Values> values = ArgumentCaptor.forClass(Values.class);
        ArgumentCaptor<String> os = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockOutputCollector).emit(os.capture(), anchor.capture(), values.capture());
        assertEquals("outputstream", os.getValue());
        assertArrayEquals(new Object[]{ mockTuple1 }, anchor.getValue().toArray());
        assertEquals(new Values("k", 1L), values.getValue());
        Mockito.verify(mockOutputCollector, Mockito.times(1)).ack(mockTuple1);
        Mockito.verify(mockKeyValueState, Mockito.times(1)).put("k", 1L);
    }

    private void setUpStatefulProcessorBolt(Processor<?> processor) {
        ProcessorNode node = new ProcessorNode(processor, "outputstream", new Fields("value"));
        node.setEmitsPair(true);
        Mockito.when(mockStreamToProcessors.get(Mockito.anyString())).thenReturn(Collections.singletonList(node));
        graph = new DefaultDirectedGraph(new StreamsEdgeFactory());
        graph.addVertex(node);
        bolt = new StatefulProcessorBolt<>("bolt1", graph, Collections.singletonList(node));
        bolt.setStreamToInitialProcessors(mockStreamToProcessors);
        bolt.prepare(new HashMap<>(), mockTopologyContext, mockOutputCollector);
        bolt.initState(mockKeyValueState);
    }

    private void setUpMockTuples(Tuple... tuples) {
        for (Tuple tuple : tuples) {
            Mockito.when(tuple.size()).thenReturn(1);
            Mockito.when(tuple.getValue(0)).thenReturn(Pair.of("k", "v"));
            Mockito.when(tuple.getSourceComponent()).thenReturn("bolt0");
            Mockito.when(tuple.getSourceStreamId()).thenReturn("inputstream");
        }
    }
}
