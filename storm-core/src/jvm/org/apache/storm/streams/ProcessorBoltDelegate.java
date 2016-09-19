/**
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
package org.apache.storm.streams;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.storm.streams.processors.ChainedProcessorContext;
import org.apache.storm.streams.processors.EmittingProcessorContext;
import org.apache.storm.streams.processors.ForwardingProcessorContext;
import org.apache.storm.streams.processors.Processor;
import org.apache.storm.streams.processors.ProcessorContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DirectedSubgraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class ProcessorBoltDelegate implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessorBoltDelegate.class);
    private final String id;
    private final DirectedGraph<Node, Edge> graph;
    private final List<ProcessorNode> nodes;
    private Map stormConf;
    private TopologyContext topologyContext;
    private OutputCollector outputCollector;
    private final List<ProcessorNode> outgoingProcessors = new ArrayList<>();
    private final Set<EmittingProcessorContext> emittingProcessorContexts = new HashSet<>();
    private final Map<ProcessorNode, Set<String>> punctuationState = new HashMap<>();
    private Multimap<String, ProcessorNode> streamToInitialProcessors;
    private String timestampField;

    ProcessorBoltDelegate(String id, DirectedGraph<Node, Edge> graph, List<ProcessorNode> nodes) {
        this.id = id;
        this.graph = graph;
        this.nodes = new ArrayList<>(nodes);
    }

    String getId() {
        return id;
    }

    void addNodes(Collection<ProcessorNode> nodes) {
        this.nodes.addAll(nodes);
    }

    List<ProcessorNode> getNodes() {
        return Collections.unmodifiableList(nodes);
    }

    void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.stormConf = stormConf;
        topologyContext = context;
        outputCollector = collector;
        DirectedSubgraph<Node, Edge> subgraph = new DirectedSubgraph<>(graph, new HashSet<>(nodes), null);
        TopologicalOrderIterator<Node, Edge> it = new TopologicalOrderIterator<>(subgraph);
        while (it.hasNext()) {
            Node node = it.next();
            if (!(node instanceof ProcessorNode)) {
                throw new IllegalStateException("Not a processor node " + node);
            }
            ProcessorNode processorNode = (ProcessorNode) node;
            List<ProcessorNode> children = StreamUtil.getChildren(subgraph, processorNode);
            ProcessorContext processorContext;
            if (children.isEmpty()) {
                processorContext = createEmittingContext(processorNode);
            } else {
                Multimap<String, ProcessorNode> streamToChildren = ArrayListMultimap.create();
                for (ProcessorNode child : children) {
                    for (String stream : child.getParentStreams(processorNode)) {
                        streamToChildren.put(stream, child);
                    }
                }
                ForwardingProcessorContext forwardingContext = new ForwardingProcessorContext(processorNode, streamToChildren);
                if (hasOutgoingChild(processorNode, new HashSet<>(children))) {
                    processorContext = new ChainedProcessorContext(processorNode, forwardingContext, createEmittingContext(processorNode));
                } else {
                    processorContext = forwardingContext;
                }
            }
            processorNode.initProcessorContext(processorContext);
        }
        if (timestampField != null) {
            for (EmittingProcessorContext ctx : emittingProcessorContexts) {
                ctx.setTimestampField(timestampField);
            }
        }
    }

    void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (ProcessorNode node : nodes) {
            for (String stream : node.getOutputStreams()) {
                if (timestampField == null) {
                    declarer.declareStream(stream, node.getOutputFields());
                } else {
                    List<String> fields = new ArrayList<>();
                    fields.addAll(node.getOutputFields().toList());
                    fields.add(timestampField);
                    declarer.declareStream(stream, new Fields(fields));
                }
            }
        }
    }

    void setAnchor(RefCountedTuple tuple) {
        for (EmittingProcessorContext ctx : emittingProcessorContexts) {
            ctx.setAnchor(tuple);
        }
    }

    Pair<Object, String> getValueAndStream(Tuple input) {
        Object value;
        String stream;
        // if tuple arrives from a spout, it can be passed as is
        // otherwise the value is in the first field of the tuple
        if (input.getSourceComponent().startsWith("spout")) {
            value = input;
            stream = input.getSourceGlobalStreamId().get_componentId() + input.getSourceGlobalStreamId().get_streamId();
        } else if (isPair(input)) {
            value = Pair.of(input.getValue(0), input.getValue(1));
            stream = input.getSourceStreamId();
        } else {
            value = input.getValue(0);
            stream = input.getSourceStreamId();
        }
        return Pair.of(value, stream);
    }

    void processAndAck(Tuple input) {
        RefCountedTuple refCountedTuple = new RefCountedTuple(input);
        setAnchor(refCountedTuple);
        if (isEventTimestamp()) {
            setEventTimestamp(input.getLongByField(getTimestampField()));
        }
        Pair<Object, String> valueAndStream = getValueAndStream(input);
        process(valueAndStream.getFirst(), valueAndStream.getSecond());
        ack(refCountedTuple);
    }

    void process(Object value, String sourceStreamId) {
        LOG.debug("Process value {}, sourceStreamId {}", value, sourceStreamId);
        Collection<ProcessorNode> initialProcessors = streamToInitialProcessors.get(sourceStreamId);
        for (ProcessorNode processorNode : initialProcessors) {
            Processor processor = processorNode.getProcessor();
            if (StreamUtil.isPunctuation(value)) {
                if (shouldPunctuate(processorNode, sourceStreamId)) {
                    processor.punctuate(null);
                    clearPunctuationState(processorNode);
                }
            } else {
                processor.execute(value, sourceStreamId);
            }
        }
    }

    void setStreamToInitialProcessors(Multimap<String, ProcessorNode> streamToInitialProcessors) {
        this.streamToInitialProcessors = streamToInitialProcessors;
    }

    void addStreamToInitialProcessors(Multimap<String, ProcessorNode> streamToInitialProcessors) {
        this.streamToInitialProcessors.putAll(streamToInitialProcessors);
    }

    Set<String> getInitialStreams() {
        return streamToInitialProcessors.keySet();
    }

    void setTimestampField(String fieldName) {
        timestampField = fieldName;
    }

    boolean isEventTimestamp() {
        return timestampField != null;
    }

    void setEventTimestamp(long timestamp) {
        for (EmittingProcessorContext ctx : emittingProcessorContexts) {
            ctx.setEventTimestamp(timestamp);
        }
    }

    private String getTimestampField() {
        return timestampField;
    }

    // if there are no windowed/batched processors, we would ack immediately
    private void ack(RefCountedTuple tuple) {
        if (tuple.shouldAck()) {
            LOG.debug("ACKing tuple {}", tuple);
            outputCollector.ack(tuple.tuple());
            tuple.setAcked();
        }
    }

    private ProcessorContext createEmittingContext(ProcessorNode processorNode) {
        List<EmittingProcessorContext> emittingContexts = new ArrayList<>();
        for (String stream : processorNode.getOutputStreams()) {
            EmittingProcessorContext emittingContext = new EmittingProcessorContext(processorNode, outputCollector, stream);
            if (StreamUtil.isSinkStream(stream)) {
                emittingContext.setEmitPunctuation(false);
            }
            emittingContexts.add(emittingContext);
        }
        emittingProcessorContexts.addAll(emittingContexts);
        outgoingProcessors.add(processorNode);
        return new ChainedProcessorContext(processorNode, emittingContexts);
    }

    private boolean hasOutgoingChild(ProcessorNode processorNode, Set<ProcessorNode> boltChildren) {
        for (Node child : getChildNodes(processorNode)) {
            if ((child instanceof ProcessorNode && !boltChildren.contains(child))
                    || child instanceof SinkNode) {
                return true;
            }
        }
        return false;
    }

    private Set<Node> getChildNodes(Node node) {
        Set<Node> children = new HashSet<>();
        for (Node child : StreamUtil.<Node>getChildren(graph, node)) {
            if (child instanceof WindowNode || child instanceof PartitionNode) {
                children.addAll(getChildNodes(child));
            } else {
                children.add(child);
            }
        }
        return children;
    }

    // if we received punctuation from all parent windowed streams
    private boolean shouldPunctuate(ProcessorNode processorNode, String sourceStreamId) {
        if (processorNode.getWindowedParentStreams().size() <= 1) {
            return true;
        }
        Set<String> receivedStreams = punctuationState.get(processorNode);
        if (receivedStreams == null) {
            receivedStreams = new HashSet<>();
            punctuationState.put(processorNode, receivedStreams);
        }
        receivedStreams.add(sourceStreamId);
        return receivedStreams.equals(processorNode.getWindowedParentStreams());
    }

    private void clearPunctuationState(ProcessorNode processorNode) {
        Set<String> state = punctuationState.get(processorNode);
        if (state != null) {
            state.clear();
        }
    }

    private boolean isPair(Tuple input) {
        return input.size() == (timestampField == null ? 2 : 3);
    }

}
