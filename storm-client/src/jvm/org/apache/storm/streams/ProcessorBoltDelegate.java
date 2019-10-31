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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.shade.com.google.common.collect.ArrayListMultimap;
import org.apache.storm.shade.com.google.common.collect.HashBasedTable;
import org.apache.storm.shade.com.google.common.collect.Multimap;
import org.apache.storm.shade.com.google.common.collect.Table;
import org.apache.storm.shade.org.jgrapht.DirectedGraph;
import org.apache.storm.shade.org.jgrapht.graph.DirectedSubgraph;
import org.apache.storm.shade.org.jgrapht.traverse.TopologicalOrderIterator;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ProcessorBoltDelegate implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessorBoltDelegate.class);
    private final String id;
    private final DirectedGraph<Node, Edge> graph;
    private final List<ProcessorNode> nodes;
    private final List<ProcessorNode> outgoingProcessors = new ArrayList<>();
    private final Set<EmittingProcessorContext> emittingProcessorContexts = new HashSet<>();
    private final Table<ProcessorNode, String, Integer> punctuationState = HashBasedTable.create();
    private final Map<String, Integer> streamToInputTaskCount = new HashMap<>();
    private Map<String, Object> topoConf;
    private TopologyContext topologyContext;
    private OutputCollector outputCollector;
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

    void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.topoConf = topoConf;
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
        for (String stream : streamToInitialProcessors.keySet()) {
            streamToInputTaskCount.put(stream, getStreamInputTaskCount(context, stream));
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
                /*
                 * Declare a separate 'punctuation' stream per output stream so that the receiving bolt
                 * can subscribe to this stream with 'ALL' grouping and process the punctuation once it
                 * receives from all upstream tasks.
                 */
                declarer.declareStream(StreamUtil.getPunctuationStream(stream), StreamUtil.getPunctuationFields());
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
        if (StreamUtil.isPunctuation(value)) {
            punctuateInitialProcessors(sourceStreamId);
        } else {
            executeInitialProcessors(value, sourceStreamId);
        }
    }

    private void punctuateInitialProcessors(String punctuationStreamId) {
        String sourceStreamId = StreamUtil.getSourceStream(punctuationStreamId);
        Collection<ProcessorNode> initialProcessors = streamToInitialProcessors.get(sourceStreamId);
        for (ProcessorNode processorNode : initialProcessors) {
            if (shouldPunctuate(processorNode, sourceStreamId)) {
                processorNode.getProcessor().punctuate(null);
                clearPunctuationState(processorNode);
            }
        }
    }

    private void executeInitialProcessors(Object value, String sourceStreamId) {
        Collection<ProcessorNode> initialProcessors = streamToInitialProcessors.get(sourceStreamId);
        for (ProcessorNode processorNode : initialProcessors) {
            Processor processor = processorNode.getProcessor();
            processor.execute(value, sourceStreamId);
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

    void setTimestampField(String fieldName) {
        timestampField = fieldName;
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

    // for the given processor node, if we received punctuation from all tasks of its parent windowed streams
    private boolean shouldPunctuate(ProcessorNode processorNode, String sourceStreamId) {
        if (!processorNode.getWindowedParentStreams().isEmpty()) {
            updateCount(processorNode, sourceStreamId);
            if (punctuationState.row(processorNode).size() != processorNode.getWindowedParentStreams().size()) {
                return false;
            }
            // size matches, check if the streams are expected
            Set<String> receivedStreams = punctuationState.row(processorNode).keySet();
            if (!receivedStreams.equals(processorNode.getWindowedParentStreams())) {
                throw new IllegalStateException("Received punctuation from streams " + receivedStreams + " expected "
                                                + processorNode.getWindowedParentStreams());
            }
            for (String receivedStream : receivedStreams) {
                Integer expected = streamToInputTaskCount.get(receivedStream);
                if (expected == null) {
                    throw new IllegalStateException("Punctuation received on unexpected stream '" + receivedStream
                            + "' for which input task count is not set.");
                }
                if (punctuationState.get(processorNode, receivedStream) < streamToInputTaskCount.get(receivedStream)) {
                    return false;
                }
            }
        }
        return true;
    }

    private void updateCount(ProcessorNode processorNode, String sourceStreamId) {
        Integer count = punctuationState.get(processorNode, sourceStreamId);
        if (count == null) {
            punctuationState.put(processorNode, sourceStreamId, 1);
        } else {
            punctuationState.put(processorNode, sourceStreamId, count + 1);
        }
    }

    private void clearPunctuationState(ProcessorNode processorNode) {
        if (!punctuationState.isEmpty()) {
            Map<String, Integer> state = punctuationState.row(processorNode);
            if (!state.isEmpty()) {
                state.clear();
            }
        }
    }

    private boolean isPair(Tuple input) {
        return input.size() == (timestampField == null ? 2 : 3);
    }

    private int getStreamInputTaskCount(TopologyContext context, String stream) {
        int count = 0;
        for (GlobalStreamId inputStream : context.getThisSources().keySet()) {
            if (stream.equals(getStreamId(inputStream))) {
                count += context.getComponentTasks(inputStream.get_componentId()).size();
            }
        }
        return count;
    }

    private String getStreamId(GlobalStreamId globalStreamId) {
        if (globalStreamId.get_componentId().startsWith("spout")) {
            return globalStreamId.get_componentId() + globalStreamId.get_streamId();
        }
        return globalStreamId.get_streamId();
    }

}
