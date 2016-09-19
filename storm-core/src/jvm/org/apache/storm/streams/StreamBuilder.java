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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.streams.operations.IdentityFunction;
import org.apache.storm.streams.operations.mappers.PairValueMapper;
import org.apache.storm.streams.operations.mappers.TupleValueMapper;
import org.apache.storm.streams.processors.JoinProcessor;
import org.apache.storm.streams.processors.MapProcessor;
import org.apache.storm.streams.processors.StateQueryProcessor;
import org.apache.storm.streams.processors.StatefulProcessor;
import org.apache.storm.streams.windowing.Window;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IComponent;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Tuple;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * A builder for constructing a {@link StormTopology} via storm streams api (DSL)
 */
public class StreamBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(StreamBuilder.class);
    private final DefaultDirectedGraph<Node, Edge> graph;
    private final Table<Node, String, GroupingInfo> nodeGroupingInfo = HashBasedTable.create();
    private final Map<Node, WindowNode> windowInfo = new HashMap<>();
    private final List<ProcessorNode> curGroup = new ArrayList<>();
    private int statefulProcessorCount = 0;
    private final Map<StreamBolt, BoltDeclarer> streamBolts = new HashMap<>();
    private String timestampFieldName = null;

    /**
     * Creates a new {@link StreamBuilder}
     */
    public StreamBuilder() {
        graph = new DefaultDirectedGraph<>(new StreamsEdgeFactory());
    }

    /**
     * Creates a new {@link Stream} of tuples from the given {@link IRichSpout}
     *
     * @param spout the spout
     * @return the new stream
     */
    public Stream<Tuple> newStream(IRichSpout spout) {
        return newStream(spout, 1);
    }

    /**
     * Creates a new {@link Stream} of tuples from the given {@link IRichSpout} with the given
     * parallelism.
     *
     * @param spout       the spout
     * @param parallelism the parallelism of the stream
     * @return the new stream
     */
    public Stream<Tuple> newStream(IRichSpout spout, int parallelism) {
        SpoutNode spoutNode = new SpoutNode(spout);
        String spoutId = UniqueIdGen.getInstance().getUniqueSpoutId();
        spoutNode.setComponentId(spoutId);
        spoutNode.setParallelism(parallelism);
        graph.addVertex(spoutNode);
        return new Stream<>(this, spoutNode);
    }

    /**
     * Creates a new {@link Stream} of values from the given {@link IRichSpout} by extracting field(s)
     * from tuples via the supplied {@link TupleValueMapper}.
     *
     * @param spout       the spout
     * @param valueMapper the value mapper
     * @param <T>         the type of values in the resultant stream
     * @return the new stream
     */
    public <T> Stream<T> newStream(IRichSpout spout, TupleValueMapper<T> valueMapper) {
        return newStream(spout).map(valueMapper);
    }


    /**
     * Creates a new {@link Stream} of values from the given {@link IRichSpout} by extracting field(s)
     * from tuples via the supplied {@link TupleValueMapper} with the given parallelism.
     *
     * @param spout       the spout
     * @param valueMapper the value mapper
     * @param parallelism the parallelism of the stream
     * @param <T>         the type of values in the resultant stream
     * @return the new stream
     */
    public <T> Stream<T> newStream(IRichSpout spout, TupleValueMapper<T> valueMapper, int parallelism) {
        return newStream(spout, parallelism).map(valueMapper);
    }

    /**
     * Creates a new {@link PairStream} of key-value pairs from the given {@link IRichSpout} by extracting key and
     * value from tuples via the supplied {@link PairValueMapper}.
     *
     * @param spout           the spout
     * @param pairValueMapper the pair value mapper
     * @param <K>             the key type
     * @param <V>             the value type
     * @return the new stream of key-value pairs
     */
    public <K, V> PairStream<K, V> newStream(IRichSpout spout, PairValueMapper<K, V> pairValueMapper) {
        return newStream(spout).mapToPair(pairValueMapper);
    }

    /**
     * Creates a new {@link PairStream} of key-value pairs from the given {@link IRichSpout} by extracting key and
     * value from tuples via the supplied {@link PairValueMapper} and with the given value of parallelism.
     *
     * @param spout           the spout
     * @param pairValueMapper the pair value mapper
     * @param parallelism     the parallelism of the stream
     * @param <K>             the key type
     * @param <V>             the value type
     * @return the new stream of key-value pairs
     */
    public <K, V> PairStream<K, V> newStream(IRichSpout spout, PairValueMapper<K, V> pairValueMapper, int parallelism) {
        return newStream(spout, parallelism).mapToPair(pairValueMapper);
    }


    /**
     * Builds a new {@link StormTopology} for the computation expressed
     * via the stream api.
     *
     * @return the storm topology
     */
    public StormTopology build() {
        nodeGroupingInfo.clear();
        windowInfo.clear();
        curGroup.clear();
        TopologicalOrderIterator<Node, Edge> iterator = new TopologicalOrderIterator<>(graph, queue());
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        while (iterator.hasNext()) {
            Node node = iterator.next();
            if (node instanceof SpoutNode) {
                addSpout(topologyBuilder, (SpoutNode) node);
            } else if (node instanceof ProcessorNode) {
                handleProcessorNode((ProcessorNode) node, topologyBuilder);
            } else if (node instanceof PartitionNode) {
                updateNodeGroupingInfo((PartitionNode) node);
                processCurGroup(topologyBuilder);
            } else if (node instanceof WindowNode) {
                updateWindowInfo((WindowNode) node);
                processCurGroup(topologyBuilder);
            } else if (node instanceof SinkNode) {
                processCurGroup(topologyBuilder);
                addSink(topologyBuilder, (SinkNode) node);
            }
        }
        processCurGroup(topologyBuilder);
        mayBeAddTsField();
        return topologyBuilder.createTopology();
    }

    Node addNode(Node parent, Node child) {
        return addNode(parent, child, parent.getParallelism(), parent.getOutputStreams().iterator().next());
    }

    Node addNode(Node parent, Node child, int parallelism) {
        return addNode(parent, child, parallelism, parent.getOutputStreams().iterator().next());
    }

    Node addNode(Node parent, Node child, String parentStreamId) {
        return addNode(parent, child, parent.getParallelism(), parentStreamId);
    }

    Node addNode(Node parent, Node child, int parallelism, String parentStreamId) {
        graph.addVertex(child);
        graph.addEdge(parent, child);
        child.setParallelism(parallelism);
        if (parent instanceof WindowNode || parent instanceof PartitionNode) {
            child.addParentStream(parentNode(parent), parentStreamId);
        } else {
            child.addParentStream(parent, parentStreamId);
        }
        return child;
    }

    private PriorityQueue<Node> queue() {
        // min-heap
        return new PriorityQueue<>(new Comparator<Node>() {
            @Override
            public int compare(Node n1, Node n2) {
                return getPriority(n1.getClass()) - getPriority(n2.getClass());
            }

            private int getPriority(Class<? extends Node> clazz) {
                /*
                 * Nodes in the descending order of priority.
                 * ProcessorNode has the highest priority so that the topological order iterator
                 * will group as many processor nodes together as possible.
                 */
                Class<?>[] p = new Class<?>[]{
                        ProcessorNode.class,
                        SpoutNode.class,
                        SinkNode.class,
                        PartitionNode.class,
                        WindowNode.class};
                for (int i = 0; i < p.length; i++) {
                    if (clazz.equals(p[i])) {
                        return i;
                    }
                }
                return Integer.MAX_VALUE;
            }
        });
    }

    private void handleProcessorNode(ProcessorNode processorNode, TopologyBuilder topologyBuilder) {
        if (processorNode.getProcessor() instanceof StatefulProcessor) {
            statefulProcessorCount++;
            Set<ProcessorNode> initialNodes = initialProcessors(
                    curGroup.isEmpty() ? Collections.singletonList(processorNode) : curGroup);
            Set<Window<?, ?>> windows = getWindowParams(initialNodes);
            // if we get more than one stateful operation, we need to process the
            // current group so that we have one stateful operation per stateful bolt
            if (statefulProcessorCount > 1 || !windows.isEmpty()) {
                if (!curGroup.isEmpty()) {
                    processCurGroup(topologyBuilder);
                } else if (!windows.isEmpty()) {
                    // a stateful processor immediately follows a window specification
                    splitStatefulProcessor(processorNode, topologyBuilder);
                }
                statefulProcessorCount = 1;
            }
        }
        curGroup.add(processorNode);
    }

    /*
     * force create a windowed bolt with identity nodes so that we don't
     * have a stateful processor inside a windowed bolt.
     */
    private void splitStatefulProcessor(ProcessorNode processorNode, TopologyBuilder topologyBuilder) {
        for (Node parent : StreamUtil.<Node>getParents(graph, processorNode)) {
            ProcessorNode identity =
                    new ProcessorNode(new MapProcessor<>(new IdentityFunction<>()),
                            UniqueIdGen.getInstance().getUniqueStreamId(),
                            parent.getOutputFields());
            addNode(parent, identity);
            graph.removeEdge(parent, processorNode);
            processorNode.removeParentStreams(parent);
            addNode(identity, processorNode);
            curGroup.add(identity);
        }
        processCurGroup(topologyBuilder);
    }

    private void mayBeAddTsField() {
        if (timestampFieldName != null) {
            for (StreamBolt streamBolt : streamBolts.keySet()) {
                streamBolt.setTimestampField(timestampFieldName);
            }
        }
    }

    private void updateNodeGroupingInfo(PartitionNode partitionNode) {
        if (partitionNode.getGroupingInfo() != null) {
            for (Node parent : parentNodes(partitionNode)) {
                for (String parentStream : partitionNode.getParentStreams(parent)) {
                    nodeGroupingInfo.put(parent, parentStream, partitionNode.getGroupingInfo());
                }
            }
        }
    }

    private void updateWindowInfo(WindowNode windowNode) {
        for (Node parent : parentNodes(windowNode)) {
            windowInfo.put(parent, windowNode);
        }
        String tsField = windowNode.getWindowParams().getTimestampField();
        if (tsField != null) {
            if (timestampFieldName != null && !tsField.equals(timestampFieldName)) {
                throw new IllegalArgumentException("Cannot set different timestamp field names");
            }
            timestampFieldName = tsField;
        }
    }

    private Node parentNode(Node curNode) {
        Set<Node> parentNode = parentNodes(curNode);
        if (parentNode.size() > 1) {
            throw new IllegalArgumentException("Node " + curNode + " has more than one parent node.");
        }
        if (parentNode.isEmpty()) {
            throw new IllegalArgumentException("Node " + curNode + " has no parent.");
        }
        return parentNode.iterator().next();
    }

    private Set<Node> parentNodes(Node curNode) {
        Set<Node> nodes = new HashSet<>();
        for (Node parent : StreamUtil.<Node>getParents(graph, curNode)) {
            if (parent instanceof ProcessorNode || parent instanceof SpoutNode) {
                nodes.add(parent);
            } else {
                nodes.addAll(parentNodes(parent));
            }
        }
        return nodes;
    }

    private void processCurGroup(TopologyBuilder topologyBuilder) {
        if (curGroup.isEmpty()) {
            return;
        }

        String boltId = UniqueIdGen.getInstance().getUniqueBoltId();
        for (ProcessorNode processorNode : curGroup) {
            processorNode.setComponentId(boltId);
            processorNode.setWindowed(isWindowed(processorNode));
            processorNode.setWindowedParentStreams(getWindowedParentStreams(processorNode));
        }
        final Set<ProcessorNode> initialProcessors = initialProcessors(curGroup);
        Set<Window<?, ?>> windowParams = getWindowParams(initialProcessors);
        if (windowParams.isEmpty()) {
            if (hasStatefulProcessor(curGroup)) {
                addStatefulBolt(topologyBuilder, boltId, initialProcessors);
            } else {
                addBolt(topologyBuilder, boltId, initialProcessors);
            }
        } else if (windowParams.size() == 1) {
            addWindowedBolt(topologyBuilder, boltId, initialProcessors, windowParams.iterator().next());
        } else {
            throw new IllegalStateException("More than one window config for current group " + curGroup);
        }
        curGroup.clear();
    }

    private boolean hasStatefulProcessor(List<ProcessorNode> processorNodes) {
        for (ProcessorNode node : processorNodes) {
            if (node.getProcessor() instanceof StatefulProcessor) {
                return true;
            }
        }
        return false;
    }

    private int getParallelism() {
        Set<Integer> parallelisms = new HashSet<>(Collections2.transform(curGroup, new Function<ProcessorNode, Integer>() {
            @Override
            public Integer apply(ProcessorNode input) {
                return input.getParallelism();
            }
        }));

        if (parallelisms.size() > 1) {
            throw new IllegalStateException("Current group does not have same parallelism " + curGroup);
        }

        return parallelisms.isEmpty() ? 1 : parallelisms.iterator().next();
    }

    private Set<Window<?, ?>> getWindowParams(Set<ProcessorNode> initialProcessors) {
        Set<WindowNode> windowNodes = new HashSet<>();
        Set<Node> parents;
        for (ProcessorNode processorNode : initialProcessors) {
            if (processorNode.getProcessor() instanceof JoinProcessor) {
                String leftStream = ((JoinProcessor) processorNode.getProcessor()).getLeftStream();
                parents = processorNode.getParents(leftStream);
            } else {
                parents = parentNodes(processorNode);
            }
            for (Node node : parents) {
                if (windowInfo.containsKey(node)) {
                    windowNodes.add(windowInfo.get(node));
                }
            }
        }

        Set<Window<?, ?>> windowParams = new HashSet<>();
        if (!windowNodes.isEmpty()) {
            windowParams.addAll(new HashSet<>(Collections2.transform(windowNodes, new Function<WindowNode, Window<?, ?>>() {
                @Override
                public Window<?, ?> apply(WindowNode input) {
                    return input.getWindowParams();
                }
            })));
        }
        return windowParams;
    }

    private void addSpout(TopologyBuilder topologyBuilder, SpoutNode spout) {
        topologyBuilder.setSpout(spout.getComponentId(), spout.getSpout(), spout.getParallelism());
    }

    private void addSink(TopologyBuilder topologyBuilder, SinkNode sinkNode) {
        IComponent bolt = sinkNode.getBolt();
        BoltDeclarer boltDeclarer;
        if (bolt instanceof IRichBolt) {
            boltDeclarer = topologyBuilder.setBolt(sinkNode.getComponentId(), (IRichBolt) bolt, sinkNode.getParallelism());
        } else if (bolt instanceof IBasicBolt) {
            boltDeclarer = topologyBuilder.setBolt(sinkNode.getComponentId(), (IBasicBolt) bolt, sinkNode.getParallelism());
        } else {
            throw new IllegalArgumentException("Expect IRichBolt or IBasicBolt in addBolt");
        }
        for (Node parent : parentNodes(sinkNode)) {
            for (String stream : sinkNode.getParentStreams(parent)) {
                declareStream(boltDeclarer, parent, stream, nodeGroupingInfo.get(parent, stream));
            }
        }
    }

    private StreamBolt addBolt(TopologyBuilder topologyBuilder,
                               String boltId,
                               Set<ProcessorNode> initialProcessors) {
        ProcessorBolt bolt = new ProcessorBolt(boltId, graph, curGroup);
        BoltDeclarer boltDeclarer = topologyBuilder.setBolt(boltId, bolt, getParallelism());
        bolt.setStreamToInitialProcessors(wireBolt(curGroup, boltDeclarer, initialProcessors));
        streamBolts.put(bolt, boltDeclarer);
        return bolt;
    }

    private StreamBolt addStatefulBolt(TopologyBuilder topologyBuilder,
                                       String boltId,
                                       Set<ProcessorNode> initialProcessors) {
        StateQueryProcessor<?, ?> stateQueryProcessor = getStateQueryProcessor();
        StatefulProcessorBolt<?, ?> bolt;
        if (stateQueryProcessor == null) {
            bolt = new StatefulProcessorBolt<>(boltId, graph, curGroup);
            BoltDeclarer boltDeclarer = topologyBuilder.setBolt(boltId, bolt, getParallelism());
            bolt.setStreamToInitialProcessors(wireBolt(curGroup, boltDeclarer, initialProcessors));
            streamBolts.put(bolt, boltDeclarer);
        } else {
            // state query is added to the existing stateful bolt
            ProcessorNode updateStateNode = stateQueryProcessor.getStreamState().getUpdateStateNode();
            bolt = findStatefulProcessorBolt(updateStateNode);
            for (ProcessorNode node : curGroup) {
                node.setComponentId(bolt.getId());
            }
            bolt.addNodes(curGroup);
            bolt.addStreamToInitialProcessors(wireBolt(bolt.getNodes(), streamBolts.get(bolt), initialProcessors));
        }
        return bolt;
    }

    private StateQueryProcessor<?, ?> getStateQueryProcessor() {
        for (ProcessorNode node : curGroup) {
            if (node.getProcessor() instanceof StateQueryProcessor) {
                return (StateQueryProcessor<?, ?>) node.getProcessor();
            }
        }
        return null;
    }

    private StreamBolt addWindowedBolt(TopologyBuilder topologyBuilder,
                                       String boltId,
                                       Set<ProcessorNode> initialProcessors,
                                       Window<?, ?> windowParam) {
        WindowedProcessorBolt bolt = new WindowedProcessorBolt(boltId, graph, curGroup, windowParam);
        BoltDeclarer boltDeclarer = topologyBuilder.setBolt(boltId, bolt, getParallelism());
        bolt.setStreamToInitialProcessors(wireBolt(curGroup, boltDeclarer, initialProcessors));
        streamBolts.put(bolt, boltDeclarer);
        return bolt;
    }

    private StatefulProcessorBolt<?, ?> findStatefulProcessorBolt(ProcessorNode updateStateNode) {
        for (StreamBolt bolt : streamBolts.keySet()) {
            if (bolt instanceof StatefulProcessorBolt) {
                StatefulProcessorBolt<?, ?> statefulProcessorBolt = (StatefulProcessorBolt) bolt;
                if (statefulProcessorBolt.getNodes().contains(updateStateNode)) {
                    return statefulProcessorBolt;
                }
            }
        }
        throw new IllegalArgumentException("Could not find Stateful bolt for node " + updateStateNode);
    }

    private Set<String> getWindowedParentStreams(ProcessorNode processorNode) {
        Set<String> res = new HashSet<>();
        for (Node parent : parentNodes(processorNode)) {
            if (parent instanceof ProcessorNode) {
                ProcessorNode pn = (ProcessorNode) parent;
                if (pn.isWindowed()) {
                    res.addAll(Collections2.filter(pn.getOutputStreams(), new Predicate<String>() {
                        @Override
                        public boolean apply(String input) {
                            return !StreamUtil.isSinkStream(input);
                        }
                    }));
                }
            }
        }
        return res;
    }

    private Multimap<String, ProcessorNode> wireBolt(List<ProcessorNode> curGroup,
                                                     BoltDeclarer boltDeclarer,
                                                     Set<ProcessorNode> initialProcessors) {
        LOG.debug("Wiring bolt with boltDeclarer {}, curGroup {}, initialProcessors {}, nodeGroupingInfo {}",
                boltDeclarer, curGroup, initialProcessors, nodeGroupingInfo);
        Multimap<String, ProcessorNode> streamToInitialProcessor = ArrayListMultimap.create();
        Set<ProcessorNode> curSet = new HashSet<>(curGroup);
        for (ProcessorNode curNode : initialProcessors) {
            for (Node parent : parentNodes(curNode)) {
                if (curSet.contains(parent)) {
                    LOG.debug("Parent {} of curNode {} is in curGroup {}", parent, curNode, curGroup);
                } else {
                    for (String stream : curNode.getParentStreams(parent)) {
                        declareStream(boltDeclarer, parent, stream, nodeGroupingInfo.get(parent, stream));
                        // put global stream id for spouts
                        if (parent.getComponentId().startsWith("spout")) {
                            stream = parent.getComponentId() + stream;
                        }
                        streamToInitialProcessor.put(stream, curNode);
                    }
                }
            }
        }
        return streamToInitialProcessor;
    }

    private void declareStream(BoltDeclarer boltDeclarer, Node parent, String streamId, GroupingInfo grouping) {
        if (grouping == null) {
            boltDeclarer.shuffleGrouping(parent.getComponentId(), streamId);
        } else {
            grouping.declareGrouping(boltDeclarer, parent.getComponentId(), streamId, grouping.getFields());
        }
    }

    private Set<ProcessorNode> initialProcessors(List<ProcessorNode> curGroup) {
        Set<ProcessorNode> nodes = new HashSet<>();
        Set<ProcessorNode> curSet = new HashSet<>(curGroup);
        for (ProcessorNode node : curGroup) {
            for (Node parent : parentNodes(node)) {
                if (!(parent instanceof ProcessorNode) || !curSet.contains(parent)) {
                    nodes.add(node);
                }
            }
        }
        return nodes;
    }

    private boolean isWindowed(Node curNode) {
        for (Node parent : StreamUtil.<Node>getParents(graph, curNode)) {
            if (parent instanceof WindowNode) {
                return true;
            } else if (parent instanceof ProcessorNode) {
                ProcessorNode p = (ProcessorNode) parent;
                if (p.isWindowed()) {
                    return true;
                }
            } else {
                return (parent instanceof PartitionNode) && isWindowed(parent);
            }
        }
        return false;
    }
}
