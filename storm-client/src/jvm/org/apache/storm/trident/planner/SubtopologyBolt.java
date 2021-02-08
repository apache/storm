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

package org.apache.storm.trident.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.shade.org.jgrapht.DirectedGraph;
import org.apache.storm.shade.org.jgrapht.graph.DefaultDirectedGraph;
import org.apache.storm.shade.org.jgrapht.graph.DirectedSubgraph;
import org.apache.storm.shade.org.jgrapht.traverse.TopologicalOrderIterator;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.trident.planner.processor.TridentContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.topology.BatchInfo;
import org.apache.storm.trident.topology.ITridentBatchBolt;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTuple.Factory;
import org.apache.storm.trident.tuple.TridentTupleView.ProjectionFactory;
import org.apache.storm.trident.tuple.TridentTupleView.RootFactory;
import org.apache.storm.trident.util.IndexedEdge;
import org.apache.storm.trident.util.TridentUtils;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

/**
 * A Bolt that does processing for a subsection of the complete graph.
 */
public class SubtopologyBolt implements ITridentBatchBolt {
    private static final long serialVersionUID = 1475508603138688412L;
    @SuppressWarnings("rawtypes")
    final DirectedGraph<Node, IndexedEdge> graph;
    final Set<Node> nodes;
    final Map<String, InitialReceiver> roots = new HashMap<>();
    final Map<Node, Factory> outputFactories = new HashMap<>();
    final Map<String, List<TridentProcessor>> myTopologicallyOrdered = new HashMap<>();
    final Map<Node, String> batchGroups;

    //given processornodes and static state nodes
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public SubtopologyBolt(DefaultDirectedGraph<Node, IndexedEdge> graph, Set<Node> nodes, Map<Node, String> batchGroups) {
        this.nodes = nodes;
        this.graph = (DirectedGraph<Node, IndexedEdge>) graph.clone();
        this.batchGroups = copyAndOnlyKeep(batchGroups, nodes);

        //Remove the unneeded entries from the graph
        //We want to keep all of our nodes, and the nodes that they are connected directly to (parents and children).
        Set<Node> nodesToKeep = new HashSet<>();
        for (IndexedEdge edge : this.graph.edgeSet()) {
            Node s = this.graph.getEdgeSource(edge);
            Node t = this.graph.getEdgeTarget(edge);
            if (this.nodes.contains(s) || this.nodes.contains(t)) {
                nodesToKeep.add(s);
                nodesToKeep.add(t);
            }
        }

        Set<Node> nodesToRemove = new HashSet<>(this.graph.vertexSet());
        nodesToRemove.removeAll(nodesToKeep);
        this.graph.removeAllVertices(nodesToRemove);
    }

    private static Map<Node, String> copyAndOnlyKeep(Map<Node, String> batchGroups, Set<Node> nodes) {
        Map<Node, String> ret = new HashMap<>(nodes.size());
        for (Map.Entry<Node, String> entry : batchGroups.entrySet()) {
            if (nodes.contains(entry.getKey())) {
                ret.put(entry.getKey(), entry.getValue());
            }
        }
        return ret;
    }

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, BatchOutputCollector batchCollector) {
        int thisComponentNumTasks = context.getComponentTasks(context.getThisComponentId()).size();
        for (Node n : nodes) {
            if (n.stateInfo != null) {
                State s = n.stateInfo.spec.stateFactory.makeState(conf, context, context.getThisTaskIndex(), thisComponentNumTasks);
                context.setTaskData(n.stateInfo.id, s);
            }
        }
        DirectedSubgraph<Node, ?> subgraph = new DirectedSubgraph<>(graph, nodes, null);
        TopologicalOrderIterator<Node, ?> it = new TopologicalOrderIterator<>(subgraph);
        int stateIndex = 0;
        while (it.hasNext()) {
            Node n = it.next();
            if (n instanceof ProcessorNode) {
                ProcessorNode pn = (ProcessorNode) n;
                String batchGroup = batchGroups.get(n);
                if (!myTopologicallyOrdered.containsKey(batchGroup)) {
                    myTopologicallyOrdered.put(batchGroup, new ArrayList<>());
                }
                myTopologicallyOrdered.get(batchGroup).add(pn.processor);
                List<String> parentStreams = new ArrayList<>();
                List<Factory> parentFactories = new ArrayList<>();
                for (Node p : TridentUtils.getParents(graph, n)) {
                    parentStreams.add(p.streamId);
                    if (nodes.contains(p)) {
                        parentFactories.add(outputFactories.get(p));
                    } else {
                        if (!roots.containsKey(p.streamId)) {
                            roots.put(p.streamId, new InitialReceiver(p.streamId, getSourceOutputFields(context, p.streamId)));
                        }
                        roots.get(p.streamId).addReceiver(pn.processor);
                        parentFactories.add(roots.get(p.streamId).getOutputFactory());
                    }
                }
                List<TupleReceiver> targets = new ArrayList<>();
                boolean outgoingNode = false;
                for (Node cn : TridentUtils.getChildren(graph, n)) {
                    if (nodes.contains(cn)) {
                        targets.add(((ProcessorNode) cn).processor);
                    } else {
                        outgoingNode = true;
                    }
                }
                if (outgoingNode) {
                    targets.add(new BridgeReceiver(batchCollector));
                }

                TridentContext triContext = new TridentContext(
                    pn.selfOutFields,
                    parentFactories,
                    parentStreams,
                    targets,
                    pn.streamId,
                    stateIndex,
                    batchCollector
                );
                pn.processor.prepare(conf, context, triContext);
                outputFactories.put(n, pn.processor.getOutputFactory());
            }
            stateIndex++;
        }
    }

    private Fields getSourceOutputFields(TopologyContext context, String sourceStream) {
        for (GlobalStreamId g : context.getThisSources().keySet()) {
            if (g.get_streamId().equals(sourceStream)) {
                return context.getComponentOutputFields(g);
            }
        }
        throw new RuntimeException("Could not find fields for source stream " + sourceStream);
    }

    @Override
    public void execute(BatchInfo batchInfo, Tuple tuple) {
        String sourceStream = tuple.getSourceStreamId();
        InitialReceiver ir = roots.get(sourceStream);
        if (ir == null) {
            throw new RuntimeException("Received unexpected tuple " + tuple.toString());
        }
        ir.receive((ProcessorContext) batchInfo.state, tuple);
    }

    @Override
    public void finishBatch(BatchInfo batchInfo) {
        for (TridentProcessor p : myTopologicallyOrdered.get(batchInfo.batchGroup)) {
            p.finishBatch((ProcessorContext) batchInfo.state);
        }
    }

    @Override
    public Object initBatchState(String batchGroup, Object batchId) {
        ProcessorContext ret = new ProcessorContext(batchId, new Object[nodes.size()]);
        for (TridentProcessor p : myTopologicallyOrdered.get(batchGroup)) {
            p.startBatch(ret);
        }
        return ret;
    }

    @Override
    public void cleanup() {
        for (String bg : myTopologicallyOrdered.keySet()) {
            for (TridentProcessor p : myTopologicallyOrdered.get(bg)) {
                p.cleanup();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (Node n : nodes) {
            declarer.declareStream(n.streamId, TridentUtils.fieldsConcat(new Fields("$batchId"), n.allOutputFields));
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }


    protected static class InitialReceiver {
        List<TridentProcessor> receivers = new ArrayList<>();
        RootFactory factory;
        ProjectionFactory project;
        String stream;

        public InitialReceiver(String stream, Fields allFields) {
            this.stream = stream;
            factory = new RootFactory(allFields);
            List<String> projected = new ArrayList<>(allFields.toList());
            projected.remove(0);
            project = new ProjectionFactory(factory, new Fields(projected));
        }

        public void receive(ProcessorContext context, Tuple tuple) {
            TridentTuple t = project.create(factory.create(tuple));
            for (TridentProcessor r : receivers) {
                r.execute(context, stream, t);
            }
        }

        public void addReceiver(TridentProcessor p) {
            receivers.add(p);
        }

        public Factory getOutputFactory() {
            return project;
        }
    }
}
