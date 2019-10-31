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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.storm.shade.com.google.common.collect.Multimap;
import org.apache.storm.shade.org.jgrapht.DirectedGraph;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.streams.processors.StatefulProcessor;
import org.apache.storm.streams.processors.UpdateStateByKeyProcessor;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Tuple;

/**
 * Stream bolt that executes stateful operations like update state and state query.
 */
class StatefulProcessorBolt<K, V> extends BaseStatefulBolt<KeyValueState<K, V>> implements StreamBolt {
    private final ProcessorBoltDelegate delegate;
    // can be UpdateStateByKey or StateQuery processors
    private final Set<StatefulProcessor<K, V>> statefulProcessors;

    StatefulProcessorBolt(String boltId, DirectedGraph<Node, Edge> graph, List<ProcessorNode> nodes) {
        delegate = new ProcessorBoltDelegate(boltId, graph, nodes);
        statefulProcessors = getStatefulProcessors(nodes);
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        delegate.prepare(topoConf, context, collector);
    }

    @Override
    public void execute(Tuple input) {
        delegate.processAndAck(input);
    }

    @Override
    public void initState(KeyValueState<K, V> state) {
        for (StatefulProcessor<K, V> statefulProcessor : statefulProcessors) {
            statefulProcessor.initState(state);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        delegate.declareOutputFields(declarer);
    }

    @Override
    public void setTimestampField(String fieldName) {
        delegate.setTimestampField(fieldName);
    }

    @Override
    public String getId() {
        return delegate.getId();
    }

    void setStreamToInitialProcessors(Multimap<String, ProcessorNode> streamToInitialProcessors) {
        delegate.setStreamToInitialProcessors(streamToInitialProcessors);
    }

    public List<ProcessorNode> getNodes() {
        return delegate.getNodes();
    }

    void addStreamToInitialProcessors(Multimap<String, ProcessorNode> streamToInitialProcessors) {
        delegate.addStreamToInitialProcessors(streamToInitialProcessors);
    }

    void addNodes(List<ProcessorNode> nodes) {
        delegate.addNodes(nodes);
        statefulProcessors.addAll(getStatefulProcessors(nodes));
    }

    @SuppressWarnings("unchecked")
    private Set<StatefulProcessor<K, V>> getStatefulProcessors(List<ProcessorNode> nodes) {
        Set<StatefulProcessor<K, V>> statefulProcessors = new HashSet<>();
        int updateStateByKeyCount = 0;
        for (ProcessorNode node : nodes) {
            if (node.getProcessor() instanceof StatefulProcessor) {
                statefulProcessors.add((StatefulProcessor<K, V>) node.getProcessor());
                if (node.getProcessor() instanceof UpdateStateByKeyProcessor) {
                    if (++updateStateByKeyCount > 1) {
                        throw new IllegalArgumentException("Cannot have more than one updateStateByKey processor "
                                + "in a StatefulProcessorBolt");
                    }
                }

            }
        }
        return statefulProcessors;
    }
}
