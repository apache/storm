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

import java.util.List;
import java.util.Map;
import org.apache.storm.shade.com.google.common.collect.Multimap;
import org.apache.storm.shade.org.jgrapht.DirectedGraph;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

/**
 * Stream bolt that executes the different processors (except windowed and stateful operations).
 */
class ProcessorBolt extends BaseRichBolt implements StreamBolt {
    private final ProcessorBoltDelegate delegate;

    ProcessorBolt(String id, DirectedGraph<Node, Edge> graph, List<ProcessorNode> nodes) {
        delegate = new ProcessorBoltDelegate(id, graph, nodes);
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
}
