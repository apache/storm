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

package org.apache.storm.trident.planner.processor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.MultiReducer;
import org.apache.storm.trident.operation.TridentMultiReducerContext;
import org.apache.storm.trident.planner.ProcessorContext;
import org.apache.storm.trident.planner.TridentProcessor;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTuple.Factory;
import org.apache.storm.trident.tuple.TridentTupleView.ProjectionFactory;
import org.apache.storm.tuple.Fields;


public class MultiReducerProcessor implements TridentProcessor {
    MultiReducer reducer;
    TridentContext context;
    Map<String, Integer> streamToIndex;
    List<Fields> projectFields;
    ProjectionFactory[] projectionFactories;
    FreshCollector collector;

    public MultiReducerProcessor(List<Fields> inputFields, MultiReducer reducer) {
        this.reducer = reducer;
        projectFields = inputFields;
    }

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, TridentContext tridentContext) {
        List<Factory> parents = tridentContext.getParentTupleFactories();
        this.context = tridentContext;
        streamToIndex = new HashMap<>();
        List<String> parentStreams = tridentContext.getParentStreams();
        for (int i = 0; i < parentStreams.size(); i++) {
            streamToIndex.put(parentStreams.get(i), i);
        }
        projectionFactories = new ProjectionFactory[projectFields.size()];
        for (int i = 0; i < projectFields.size(); i++) {
            projectionFactories[i] = new ProjectionFactory(parents.get(i), projectFields.get(i));
        }
        collector = new FreshCollector(tridentContext);
        reducer.prepare(conf, new TridentMultiReducerContext((List) Arrays.asList(projectionFactories)));
    }

    @Override
    public void cleanup() {
        reducer.cleanup();
    }

    @Override
    public void startBatch(ProcessorContext processorContext) {
        collector.setContext(processorContext);
        processorContext.state[context.getStateIndex()] = reducer.init(collector);
    }

    @Override
    public void execute(ProcessorContext processorContext, String streamId, TridentTuple tuple) {
        collector.setContext(processorContext);
        int i = streamToIndex.get(streamId);
        reducer.execute(processorContext.state[context.getStateIndex()], i, projectionFactories[i].create(tuple), collector);
    }

    @Override
    public void flush() {
        collector.flush();
    }

    @Override
    public void finishBatch(ProcessorContext processorContext) {
        collector.setContext(processorContext);
        reducer.complete(processorContext.state[context.getStateIndex()], collector);
    }

    @Override
    public Factory getOutputFactory() {
        return collector.getOutputFactory();
    }
}
