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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.planner.ProcessorContext;
import org.apache.storm.trident.planner.TridentProcessor;
import org.apache.storm.trident.state.QueryFunction;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTuple.Factory;
import org.apache.storm.trident.tuple.TridentTupleView.ProjectionFactory;
import org.apache.storm.tuple.Fields;


public class StateQueryProcessor implements TridentProcessor {
    QueryFunction function;
    State state;
    String stateId;
    TridentContext context;
    Fields inputFields;
    ProjectionFactory projection;
    AppendCollector collector;

    public StateQueryProcessor(String stateId, Fields inputFields, QueryFunction function) {
        this.stateId = stateId;
        this.function = function;
        this.inputFields = inputFields;
    }

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, TridentContext tridentContext) {
        List<Factory> parents = tridentContext.getParentTupleFactories();
        if (parents.size() != 1) {
            throw new RuntimeException("State query operation can only have one parent");
        }
        this.context = tridentContext;
        state = (State) context.getTaskData(stateId);
        projection = new ProjectionFactory(parents.get(0), inputFields);
        collector = new AppendCollector(tridentContext);
        function.prepare(conf, new TridentOperationContext(context, projection));
    }

    @Override
    public void cleanup() {
        function.cleanup();
    }

    @Override
    public void startBatch(ProcessorContext processorContext) {
        processorContext.state[context.getStateIndex()] = new BatchState();
    }

    @Override
    public void execute(ProcessorContext processorContext, String streamId, TridentTuple tuple) {
        BatchState state = (BatchState) processorContext.state[context.getStateIndex()];
        state.tuples.add(tuple);
        state.args.add(projection.create(tuple));
    }

    @Override
    public void flush() {
        // NO-OP
    }

    @Override
    public void finishBatch(ProcessorContext processorContext) {
        BatchState state = (BatchState) processorContext.state[context.getStateIndex()];
        if (!state.tuples.isEmpty()) {
            List<Object> results = function.batchRetrieve(this.state, Collections.unmodifiableList(state.args));
            if (results.size() != state.tuples.size()) {
                throw new RuntimeException(
                    "Results size is different than argument size: " + results.size() + " vs " + state.tuples.size());
            }
            for (int i = 0; i < state.tuples.size(); i++) {
                TridentTuple tuple = state.tuples.get(i);
                Object result = results.get(i);
                collector.setContext(processorContext, tuple);
                function.execute(state.args.get(i), result, collector);
            }
        }
    }

    @Override
    public Factory getOutputFactory() {
        return collector.getOutputFactory();
    }

    private static class BatchState {
        public List<TridentTuple> tuples = new ArrayList<TridentTuple>();
        public List<TridentTuple> args = new ArrayList<TridentTuple>();
    }
}
