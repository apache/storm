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

import java.util.List;
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.planner.ProcessorContext;
import org.apache.storm.trident.planner.TridentProcessor;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTuple.Factory;
import org.apache.storm.trident.tuple.TridentTupleView.ProjectionFactory;
import org.apache.storm.tuple.Fields;


public class EachProcessor implements TridentProcessor {
    Function function;
    TridentContext context;
    AppendCollector collector;
    Fields inputFields;
    ProjectionFactory projection;

    public EachProcessor(Fields inputFields, Function function) {
        this.function = function;
        this.inputFields = inputFields;
    }

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, TridentContext tridentContext) {
        List<Factory> parents = tridentContext.getParentTupleFactories();
        if (parents.size() != 1) {
            throw new RuntimeException("Each operation can only have one parent");
        }
        this.context = tridentContext;
        collector = new AppendCollector(tridentContext);
        projection = new ProjectionFactory(parents.get(0), inputFields);
        function.prepare(conf, new TridentOperationContext(context, projection));
    }

    @Override
    public void flush() {
        collector.flush();
    }

    @Override
    public void cleanup() {
        function.cleanup();
    }

    @Override
    public void execute(ProcessorContext processorContext, String streamId, TridentTuple tuple) {
        collector.setContext(processorContext, tuple);
        function.execute(projection.create(tuple), collector);
    }


    @Override
    public void startBatch(ProcessorContext processorContext) {
    }

    @Override
    public void finishBatch(ProcessorContext processorContext) {
    }

    @Override
    public Factory getOutputFactory() {
        return collector.getOutputFactory();
    }
}
