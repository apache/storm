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
import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.planner.ProcessorContext;
import org.apache.storm.trident.planner.TridentProcessor;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTuple.Factory;
import org.apache.storm.trident.tuple.TridentTupleView.ProjectionFactory;
import org.apache.storm.tuple.Fields;


public class AggregateProcessor implements TridentProcessor {
    Aggregator agg;
    TridentContext context;
    FreshCollector collector;
    Fields inputFields;
    ProjectionFactory projection;

    public AggregateProcessor(Fields inputFields, Aggregator agg) {
        this.agg = agg;
        this.inputFields = inputFields;
    }

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, TridentContext tridentContext) {
        List<Factory> parents = tridentContext.getParentTupleFactories();
        if (parents.size() != 1) {
            throw new RuntimeException("Aggregate operation can only have one parent");
        }
        this.context = tridentContext;
        collector = new FreshCollector(tridentContext);
        projection = new ProjectionFactory(parents.get(0), inputFields);
        agg.prepare(conf, new TridentOperationContext(context, projection));
    }

    @Override
    public void cleanup() {
        agg.cleanup();
    }

    @Override
    public void startBatch(ProcessorContext processorContext) {
        collector.setContext(processorContext);
        processorContext.state[context.getStateIndex()] = agg.init(processorContext.batchId, collector);
    }

    @Override
    public void execute(ProcessorContext processorContext, String streamId, TridentTuple tuple) {
        collector.setContext(processorContext);
        agg.aggregate(processorContext.state[context.getStateIndex()], projection.create(tuple), collector);
    }

    @Override
    public void flush() {
        collector.flush();
    }

    @Override
    public void finishBatch(ProcessorContext processorContext) {
        collector.setContext(processorContext);
        agg.complete(processorContext.state[context.getStateIndex()], collector);
    }

    @Override
    public Factory getOutputFactory() {
        return collector.getOutputFactory();
    }
}
