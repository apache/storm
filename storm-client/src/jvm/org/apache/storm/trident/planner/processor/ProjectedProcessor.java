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

import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.planner.ProcessorContext;
import org.apache.storm.trident.planner.TridentProcessor;
import org.apache.storm.trident.planner.TupleReceiver;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTuple.Factory;
import org.apache.storm.trident.tuple.TridentTupleView.ProjectionFactory;
import org.apache.storm.tuple.Fields;


public class ProjectedProcessor implements TridentProcessor {
    Fields projectFields;
    ProjectionFactory factory;
    TridentContext context;

    public ProjectedProcessor(Fields projectFields) {
        this.projectFields = projectFields;
    }

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, TridentContext tridentContext) {
        if (tridentContext.getParentTupleFactories().size() != 1) {
            throw new RuntimeException("Projection processor can only have one parent");
        }
        this.context = tridentContext;
        factory = new ProjectionFactory(tridentContext.getParentTupleFactories().get(0), projectFields);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void startBatch(ProcessorContext processorContext) {
    }

    @Override
    public void execute(ProcessorContext processorContext, String streamId, TridentTuple tuple) {
        TridentTuple toEmit = factory.create(tuple);
        for (TupleReceiver r : context.getReceivers()) {
            r.execute(processorContext, context.getOutStreamId(), toEmit);
        }
    }

    @Override
    public void flush() {
        for (TupleReceiver r : context.getReceivers()) {
            r.flush();
        }
    }

    @Override
    public void finishBatch(ProcessorContext processorContext) {
    }

    @Override
    public Factory getOutputFactory() {
        return factory;
    }
}
