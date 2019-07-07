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

package org.apache.storm.trident.operation.impl;

import java.util.List;
import java.util.Map;
import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.ComboList;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.trident.tuple.TridentTupleView.ProjectionFactory;
import org.apache.storm.tuple.Fields;

public class ChainedAggregatorImpl implements Aggregator<ChainedResult> {
    Aggregator[] aggs;
    ProjectionFactory[] inputFactories;
    ComboList.Factory fact;
    Fields[] inputFields;


    public ChainedAggregatorImpl(Aggregator[] aggs, Fields[] inputFields, ComboList.Factory fact) {
        this.aggs = aggs;
        this.inputFields = inputFields;
        this.fact = fact;
        if (this.aggs.length != this.inputFields.length) {
            throw new IllegalArgumentException("Require input fields for each aggregator");
        }
    }

    @Override
    public void prepare(Map<String, Object> conf, TridentOperationContext context) {
        inputFactories = new ProjectionFactory[inputFields.length];
        for (int i = 0; i < inputFields.length; i++) {
            inputFactories[i] = context.makeProjectionFactory(inputFields[i]);
            aggs[i].prepare(conf, new TridentOperationContext(context, inputFactories[i]));
        }
    }

    @Override
    public ChainedResult init(Object batchId, TridentCollector collector) {
        ChainedResult initted = new ChainedResult(collector, aggs.length);
        for (int i = 0; i < aggs.length; i++) {
            initted.objs[i] = aggs[i].init(batchId, initted.collectors[i]);
        }
        return initted;
    }

    @Override
    public void aggregate(ChainedResult val, TridentTuple tuple, TridentCollector collector) {
        val.setFollowThroughCollector(collector);
        for (int i = 0; i < aggs.length; i++) {
            TridentTuple projected = inputFactories[i].create((TridentTupleView) tuple);
            aggs[i].aggregate(val.objs[i], projected, val.collectors[i]);
        }
    }

    @Override
    public void complete(ChainedResult val, TridentCollector collector) {
        val.setFollowThroughCollector(collector);
        for (int i = 0; i < aggs.length; i++) {
            aggs[i].complete(val.objs[i], val.collectors[i]);
        }
        if (aggs.length > 1) { // otherwise, tuples were emitted directly
            int[] indices = new int[val.collectors.length];
            for (int i = 0; i < indices.length; i++) {
                indices[i] = 0;
            }
            boolean keepGoing = true;
            //emit cross-join of all emitted tuples
            while (keepGoing) {
                List[] combined = new List[aggs.length];
                for (int i = 0; i < aggs.length; i++) {
                    CaptureCollector capturer = (CaptureCollector) val.collectors[i];
                    combined[i] = capturer.captured.get(indices[i]);
                }
                collector.emit(fact.create(combined));
                keepGoing = increment(val.collectors, indices, indices.length - 1);
            }
        }
    }

    //return false if can't increment anymore
    private boolean increment(TridentCollector[] lengths, int[] indices, int j) {
        if (j == -1) {
            return false;
        }
        indices[j]++;
        CaptureCollector capturer = (CaptureCollector) lengths[j];
        if (indices[j] >= capturer.captured.size()) {
            indices[j] = 0;
            return increment(lengths, indices, j - 1);
        }
        return true;
    }

    @Override
    public void cleanup() {
        for (Aggregator a : aggs) {
            a.cleanup();
        }
    }
}
