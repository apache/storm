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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.trident.operation.GroupedMultiReducer;
import org.apache.storm.trident.operation.MultiReducer;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentMultiReducerContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView.ProjectionFactory;
import org.apache.storm.tuple.Fields;


public class GroupedMultiReducerExecutor implements MultiReducer<Map<TridentTuple, Object>> {
    GroupedMultiReducer reducer;
    List<Fields> groupFields;
    List<Fields> inputFields;
    List<ProjectionFactory> groupFactories = new ArrayList<ProjectionFactory>();
    List<ProjectionFactory> inputFactories = new ArrayList<ProjectionFactory>();

    public GroupedMultiReducerExecutor(GroupedMultiReducer reducer, List<Fields> groupFields, List<Fields> inputFields) {
        if (inputFields.size() != groupFields.size()) {
            throw new IllegalArgumentException("Multireducer groupFields and inputFields must be the same size");
        }
        this.groupFields = groupFields;
        this.inputFields = inputFields;
        this.reducer = reducer;
    }

    @Override
    public void prepare(Map<String, Object> conf, TridentMultiReducerContext context) {
        for (int i = 0; i < groupFields.size(); i++) {
            groupFactories.add(context.makeProjectionFactory(i, groupFields.get(i)));
            inputFactories.add(context.makeProjectionFactory(i, inputFields.get(i)));
        }
        reducer.prepare(conf, new TridentMultiReducerContext((List) inputFactories));
    }

    @Override
    public Map<TridentTuple, Object> init(TridentCollector collector) {
        return new HashMap();
    }

    @Override
    public void execute(Map<TridentTuple, Object> state, int streamIndex, TridentTuple full, TridentCollector collector) {
        ProjectionFactory groupFactory = groupFactories.get(streamIndex);
        ProjectionFactory inputFactory = inputFactories.get(streamIndex);

        TridentTuple group = groupFactory.create(full);
        TridentTuple input = inputFactory.create(full);

        Object curr;
        if (!state.containsKey(group)) {
            curr = reducer.init(collector, group);
            state.put(group, curr);
        } else {
            curr = state.get(group);
        }
        reducer.execute(curr, streamIndex, group, input, collector);
    }

    @Override
    public void complete(Map<TridentTuple, Object> state, TridentCollector collector) {
        for (Map.Entry e : state.entrySet()) {
            TridentTuple group = (TridentTuple) e.getKey();
            Object val = e.getValue();
            reducer.complete(val, group, collector);
        }
    }

    @Override
    public void cleanup() {
        reducer.cleanup();
    }

}
