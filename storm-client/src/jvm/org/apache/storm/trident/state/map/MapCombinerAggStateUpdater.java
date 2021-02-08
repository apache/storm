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

package org.apache.storm.trident.state.map;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.CombinerValueUpdater;
import org.apache.storm.trident.state.StateUpdater;
import org.apache.storm.trident.state.ValueUpdater;
import org.apache.storm.trident.tuple.ComboList;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView.ProjectionFactory;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class MapCombinerAggStateUpdater implements StateUpdater<MapState> {
    //ANY CHANGE TO THIS CODE MUST BE SERIALIZABLE COMPATIBLE OR THERE WILL BE PROBLEMS
    private static final long serialVersionUID = -3960578785572592092L;

    CombinerAggregator agg;
    Fields groupFields;
    Fields inputFields;
    transient ProjectionFactory groupFactory;
    transient ProjectionFactory inputFactory;
    ComboList.Factory factory;

    public MapCombinerAggStateUpdater(CombinerAggregator agg, Fields groupFields, Fields inputFields) {
        this.agg = agg;
        this.groupFields = groupFields;
        this.inputFields = inputFields;
        if (inputFields.size() != 1) {
            throw new IllegalArgumentException(
                "Combiner aggs only take a single field as input. Got this instead: " + inputFields.toString());
        }
        factory = new ComboList.Factory(groupFields.size(), inputFields.size());
    }

    @Override
    public void updateState(MapState map, List<TridentTuple> tuples, TridentCollector collector) {
        List<List<Object>> groups = new ArrayList<List<Object>>(tuples.size());
        List<ValueUpdater> updaters = new ArrayList<ValueUpdater>(tuples.size());

        for (TridentTuple t : tuples) {
            groups.add(groupFactory.create(t));
            updaters.add(new CombinerValueUpdater(agg, inputFactory.create(t).getValue(0)));
        }
        List<Object> newVals = map.multiUpdate(groups, updaters);

        for (int i = 0; i < tuples.size(); i++) {
            List<Object> key = groups.get(i);
            Object result = newVals.get(i);
            collector.emit(factory.create(new List[]{ key, new Values(result) }));
        }
    }

    @Override
    public void prepare(Map<String, Object> conf, TridentOperationContext context) {
        groupFactory = context.makeProjectionFactory(groupFields);
        inputFactory = context.makeProjectionFactory(inputFields);
    }

    @Override
    public void cleanup() {
    }

}
