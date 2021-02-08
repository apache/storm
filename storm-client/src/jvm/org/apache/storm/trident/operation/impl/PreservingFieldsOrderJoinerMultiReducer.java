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
import java.util.List;
import java.util.Map;
import org.apache.storm.trident.JoinType;
import org.apache.storm.trident.operation.GroupedMultiReducer;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentMultiReducerContext;
import org.apache.storm.trident.tuple.ComboList;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;

public class PreservingFieldsOrderJoinerMultiReducer implements GroupedMultiReducer<JoinState> {
    List<JoinType> types;
    List<Fields> sideFields;
    List<Fields> joiningFields;
    List<Fields> originFields;
    int numGroupFields;
    ComboList.Factory factory;


    public PreservingFieldsOrderJoinerMultiReducer(List<JoinType> types, int numGroupFields, List<Fields> origins,
                                                   List<Fields> joins, List<Fields> sides) {
        this.types = types;
        originFields = origins;
        joiningFields = joins;
        sideFields = sides;

        // we already checked this
        this.numGroupFields = numGroupFields;
    }

    @Override
    public void prepare(Map<String, Object> conf, TridentMultiReducerContext context) {
        int[] sizes = new int[originFields.size()];
        for (int i = 0; i < originFields.size(); i++) {
            sizes[i] = originFields.get(i).size();
        }
        factory = new ComboList.Factory(sizes);
    }

    @Override
    public JoinState init(TridentCollector collector, TridentTuple group) {
        return new JoinState(types.size(), group);
    }

    @Override
    public void execute(JoinState state, int streamIndex, TridentTuple group, TridentTuple input, TridentCollector collector) {
        //TODO: do the inner join incrementally, emitting the cross join with this tuple, against all other sides
        //TODO: only do cross join if at least one tuple in each side
        List<List> side = state.sides[streamIndex];
        if (side.isEmpty()) {
            state.numSidesReceived++;
        }

        side.add(input);
        if (state.numSidesReceived == state.sides.length) {
            emitCrossJoin(state, collector, streamIndex, input);
        }
    }

    @Override
    public void complete(JoinState state, TridentTuple group, TridentCollector collector) {
        List<List>[] sides = state.sides;
        boolean wasEmpty = state.numSidesReceived < sides.length;
        for (int i = 0; i < sides.length; i++) {
            if (sides[i].isEmpty() && types.get(i) == JoinType.OUTER) {
                state.numSidesReceived++;
                sides[i].add(null);
            }
        }
        if (wasEmpty && state.numSidesReceived == sides.length) {
            emitCrossJoin(state, collector, -1, null);
        }
    }

    @Override
    public void cleanup() {
    }

    private List<Object> makeNullList(int size) {
        List<Object> ret = new ArrayList(size);
        for (int i = 0; i < size; i++) {
            ret.add(null);
        }
        return ret;
    }

    private void emitCrossJoin(JoinState state, TridentCollector collector, int overrideIndex, TridentTuple overrideTuple) {
        List<List>[] sides = state.sides;
        int[] indices = state.indices;
        for (int i = 0; i < indices.length; i++) {
            indices[i] = 0;
        }

        boolean keepGoing = true;
        //emit cross-join of all emitted tuples
        while (keepGoing) {
            List[] combined = new List[sides.length];

            for (int i = 0; i < sides.length; i++) {
                List<Object> values = buildValuesForStream(state, overrideIndex, overrideTuple, sides, indices, combined, i);
                combined[i] = values;
            }
            collector.emit(factory.create(combined));
            keepGoing = increment(sides, indices, indices.length - 1, overrideIndex);
        }
    }

    private List<Object> buildValuesForStream(JoinState state, int overrideIndex, TridentTuple overrideTuple, List<List>[] sides,
                                              int[] indices, List[] combined, int streamIdx) {
        List sideValues;
        if (streamIdx == overrideIndex) {
            sideValues = overrideTuple;
        } else {
            sideValues = sides[streamIdx].get(indices[streamIdx]);
        }

        Fields originFields = this.originFields.get(streamIdx);
        if (sideValues == null) {
            return makeNullList(originFields.size());
        } else {
            List<Object> ret = new ArrayList<>(originFields.size());
            Fields sideFields = this.sideFields.get(streamIdx);
            Fields joinFields = joiningFields.get(streamIdx);
            int sideIdx = 0;
            for (String field : originFields) {
                // assuming sideFields are preserving its order
                if (sideFields.contains(field)) {
                    ret.add(sideValues.get(sideIdx++));
                } else {
                    // group field
                    ret.add(state.group.get(joinFields.fieldIndex(field)));
                }
            }
            return ret;
        }
    }


    //return false if can't increment anymore
    //TODO: DRY this code up with what's in ChainedAggregatorImpl
    private boolean increment(List[] lengths, int[] indices, int j, int overrideIndex) {
        if (j == -1) {
            return false;
        }
        if (j == overrideIndex) {
            return increment(lengths, indices, j - 1, overrideIndex);
        }
        indices[j]++;
        if (indices[j] >= lengths[j].size()) {
            indices[j] = 0;
            return increment(lengths, indices, j - 1, overrideIndex);
        }
        return true;
    }

}
