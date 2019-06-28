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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.ComboList;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.trident.tuple.TridentTupleView.ProjectionFactory;
import org.apache.storm.tuple.Fields;

public class GroupedAggregator implements Aggregator<Object[]> {
    ProjectionFactory groupFactory;
    ProjectionFactory inputFactory;
    Aggregator agg;
    ComboList.Factory fact;
    Fields inFields;
    Fields groupFields;

    public GroupedAggregator(Aggregator agg, Fields group, Fields input, int outSize) {
        groupFields = group;
        inFields = input;
        this.agg = agg;
        int[] sizes = new int[2];
        sizes[0] = groupFields.size();
        sizes[1] = outSize;
        fact = new ComboList.Factory(sizes);
    }

    @Override
    public void prepare(Map<String, Object> conf, TridentOperationContext context) {
        inputFactory = context.makeProjectionFactory(inFields);
        groupFactory = context.makeProjectionFactory(groupFields);
        agg.prepare(conf, new TridentOperationContext(context, inputFactory));
    }

    @Override
    public Object[] init(Object batchId, TridentCollector collector) {
        return new Object[]{ new GroupCollector(collector, fact), new HashMap(), batchId };
    }

    @Override
    public void aggregate(Object[] arr, TridentTuple tuple, TridentCollector collector) {
        GroupCollector groupColl = (GroupCollector) arr[0];
        Map<List, Object> val = (Map) arr[1];
        TridentTuple group = groupFactory.create((TridentTupleView) tuple);
        TridentTuple input = inputFactory.create((TridentTupleView) tuple);
        Object curr;
        if (!val.containsKey(group)) {
            curr = agg.init(arr[2], groupColl);
            val.put((List) group, curr);
        } else {
            curr = val.get(group);
        }
        groupColl.currGroup = group;
        agg.aggregate(curr, input, groupColl);

    }

    @Override
    public void complete(Object[] arr, TridentCollector collector) {
        Map<List, Object> val = (Map) arr[1];
        GroupCollector groupColl = (GroupCollector) arr[0];
        for (Entry<List, Object> e : val.entrySet()) {
            groupColl.currGroup = e.getKey();
            agg.complete(e.getValue(), groupColl);
        }
    }

    @Override
    public void cleanup() {
        agg.cleanup();
    }

}
